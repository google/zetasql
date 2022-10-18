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

#ifndef ZETASQL_PUBLIC_TYPES_CONTAINER_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_CONTAINER_TYPE_H_

#include <string>

#include "zetasql/public/types/type.h"

namespace zetasql {

static constexpr uint64_t kNullHashCode = 0xCBFD5377B126E80Dull;

// Representation of type which value is a container of Values, such as
// Array, Struct, or Range.
class ContainerType : public Type {
 public:
#ifndef SWIG
  ContainerType(const ContainerType&) = delete;
  ContainerType& operator=(const ContainerType&) = delete;
#endif  // SWIG

 protected:
  ContainerType(const TypeFactory* factory, TypeKind kind)
      : Type(factory, kind) {}

  struct ValueContentContainerElementEq {
    explicit ValueContentContainerElementEq(
        const ValueEqualityCheckOptions& options, const Type* type)
        : options(options), type(type) {}
    size_t operator()(const internal::ValueContentContainerElement x,
                      const internal::ValueContentContainerElement y) const {
      if (x.is_null() != y.is_null()) return false;
      if (x.is_null() && y.is_null()) return true;
      return type->ValueContentEquals(x.value_content(), y.value_content(),
                                      options);
    }
    const ValueEqualityCheckOptions options;
    const Type* type;
  };

  struct HashableValueContentContainerElement {
    const internal::ValueContentContainerElement element;
    const Type* type;

    template <typename H>
    H Hash(H h) const {
      type->HashValueContent(element.value_content(),
                             absl::HashState::Create(&h));
      return h;
    }

    template <typename H>
    friend H AbslHashValue(H h, const HashableValueContentContainerElement& v) {
      if (v.element.is_null()) {
        return H::combine(std::move(h), kNullHashCode);
      }
      return v.Hash(std::move(h));
    }
  };

  struct ValueContentContainerElementHasher {
    explicit ValueContentContainerElementHasher(const Type* type)
        : type(type) {}

    size_t operator()(const internal::ValueContentContainerElement& x) const {
      return absl::Hash<HashableValueContentContainerElement>()(
          HashableValueContentContainerElement{x, type});
    }

   private:
    const Type* type;
  };

  std::string FormatValueContentContainerElement(
      const internal::ValueContentContainerElement element, const Type* type,
      const FormatValueContentOptions& options) const {
    if (element.is_null()) {
      return options.as_literal()
                 ? "NULL"
                 : absl::StrCat("CAST(NULL AS ",
                                type->TypeName(options.product_mode), ")");
    }
    return type->FormatValueContent(element.value_content(), options);
  }

  std::optional<bool> ValueContentContainerElementLess(
      const internal::ValueContentContainerElement& x,
      const internal::ValueContentContainerElement& y, const Type* x_type,
      const Type* y_type) const {
    if (x.is_null() && y.is_null()) return std::nullopt;
    if (x.is_null() && !y.is_null()) {
      return true;
    }
    if (y.is_null()) {
      return false;
    }
    if (x_type->ValueContentLess(x.value_content(), y.value_content(),
                                 y_type)) {
      return true;
    } else if (y_type->ValueContentLess(y.value_content(), x.value_content(),
                                        x_type)) {
      return false;
    }
    return std::nullopt;
  }

  FormatValueContentOptions DebugFormatValueContentOptions() const {
    Type::FormatValueContentOptions format_options;
    format_options.product_mode = ProductMode::PRODUCT_INTERNAL;
    format_options.mode = Type::FormatValueContentOptions::Mode::kDebug;
    return format_options;
  }

  // FormatValueContent is defined for ContainerType.
  // Instead of providing implementation of FormatValueContent method, child
  // classes need to provide implementations of formatting methods defined
  // below.
  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override;

  // When formatting a value of a container type, how should the content start?
  // E.g. array may start with "[", and struct with "{"
  virtual std::string GetFormatPrefix(
      const ValueContent& value_content,
      const FormatValueContentOptions& options) const = 0;

  // When formatting a value of a container type, how should the content end?
  // E.g. array may end with "]", and struct with "}"
  virtual char GetFormatClosingCharacter(
      const FormatValueContentOptions& options) const = 0;

  // Get type of the index-th element of the container
  virtual const Type* GetElementType(int index) const = 0;

  // When formatting a value of a container type, should the element have a
  // prefix? E.g. when formatting a struct we may want to prepend each element
  // with the name of the struct field: "{foo: 1, bar: 2}"
  virtual std::string GetFormatElementPrefix(
      const int index, const bool is_null,
      const FormatValueContentOptions& options) const = 0;

  friend struct MultisetValueContentContainerElementHasher;
  friend struct HashableValueContentContainerElementIgnoringFloat;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_CONTAINER_TYPE_H_
