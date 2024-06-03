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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "zetasql/public/types/type.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/types/value_representations.h"
#include "absl/strings/str_cat.h"

namespace zetasql {

static constexpr uint64_t kNullHashCode = 0xCBFD5377B126E80Dull;

// Represents a type which composites other types.
class ContainerType : public Type {
 public:
#ifndef SWIG
  ContainerType(const ContainerType&) = delete;
  ContainerType& operator=(const ContainerType&) = delete;
#endif  // SWIG

 protected:
  ContainerType(const TypeFactory* factory, TypeKind kind)
      : Type(factory, kind) {}

  struct NullableValueContentEq {
    explicit NullableValueContentEq(const ValueEqualityCheckOptions& options,
                                    const Type* type)
        : options(options), type(type) {}
    size_t operator()(const internal::NullableValueContent x,
                      const internal::NullableValueContent y) const {
      if (x.is_null() != y.is_null()) return false;
      if (x.is_null() && y.is_null()) return true;
      return type->ValueContentEquals(x.value_content(), y.value_content(),
                                      options);
    }
    const ValueEqualityCheckOptions options;
    const Type* type;
  };

  struct HashableNullableValueContent {
    const internal::NullableValueContent element;
    const Type* type;

    template <typename H>
    H Hash(H h) const {
      type->HashValueContent(element.value_content(),
                             absl::HashState::Create(&h));
      return h;
    }

    template <typename H>
    friend H AbslHashValue(H h, const HashableNullableValueContent& v) {
      if (v.element.is_null()) {
        return H::combine(std::move(h), kNullHashCode);
      }
      return v.Hash(std::move(h));
    }
  };

  struct NullableValueContentHasher {
    explicit NullableValueContentHasher(const Type* type) : type(type) {}

    size_t operator()(const internal::NullableValueContent& x) const {
      return absl::Hash<HashableNullableValueContent>()(
          HashableNullableValueContent{x, type});
    }

   private:
    const Type* type;
  };

  std::optional<bool> NullableValueContentLess(
      const internal::NullableValueContent& x,
      const internal::NullableValueContent& y, const Type* x_type,
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

  std::string FormatNullableValueContent(
      const internal::NullableValueContent element, const Type* type,
      const FormatValueContentOptions& options) const {
    if (element.is_null()) {
      return options.as_literal()
                 ? "NULL"
                 : absl::StrCat("CAST(NULL AS ",
                                type->TypeName(options.product_mode,
                                               options.use_external_float32),
                                ")");
    }
    return type->FormatValueContent(element.value_content(), options);
  }
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_CONTAINER_TYPE_H_
