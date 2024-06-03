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

#ifndef ZETASQL_PUBLIC_TYPES_LIST_BACKED_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_LIST_BACKED_TYPE_H_

#include <optional>
#include <string>

#include "zetasql/public/types/container_type.h"
#include "zetasql/public/types/type.h"

namespace zetasql {

// A `ContainerType` whose `ValueContent` is stored in by a list data structure.
class ListBackedType : public ContainerType {
 public:
#ifndef SWIG
  ListBackedType(const ListBackedType&) = delete;
  ListBackedType& operator=(const ListBackedType&) = delete;
#endif  // SWIG

 protected:
  ListBackedType(const TypeFactory* factory, TypeKind kind)
      : ContainerType(factory, kind) {}

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
      int index, bool is_null,
      const FormatValueContentOptions& options) const = 0;

  friend struct MultisetValueContentContainerElementHasher;
  friend struct HashableValueContentContainerElementIgnoringFloat;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_LIST_BACKED_TYPE_H_
