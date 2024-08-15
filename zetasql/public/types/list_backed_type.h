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

  friend struct MultisetValueContentContainerElementHasher;
  friend struct HashableValueContentContainerElementIgnoringFloat;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_LIST_BACKED_TYPE_H_
