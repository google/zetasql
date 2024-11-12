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

#include "zetasql/common/graph_element_utils.h"

#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"

namespace zetasql {

bool TypeIsOrContainsGraphElement(const Type* type) {
  if (type->IsGraphElement() || type->IsGraphPath()) {
    return true;
  }
  if (type->IsArray() &&
      TypeIsOrContainsGraphElement(type->AsArray()->element_type())) {
    return true;
  }
  if (type->IsStruct()) {
    for (const StructType::StructField& field : type->AsStruct()->fields()) {
      if (TypeIsOrContainsGraphElement(field.type)) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace zetasql
