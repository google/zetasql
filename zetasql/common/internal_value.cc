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

#include "zetasql/common/internal_value.h"

namespace zetasql {

// static
bool InternalValue::ContainsArrayWithUncertainOrder(const Value& val) {
  if (val.is_null()) {
    return false;
  }
  if (val.type()->IsArray()) {
    for (int i = 0; i < val.num_elements(); ++i) {
      if (ContainsArrayWithUncertainOrder(val.element(i))) {
        return true;
      }
    }
    if (InternalValue::GetOrderKind(val) == Value::kIgnoresOrder) {
      return val.num_elements() >= 2;
    }
  }
  if (val.type()->IsStruct()) {
    for (int i = 0; i < val.num_fields(); ++i) {
      if (ContainsArrayWithUncertainOrder(val.field(i))) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace zetasql
