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

#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/functions/json.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

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
  if (type->IsMap()) {
    if (TypeIsOrContainsGraphElement(type->AsMap()->key_type())) {
      return true;
    }
    if (TypeIsOrContainsGraphElement(type->AsMap()->value_type())) {
      return true;
    }
  }
  return false;
}

absl::StatusOr<JSONValue> MakePropertiesJsonValue(
    absl::Span<Value::Property> properties,
    const LanguageOptions& language_options) {
  functions::JsonObjectBuilder json_builder(language_options,
                                            /*canonicalize_zero=*/false);
  std::vector<absl::string_view> json_keys;
  std::vector<const Value*> json_values;
  for (const auto& p : properties) {
    json_keys.emplace_back(p.first);
    json_values.emplace_back(&p.second);
  }
  ZETASQL_ASSIGN_OR_RETURN(JSONValue json_value,
                   functions::JsonObject(json_keys, absl::MakeSpan(json_values),
                                         json_builder));
  return json_value;
}

}  // namespace zetasql
