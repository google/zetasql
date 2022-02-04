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

#include "zetasql/public/types/type_modifiers.h"

#include <string>

#include "absl/status/status.h"

namespace zetasql {

// static
TypeModifiers TypeModifiers::MakeTypeModifiers(TypeParameters type_parameters,
                                               Collation collation) {
  return TypeModifiers(std::move(type_parameters), std::move(collation));
}

bool TypeModifiers::Equals(const TypeModifiers& that) const {
  return type_parameters_.Equals(that.type_parameters()) &&
         collation_.Equals(that.collation());
}

absl::Status TypeModifiers::Serialize(TypeModifiersProto* proto) const {
  ZETASQL_RETURN_IF_ERROR(type_parameters_.Serialize(proto->mutable_type_parameters()));
  ZETASQL_RETURN_IF_ERROR(collation_.Serialize(proto->mutable_collation()));
  return absl::OkStatus();
}

// static
absl::StatusOr<TypeModifiers> TypeModifiers::Deserialize(
    const TypeModifiersProto& proto) {
  TypeModifiers type_modifiers;

  if (proto.has_type_parameters()) {
    ZETASQL_ASSIGN_OR_RETURN(type_modifiers.type_parameters_,
                     TypeParameters::Deserialize(proto.type_parameters()));
  }
  if (proto.has_collation()) {
    ZETASQL_ASSIGN_OR_RETURN(type_modifiers.collation_,
                     Collation::Deserialize(proto.collation()));
  }
  return type_modifiers;
}

std::string TypeModifiers::DebugString(absl::string_view indent) const {
  std::string debug_string;
  absl::StrAppend(&debug_string, indent,
                  "type_parameters: ", type_parameters_.DebugString(), "\n");
  absl::StrAppend(&debug_string, indent,
                  "collation: ", collation_.DebugString());
  return debug_string;
}

}  // namespace zetasql
