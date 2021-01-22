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

#ifndef ZETASQL_PUBLIC_TYPES_TYPE_PARAMETERS_H_
#define ZETASQL_PUBLIC_TYPES_TYPE_PARAMETERS_H_

#include "zetasql/public/type_parameters.pb.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"

namespace zetasql {

// Type parameters for extension types, which are represented
// by List<zetasql::Value> to allow maximum flexibility. Implementations can
// interpret the semantic of these type parameters.
class ExtendedTypeParameters {
 public:
  explicit ExtendedTypeParameters(std::vector<SimpleValue> parameters)
      : parameters_(std::move(parameters)) {}

  uint64_t num_parameters() const { return parameters_.size(); }
  const SimpleValue& parameter(int i) const { return parameters_[i]; }

  std::string DebugString() const;
  absl::Status Serialize(ExtendedTypeParametersProto* proto) const;
  static zetasql_base::StatusOr<ExtendedTypeParameters> Deserialize(
      const ExtendedTypeParametersProto& proto);
  bool Equals(const ExtendedTypeParameters& that) const;

 private:
  std::vector<SimpleValue> parameters_;
};

// Type parameters for erasable types (parameterized types), for example
// STRING(L) is an erasable string type with length limit. L is the type
// parameter of STRING(L) type.
class TypeParameters {
 public:
  // Constructs empty type parameters for types without parameters. Default
  // constructor must be public to be used in the ResolvedAST.
  TypeParameters();

  // Constructs type parameters for STRING(L) or BYTES(L) type.
  static zetasql_base::StatusOr<TypeParameters> MakeStringTypeParameters(
      const StringTypeParametersProto& string_type_parameters);

  // Constructs type parameters for NUMERIC(P[,S]) or BIGNUMERIC(P[,S]) type.
  static zetasql_base::StatusOr<TypeParameters> MakeNumericTypeParameters(
      const NumericTypeParametersProto& numeric_type_parameters);

  // Constructs type parameters for extended type. <child_list> is optional;
  // if present, it stores sub-fields for extended type.
  static TypeParameters MakeExtendedTypeParameters(
      const ExtendedTypeParameters& extended_type_parameters,
      std::vector<TypeParameters> child_list = std::vector<TypeParameters>());

  // Constructs type parameters for STRUCT or ARRAY type.
  static TypeParameters MakeTypeParametersWithChildList(
      std::vector<TypeParameters> child_list);

  // Whether <type> matches this type parameter instance.
  // For example, StringTypeParameters only matches STRING and BYTES type.
  // Note, this function doesn't check whether sub-fields of STRUCT/ARRAY type
  // matches the corresponding type parameters, please call this function for
  // sub-fields if need to access sub-field type parameters.
  bool MatchType(const Type* type) const;

  // Returns true if type parameter is empty and has no children. Empty type
  // parameter is used as placeholder for type without parameters. E.g. in
  // STRUCT<INT64, STRING(10)>, the type parameter for INT64 is empty.
  bool IsEmpty() const {
    return absl::holds_alternative<absl::monostate>(type_parameters_holder_) &&
           child_list().empty();
  }
  bool IsStringTypeParameters() const {
    return absl::holds_alternative<StringTypeParametersProto>(
        type_parameters_holder_);
  }
  bool IsNumericTypeParameters() const {
    return absl::holds_alternative<NumericTypeParametersProto>(
        type_parameters_holder_);
  }
  bool IsExtendedTypeParameters() const {
    return absl::holds_alternative<ExtendedTypeParameters>(
        type_parameters_holder_);
  }
  // Returns true if this contains parameters for child types of a complex type
  // (STRUCT or ARRAY).
  bool IsTypeParametersInStructOrArray() const { return !child_list().empty(); }

  const StringTypeParametersProto& string_type_parameters() const {
    ZETASQL_CHECK(IsStringTypeParameters()) << "Not STRING type parameters";
    return absl::get<StringTypeParametersProto>(type_parameters_holder_);
  }
  const NumericTypeParametersProto& numeric_type_parameters() const {
    ZETASQL_CHECK(IsNumericTypeParameters()) << "Not NUMERIC type parameters";
    return absl::get<NumericTypeParametersProto>(type_parameters_holder_);
  }
  const ExtendedTypeParameters& extended_type_parameters() const {
    ZETASQL_CHECK(IsExtendedTypeParameters()) << "Not EXTENDED type parameters";
    return absl::get<ExtendedTypeParameters>(type_parameters_holder_);
  }

  // Returns type parameters for subfields for ARRAY/STRUCT types
  // For ARRAY:
  //   If the element or its subfield has type parameters, then
  //   child_list.size() is 1, and child_list(0) is the element type parameters.
  //   Otherwise child_list is empty.
  // For STRUCT:
  //   If the i-th field has type parameters then child_list(i) is the field
  //   type parameters,
  //   Otherwise either child_list.size() <= i or child_list(i) is empty.
  //   If none of the fields and none of their subfields has type parameters,
  //   then child_list is empty.
  // For other types, child_list is empty.
  const std::vector<TypeParameters>& child_list() const { return child_list_; }
  const TypeParameters& child(int i) const { return child_list_[i]; }
  uint64_t num_children() const { return child_list_.size(); }

  absl::Status Serialize(TypeParametersProto* proto) const;
  static zetasql_base::StatusOr<TypeParameters> Deserialize(
      const TypeParametersProto& proto);
  std::string DebugString() const;
  bool Equals(const TypeParameters& that) const;

 private:
  explicit TypeParameters(const StringTypeParametersProto& string_parameters);
  explicit TypeParameters(const NumericTypeParametersProto& numeric_parameters);
  TypeParameters(const ExtendedTypeParameters& extended_parameters,
                 std::vector<TypeParameters> child_list);
  explicit TypeParameters(std::vector<TypeParameters> child_list);

  // Default value is the 1st type (absl::monostate), meaning the type parameter
  // is empty.
  absl::variant<absl::monostate, StringTypeParametersProto,
                NumericTypeParametersProto, ExtendedTypeParameters>
      type_parameters_holder_;
  // Stores type parameters for subfields for ARRAY/STRUCT types
  std::vector<TypeParameters> child_list_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_TYPE_PARAMETERS_H_
