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

#ifndef ZETASQL_PUBLIC_TYPES_EXTENDED_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_EXTENDED_TYPE_H_

#include <string>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_parameters.h"
#include "absl/status/statusor.h"

namespace zetasql {

// ExtendedType is an abstract class that serves as a base class for all
// types implemented outside of ZetaSQL codebase. ExtendedType instances
// are fed to ZetaSQL resolver by an implementation of a zetasql::Catalog
// provided by an engine. Such catalog can return ExtendedType instance as a
// result of FindType call and can have engine defined (extended) types
// presented in function signatures as a type of result value or arguments'
// types. If the extended type has any casts or coercions, catalog can overwrite
// FindConversion function to provide these conversions to a resolver. Extended
// types are enabled when LanguageFeature::FEATURE_EXTENDED_TYPES is presented
// in the set of LanguageOptions features.
//
// For an ExtendedType to support serialization/deserialization it needs to:
// 1) Override Type's SerializeToProtoAndDistinctFileDescriptorsImpl method to
//  serialize this extended type to a TypeProto. The `type_kind` field of this
//  TypeProto should be set to TYPE_EXTENDED. It's up to extended type how to
//  serialize its representation. E.g. it can serialize itself into TypeProto
//  extended_type_name field or define its own proto extensions.
// 2) Create implementation of ExtendedTypeDeserializer that deserializes
//  TypeProto and return an instance of corresponding extended type.
// 3) ExtendedTypeDeserializer then can be passed to TypeDeserializer, so it
//  can deserialize extended types or built-in compound types that reference
//  extended types.
class LanguageOptions;
class TypeFactory;

class ExtendedType : public Type {
 public:
#ifndef SWIG
  ExtendedType(const ExtendedType&) = delete;
  ExtendedType& operator=(const ExtendedType&) = delete;
#endif  // SWIG

  explicit ExtendedType(const TypeFactory* factory)
      : Type(factory, TYPE_EXTENDED) {}

  const ExtendedType* AsExtendedType() const override { return this; }

  bool IsSupportedType(const LanguageOptions& language_options) const override;

  // TODO Change "final" to "override" after adding a test so that
  // extension types can use type parameters.
  absl::StatusOr<std::string> TypeNameWithParameters(
      const TypeParameters& type_params, ProductMode mode) const final;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_EXTENDED_TYPE_H_
