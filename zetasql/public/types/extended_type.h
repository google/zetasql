//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/public/types/type.h"

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
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_EXTENDED_TYPE_H_
