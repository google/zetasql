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

// Types for representing named constants in the catalog.
//
// To resolve a query, ZetaSQL only needs types for constants. Values are only
// needed after query analysis, and could potentially be provided by arbitrary
// engine-defined mechanisms. Engines can load Constants that don't yet have
// values into their Catalogs.
//
// The Constant interface only provides name and type. This is what gets loaded
// into the Catalog, and what shows up in the resolved AST.
//
// The two classes that implement the Constant interface are:
// * SimpleConstant - a simple Constant with a valid zetasql::Value
// * SQLConstant - a Constant derived from a SQL expression, for example
//   Constants created from CREATE CONSTANT statements
//
// For details on the syntax and semantics of named constants in ZetaSQL, see
// (broken link). For details on the implementation of
// named constants in ZetaSQL, see (broken link).

#ifndef ZETASQL_PUBLIC_CONSTANT_H_
#define ZETASQL_PUBLIC_CONSTANT_H_

#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/strings/str_join.h"

namespace zetasql {

// Main interface of named constants in the catalog.
//
// Provides only the name and the type of the named constant. Its value may not
// be available at all stages of analysis (see the file comment above).
// Implementing classes determine if and how the value can be obtained.
class Constant {
 public:
  // Creates a named constant with <name_path>. Crashes if <name_path> is empty.
  explicit Constant(std::vector<std::string> name_path)
      : name_path_(std::move(name_path)) {
    // ZETASQL_CHECK validated: Constants must have names. A call to Name() would fail
    // anyway.
    ZETASQL_CHECK(!name_path_.empty()) << FullName();
  }

  virtual ~Constant() {}

  // This class is neither copyable nor assignable.
  Constant(const Constant& other_constant) = delete;
  Constant& operator=(const Constant& other_constant) = delete;

  // Returns the unqualified name of this Constant.
  const std::string& Name() const { return name_path_.back(); }

  // Returns the name path of this Constant, top-down in the Catalog hierarchy
  // and fully qualified. Name components are separated by ".".
  std::string FullName() const { return absl::StrJoin(name_path_, "."); }

  // Returns the name path of this Constant, top-down and fully qualified.
  const std::vector<std::string>& name_path() const { return name_path_; }

  // Returns the type of this Constant.
  virtual const Type* type() const = 0;

  // Returns whether or not this Constant is a specific constant interface or
  // implementation.
  template <class ConstantSubclass>
  bool Is() const {
    return dynamic_cast<const ConstantSubclass*>(this) != nullptr;
  }

  // Returns this Constant as ConstantSubclass*. Must only be used when it is
  // known that the object *is* this subclass, which can be checked using Is()
  // before calling GetAs().
  template <class ConstantSubclass>
  const ConstantSubclass* GetAs() const {
    return static_cast<const ConstantSubclass*>(this);
  }

  // Returns a string describing this Constant for debugging purposes.
  virtual std::string DebugString() const = 0;

 private:
  // The name path of this Constant, top-down in the Catalog hierarchy and
  // fully qualified.
  const std::vector<std::string> name_path_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_CONSTANT_H_
