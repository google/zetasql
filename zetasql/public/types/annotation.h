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

// This is a framework for annotated types.  The possible annotations are
// defined using an AnnotationSpec.  Some annotations are built in, and others
// can be engine-defined.
//
// Annotations can be added onto source columns in the Catalog, and will be
// propagated to query output columns.  Annotations can also be generated
// automatically as part of analysis.
//
// Annotation propagation behavior is defined using AnnotationSpec.
// Specific annotations can modify function behavior as defined in the
// AnnotationSpec or FunctionSignature.
//
#ifndef ZETASQL_PUBLIC_TYPES_ANNOTATION_H_
#define ZETASQL_PUBLIC_TYPES_ANNOTATION_H_

#include <memory>

#include "zetasql/public/annotation.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/value_representations.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/no_destructor.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Represents an immutable annotation value.  AnnotationValue supports cheap
// copying and assignment. More general scalar types or vector types (e.g.
// vector<string>) could be added when needed.
class AnnotationValue {
 public:
  enum ValueType {
    TYPE_INVALID = 0,
    TYPE_INT64,
    TYPE_STRING,
  };
  // Factory methods to create atomic values.
  static AnnotationValue Int64(int64_t v);
  static AnnotationValue String(std::string v);

  // Constructs an invalid value.  Needed for using values with STL.  All
  // methods that fetch value will crash if called on invalid values.
  AnnotationValue() : type_(TYPE_INVALID) {}

  AnnotationValue(const AnnotationValue& that) { CopyFrom(that); }
  AnnotationValue(AnnotationValue&&);
  ~AnnotationValue() { Clear(); }
  AnnotationValue& operator=(const AnnotationValue& that);
  AnnotationValue& operator=(AnnotationValue&& that);

  bool operator==(const AnnotationValue& that) const { return Equals(that); }
  bool operator!=(const AnnotationValue& that) const { return !Equals(that); }

  // Returns true if the value is valid (invalid values are created by the
  // default constructor).
  bool IsValid() const { return type_ != TYPE_INVALID; }
  bool has_int64_value() const { return type_ == TYPE_INT64; }
  bool has_string_value() const { return type_ == TYPE_STRING; }
  // Crashes if type is not TYPE_INT64.
  int64_t int64_value() const;
  // Crashes if type is not TYPE_STRING.
  const std::string& string_value() const;

  // Returns true if this instance equals <that>.
  bool Equals(const AnnotationValue& that) const;

  // Serializes this instance to proto.
  absl::Status Serialize(AnnotationProto* proto) const;

  // Deserializes and returns an instance from a proto.
  static zetasql_base::StatusOr<AnnotationValue> Deserialize(
      const AnnotationProto& proto);

  std::string DebugString() const;

 private:
  friend class AnnotationTest;
  // Hide constructors below.  Users should always use factory method to create
  // an instance.
  AnnotationValue(ValueType type, int64_t value)
      : type_(type), int64_value_(value) {}
  AnnotationValue(ValueType type, std::string value)
      : type_(type), string_ptr_(new internal::StringRef(std::move(value))) {}

  // Clears the contents and makes it invalid.
  void Clear();

  // Copies the contents of this instance from <that>. Crashes if this pointer
  // equals to <that>.
  void CopyFrom(const AnnotationValue& that);

  ValueType type_;
  union {
    int64_t int64_value_ = 0;            // Assigned for TYPE_INT64.
    // Assigned for TYPE_STRING.  An instance of AnnotationValue may share
    // ownership of the pointer with other instances with references being
    // counted.
    internal::StringRef* string_ptr_;
  };
};

class AnnotationSpec;
class StructAnnotationMap;
class ArrayAnnotationMap;

// Built-in annotation IDs.
enum class AnnotationKind {
  COLLATION = 0,
  // Annotation ID up to kMaxBuiltinAnnotationKind are reserved for zetasql
  // built-in annotations.
  kMaxBuiltinAnnotationKind = 10000,
};

// Maps from AnnotationSpec ID to AnnotationValue.
class AnnotationMap {
 public:
  // Creates an instance of AnnotationMap. Returns a StructAnnotationMap
  // instance if <type> is a STRUCT. Returns an ArrayAnnotationMap if <type>
  // is an ARRAY.
  static std::unique_ptr<AnnotationMap> Create(const Type* type);

  AnnotationMap(const AnnotationMap&) = delete;
  AnnotationMap& operator=(const AnnotationMap&) = delete;
  virtual ~AnnotationMap() {}

  // Sets annotation value for given AnnotationSpec ID, overwriting existing
  // value if it exists.
  // Returns a self reference for caller to be able to chain SetAnnotation()
  // calls.
  AnnotationMap& SetAnnotation(int id, const AnnotationValue& value) {
    ZETASQL_DCHECK(value.IsValid());
    annotations_[id] = value;
    return *this;
  }
  // Returns annotation value for given AnnotationSpec ID. Returns nullptr if
  // the ID is not in the map.
  const AnnotationValue* GetAnnotation(int id) const {
    return zetasql_base::FindOrNull(annotations_, id);
  }

  virtual bool IsStructMap() const { return false; }
  virtual bool IsArrayMap() const { return false; }
  virtual StructAnnotationMap* AsStructMap() { return nullptr; }
  virtual const StructAnnotationMap* AsStructMap() const { return nullptr; }
  virtual ArrayAnnotationMap* AsArrayMap() { return nullptr; }
  virtual const ArrayAnnotationMap* AsArrayMap() const { return nullptr; }

  virtual std::string DebugString() const;

  // Decides if two AnnotationMap instances are equal.
  bool Equals(const AnnotationMap& that) const;

  // Returns true if this and all the nested AnnotationMap are empty.
  bool Empty() const;

  // Returns true if this AnnotationMap has the matching nested structure with
  // <type>. The structures match when they meet one of the conditions below:
  // * This instance and <type> both are non-STRUCT/non-ARRAY
  // * This instance is a StructAnnotationMap and <type> is a STRUCT (and the
  //   number of fields matches),
  // * This instance is an ArrayAnnotationMap and <type> is an ARRAY.
  //
  // Such check is done recursively on struct fields and array element.
  bool HasMatchingStructure(const Type* type) const;

  // Serializes this instance to protobuf.
  virtual absl::Status Serialize(AnnotationMapProto* proto) const;

  // Deserializes and creates an instance of AnnotationMap from protobuf.
  static zetasql_base::StatusOr<std::unique_ptr<AnnotationMap>> Deserialize(
      const AnnotationMapProto& proto);

 protected:
  AnnotationMap() {}

 private:
  // Maps from AnnotationSpec ID to AnnotationValue.
  absl::flat_hash_map<int, AnnotationValue> annotations_;
};

// Represents annotations of a STRUCT type. In addition to the annotation on the
// whole type, this class also keeps an AnnotationMap for each field of the
// STRUCT type.
class StructAnnotationMap : public AnnotationMap {
 public:
  bool IsStructMap() const override { return true; }
  StructAnnotationMap* AsStructMap() override { return this; }
  const StructAnnotationMap* AsStructMap() const override { return this; }

  int num_fields() const { return static_cast<int>(fields_.size()); }
  const AnnotationMap& field(int i) const { return *fields_[i]; }
  AnnotationMap* mutable_field(int i) { return fields_[i].get(); }
  const std::vector<std::unique_ptr<AnnotationMap>>& fields() const {
    return fields_;
  }
  std::string DebugString() const override;

  absl::Status Serialize(AnnotationMapProto* proto) const override;

 private:
  friend class AnnotationMap;
  // Accessed only by AnnotationMap.
  StructAnnotationMap() = default;
  explicit StructAnnotationMap(const StructType* struct_type);

  // AnnotationMap on each struct field. Number of fields always match the
  // number of fields of the struct type that is used to create this
  // StructAnnotationMap. Each field cannot be empty.
  std::vector<std::unique_ptr<AnnotationMap>> fields_;
};

// Represents annotation of an ARRAY type. In addition to the annotation on the
// whole type, this class also keeps an AnnotationMap for the ARRAY's element
// type.
class ArrayAnnotationMap : public AnnotationMap {
 public:
  bool IsArrayMap() const override { return true; }
  ArrayAnnotationMap* AsArrayMap() override { return this; }
  const ArrayAnnotationMap* AsArrayMap() const override { return this; }

  AnnotationMap* mutable_element() { return element_.get(); }
  const AnnotationMap& element() const { return *element_; }
  std::string DebugString() const override;

  absl::Status Serialize(AnnotationMapProto* proto) const override;

 private:
  friend class AnnotationMap;
  // Accessed only by AnnotationMap.
  ArrayAnnotationMap() = default;
  explicit ArrayAnnotationMap(const ArrayType* array_type);

  // AnnotationMap on array element. Cannot be null.
  std::unique_ptr<AnnotationMap> element_;
};

// Holds unowned pointers to Type and AnnoationMap. <annotation_map> could be
// nullptr to indicate that the <type> doesn't have annotation. This struct is
// cheap to copy, should always be passed by value.
struct AnnotatedType {
  AnnotatedType(const Type* type, const AnnotationMap* annotation_map)
      : type(type), annotation_map(annotation_map) {}

  const Type* type = nullptr;

  // Maps from AnnotationSpec ID to annotation value. Could be null to indicate
  // the <type> doesn't have annotation.
  const AnnotationMap* annotation_map = nullptr;

 private:
  // Friend classes that are allowed to access default constructor.
  friend class ResolvedColumn;
  AnnotatedType() = default;
};

class ResolvedColumnRef;
class ResolvedFunctionCall;
class ResolvedGetStructField;

// Interface to define a possible annotation, with resolution and propagation
// logic.
//
// If an annotation check fails when propagating an annotation, each
// CheckAndPropagateFor<resolved_node_name>() function should return
// INVALID_ARGUMENT (normally with MakeSqlError()) to indicate a
// an analysis error. Other types of errors will be converted into an internal
// error.
//
class AnnotationSpec {
 public:
  virtual ~AnnotationSpec() {}

  // Returns a unique ID for this kind of annotation.
  virtual int Id() const = 0;

  // Checks annotation in <function_call>.argument_list and propagates to
  // <result_annotation_map>.
  //
  // To override logic for checking or propagation logic for a specific
  // function, an implementation could look at <function_call>.function and do
  // something differently.
  virtual absl::Status CheckAndPropagateForFunctionCall(
      const ResolvedFunctionCall& function_call,
      AnnotationMap* result_annotation_map) = 0;

  // Propagates annotation from <column_ref>.column to <result_annotation_map>.
  virtual absl::Status CheckAndPropagateForColumnRef(
      const ResolvedColumnRef& column_ref,
      AnnotationMap* result_annotation_map) = 0;

  // Propagates annotation from the referenced struct field to
  // <result_annotation_map>.
  virtual absl::Status CheckAndPropagateForGetStructField(
      const ResolvedGetStructField& get_struct_field,
      AnnotationMap* annotation_to_propagate) = 0;
  // TODO: add more functions to handle different resolved nodes.
};

}  // namespace zetasql
#endif  // ZETASQL_PUBLIC_TYPES_ANNOTATION_H_
