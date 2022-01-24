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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/annotation.pb.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

class AnnotationSpec;
class ArrayAnnotationMap;
class StructAnnotationMap;
class ArrayType;
class StructType;

// Maps from AnnotationSpec ID to SimpleValue.
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
  AnnotationMap& SetAnnotation(int id, const SimpleValue& value) {
    ZETASQL_DCHECK(value.IsValid());
    annotations_[id] = value;
    return *this;
  }

  // Sets annotation value for the given AnnotationSpec type, overwriting
  // existing value if it exists.
  // Returns a self reference for caller to be able to chain SetAnnotation()
  // calls.
  template <class T>
  AnnotationMap& SetAnnotation(const SimpleValue& value) {
    static_assert(std::is_base_of<AnnotationSpec, T>::value,
                  "Must be a subclass of AnnotationSpec");
    return SetAnnotation(T::GetId(), value);
  }

  // Clears annotation value for the given AnnotationSpec ID if it exists.
  void UnsetAnnotation(int id) { annotations_.erase(id); }

  // Clears annotation value for the given AnnotationSpec ID if it exists.
  template <class T>
  void UnsetAnnotation() {
    static_assert(std::is_base_of<AnnotationSpec, T>::value,
                  "Must be a subclass of AnnotationSpec");
    return UnsetAnnotation(T::GetId());
  }

  // Returns annotation value for given AnnotationSpec ID. Returns nullptr if
  // the ID is not in the map.
  const SimpleValue* GetAnnotation(int id) const {
    return zetasql_base::FindOrNull(annotations_, id);
  }

  virtual bool IsStructMap() const { return false; }
  virtual bool IsArrayMap() const { return false; }
  virtual StructAnnotationMap* AsStructMap() { return nullptr; }
  virtual const StructAnnotationMap* AsStructMap() const { return nullptr; }
  virtual ArrayAnnotationMap* AsArrayMap() { return nullptr; }
  virtual const ArrayAnnotationMap* AsArrayMap() const { return nullptr; }

  virtual std::string DebugString() const {
    return DebugStringInternal(/*annotation_spec_id=*/{});
  }

  virtual std::string DebugString(int annotation_spec_id) const {
    return DebugStringInternal(annotation_spec_id);
  }

  // Print annotation values of an AnnotationMap recursively. If
  // <annotation_spec_id> has value, we only print annotation values for the
  // specific AnnotationSpec ID.
  virtual std::string DebugStringInternal(
      std::optional<int> annotation_spec_id) const;

  // Decides if two AnnotationMap instances are equal.
  bool Equals(const AnnotationMap& that) const {
    return EqualsInternal(this, &that, /*annotation_spec_id=*/{});
  }

  // Determines whether two AnnotationMap instances have equal annotation values
  // recursively on all nested levels for the specified AnnotationSpec ID (all
  // other annotations are ignored for this comparison).
  bool HasEqualAnnotations(const AnnotationMap& that,
                           int annotation_spec_id) const {
    return EqualsInternal(this, &that, annotation_spec_id);
  }

  // Determines whether two AnnotationMap instances have equal annotation values
  // given specified AnnotationSpec ID.
  // Accepts nullptr and treats nullptr to be equal to a non-nullptr
  // AnnotationMap that does not contain the specified AnnotationSpec ID.
  static bool HasEqualAnnotations(const AnnotationMap* lhs,
                                  const AnnotationMap* rhs,
                                  int annotation_spec_id) {
    return EqualsInternal(lhs, rhs, annotation_spec_id);
  }

  // Decides if two AnnotationMap instances are equal.
  // Accepts nullptr and treats nullptr to be equal to an empty AnnotationMap
  // (both for <lhs> and <rhs> as well as for any nested maps).
  static bool Equals(const AnnotationMap* lhs, const AnnotationMap* rhs) {
    return EqualsInternal(lhs, rhs, /*annotation_spec_id=*/{});
  }

  // Returns true if this and all the nested AnnotationMap are empty.
  bool Empty() const { return EmptyInternal(/*annotation_spec_id=*/{}); }

  // Returns true if this or any of the nested AnnotationMaps have an annotation
  // for the given AnnotationSpec type.
  template <class T>
  bool Has() const {
    static_assert(std::is_base_of<AnnotationSpec, T>::value,
                  "Must be a subclass of AnnotationSpec");
    return !EmptyInternal(T::GetId());
  }

  // Returns true if this AnnotationMap has compatible nested structure with
  // <type>. The structures are compatible when they meet one of the conditions
  // below:
  // * This instance and <type> both are non-STRUCT/non-ARRAY.
  // * This instance is a StructAnnotationMap and <type> is a STRUCT (and the
  //   number of fields matches).
  // * This instance is an ArrayAnnotationMap and <type> is an ARRAY.
  // * The StructAnnotationMap field or ArrayAnnotationMap element is either
  //   NULL or is compatible by recursively following these rules. When it is
  //   NULL, it indicates that the annotation map is empty on all the nested
  //   levels, and therefore such maps are compatible with any Type (including
  //   structs and arrays).
  bool HasCompatibleStructure(const Type* type) const;

  // Returns a clone of this instance.
  std::unique_ptr<AnnotationMap> Clone() const;

  // Normalizes AnnotationMap by replacing empty annotation maps with NULL.
  // After normalization, on all the nested levels:
  //  * For a StructAnnotationMap, each one of its fields is either null or
  //    non-empty.
  //  * For an ArrayAnnotationMap, its element is either null or non-empty.
  void Normalize() { NormalizeInternal(); }

  // Returns true if this instance is in the simplest form described in
  // Normalize() comments. This function is mainly for testing purpose.
  bool IsNormalized() const;

  // Serializes this instance to protobuf.
  virtual absl::Status Serialize(AnnotationMapProto* proto) const;

  // Deserializes and creates an instance of AnnotationMap from protobuf.
  static absl::StatusOr<std::unique_ptr<AnnotationMap>> Deserialize(
      const AnnotationMapProto& proto);

 protected:
  AnnotationMap() {}

 private:
  friend class AnnotationTest;
  friend class StructAnnotationMap;
  friend class ArrayAnnotationMap;
  friend class TypeFactory;

  // Returns estimated size of memory owned by this AnnotationMap. The estimated
  // size includes size of the fields if this instance is a StructAnnotationMap
  // and size of the element if this instance is an ArrayAnnotationMap.
  int64_t GetEstimatedOwnedMemoryBytesSize() const;

  // Decides if two AnnotationMap instances are equal.
  // Accepts nullptr and treats nullptr to be equal to an empty AnnotationMap
  // (both for <lhs> and <rhs> as well as for any nested maps).
  // If <annotation_spec_id> has value, only compares annotation value for the
  // given AnnotationSpec ID.
  static bool EqualsInternal(const AnnotationMap* lhs, const AnnotationMap* rhs,
                             std::optional<int> annotation_spec_id);

  // Returns true if this and all the nested AnnotationMaps are empty.
  // If <annotation_spec_id> has value, then this method only checks annotation
  // value for the given AnnotationSpec ID (all other annotations are ignored).
  bool EmptyInternal(std::optional<int> annotation_spec_id = {}) const;

  // Returns true if two SimpleValue instances are equal.
  static bool SimpleValueEqualsHelper(const SimpleValue* lhs,
                                      const SimpleValue* rhs);

  // Returns true if <lhs> has compatible nested structure with <rhs>. The
  // structures are compatible when they meet one of the conditions below:
  // * <lhs> and <rhs> are AnnotationMap, or StructAnnotationMap (with the same
  //   number of fields), or ArrayAnnotationMap.
  // * <lhs> or <rhs> is either NULL or they are compatible recursively.
  static bool HasCompatibleStructure(const AnnotationMap* lhs,
                                     const AnnotationMap* rhs);

  // Normalizes AnnotationMap as described in Normalize() function.
  // Returns true if the AnnotationMap is empty on all the nested levels.
  bool NormalizeInternal();

  // Returns true if this instance is normalized (as described in Normalize()
  // comments) and non-empty.
  // When <check_non_empty> is false, it doesn't check whether the instance is
  // empty or not.
  bool IsNormalizedAndNonEmpty(bool check_non_empty) const;

  // Maps from AnnotationSpec ID to SimpleValue.
  absl::flat_hash_map<int, SimpleValue> annotations_;
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
  const AnnotationMap* field(int i) const { return fields_[i].get(); }
  AnnotationMap* mutable_field(int i) { return fields_[i].get(); }

  // Clones <from> and overwrites what's in the struct field <i>.
  // If <from> is nullptr, the struct field is set to NULL.
  // Returns an error if the struct field and <from> don't have compatible
  // structure as defined in AnnotationMap::HasCompatibleStructure(lhs, rhs)
  absl::Status CloneIntoField(int i, const AnnotationMap* from);

  const std::vector<std::unique_ptr<AnnotationMap>>& fields() const {
    return fields_;
  }

  std::string DebugStringInternal(
      std::optional<int> annotation_spec_id) const override;

  absl::Status Serialize(AnnotationMapProto* proto) const override;

 private:
  friend class AnnotationMap;
  friend class AnnotationTest;
  // Accessed only by AnnotationMap.
  StructAnnotationMap() = default;
  explicit StructAnnotationMap(const StructType* struct_type);

  // AnnotationMap on each struct field. Number of fields always match the
  // number of fields of the struct type that is used to create this
  // StructAnnotationMap. The unique_ptr for each field can be null, which
  // indicates that the AnnotationMap for the field (and all its children if
  // applicable) is empty.
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
  const AnnotationMap* element() const { return element_.get(); }

  // Clones <from> and overwrites what's in the array element.
  // If <from> is nullptr, the array element is set to NULL.
  // Returns an error if the array element and <from> don't have compatible
  // structure as defined in AnnotationMap::HasCompatibleStructure(lhs, rhs)
  absl::Status CloneIntoElement(const AnnotationMap* from);

  std::string DebugStringInternal(
      std::optional<int> annotation_spec_id) const override;

  absl::Status Serialize(AnnotationMapProto* proto) const override;

 private:
  friend class AnnotationMap;
  friend class AnnotationTest;
  // Accessed only by AnnotationMap.
  ArrayAnnotationMap() = default;
  explicit ArrayAnnotationMap(const ArrayType* array_type);

  // AnnotationMap on array element. The unique_ptr can be null, which indicates
  // that the AnnotationMap for the element (and all its children if applicable)
  // is empty.
  std::unique_ptr<AnnotationMap> element_;
};

// Holds unowned pointers to Type and AnnoationMap. <annotation_map> could be
// nullptr to indicate that the <type> doesn't have annotation. This struct is
// cheap to copy, should always be passed by value.
struct AnnotatedType {
  // TODO: Add a constructor that only takes <type> and sets
  // <annotation_map> to nullptr implicitly.
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
class ResolvedFunctionCallBase;
class ResolvedGetStructField;
class ResolvedMakeStruct;
class ResolvedSubqueryExpr;

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
  // For zetasql AnnotationSpecs, the returned ID should be the same as the
  // enum value of corresponding AnnotationKind.
  virtual int Id() const = 0;

  // Checks annotation in <function_call>.argument_list and propagates to
  // <result_annotation_map>.
  //
  // To override logic for checking or propagation logic for a specific
  // function, an implementation could look at <function_call>.function and do
  // something differently.
  virtual absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) = 0;

  // Propagates annotation from <column_ref>.column to <result_annotation_map>.
  virtual absl::Status CheckAndPropagateForColumnRef(
      const ResolvedColumnRef& column_ref,
      AnnotationMap* result_annotation_map) = 0;

  // Propagates annotation from the referenced struct field to
  // <result_annotation_map>.
  virtual absl::Status CheckAndPropagateForGetStructField(
      const ResolvedGetStructField& get_struct_field,
      AnnotationMap* result_annotation_map) = 0;

  // Propagates annotation from the referenced struct field to
  // <result_annotation_map>.
  virtual absl::Status CheckAndPropagateForMakeStruct(
      const ResolvedMakeStruct& make_struct,
      StructAnnotationMap* result_annotation_map) = 0;

  // Propagates annotation from the subquery to result_annotation_map>.
  virtual absl::Status CheckAndPropagateForSubqueryExpr(
      const ResolvedSubqueryExpr& subquery_expr,
      AnnotationMap* result_annotation_map) = 0;

  // TODO: add more functions to handle different resolved nodes.
};

// Built-in annotation IDs.
enum class AnnotationKind {
  // Annotation id for zetasql::CollationAnnotation.
  kCollation = 1,
  // Annotation ID for the SampleAnnotation, which is used for testing
  // purposes only.
  kSampleAnnotation = 2,
  // Annotation ID up to kMaxBuiltinAnnotationKind are reserved for zetasql
  // built-in annotations.
  kMaxBuiltinAnnotationKind = 10000,
};

}  // namespace zetasql
#endif  // ZETASQL_PUBLIC_TYPES_ANNOTATION_H_
