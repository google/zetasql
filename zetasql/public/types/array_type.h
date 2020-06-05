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

#ifndef ZETASQL_PUBLIC_TYPES_ARRAY_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_ARRAY_TYPE_H_

#include "zetasql/public/types/type.h"

namespace zetasql {

// An array type.
// Arrays of arrays are not supported.
class ArrayType : public Type {
 public:
#ifndef SWIG
  ArrayType(const ArrayType&) = delete;
  ArrayType& operator=(const ArrayType&) = delete;
#endif  // SWIG

  const Type* element_type() const { return element_type_; }

  const ArrayType* AsArray() const override { return this; }

  // Helper function to determine deep equality or equivalence for array types.
  static bool EqualsImpl(const ArrayType* type1, const ArrayType* type2,
                         bool equivalent);

  // Arrays support ordering if FEATURE_V_1_3_ARRAY_ORDERING is enabled
  // and the array's element Type supports ordering.
  bool SupportsOrdering(const LanguageOptions& language_options,
                        std::string* type_description) const override;
  bool SupportsEquality() const override;

  std::string ShortTypeName(ProductMode mode) const override;
  std::string TypeName(ProductMode mode) const override;

  bool UsingFeatureV12CivilTimeType() const override {
    return element_type_->UsingFeatureV12CivilTimeType();
  }

  bool IsSupportedType(const LanguageOptions& language_options) const override;

  int nesting_depth() const override {
    return element_type_->nesting_depth() + 1;
  }

 protected:
  bool EqualsForSameKind(const Type* that, bool equivalent) const override;

  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override;

  // Return estimated size of memory owned by this type. Array's owned memory
  // does not include its element type's memory (which is owned by some
  // TypeFactory).
  int64_t GetEstimatedOwnedMemoryBytesSize() const override {
    return sizeof(*this);
  }

  void InitializeValueContent(ValueContent* value) const override;
  void CopyValueContent(const ValueContent& from,
                        ValueContent* to) const override;
  void ClearValueContent(const ValueContent& value) const override;
  absl::HashState HashTypeParameter(absl::HashState state) const override;
  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const override;
  bool ValueContentEqualsImpl(
      const ValueContent& x, const ValueContent& y,
      const ValueEqualityCheckOptions& options) const override;

  // This function shouldn't be called until b/155192766 is fixed.
  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override;

 private:
  ArrayType(const TypeFactory* factory, const Type* element_type);
  ~ArrayType() override;

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override;

  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override;

  absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  const Type* const element_type_;

  friend class TypeFactory;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_ARRAY_TYPE_H_
