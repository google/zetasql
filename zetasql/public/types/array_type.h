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

  int nesting_depth() const override {
    return element_type_->nesting_depth() + 1;
  }

 protected:
  // Return estimated size of memory owned by this type. Array's owned memory
  // does not include its element type's memory (which is owned by some
  // TypeFactory).
  int64_t GetEstimatedOwnedMemoryBytesSize() const override {
    return sizeof(*this);
  }

 private:
  ArrayType(const TypeFactory* factory, const Type* element_type);
  ~ArrayType() override;

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override;

  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override;

  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  const Type* const element_type_;

  friend class TypeFactory;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_ARRAY_TYPE_H_
