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

#ifndef ZETASQL_PUBLIC_TYPES_SIMPLE_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_SIMPLE_TYPE_H_

#include "zetasql/public/types/type.h"

namespace zetasql {

// SimpleType includes all the non-parameterized types (all scalar types
// except enum).
class SimpleType : public Type {
 public:
  SimpleType(const TypeFactory* factory, TypeKind kind);
#ifndef SWIG
  SimpleType(const SimpleType&) = delete;
  SimpleType& operator=(const SimpleType&) = delete;
#endif  // SWIG

  std::string TypeName(ProductMode mode) const override;

 protected:
  ~SimpleType() override;

  int64_t GetEstimatedOwnedMemoryBytesSize() const override {
    return sizeof(*this);
  }

 private:
  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  friend class TypeFactory;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_SIMPLE_TYPE_H_
