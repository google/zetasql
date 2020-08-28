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

#include "zetasql/resolved_ast/resolved_column.h"

#include <memory>

#include "zetasql/resolved_ast/serialization.pb.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// TODO This version is allocating the names in the global pool, so
// they leak.  This constructor should not be used by any production zetasql
// code, and should probably only be used in tests, so this may be okay for
// now, but there's a TODO in the header to try to remove this.
// Maybe this should live in a testonly linker target.
ResolvedColumn::ResolvedColumn(int column_id, const std::string& table_name,
                               const std::string& name, const Type* type)
    : ResolvedColumn(column_id, IdString::MakeGlobal(table_name),
                     IdString::MakeGlobal(name), type) {}

std::string ResolvedColumn::DebugString() const {
  return absl::StrCat(table_name_.ToStringView(), ".", name_.ToStringView(),
                      "#", column_id_);
}

std::string ResolvedColumn::ShortDebugString() const {
  return absl::StrCat(name_.ToStringView(), "#", column_id_);
}

absl::Status ResolvedColumn::SaveTo(
    FileDescriptorSetMap* file_descriptor_set_map,
    ResolvedColumnProto* proto) const {
  // Consider serializing ResolvedColumn in a separate table, indexed by
  // column_id_, and then only serialize the column_id_ in the AST.
  proto->set_table_name(std::string(table_name_.ToStringView()));
  proto->set_name(std::string(name_.ToStringView()));

  proto->set_column_id(column_id_);
  return type_->SerializeToProtoAndDistinctFileDescriptors(
      proto->mutable_type(), file_descriptor_set_map);
}

zetasql_base::StatusOr<ResolvedColumn> ResolvedColumn::RestoreFrom(
    const ResolvedColumnProto& proto,
    const ResolvedNode::RestoreParams& params) {
  const IdString table_name = params.string_pool->Make(proto.table_name());
  const IdString column_name = params.string_pool->Make(proto.name());
  const Type* type;
  ZETASQL_RETURN_IF_ERROR(params.type_factory->DeserializeFromProtoUsingExistingPools(
      proto.type(), params.pools, &type));
  return ResolvedColumn(proto.column_id(), table_name, column_name, type);
}

std::string ResolvedColumnListToString(const ResolvedColumnList& columns) {
  if (columns.empty()) return "[]";
  const std::string& common_table_name = columns[0].table_name();
  // Use the regular format if we have only one column.
  bool use_common_table_name = (columns.size() > 1);
  for (int i = 1; i < columns.size(); ++i) {
    if (columns[i].table_name() != common_table_name) {
      use_common_table_name = false;
      break;
    }
  }

  std::string ret;
  if (use_common_table_name) {
    absl::StrAppend(&ret, common_table_name, ".[");
    for (const ResolvedColumn& column : columns) {
      if (&column != &columns[0]) ret += ", ";
      ret += column.ShortDebugString();
    }
  } else {
    ret += "[";
    for (const ResolvedColumn& column : columns) {
      if (&column != &columns[0]) ret += ", ";
      ret += column.DebugString();
    }
  }
  ret += "]";
  return ret;
}

}  // namespace zetasql
