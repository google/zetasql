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

#include "zetasql/reference_impl/common.h"

#include <string>

#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::Status ValidateTypeSupportsEqualityComparison(const Type* type) {
  switch (type->kind()) {
    case TYPE_INT32:
    case TYPE_INT64:
    case TYPE_UINT32:
    case TYPE_UINT64:
    case TYPE_BOOL:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_NUMERIC:
    case TYPE_BIGNUMERIC:
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_DATE:
    case TYPE_TIMESTAMP:
    case TYPE_TIME:
    case TYPE_DATETIME:
    case TYPE_INTERVAL:
    case TYPE_ENUM:
    case TYPE_PROTO:
    case TYPE_STRUCT:
    case TYPE_ARRAY:
      return absl::OkStatus();
    case TYPE_GEOGRAPHY:
    case TYPE_JSON:
    case TYPE_UNKNOWN:
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "No equality comparison for type " << type->DebugString();
  }
}

absl::Status ValidateTypeSupportsOrderComparison(const Type* type) {
  switch (type->kind()) {
    case TYPE_INT32:
    case TYPE_INT64:
    case TYPE_UINT32:
    case TYPE_UINT64:
    case TYPE_BOOL:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_NUMERIC:
    case TYPE_BIGNUMERIC:
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_DATE:
    case TYPE_TIMESTAMP:
    case TYPE_TIME:
    case TYPE_DATETIME:
    case TYPE_INTERVAL:
    case TYPE_ENUM:
      return absl::OkStatus();
    case TYPE_ARRAY: {
      const ArrayType* array_type = type->AsArray();
      if (ValidateTypeSupportsOrderComparison(
              array_type->element_type()).ok()) {
        return absl::OkStatus();
      }
    }
    ABSL_FALLTHROUGH_INTENDED;
    case TYPE_GEOGRAPHY:
    case TYPE_PROTO:
    case TYPE_STRUCT:
    case TYPE_JSON:
    case TYPE_UNKNOWN:
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "No order comparison for type " << type->DebugString();
  }
}

absl::StatusOr<std::string>
GetCollationNameFromResolvedCollation(
    const ResolvedCollation& resolved_collation) {
  ZETASQL_RET_CHECK(!resolved_collation.Empty())
      << "Cannot get collation name from empty ResolvedCollation.";
  // TODO: So far we only support to get collation name from
  // ResolvedCollation for String type. Will extend to get collation info for
  // other types such as Array and Struct.
  if (!resolved_collation.HasCompatibleStructure(types::StringType())) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Collation for non-String type is not supported: "
           << resolved_collation.DebugString();
  }
  ZETASQL_RET_CHECK(resolved_collation.HasCollation());
  return std::string(resolved_collation.CollationName());
}

absl::StatusOr<std::unique_ptr<const ZetaSqlCollator>>
GetCollatorFromResolvedCollation(const ResolvedCollation& resolved_collation) {
  ZETASQL_ASSIGN_OR_RETURN(std::string collation_name,
                   GetCollationNameFromResolvedCollation(resolved_collation));
  return MakeSqlCollatorLite(collation_name);
}

absl::StatusOr<std::unique_ptr<const ZetaSqlCollator>>
GetCollatorFromResolvedCollationValue(const Value& collation_value) {
  ResolvedCollationProto resolved_collation_proto;
  bool is_valid = resolved_collation_proto.ParsePartialFromString(
      std::string(collation_value.ToCord()));
  ZETASQL_RET_CHECK(is_valid)
      << "Failed to parse collation_value to ResolvedCollation proto: "
      << collation_value.ToCord();

  ResolvedCollation resolved_collation;
  ZETASQL_ASSIGN_OR_RETURN(resolved_collation,
                   ResolvedCollation::Deserialize(resolved_collation_proto));
  return GetCollatorFromResolvedCollation(resolved_collation);
}

}  // namespace zetasql
