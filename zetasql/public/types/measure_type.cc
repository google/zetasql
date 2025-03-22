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

#include "zetasql/public/types/measure_type.h"

#include <string>
#include <utility>

#include "zetasql/common/errors.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/types/collation.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "absl/hash/hash.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

std::string MeasureType::ShortTypeName(ProductMode mode,
                                       bool use_external_float32) const {
  return absl::StrCat(
      "MEASURE<", result_type_->ShortTypeName(mode, use_external_float32), ">");
}

std::string MeasureType::TypeName(ProductMode mode,
                                  bool use_external_float32) const {
  return absl::StrCat("MEASURE<",
                      result_type_->TypeName(mode, use_external_float32), ">");
}

absl::StatusOr<std::string> MeasureType::TypeNameWithModifiers(
    const TypeModifiers& type_modifiers, ProductMode mode,
    bool use_external_float32) const {
  const TypeParameters& type_params = type_modifiers.type_parameters();
  if (!type_params.IsEmpty() && type_params.num_children() != 1) {
    return MakeSqlError()
           << "Input type parameter does not correspond to MeasureType";
  }

  // TODO: b/350555383 - Support collation if it's needed in the future. Adding
  // collation support to this function is trivial, but general collation code
  // needs to support MeasureType.
  const Collation& collation = type_modifiers.collation();
  if (!collation.Empty()) {
    return MakeSqlError() << "MeasureType does not support collation";
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::string result_type_name,
      result_type_->TypeNameWithModifiers(
          TypeModifiers::MakeTypeModifiers(
              type_params.IsEmpty() ? TypeParameters() : type_params.child(0),
              collation),
          mode, use_external_float32));

  return absl::StrCat("MEASURE<", result_type_name, ">");
}

std::string MeasureType::CapitalizedName() const {
  ABSL_CHECK_EQ(kind(), TYPE_MEASURE);  // Crash OK
  return absl::StrCat("Measure<", AsMeasure()->result_type()->CapitalizedName(),
                      ">");
}

bool MeasureType::SupportsOrdering(const LanguageOptions& language_options,
                                   std::string* type_description) const {
  if (type_description != nullptr) {
    *type_description = "MEASURE";
  }
  return false;
}

bool MeasureType::SupportsEquality() const { return false; }

bool MeasureType::SupportsPartitioningImpl(
    const LanguageOptions& language_options,
    const Type** no_partitioning_type) const {
  *no_partitioning_type = this;
  return false;
}

bool MeasureType::IsSupportedType(
    const LanguageOptions& language_options) const {
  return language_options.LanguageFeatureEnabled(
             FEATURE_V_1_4_ENABLE_MEASURES) &&
         result_type_->IsSupportedType(language_options);
}

void MeasureType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                                  std::string* debug_string) const {
  absl::StrAppend(debug_string, "MEASURE<");
  stack->push_back(">");
  stack->push_back(result_type());
}

absl::HashState MeasureType::HashTypeParameter(absl::HashState state) const {
  return result_type()->Hash(std::move(state));
}

absl::HashState MeasureType::HashValueContent(const ValueContent& value,
                                              absl::HashState state) const {
  // It is not required that HashValueContent depend on the type, but only on
  // the content. Since a Measure type doesn't inherently have a value, this is
  // a no-op on the hash state.
  return state;
}

absl::Status MeasureType::SerializeValueContent(const ValueContent& value,
                                                ValueProto* value_proto) const {
  // A Measure type doesn't inherently have a value.
  return absl::UnimplementedError(
      "SerializeValueContent is unsupported for MeasureType.");
}

absl::Status MeasureType::DeserializeValueContent(const ValueProto& value_proto,
                                                  ValueContent* value) const {
  // A Measure type doesn't inherently have a value.
  return absl::UnimplementedError(
      "DeserializeValueContent is unsupported for MeasureType.");
}

absl::Status MeasureType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  return result_type()->SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto->mutable_measure_type()->mutable_result_type(),
      file_descriptor_set_map);
}

bool MeasureType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  // A Measure type doesn't inherently have a value.
  ABSL_DCHECK(false) << "ValueContentEquals is unsupported for MeasureType.";
  return false;
}
bool MeasureType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                   const Type* other_type) const {
  // A Measure type doesn't inherently have a value.
  ABSL_DCHECK(false) << "ValueContentLess is unsupported for MeasureType.";
  return false;
}

std::string MeasureType::FormatValueContent(
    const ValueContent& value, const FormatValueContentOptions& options) const {
  // A Measure type doesn't inherently have a value.
  ABSL_DCHECK(false) << "FormatValueContent is unsupported for MeasureType.";
  return "FormatValueContent is unsupported for MeasureType.";
}

}  // namespace zetasql
