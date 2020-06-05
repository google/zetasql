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

#include "zetasql/public/language_options.h"

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/base/macros.h"
#include "absl/strings/str_join.h"

namespace zetasql {

std::set<LanguageFeature> LanguageOptions::GetLanguageFeaturesForVersion(
    LanguageVersion version) {
  std::set<LanguageFeature> features;
  switch (version) {
    // Include versions in decreasing order here, falling through to include
    // all features from previous versions.
    case VERSION_CURRENT:
    case VERSION_1_3:
      // Add new features here.
      features.insert(FEATURE_V_1_3_PROTO_DEFAULT_IF_NULL);
      features.insert(FEATURE_V_1_3_EXTRACT_FROM_PROTO);
      features.insert(FEATURE_V_1_3_DISALLOW_PROTO3_HAS_SCALAR_FIELD);
      features.insert(FEATURE_V_1_3_ARRAY_ORDERING);
      features.insert(FEATURE_V_1_3_OMIT_INSERT_COLUMN_LIST);
      features.insert(FEATURE_V_1_3_IGNORE_PROTO3_USE_DEFAULTS);
      features.insert(FEATURE_V_1_3_REPLACE_FIELDS);
      features.insert(FEATURE_V_1_3_NULLS_FIRST_LAST_IN_ORDER_BY);
      features.insert(FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME);
      features.insert(FEATURE_V_1_3_CONCAT_MIXED_TYPES);
      features.insert(FEATURE_V_1_3_WITH_RECURSIVE);
      features.insert(FEATURE_V_1_3_PROTO_MAPS);
      features.insert(FEATURE_V_1_3_ENUM_VALUE_DESCRIPTOR_PROTO);
      features.insert(FEATURE_V_1_3_DECIMAL_ALIAS);
      features.insert(FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS);
      features.insert(FEATURE_V_1_3_ALLOW_CONSECUTIVE_ON);
      ABSL_FALLTHROUGH_INTENDED;
    // NO CHANGES SHOULD HAPPEN INSIDE THE VERSIONS BELOW, which are
    // supposed to be stable and frozen, except possibly for bug fixes.
    case VERSION_1_2:
      features.insert(FEATURE_V_1_2_ARRAY_ELEMENTS_WITH_SET);
      features.insert(FEATURE_V_1_2_CIVIL_TIME);
      features.insert(FEATURE_V_1_2_CORRELATED_REFS_IN_NESTED_DML);
      features.insert(FEATURE_V_1_2_GENERATED_COLUMNS);
      features.insert(FEATURE_V_1_2_GROUP_BY_ARRAY);
      features.insert(FEATURE_V_1_2_GROUP_BY_STRUCT);
      features.insert(FEATURE_V_1_2_NESTED_UPDATE_DELETE_WITH_OFFSET);
      features.insert(FEATURE_V_1_2_PROTO_EXTENSIONS_WITH_NEW);
      features.insert(FEATURE_V_1_2_PROTO_EXTENSIONS_WITH_SET);
      features.insert(FEATURE_V_1_2_SAFE_FUNCTION_CALL);
      features.insert(FEATURE_V_1_2_WEEK_WITH_WEEKDAY);
      ABSL_FALLTHROUGH_INTENDED;
    case VERSION_1_1:
      features.insert(FEATURE_V_1_1_ORDER_BY_COLLATE);
      features.insert(FEATURE_V_1_1_WITH_ON_SUBQUERY);
      features.insert(FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE);
      features.insert(FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE);
      features.insert(FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES);
      features.insert(FEATURE_V_1_1_ARRAY_EQUALITY);
      features.insert(FEATURE_V_1_1_LIMIT_IN_AGGREGATE);
      features.insert(FEATURE_V_1_1_HAVING_IN_AGGREGATE);
      features.insert(FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_ANALYTIC);
      features.insert(FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE);
      features.insert(FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF);
      ABSL_FALLTHROUGH_INTENDED;
    case VERSION_1_0:
      break;
    case __LanguageVersion__switch_must_have_a_default__:
      LOG(DFATAL) << "GetLanguageFeaturesForVersion called with " << version;
      break;
  }
  return features;
}

void LanguageOptions::SetLanguageVersion(LanguageVersion version) {
  enabled_language_features_ = GetLanguageFeaturesForVersion(version);
}

LanguageOptions LanguageOptions::MaximumFeatures() {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  return options;
}

std::string LanguageOptions::GetEnabledLanguageFeaturesAsString() const {
  return ToString(enabled_language_features_);
}

std::string LanguageOptions::ToString(
    const std::set<LanguageFeature>& features) {
  std::set<std::string> strings;
  for (LanguageFeature feature : features) {
    strings.insert(LanguageFeature_Name(feature));
  }
  return absl::StrJoin(strings.begin(), strings.end(), ", ");
}

LanguageOptions::LanguageOptions(const LanguageOptionsProto& proto)
    : name_resolution_mode_(proto.name_resolution_mode()),
      product_mode_(proto.product_mode()),
      error_on_deprecated_syntax_(proto.error_on_deprecated_syntax()) {
  supported_statement_kinds_.clear();
  for (int i = 0; i <  proto.supported_statement_kinds_size(); ++i) {
    supported_statement_kinds_.insert(proto.supported_statement_kinds(i));
  }
  if (proto.enabled_language_features_size() > 0) {
    enabled_language_features_.clear();
    for (int i = 0; i <  proto.enabled_language_features_size(); ++i) {
      enabled_language_features_.insert(proto.enabled_language_features(i));
    }
  }
}

void LanguageOptions::Serialize(LanguageOptionsProto* proto) const {
  proto->set_name_resolution_mode(name_resolution_mode_);
  proto->set_product_mode(product_mode_);
  proto->set_error_on_deprecated_syntax(error_on_deprecated_syntax_);

  for (ResolvedNodeKind kind : supported_statement_kinds_) {
    proto->add_supported_statement_kinds(kind);
  }
  for (LanguageFeature feature : enabled_language_features_) {
    proto->add_enabled_language_features(feature);
  }
}

void LanguageOptions::EnableMaximumLanguageFeatures(bool for_development) {
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<LanguageFeature>();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor = descriptor->value(i);
    const LanguageFeature feature =
        static_cast<LanguageFeature>(value_descriptor->number());
    const LanguageFeatureOptions& options =
        value_descriptor->options().GetExtension(language_feature_options);
    const bool enabled = options.ideally_enabled() &&
                         (for_development || !options.in_development());
    if (feature != __LanguageFeature__switch_must_have_a_default__ && enabled) {
      EnableLanguageFeature(feature);
    }
  }
}

}  // namespace zetasql
