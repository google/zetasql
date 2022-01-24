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

#include "zetasql/public/analyzer_options.h"

#include <algorithm>
#include <string>
#include <utility>

#include "zetasql/base/case.h"

namespace zetasql {

bool StringVectorCaseLess::operator()(
    const std::vector<std::string>& v1,
    const std::vector<std::string>& v2) const {
  auto common_length = std::min(v1.size(), v2.size());
  for (int idx = 0; idx < common_length; ++idx) {
    const int cmp = zetasql_base::CaseCompare(v1[idx], v2[idx]);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    // Otherwise the strings compared equal, so compare the next array
    // elements.
  }
  // The first <common_length> elements are equal, so if the array length
  // of v1 < v2 then v1 is less.
  return (v1.size() < v2.size());
}

void AllowedHintsAndOptions::AddOption(const std::string& name,
                                       const Type* type) {
  ZETASQL_CHECK_OK(AddOptionImpl(name, type));
}

absl::Status AllowedHintsAndOptions::AddOptionImpl(const std::string& name,
                                                   const Type* type) {
  if (name.empty()) {
    return MakeSqlError() << "Option name should not be empty.";
  }
  if (!zetasql_base::InsertIfNotPresent(&options_lower, absl::AsciiStrToLower(name),
                               type)) {
    return MakeSqlError() << "Duplicate option: " << name;
  }
  return absl::OkStatus();
}

void AllowedHintsAndOptions::AddHint(const std::string& qualifier,
                                     const std::string& name, const Type* type,
                                     bool allow_unqualified) {
  ZETASQL_CHECK_OK(AddHintImpl(qualifier, name, type, allow_unqualified));
}

absl::Status AllowedHintsAndOptions::AddHintImpl(const std::string& qualifier,
                                                 const std::string& name,
                                                 const Type* type,
                                                 bool allow_unqualified) {
  if (name.empty()) {
    return MakeSqlError() << "Hint name should not be empty";
  }
  if (qualifier.empty() && !allow_unqualified) {
    return MakeSqlError()
           << "Cannot have hint with no qualifier and !allow_unqualified";
  }
  if (!zetasql_base::InsertIfNotPresent(&hints_lower,
                               std::make_pair(absl::AsciiStrToLower(qualifier),
                                              absl::AsciiStrToLower(name)),
                               type)) {
    return MakeSqlError() << "Duplicate hint: " << name
                          << ", with qualifier: " << qualifier;
  }
  if (allow_unqualified && !qualifier.empty()) {
    if (!zetasql_base::InsertIfNotPresent(
            &hints_lower, std::make_pair("", absl::AsciiStrToLower(name)),
            type)) {
      return MakeSqlError()
             << "Duplicate hint: " << name << ", with no qualifier";
    }
  }
  return absl::OkStatus();
}

absl::Status AllowedHintsAndOptions::Deserialize(
    const AllowedHintsAndOptionsProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, AllowedHintsAndOptions* result) {
  *result = AllowedHintsAndOptions();
  for (const auto& qualifier : proto.disallow_unknown_hints_with_qualifier()) {
    if (!zetasql_base::InsertIfNotPresent(
            &(result->disallow_unknown_hints_with_qualifiers), qualifier)) {
      return ::zetasql_base::UnknownErrorBuilder()
             << "Duplicate qualifier:" << qualifier;
    }
  }
  result->disallow_unknown_options = proto.disallow_unknown_options();
  for (const auto& hint : proto.hint()) {
    if (hint.has_type()) {
      const Type* type;
      ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
          hint.type(), pools, &type));
      ZETASQL_RETURN_IF_ERROR(result->AddHintImpl(hint.qualifier(), hint.name(), type,
                                          hint.allow_unqualified()));
    } else {
      ZETASQL_RETURN_IF_ERROR(result->AddHintImpl(hint.qualifier(), hint.name(),
                                          nullptr, hint.allow_unqualified()));
    }
  }
  for (const auto& option : proto.option()) {
    if (option.has_type()) {
      const Type* type;
      ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
          option.type(), pools, &type));
      ZETASQL_RETURN_IF_ERROR(result->AddOptionImpl(option.name(), type));
    } else {
      ZETASQL_RETURN_IF_ERROR(result->AddOptionImpl(option.name(), nullptr));
    }
  }
  return absl::OkStatus();
}

absl::Status AllowedHintsAndOptions::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    AllowedHintsAndOptionsProto* proto) const {
  proto->set_disallow_unknown_options(disallow_unknown_options);
  for (const auto& qualifier : disallow_unknown_hints_with_qualifiers) {
    proto->add_disallow_unknown_hints_with_qualifier(qualifier);
  }

  // Add hints to proto.
  //
  // For hints, the key is (qualifier, hint).  Unqualified hints are declared
  // using an empty qualifier. The same hint is typically added twice, once
  // qualified and once unqualified.
  //
  // To make the proto a minimum one, the unqualified ones which have the
  // same name and type with a qualified one should be omitted
  // by setting <allow_unqualified> field of the qualified one
  // as true in HintProto.
  //
  // To do this, we use <serialized_unqualified_hints_lower> to store those
  // unqualified hints. The detailed logic is as below:
  //
  // for each qualified hint:
  //   if
  //      it has an unqualified version and the unqualified hint has not been
  //      added into <serialized_unqualified_hints_lower>:
  //   then
  //     add the qualified one to <serialized_unqualified_hints_lower>
  //     set <allow_unqualified> true in proto
  //
  //  for each unqualified hint:
  //    if
  //      it is not in <serialized_unqualified_hints_lower>
  //    then
  //      serialize it into proto
  absl::flat_hash_set<std::string> serialized_unqualified_hints_lower;
  for (const auto& hint : hints_lower) {
    if (!hint.first.first.empty()) {
      auto* hint_proto = proto->add_hint();
      hint_proto->set_qualifier(hint.first.first);
      hint_proto->set_name(hint.first.second);
      const std::string name_lower = absl::AsciiStrToLower(hint.first.second);
      const std::pair<std::string, std::string> unqualified_key =
          std::make_pair("", name_lower);
      if (hints_lower.contains(unqualified_key) &&
          !serialized_unqualified_hints_lower.contains(name_lower)) {
        const auto unqualified_hint = hints_lower.find(unqualified_key);
        if ((unqualified_hint->second == nullptr && hint.second == nullptr) ||
            (unqualified_hint->second != nullptr && hint.second != nullptr &&
             unqualified_hint->second->Equals(hint.second))) {
          hint_proto->set_allow_unqualified(true);
          serialized_unqualified_hints_lower.insert(name_lower);
        }
      }
      if (hint.second != nullptr) {
        ZETASQL_RETURN_IF_ERROR(hint.second->SerializeToProtoAndDistinctFileDescriptors(
            hint_proto->mutable_type(), file_descriptor_set_map));
      }
    }
  }
  for (const auto& hint : hints_lower) {
    if (hint.first.first.empty() &&
        !serialized_unqualified_hints_lower.contains(
            absl::AsciiStrToLower(hint.first.second))) {
      auto* hint_proto = proto->add_hint();
      hint_proto->set_name(hint.first.second);
      hint_proto->set_allow_unqualified(true);
      if (hint.second != nullptr) {
        ZETASQL_RETURN_IF_ERROR(hint.second->SerializeToProtoAndDistinctFileDescriptors(
            hint_proto->mutable_type(), file_descriptor_set_map));
      }
    }
  }

  for (const auto& option : options_lower) {
    auto* option_proto = proto->add_option();
    option_proto->set_name(option.first);
    if (option.second != nullptr) {
      ZETASQL_RETURN_IF_ERROR(option.second->SerializeToProtoAndDistinctFileDescriptors(
          option_proto->mutable_type(), file_descriptor_set_map));
    }
  }
  return absl::OkStatus();
}

AnalyzerOptions::AnalyzerOptions() : AnalyzerOptions(LanguageOptions()) {}

AnalyzerOptions::AnalyzerOptions(const LanguageOptions& language_options)
    : language_options_(language_options) {
  ZETASQL_CHECK(absl::LoadTimeZone("America/Los_Angeles", &default_timezone_));
}

AnalyzerOptions::~AnalyzerOptions() {}

void AnalyzerOptions::CreateDefaultArenasIfNotSet() {
  if (arena_ == nullptr) {
    arena_ = std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/4096);
  }
  if (id_string_pool_ == nullptr) {
    id_string_pool_ = std::make_shared<IdStringPool>(arena_);
  }
}

absl::Status AnalyzerOptions::Deserialize(
    const AnalyzerOptionsProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, AnalyzerOptions* result) {
  *result = AnalyzerOptions();
  result->set_language(LanguageOptions(proto.language_options()));

  for (const auto& param : proto.query_parameters()) {
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        param.type(), pools, &type));
    ZETASQL_RETURN_IF_ERROR(result->AddQueryParameter(param.name(), type));
  }

  for (const auto& system_variable_proto : proto.system_variables()) {
    std::vector<std::string> name_path;
    for (const auto& path_part : system_variable_proto.name_path()) {
      name_path.push_back(path_part);
    }

    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        system_variable_proto.type(), pools, &type));

    ZETASQL_RETURN_IF_ERROR(result->AddSystemVariable(name_path, type));
  }

  for (const TypeProto& param_type : proto.positional_query_parameters()) {
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        param_type, pools, &type));
    ZETASQL_RETURN_IF_ERROR(result->AddPositionalQueryParameter(type));
  }

  for (const auto& column : proto.expression_columns()) {
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        column.type(), pools, &type));
    ZETASQL_RETURN_IF_ERROR(result->AddExpressionColumn(column.name(), type));
  }

  if (proto.has_in_scope_expression_column()) {
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        proto.in_scope_expression_column().type(), pools, &type));
    ZETASQL_RETURN_IF_ERROR(result->SetInScopeExpressionColumn(
        proto.in_scope_expression_column().name(), type));
  }

  std::vector<std::pair<std::string, const Type*>> ddl_pseudo_columns;
  for (const auto& column : proto.ddl_pseudo_columns()) {
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        column.type(), pools, &type));
    ddl_pseudo_columns.push_back(std::make_pair(column.name(), type));
  }
  result->SetDdlPseudoColumns(ddl_pseudo_columns);

  if (proto.has_default_timezone() &&
      !absl::LoadTimeZone(proto.default_timezone(),
                          &result->default_timezone_)) {
    return MakeSqlError() << "Timezone string not parseable: "
                          << proto.default_timezone();
  }

  std::vector<const Type*> expected_types;
  for (const TypeProto& type_proto : proto.target_column_types()) {
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        type_proto, pools, &type));
    expected_types.push_back(type);
  }
  result->set_target_column_types(expected_types);

  result->set_statement_context(proto.statement_context());
  result->set_error_message_mode(proto.error_message_mode());
  result->set_create_new_column_for_each_projected_output(
      proto.create_new_column_for_each_projected_output());
  result->set_prune_unused_columns(proto.prune_unused_columns());
  result->set_allow_undeclared_parameters(proto.allow_undeclared_parameters());
  result->set_parameter_mode(proto.parameter_mode());
  result->set_preserve_column_aliases(proto.preserve_column_aliases());

  if (proto.has_allowed_hints_and_options()) {
    AllowedHintsAndOptions hints_and_options("");
    ZETASQL_RETURN_IF_ERROR(AllowedHintsAndOptions::Deserialize(
        proto.allowed_hints_and_options(), pools, factory, &hints_and_options));
    result->set_allowed_hints_and_options(hints_and_options);
  }

  result->enabled_rewrites_.clear();
  for (int rewrite : proto.enabled_rewrites()) {
    result->enabled_rewrites_.insert(static_cast<ResolvedASTRewrite>(rewrite));
  }

  if (proto.has_parse_location_record_type()) {
    result->parse_location_record_type_ = proto.parse_location_record_type();
  }

  return absl::OkStatus();
}

absl::Status AnalyzerOptions::Serialize(FileDescriptorSetMap* map,
                                        AnalyzerOptionsProto* proto) const {
  language_options_.Serialize(proto->mutable_language_options());

  for (const auto& param : query_parameters_) {
    auto* param_proto = proto->add_query_parameters();
    param_proto->set_name(param.first);
    ZETASQL_RETURN_IF_ERROR(param.second->SerializeToProtoAndDistinctFileDescriptors(
        param_proto->mutable_type(), map));
  }

  for (const auto& system_variable : system_variables_) {
    auto* system_variable_proto = proto->add_system_variables();
    for (const std::string& path_part : system_variable.first) {
      system_variable_proto->add_name_path(path_part);
    }
    ZETASQL_RETURN_IF_ERROR(
        system_variable.second->SerializeToProtoAndDistinctFileDescriptors(
            system_variable_proto->mutable_type(), map));
  }

  for (const Type* param_type : positional_query_parameters_) {
    ZETASQL_RETURN_IF_ERROR(param_type->SerializeToProtoAndDistinctFileDescriptors(
        proto->add_positional_query_parameters(), map));
  }

  for (const auto& column : expression_columns_) {
    auto* column_proto = proto->add_expression_columns();
    column_proto->set_name(column.first);
    ZETASQL_RETURN_IF_ERROR(column.second->SerializeToProtoAndDistinctFileDescriptors(
        column_proto->mutable_type(), map));
  }

  if (!in_scope_expression_column_.first.empty()) {
    auto* in_scope_expression = proto->mutable_in_scope_expression_column();
    in_scope_expression->set_name(in_scope_expression_column_.first);
    const Type* type = in_scope_expression_column_.second;
    ZETASQL_RETURN_IF_ERROR(type->SerializeToProtoAndDistinctFileDescriptors(
        in_scope_expression->mutable_type(), map));
  }

  for (const auto& ddl_pseudo_column : ddl_pseudo_columns_) {
    auto* column_proto = proto->add_ddl_pseudo_columns();
    column_proto->set_name(ddl_pseudo_column.first);
    ZETASQL_RETURN_IF_ERROR(
        ddl_pseudo_column.second->SerializeToProtoAndDistinctFileDescriptors(
            column_proto->mutable_type(), map));
  }

  proto->set_default_timezone(default_timezone_.name());
  proto->set_statement_context(statement_context_);
  proto->set_error_message_mode(error_message_mode_);
  proto->set_create_new_column_for_each_projected_output(
      create_new_column_for_each_projected_output_);
  proto->set_prune_unused_columns(prune_unused_columns_);
  proto->set_allow_undeclared_parameters(allow_undeclared_parameters_);
  proto->set_parameter_mode(parameter_mode_);
  proto->set_preserve_column_aliases(preserve_column_aliases_);

  ZETASQL_RETURN_IF_ERROR(allowed_hints_and_options_.Serialize(
      map, proto->mutable_allowed_hints_and_options()));

  if (parse_location_record_type_ != PARSE_LOCATION_RECORD_NONE) {
    proto->set_parse_location_record_type(parse_location_record_type_);
  }

  for (const Type* type : target_column_types_) {
    ZETASQL_RETURN_IF_ERROR(type->SerializeToProtoAndDistinctFileDescriptors(
        proto->add_target_column_types(), map));
  }

  for (ResolvedASTRewrite rewrite : enabled_rewrites_) {
    proto->add_enabled_rewrites(rewrite);
  }

  return absl::OkStatus();
}

absl::Status AnalyzerOptions::AddSystemVariable(
    const std::vector<std::string>& name_path, const Type* type) {
  if (type == nullptr) {
    return MakeSqlError()
           << "Type associated with system variable cannot be NULL";
  }
  if (name_path.empty()) {
    return MakeSqlError() << "System variable cannot have empty name path";
  }
  for (const std::string& name_path_part : name_path) {
    if (name_path_part.empty()) {
      return MakeSqlError()
             << "System variable cannot have empty string as path part";
    }
  }

  if (!type->IsSupportedType(language())) {
    return MakeSqlError() << "System variable " << absl::StrJoin(name_path, ".")
                          << " has unsupported type: "
                          << type->TypeName(language().product_mode());
  }

  if (!zetasql_base::InsertIfNotPresent(&system_variables_,
                               std::make_pair(name_path, type))) {
    return MakeSqlError() << "Duplicate system variable "
                          << absl::StrJoin(name_path, ".");
  }

  return absl::OkStatus();
}

absl::Status AnalyzerOptions::AddQueryParameter(const std::string& name,
                                                const Type* type) {
  if (type == nullptr) {
    return MakeSqlError()
           << "Type associated with query parameter cannot be NULL";
  }
  if (name.empty()) {
    return MakeSqlError() << "Query parameter cannot have empty name";
  }

  if (!type->IsSupportedType(language())) {
    return MakeSqlError() << "Parameter " << name << " has unsupported type: "
                          << type->TypeName(language().product_mode());
  }

  if (!zetasql_base::InsertIfNotPresent(
          &query_parameters_,
          std::make_pair(absl::AsciiStrToLower(name), type))) {
    return MakeSqlError() << "Duplicate parameter name "
                          << absl::AsciiStrToLower(name);
  }

  return absl::OkStatus();
}

absl::Status AnalyzerOptions::AddPositionalQueryParameter(const Type* type) {
  if (type == nullptr) {
    return MakeSqlError()
           << "Type associated with query parameter cannot be NULL";
  }

  if (allow_undeclared_parameters_) {
    return MakeSqlError()
           << "Positional query parameters cannot be provided when "
              "undeclared parameters are allowed";
  }

  if (!type->IsSupportedType(language())) {
    return MakeSqlError() << "Parameter at position "
                          << positional_query_parameters_.size()
                          << " has unsupported type: "
                          << type->TypeName(language().product_mode());
  }

  positional_query_parameters_.push_back(type);

  return absl::OkStatus();
}

absl::Status AnalyzerOptions::AddExpressionColumn(const std::string& name,
                                                  const Type* type) {
  if (type == nullptr) {
    return MakeSqlError()
           << "Type associated with expression column cannot be NULL";
  }
  if (name.empty()) {
    return MakeSqlError() << "Expression column cannot have empty name";
  }

  if (!type->IsSupportedType(language())) {
    return MakeSqlError() << "Parameter " << name << " has unsupported type: "
                          << type->TypeName(language().product_mode());
  }

  if (!zetasql_base::InsertIfNotPresent(
          &expression_columns_,
          std::make_pair(absl::AsciiStrToLower(name), type))) {
    return MakeSqlError() << "Duplicate expression column name "
                          << absl::AsciiStrToLower(name);
  }

  return absl::OkStatus();
}

absl::Status AnalyzerOptions::SetInScopeExpressionColumn(
    const std::string& name, const Type* type) {
  if (type == nullptr) {
    return MakeSqlError()
           << "Type associated with in-scope expression column cannot be NULL";
  }
  if (has_in_scope_expression_column()) {
    return MakeSqlError() << "Cannot call SetInScopeExpressionColumn twice";
  }

  if (!type->IsSupportedType(language())) {
    return MakeSqlError() << "Parameter " << name << " has unsupported type: "
                          << type->TypeName(language().product_mode());
  }

  const std::pair<std::string, const Type*> name_and_type(
      absl::AsciiStrToLower(name), type);
  if (!zetasql_base::InsertIfNotPresent(&expression_columns_, name_and_type)) {
    return MakeSqlError() << "Duplicate expression column name "
                          << absl::AsciiStrToLower(name);
  }
  in_scope_expression_column_ = name_and_type;

  return absl::OkStatus();
}

void AnalyzerOptions::SetDdlPseudoColumnsCallback(
    DdlPseudoColumnsCallback ddl_pseudo_columns_callback) {
  ddl_pseudo_columns_callback_ = std::move(ddl_pseudo_columns_callback);
  ddl_pseudo_columns_.clear();
}
void AnalyzerOptions::SetDdlPseudoColumns(
    const std::vector<std::pair<std::string, const Type*>>&
        ddl_pseudo_columns) {
  ddl_pseudo_columns_ = ddl_pseudo_columns;
  // We explicitly make the lambda capture a copy of ddl_pseudo_columns to be
  // safe. If we capture by reference instead ("this" or "&"), then when a copy
  // of AnalyzerOptions is made, the copied lambda would still point to
  // references of the old AnalyzerOptions that may not exist anymore leading to
  // memory errors.
  ddl_pseudo_columns_callback_ =
      [ddl_pseudo_columns](
          const std::vector<std::string>& table_name,
          const std::vector<const ResolvedOption*>& options,
          std::vector<std::pair<std::string, const Type*>>* pseudo_columns) {
        *pseudo_columns = ddl_pseudo_columns;
        return absl::OkStatus();
      };
}

ParserOptions AnalyzerOptions::GetParserOptions() const {
  return ParserOptions(id_string_pool(), arena(), &language_options_);
}

void AnalyzerOptions::enable_rewrite(ResolvedASTRewrite rewrite, bool enable) {
  if (enable) {
    enabled_rewrites_.insert(rewrite);
  } else {
    enabled_rewrites_.erase(rewrite);
  }
}

absl::btree_set<ResolvedASTRewrite> AnalyzerOptions::DefaultRewrites() {
  absl::btree_set<ResolvedASTRewrite> default_rewrites;
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<ResolvedASTRewrite>();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor = descriptor->value(i);
    const ResolvedASTRewrite rewrite =
        static_cast<ResolvedASTRewrite>(value_descriptor->number());
    if (value_descriptor->options()
            .GetExtension(rewrite_options)
            .default_enabled()) {
      default_rewrites.insert(rewrite);
    }
  }
  return default_rewrites;
}

const AnalyzerOptions& GetOptionsWithArenas(
    const AnalyzerOptions* options, std::unique_ptr<AnalyzerOptions>* copy) {
  if (options->AllArenasAreInitialized()) {
    return *options;
  }
  *copy = absl::make_unique<AnalyzerOptions>(*options);
  (*copy)->CreateDefaultArenasIfNotSet();
  return **copy;
}

absl::Status ValidateAnalyzerOptions(const AnalyzerOptions& options) {
  switch (options.parameter_mode()) {
    case PARAMETER_NAMED:
      ZETASQL_RET_CHECK(options.positional_query_parameters().empty())
          << "Positional parameters cannot be provided in named parameter "
             "mode";
      break;
    case PARAMETER_POSITIONAL:
      ZETASQL_RET_CHECK(options.query_parameters().empty())
          << "Named parameters cannot be provided in positional parameter "
             "mode";
      // AddPositionalQueryParameter guards against the case where
      // allow_undeclared_parameters is true and a positional parameter is
      // added, but not the reverse order.
      ZETASQL_RET_CHECK(!options.allow_undeclared_parameters() ||
                options.positional_query_parameters().empty())
          << "When undeclared parameters are allowed, no positional query "
             "parameters can be provided";
      break;
    case PARAMETER_NONE:
      ZETASQL_RET_CHECK(options.query_parameters().empty() &&
                options.positional_query_parameters().empty())
          << "Parameters are disabled and cannot be provided";
      break;
  }

  return absl::OkStatus();
}

}  // namespace zetasql
