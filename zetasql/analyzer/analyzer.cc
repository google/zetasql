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

#include "zetasql/public/analyzer.h"

#include <iostream>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/function_resolver.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/table_name_resolver.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(bool, zetasql_validate_resolved_ast, true,
          "Run validator on resolved AST before returning it.");

// This provides a way to extract and look at the zetasql resolved AST
// from within some other test or tool.  It prints to cout rather than logging
// because the output is often too big to log without truncating.
ABSL_FLAG(bool, zetasql_print_resolved_ast, false,
          "Print resolved AST to stdout after resolving (for debugging)");

namespace zetasql {

bool StringVectorCaseLess::operator()(
    const std::vector<std::string>& v1,
    const std::vector<std::string>& v2) const {
  auto common_length = std::min(v1.size(), v2.size());
  for (int idx = 0; idx < common_length; ++idx) {
    const int cmp = zetasql_base::StringCaseCompare(v1[idx], v2[idx]);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    // Otherwise the strings compared equal, so compare the next array
    // elements.
  }
  // The first <common_length> elements are equal, so if the array length
  // of v1 < v2 then v1 is less.
  return (v1.size() < v2.size());
}

namespace {

// Verifies that the provided AnalyzerOptions have a valid combination of
// settings.
zetasql_base::Status ValidateAnalyzerOptions(const AnalyzerOptions& options) {
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

  return ::zetasql_base::OkStatus();
}

// Sets <has_default_resolution_time> to true for every table name in
// 'table_names' if it does not have "FOR SYSTEM_TIME AS OF" expression.
void EnsureResolutionTimeInfoForEveryTable(
    const TableNamesSet& table_names,
    TableResolutionTimeInfoMap* table_resolution_time_info_map) {
  for (const auto& table_name : table_names) {
    TableResolutionTimeInfo& expressions =
        (*table_resolution_time_info_map)[table_name];
    if (expressions.exprs.empty()) {
      expressions.has_default_resolution_time = true;
    }
  }
}

// Returns <options> if it already has all arenas initialized, or otherwise
// populates <copy> as a copy for <options>, creates arenas in <copy> and
// returns it. This avoids unnecessary duplication of AnalyzerOptions, which
// might be expensive.
const AnalyzerOptions& GetOptionsWithArenas(
    const AnalyzerOptions* options, std::unique_ptr<AnalyzerOptions>* copy) {
  if (options->AreAllArenasInitialized()) {
    return *options;
  }
  *copy = absl::make_unique<AnalyzerOptions>(*options);
  (*copy)->CreateDefaultArenasIfNotSet();
  return **copy;
}

}  // namespace

void AllowedHintsAndOptions::AddOption(const std::string& name,
                                       const Type* type) {
  ZETASQL_CHECK_OK(AddOptionImpl(name, type));
}

zetasql_base::Status AllowedHintsAndOptions::AddOptionImpl(const std::string& name,
                                                   const Type* type) {
  if (name.empty()) {
    return MakeSqlError() << "Option name should not be empty.";
  }
  if (!zetasql_base::InsertIfNotPresent(&options_lower, absl::AsciiStrToLower(name),
                               type)) {
    return MakeSqlError() << "Duplicate option: " << name;
  }
  return ::zetasql_base::OkStatus();
}

void AllowedHintsAndOptions::AddHint(const std::string& qualifier,
                                     const std::string& name, const Type* type,
                                     bool allow_unqualified) {
  ZETASQL_CHECK_OK(AddHintImpl(qualifier, name, type, allow_unqualified));
}

zetasql_base::Status AllowedHintsAndOptions::AddHintImpl(const std::string& qualifier,
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
    return MakeSqlError()
           << "Duplicate hint: " << name << ", with qualifier: " << qualifier;
  }
  if (allow_unqualified && !qualifier.empty()) {
    if (!zetasql_base::InsertIfNotPresent(
            &hints_lower, std::make_pair("", absl::AsciiStrToLower(name)),
            type)) {
      return MakeSqlError()
             << "Duplicate hint: " << name << ", with no qualifier";
    }
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AllowedHintsAndOptions::Deserialize(
    const AllowedHintsAndOptionsProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory,
    AllowedHintsAndOptions* result) {
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
      ZETASQL_RETURN_IF_ERROR(result->AddHintImpl(
          hint.qualifier(), hint.name(), type, hint.allow_unqualified()));
    } else {
      ZETASQL_RETURN_IF_ERROR(result->AddHintImpl(
          hint.qualifier(), hint.name(), nullptr, hint.allow_unqualified()));
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
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AllowedHintsAndOptions::Serialize(
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
      if (zetasql_base::ContainsKey(hints_lower, unqualified_key) &&
          !zetasql_base::ContainsKey(serialized_unqualified_hints_lower, name_lower)) {
        const auto unqualified_hint = hints_lower.find(unqualified_key);
        if ((unqualified_hint->second == nullptr && hint.second == nullptr)||
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
        !zetasql_base::ContainsKey(serialized_unqualified_hints_lower,
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
  return ::zetasql_base::OkStatus();
}

AnalyzerOptions::AnalyzerOptions()
    : AnalyzerOptions(LanguageOptions()) {}

AnalyzerOptions::AnalyzerOptions(const LanguageOptions& language_options)
    : language_options_(language_options) {
  CHECK(LoadTimeZone("America/Los_Angeles", &default_timezone_));
}

AnalyzerOptions::~AnalyzerOptions() {
}

void AnalyzerOptions::CreateDefaultArenasIfNotSet() {
  if (arena_ == nullptr) {
    arena_ = std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/4096);
  }
  if (id_string_pool_ == nullptr) {
    id_string_pool_ = std::make_shared<IdStringPool>(arena_);
  }
}

zetasql_base::Status AnalyzerOptions::Deserialize(
    const AnalyzerOptionsProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory,
    AnalyzerOptions* result) {
  *result = AnalyzerOptions();
  result->set_language_options(LanguageOptions(proto.language_options()));

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
      !LoadTimeZone(proto.default_timezone(), &result->default_timezone_)) {
    return MakeSqlError() << "Timezone std::string not parseable: "
                          << proto.default_timezone();
  }

  result->set_statement_context(proto.statement_context());
  result->set_error_message_mode(proto.error_message_mode());
  result->set_record_parse_locations(proto.record_parse_locations());
  result->set_prune_unused_columns(proto.prune_unused_columns());
  result->set_allow_undeclared_parameters(proto.allow_undeclared_parameters());
  result->set_parameter_mode(proto.parameter_mode());
  result->set_strict_validation_on_column_replacements(
      proto.strict_validation_on_column_replacements());
  result->set_preserve_column_aliases(proto.preserve_column_aliases());

  if (proto.has_allowed_hints_and_options()) {
    AllowedHintsAndOptions hints_and_options("");
    ZETASQL_RETURN_IF_ERROR(AllowedHintsAndOptions::Deserialize(
        proto.allowed_hints_and_options(), pools, factory, &hints_and_options));
    result->set_allowed_hints_and_options(hints_and_options);
  }
  return ::zetasql_base::OkStatus();
}


zetasql_base::Status AnalyzerOptions::Serialize(
    FileDescriptorSetMap* map, AnalyzerOptionsProto* proto) const {
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
  proto->set_record_parse_locations(record_parse_locations_);
  proto->set_prune_unused_columns(prune_unused_columns_);
  proto->set_allow_undeclared_parameters(allow_undeclared_parameters_);
  proto->set_parameter_mode(parameter_mode_);
  proto->set_strict_validation_on_column_replacements(
      strict_validation_on_column_replacements_);
  proto->set_preserve_column_aliases(preserve_column_aliases_);

  ZETASQL_RETURN_IF_ERROR(allowed_hints_and_options_.Serialize(
      map, proto->mutable_allowed_hints_and_options()));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AnalyzerOptions::AddSystemVariable(
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
             << "System variable cannot have empty std::string as path part";
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

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AnalyzerOptions::AddQueryParameter(const std::string& name,
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

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AnalyzerOptions::AddPositionalQueryParameter(const Type* type) {
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

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AnalyzerOptions::AddExpressionColumn(const std::string& name,
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

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AnalyzerOptions::SetInScopeExpressionColumn(
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

  return ::zetasql_base::OkStatus();
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
  ddl_pseudo_columns_callback_ =
      [this](const std::vector<std::string>& table_name,
             const std::vector<const ResolvedOption*>& options,
             std::vector<std::pair<std::string, const Type*>>* pseudo_columns) {
        *pseudo_columns = ddl_pseudo_columns_;
        return zetasql_base::OkStatus();
      };
}

ParserOptions AnalyzerOptions::GetParserOptions() const {
  return ParserOptions(id_string_pool(), arena());
}

AnalyzerOutput::AnalyzerOutput(
    std::shared_ptr<IdStringPool> id_string_pool,
    std::shared_ptr<zetasql_base::UnsafeArena> arena,
    std::unique_ptr<const ResolvedStatement> statement,
    const AnalyzerOutputProperties& analyzer_output_properties,
    std::unique_ptr<ParserOutput> parser_output,
    const std::vector<zetasql_base::Status>& deprecation_warnings,
    const QueryParametersMap& undeclared_parameters,
    const std::vector<const Type*>& undeclared_positional_parameters)
    : id_string_pool_(std::move(id_string_pool)),
      arena_(std::move(arena)),
      statement_(std::move(statement)),
      analyzer_output_properties_(analyzer_output_properties),
      parser_output_(std::move(parser_output)),
      deprecation_warnings_(deprecation_warnings),
      undeclared_parameters_(undeclared_parameters),
      undeclared_positional_parameters_(undeclared_positional_parameters) {}

AnalyzerOutput::AnalyzerOutput(
    std::shared_ptr<IdStringPool> id_string_pool,
    std::shared_ptr<zetasql_base::UnsafeArena> arena,
    std::unique_ptr<const ResolvedExpr> expr,
    const AnalyzerOutputProperties& analyzer_output_properties,
    std::unique_ptr<ParserOutput> parser_output,
    const std::vector<zetasql_base::Status>& deprecation_warnings,
    const QueryParametersMap& undeclared_parameters,
    const std::vector<const Type*>& undeclared_positional_parameters)
    : id_string_pool_(std::move(id_string_pool)),
      arena_(std::move(arena)),
      expr_(std::move(expr)),
      analyzer_output_properties_(analyzer_output_properties),
      parser_output_(std::move(parser_output)),
      deprecation_warnings_(deprecation_warnings),
      undeclared_parameters_(undeclared_parameters),
      undeclared_positional_parameters_(undeclared_positional_parameters) {}

AnalyzerOutput::~AnalyzerOutput() {
}

// Common post-parsing work for AnalyzeStatement() series.
static zetasql_base::Status FinishAnalyzeStatementImpl(
    absl::string_view sql, const ParserOutput& parser_output,
    Resolver* resolver, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory,
    std::unique_ptr<const ResolvedStatement>* resolved_statement) {
  VLOG(5) << "Parsed AST:\n" << parser_output.statement()->DebugString();

  ZETASQL_RETURN_IF_ERROR(resolver->ResolveStatement(sql, parser_output.statement(),
                                             resolved_statement));

  VLOG(3) << "Resolved AST:\n" << (*resolved_statement)->DebugString();

  if (absl::GetFlag(FLAGS_zetasql_validate_resolved_ast)) {
    Validator validator(options.language_options());
    ZETASQL_RETURN_IF_ERROR(
        validator.ValidateResolvedStatement(resolved_statement->get()));
  }

  if (absl::GetFlag(FLAGS_zetasql_print_resolved_ast)) {
    std::cout << "Resolved AST from thread "
              << std::this_thread::get_id()
              << ":" << std::endl
              << (*resolved_statement)->DebugString() << std::endl;
  }

  if (options.language().error_on_deprecated_syntax() &&
      !resolver->deprecation_warnings().empty()) {
    return resolver->deprecation_warnings().front();
  }

  // Make sure we're starting from a clean state for CheckFieldsAccessed.
  (*resolved_statement)->ClearFieldsAccessed();

  return zetasql_base::OkStatus();
}

static zetasql_base::Status UnsupportedStatementErrorOrStatus(
    const zetasql_base::Status& status, const ParseResumeLocation& resume_location,
    const AnalyzerOptions& options) {
  ZETASQL_RET_CHECK(!status.ok()) << "Expected an error status";
  const ResolvedNodeKind kind = GetNextStatementKind(resume_location);
  if (kind != RESOLVED_LITERAL
      && !options.language().SupportsStatementKind(kind)) {
    ParseLocationPoint location_point = ParseLocationPoint::FromByteOffset(
        resume_location.filename(), resume_location.byte_position());
    return MakeSqlErrorAtPoint(location_point) << "Statement not supported: "
        << ResolvedNodeKindToString(kind);
  }
  return status;
}

static zetasql_base::Status AnalyzeStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output) {
  output->reset();

  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  VLOG(1) << "Parsing statement:\n" << sql;
  std::unique_ptr<ParserOutput> parser_output;
  const zetasql_base::Status status = ParseStatement(
      sql, options.GetParserOptions(), &parser_output);
  if (!status.ok()) {
    return UnsupportedStatementErrorOrStatus(
        status, ParseResumeLocation::FromStringView(sql), options);
  }

  return AnalyzeStatementFromParserOutputOwnedOnSuccess(
      &parser_output, options, sql, catalog, type_factory, output);
}

zetasql_base::Status AnalyzeStatement(absl::string_view sql,
                              const AnalyzerOptions& options_in,
                              Catalog* catalog, TypeFactory* type_factory,
                              std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status =
      AnalyzeStatementImpl(sql, options, catalog, type_factory, output);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static zetasql_base::Status AnalyzeNextStatementImpl(
    ParseResumeLocation* resume_location,
    const AnalyzerOptions& options,
    Catalog* catalog,
    TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output,
    bool* at_end_of_input) {
  output->reset();

  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  if (resume_location->byte_position() == 0) {
    VLOG(1) << "Parsing first statement from:\n" << resume_location->input();
  } else {
    VLOG(2) << "Parsing next statement at position "
            << resume_location->byte_position();
  }

  std::unique_ptr<ParserOutput> parser_output;
  const zetasql_base::Status status = ParseNextStatement(
      resume_location, options.GetParserOptions(), &parser_output,
      at_end_of_input);
  if (!status.ok()) {
    return UnsupportedStatementErrorOrStatus(status, *resume_location, options);
  }
  ZETASQL_RET_CHECK(parser_output != nullptr);

  return AnalyzeStatementFromParserOutputOwnedOnSuccess(
      &parser_output, options, resume_location->input(), catalog,
      type_factory, output);
}

zetasql_base::Status AnalyzeNextStatement(
    ParseResumeLocation* resume_location,
    const AnalyzerOptions& options_in,
    Catalog* catalog,
    TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output,
    bool* at_end_of_input) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status =
      AnalyzeNextStatementImpl(resume_location, options, catalog,
                               type_factory, output, at_end_of_input);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), resume_location->input(), status);
}

static zetasql_base::Status AnalyzeStatementFromParserOutputImpl(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    bool take_ownership_on_success, const AnalyzerOptions& options,
    absl::string_view sql, Catalog* catalog, TypeFactory* type_factory,
    std::unique_ptr<const AnalyzerOutput>* output) {
  AnalyzerOptions local_options = options;

  // If the arena and IdStringPool are not set in <options>, use the
  // arena and IdStringPool from the parser output by default.
  if (local_options.arena() == nullptr) {
    ZETASQL_RET_CHECK((*statement_parser_output)->arena() != nullptr);
    local_options.set_arena((*statement_parser_output)->arena());
  }
  if (local_options.id_string_pool() == nullptr) {
    ZETASQL_RET_CHECK((*statement_parser_output)->id_string_pool() != nullptr);
    local_options.set_id_string_pool(
        (*statement_parser_output)->id_string_pool());
  }
  output->reset();

  std::unique_ptr<const ResolvedStatement> resolved_statement;
  Resolver resolver(catalog, type_factory, &local_options);
  const zetasql_base::Status status =
      FinishAnalyzeStatementImpl(
          sql, *(statement_parser_output->get()), &resolver, local_options,
          catalog, type_factory, &resolved_statement);
  if (!status.ok()) {
    return ConvertInternalErrorLocationAndAdjustErrorString(
        local_options.error_message_mode(), sql, status);
  }
  std::unique_ptr<ParserOutput> owned_parser_output(
      take_ownership_on_success ? statement_parser_output->release() : nullptr);
  *output = absl::make_unique<AnalyzerOutput>(
      local_options.id_string_pool(), local_options.arena(),
      std::move(resolved_statement),
      AnalyzerOutputProperties(),
      std::move(owned_parser_output),
      ConvertInternalErrorLocationsAndAdjustErrorStrings(
          local_options.error_message_mode(), sql,
          resolver.deprecation_warnings()),
      resolver.undeclared_parameters(),
      resolver.undeclared_positional_parameters());
  return zetasql_base::OkStatus();
}

zetasql_base::Status AnalyzeStatementFromParserOutputOwnedOnSuccess(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    const AnalyzerOptions& options, absl::string_view sql, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output) {
  return AnalyzeStatementFromParserOutputImpl(
      statement_parser_output, /*take_ownership_on_success=*/true, options,
      sql, catalog, type_factory, output);
}

zetasql_base::Status AnalyzeStatementFromParserOutputUnowned(
    std::unique_ptr<ParserOutput>* statement_parser_output,
    const AnalyzerOptions& options, absl::string_view sql, Catalog* catalog,
    TypeFactory* type_factory, std::unique_ptr<const AnalyzerOutput>* output) {
  return AnalyzeStatementFromParserOutputImpl(
      statement_parser_output, /*take_ownership_on_success=*/false, options,
      sql, catalog, type_factory, output);
}

// Coerces <resolved_expr> to <target_type>, using assignment semantics
// For details, see Coercer::AssignableTo() in
// .../public/coercer.h
//
// Upon success, a resolved tree that implements the conversion is stored in
// <resolved_expr>, replacing the tree that was previously there.
static zetasql_base::Status ConvertExprToTargetType(
    const ASTExpression& ast_expression, absl::string_view sql,
    const AnalyzerOptions& analyzer_options, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) {
  InputArgumentType expr_arg_type =
      GetInputArgumentTypeForExpr(resolved_expr->get());
  SignatureMatchResult sig_match_result;
  const LanguageOptions& language_options = analyzer_options.language_options();
  if (!Coercer(type_factory, analyzer_options.default_time_zone(),
               &language_options)
           .AssignableTo(expr_arg_type, target_type, /*is_explicit=*/false,
                         &sig_match_result)) {
    return ConvertInternalErrorLocationToExternal(
        MakeSqlErrorAt(&ast_expression) << absl::StrCat(
            "Expected type ",
            target_type->ShortTypeName(language_options.product_mode()),
            "; found ",
            resolved_expr->get()->type()->ShortTypeName(
                language_options.product_mode())),
        sql);
  }

  Resolver resolver(catalog, type_factory, &analyzer_options);

  ZETASQL_RETURN_IF_ERROR(FunctionResolver(catalog, type_factory, &resolver)
                      .AddCastOrConvertLiteral(&ast_expression, target_type,
                                               /*scan=*/nullptr,
                                               /*set_has_explicit_type=*/true,
                                               /*return_null_on_error=*/false,
                                               resolved_expr));
  return zetasql_base::OkStatus();
}

static zetasql_base::Status AnalyzeExpressionFromParserASTImpl(
    const ASTExpression& ast_expression,
    std::unique_ptr<ParserOutput> parser_output, absl::string_view sql,
    const AnalyzerOptions& options, Catalog* catalog, TypeFactory* type_factory,
    const Type* target_type, std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  Resolver resolver(catalog, type_factory, &options);
  ZETASQL_RETURN_IF_ERROR(resolver.ResolveStandaloneExpr(
      sql, &ast_expression, &resolved_expr));
  VLOG(3) << "Resolved AST:\n" << resolved_expr->DebugString();

  if (target_type != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ConvertExprToTargetType(ast_expression, sql, options,
                                            catalog, type_factory, target_type,
                                            &resolved_expr));
  }

  if (absl::GetFlag(FLAGS_zetasql_validate_resolved_ast)) {
    Validator validator(options.language_options());
    ZETASQL_RETURN_IF_ERROR(
        validator.ValidateStandaloneResolvedExpr(resolved_expr.get()));
  }

  if (absl::GetFlag(FLAGS_zetasql_print_resolved_ast)) {
    std::cout << "Resolved AST from thread "
              << std::this_thread::get_id()
              << ":" << std::endl
              << resolved_expr->DebugString() << std::endl;
  }

  if (options.language().error_on_deprecated_syntax() &&
      !resolver.deprecation_warnings().empty()) {
    return resolver.deprecation_warnings().front();
  }

  // Make sure we're starting from a clean state for CheckFieldsAccessed.
  resolved_expr->ClearFieldsAccessed();

  *output = absl::make_unique<AnalyzerOutput>(
      options.id_string_pool(), options.arena(), std::move(resolved_expr),
      AnalyzerOutputProperties(),
      std::move(parser_output),
      ConvertInternalErrorLocationsAndAdjustErrorStrings(
          options.error_message_mode(), sql, resolver.deprecation_warnings()),
      resolver.undeclared_parameters(),
      resolver.undeclared_positional_parameters());
  return zetasql_base::OkStatus();
}

static zetasql_base::Status AnalyzeExpressionImpl(
    absl::string_view sql, const AnalyzerOptions& options_in, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const AnalyzerOutput>* output) {
  output->reset();

  VLOG(1) << "Parsing expression:\n" << sql;
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  std::unique_ptr<ParserOutput> parser_output;
  ParserOptions parser_options = options.GetParserOptions();
  ZETASQL_RETURN_IF_ERROR(ParseExpression(sql, parser_options, &parser_output));
  const ASTExpression* expression = parser_output->expression();
  VLOG(5) << "Parsed AST:\n" << expression->DebugString();

  return AnalyzeExpressionFromParserASTImpl(
      *expression, std::move(parser_output), sql, options, catalog,
      type_factory, target_type, output);
}

zetasql_base::Status AnalyzeExpression(absl::string_view sql,
                               const AnalyzerOptions& options, Catalog* catalog,
                               TypeFactory* type_factory,
                               std::unique_ptr<const AnalyzerOutput>* output) {
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql,
      AnalyzeExpressionImpl(sql, options, catalog, type_factory, nullptr,
                            output));
}

zetasql_base::Status AnalyzeExpressionForAssignmentToType(
    absl::string_view sql, const AnalyzerOptions& options, Catalog* catalog,
    TypeFactory* type_factory, const Type* target_type,
    std::unique_ptr<const AnalyzerOutput>* output) {
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql,
      AnalyzeExpressionImpl(sql, options, catalog, type_factory, target_type,
                            output));
}

zetasql_base::Status AnalyzeExpressionFromParserAST(
    const ASTExpression& ast_expression, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    std::unique_ptr<const AnalyzerOutput>* output) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status = AnalyzeExpressionFromParserASTImpl(
      ast_expression, /* parser_output = */ nullptr, sql, options, catalog,
      type_factory, /*target_type=*/nullptr, output);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static zetasql_base::Status AnalyzeTypeImpl(const std::string& type_name,
                                    const AnalyzerOptions& options,
                                    Catalog* catalog, TypeFactory* type_factory,
                                    const Type** output_type) {
  *output_type = nullptr;

  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  VLOG(1) << "Resolving type: " << type_name;

  Resolver resolver(catalog, type_factory, &options);
  ZETASQL_RETURN_IF_ERROR(resolver.ResolveTypeName(type_name, output_type));

  VLOG(3) << "Resolved type: " << (*output_type)->DebugString();
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status AnalyzeType(const std::string& type_name,
                         const AnalyzerOptions& options_in, Catalog* catalog,
                         TypeFactory* type_factory, const Type** output_type) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status =
      AnalyzeTypeImpl(type_name, options, catalog, type_factory, output_type);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), type_name, status);
}

static zetasql_base::Status ExtractTableNamesFromStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options,
    TableNamesSet* table_names) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  VLOG(3) << "Extracting table names from statement:\n" << sql;
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(
      ParseStatement(sql, options.GetParserOptions(), &parser_output));
  VLOG(5) << "Parsed AST:\n" << parser_output->statement()->DebugString();

  return table_name_resolver::FindTables(sql, *parser_output->statement(),
                                         options, table_names);
}

static zetasql_base::Status ExtractTableResolutionTimeFromStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options,
    TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map,
    std::unique_ptr<ParserOutput>* parser_output) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  VLOG(3) << "Extracting table resolution time from statement:\n" << sql;
  ZETASQL_RETURN_IF_ERROR(
      ParseStatement(sql, options.GetParserOptions(), parser_output));
  VLOG(5) << "Parsed AST:\n" << (*parser_output)->statement()->DebugString();

  TableNamesSet table_names;
  ZETASQL_RETURN_IF_ERROR(table_name_resolver::FindTableNamesAndResolutionTime(
      sql, *(*parser_output)->statement(), options, type_factory, catalog,
      &table_names, table_resolution_time_info_map));
  // Note that "FOR SYSTEM_TIME AS OF ..." expressions are only valid inside
  // table path expressions. However, we want to have an entry in the output
  // map for every source table,
  EnsureResolutionTimeInfoForEveryTable(table_names,
                                        table_resolution_time_info_map);
  return zetasql_base::OkStatus();
}

zetasql_base::Status ExtractTableNamesFromStatement(absl::string_view sql,
                                            const AnalyzerOptions& options_in,
                                            TableNamesSet* table_names) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status =
      ExtractTableNamesFromStatementImpl(sql, options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

zetasql_base::Status ExtractTableResolutionTimeFromStatement(
    absl::string_view sql, const AnalyzerOptions& options_in,
    TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map,
    std::unique_ptr<ParserOutput>* parser_output) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status = ExtractTableResolutionTimeFromStatementImpl(
      sql, options, type_factory, catalog, table_resolution_time_info_map,
      parser_output);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static zetasql_base::Status ExtractTableNamesFromNextStatementImpl(
    ParseResumeLocation* resume_location, const AnalyzerOptions& options,
    TableNamesSet* table_names, bool* at_end_of_input) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));

  VLOG(2) << "Extracting table names from next statement at position "
          << resume_location->byte_position();
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(ParseNextStatement(resume_location,
                                     options.GetParserOptions(), &parser_output,
                                     at_end_of_input));
  VLOG(5) << "Parsed AST:\n" << parser_output->statement()->DebugString();

  return table_name_resolver::FindTables(resume_location->input(),
                                         *parser_output->statement(),
                                         options, table_names);
}

zetasql_base::Status ExtractTableNamesFromNextStatement(
    ParseResumeLocation* resume_location, const AnalyzerOptions& options_in,
    TableNamesSet* table_names, bool* at_end_of_input) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status = ExtractTableNamesFromNextStatementImpl(
      resume_location, options, table_names, at_end_of_input);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), resume_location->input(), status);
}

zetasql_base::Status ExtractTableNamesFromASTStatement(
    const ASTStatement& ast_statement, const AnalyzerOptions& options_in,
    absl::string_view sql, TableNamesSet* table_names) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status = table_name_resolver::FindTables(
      sql, ast_statement, options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

static zetasql_base::Status ExtractTableResolutionTimeFromASTStatementImpl(
    absl::string_view sql, const AnalyzerOptions& options,
    const ASTStatement& ast_statement, TypeFactory* type_factory,
    Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options));
  VLOG(3) << "Extracting table resolution time from parsed AST statement:\n"
          << ast_statement.DebugString();

  TableNamesSet table_names;
  ZETASQL_RETURN_IF_ERROR(table_name_resolver::FindTableNamesAndResolutionTime(
      sql, ast_statement, options, type_factory, catalog, &table_names,
      table_resolution_time_info_map));
  // Note that "FOR SYSTEM_TIME AS OF ..." expressions are only valid inside
  // table path expressions. However, we want to have an entry in the output
  // map for every source table,
  EnsureResolutionTimeInfoForEveryTable(table_names,
                                        table_resolution_time_info_map);
  return zetasql_base::OkStatus();
}

zetasql_base::Status ExtractTableResolutionTimeFromASTStatement(
    const ASTStatement& ast_statement, const AnalyzerOptions& options_in,
    absl::string_view sql, TypeFactory* type_factory, Catalog* catalog,
    TableResolutionTimeInfoMap* table_resolution_time_info_map) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status =
      ExtractTableResolutionTimeFromASTStatementImpl(
          sql, options, ast_statement, type_factory, catalog,
          table_resolution_time_info_map);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

zetasql_base::Status ExtractTableNamesFromScript(absl::string_view sql,
                                         const AnalyzerOptions& options_in,
                                         TableNamesSet* table_names) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnalyzerOptions(options_in));
  VLOG(3) << "Extracting table names from script:\n" << sql;
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(ParseScript(sql, options.GetParserOptions(),
                              options.error_message_mode(), &parser_output));
  VLOG(5) << "Parsed AST:\n" << parser_output->script()->DebugString();

  zetasql_base::Status status = table_name_resolver::FindTableNamesInScript(
      sql, *(parser_output->script()), options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

zetasql_base::Status ExtractTableNamesFromASTScript(const ASTScript& ast_script,
                                            const AnalyzerOptions& options_in,
                                            absl::string_view sql,
                                            TableNamesSet* table_names) {
  std::unique_ptr<AnalyzerOptions> copy;
  const AnalyzerOptions& options = GetOptionsWithArenas(&options_in, &copy);
  const zetasql_base::Status status = table_name_resolver::FindTableNamesInScript(
      sql, ast_script, options, table_names);
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options.error_message_mode(), sql, status);
}

}  // namespace zetasql
