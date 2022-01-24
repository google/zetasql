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

#include "zetasql/reference_impl/reference_driver.h"

#include <cstdint>
#include <map>
#include <memory>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/evaluator_registration_utils.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/compliance/test_util.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/multi_catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/algebrizer.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/functions/register_all.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/parameters.h"
#include "zetasql/reference_impl/rewrite_flags.h"
#include "zetasql/reference_impl/statement_evaluator.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/type_helpers.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/scripting/error_helpers.h"
#include "zetasql/scripting/script_executor.h"
#include "zetasql/testing/test_value.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

// Ideally we would rename this to
// --reference_driver_statement_eval_timeout_sec, but that could break existing
// command lines.
ABSL_FLAG(int32_t, reference_driver_query_eval_timeout_sec, 0,
          "Maximum statement evaluation timeout in seconds. A value of 0 "
          "means no maximum timeout is specified.");
ABSL_FLAG(bool, force_reference_product_mode_external, false,
          "If true, ignore the provided product mode setting and force "
          "the reference to use PRODUCT_EXTERNAL.");

// TODO: Remove when zetasql::FEATURE_ANONYMIZATION is no longer
// marked as in_development.
ABSL_FLAG(bool, reference_driver_enable_anonymization, false,
          "If true, enable the ZetaSQL Anonymization feature. See "
          "(broken link).");

namespace zetasql {

class ReferenceDriver::BuiltinFunctionCache {
 public:
  ~BuiltinFunctionCache() { DumpStats(); }
  void SetLanguageOptions(const LanguageOptions& options,
                          SimpleCatalog* catalog) {
    ++total_calls_;
    const BuiltinFunctionMap* builtin_function_map = nullptr;
    if (auto it = function_cache_.find(options); it != function_cache_.end()) {
      cache_hit_++;
      builtin_function_map = &it->second;
    } else {
      std::map<std::string, std::unique_ptr<Function>> function_map;
      // We have to call type_factory() while not holding mutex_.
      TypeFactory* type_factory = catalog->type_factory();
      GetZetaSQLFunctions(type_factory, options, &function_map);
      builtin_function_map =
          &(function_cache_.emplace(options, std::move(function_map))
                .first->second);
    }
    std::vector<const Function*> functions;
    functions.reserve(builtin_function_map->size());
    for (const auto& entry : *builtin_function_map) {
      functions.push_back(entry.second.get());
    }
    catalog->ClearFunctions();
    catalog->AddZetaSQLFunctions(functions);
  }
  void DumpStats() {
    ZETASQL_LOG(INFO) << "BuiltinFunctionCache: hit: " << cache_hit_ << " / "
              << total_calls_ << "(" << (cache_hit_ * 100. / total_calls_)
              << "%)"
              << " size: " << function_cache_.size();
  }

 private:
  using BuiltinFunctionMap = std::map<std::string, std::unique_ptr<Function>>;
  int total_calls_ = 0;
  int cache_hit_ = 0;
  absl::flat_hash_map<LanguageOptions, BuiltinFunctionMap> function_cache_;
};

ReferenceDriver::ReferenceDriver()
    : type_factory_(new TypeFactory),
      function_cache_(absl::make_unique<BuiltinFunctionCache>()),
      default_time_zone_(GetDefaultDefaultTimeZone()),
      statement_evaluation_timeout_(absl::Seconds(
          absl::GetFlag(FLAGS_reference_driver_query_eval_timeout_sec))) {
  language_options_.EnableMaximumLanguageFeatures();
  if (absl::GetFlag(FLAGS_reference_driver_enable_anonymization)) {
    language_options_.EnableLanguageFeature(zetasql::FEATURE_ANONYMIZATION);
  }
  language_options_.SetSupportedStatementKinds(
      Algebrizer::GetSupportedStatementKinds());
  if (absl::GetFlag(FLAGS_force_reference_product_mode_external)) {
    ZETASQL_LOG(WARNING) << "Overriding default Reference ProductMode PRODUCT_INTERNAL "
                    "with PRODUCT_EXTERNAL.";
    language_options_.set_product_mode(ProductMode::PRODUCT_EXTERNAL);
  }
  // Optional evaluator features need to be enabled "manually" here since we do
  // not go through the public PreparedExpression/PreparedQuery interface, which
  // normally handles it.
  internal::EnableFullEvaluatorFeatures();
}

ReferenceDriver::ReferenceDriver(const LanguageOptions& options)
    : type_factory_(new TypeFactory),
      language_options_(options),
      function_cache_(absl::make_unique<BuiltinFunctionCache>()),
      default_time_zone_(GetDefaultDefaultTimeZone()),
      statement_evaluation_timeout_(absl::Seconds(
          absl::GetFlag(FLAGS_reference_driver_query_eval_timeout_sec))) {
  if (absl::GetFlag(FLAGS_force_reference_product_mode_external) &&
      options.product_mode() != ProductMode::PRODUCT_EXTERNAL) {
    ZETASQL_LOG(WARNING) << "Overriding requested Reference ProductMode "
                 << ProductMode_Name(options.product_mode())
                 << " with PRODUCT_EXTERNAL.";
    language_options_.set_product_mode(ProductMode::PRODUCT_EXTERNAL);
  }
  if (absl::GetFlag(FLAGS_reference_driver_enable_anonymization)) {
    language_options_.EnableLanguageFeature(zetasql::FEATURE_ANONYMIZATION);
  }
  // Optional evaluator features need to be enabled "manually" here since we do
  // not go through the public PreparedExpression/PreparedQuery interface, which
  // normally handles it.
  internal::EnableFullEvaluatorFeatures();
}

ReferenceDriver::~ReferenceDriver() {}

absl::Status ReferenceDriver::LoadProtoEnumTypes(
    const std::set<std::string>& filenames,
    const std::set<std::string>& proto_names,
    const std::set<std::string>& enum_names) {
  errors_.clear();
  for (const std::string& filename : filenames) {
    importer_->Import(filename);
  }
  if (!errors_.empty()) {
    return ::zetasql_base::InternalErrorBuilder() << absl::StrJoin(errors_, "\n");
  }

  std::set<std::string> proto_closure;
  std::set<std::string> enum_closure;
  ZETASQL_RETURN_IF_ERROR(ComputeTransitiveClosure(importer_->pool(), proto_names,
                                           enum_names, &proto_closure,
                                           &enum_closure));

  for (const std::string& proto : proto_closure) {
    const google::protobuf::Descriptor* descriptor =
        importer_->pool()->FindMessageTypeByName(proto);
    if (!descriptor) {
      return ::zetasql_base::NotFoundErrorBuilder() << "Proto Message Type: " << proto;
    }
    const ProtoType* proto_type;
    ZETASQL_RETURN_IF_ERROR(
        catalog_->type_factory()->MakeProtoType(descriptor, &proto_type));
    catalog_->AddType(descriptor->full_name(), proto_type);
  }
  for (const std::string& enum_name : enum_closure) {
    const google::protobuf::EnumDescriptor* enum_descriptor =
        importer_->pool()->FindEnumTypeByName(enum_name);
    if (!enum_descriptor) {
      return ::zetasql_base::NotFoundErrorBuilder() << "Enum Type: " << enum_name;
    }
    const EnumType* enum_type;
    ZETASQL_RETURN_IF_ERROR(
        catalog_->type_factory()->MakeEnumType(enum_descriptor, &enum_type));
    catalog_->AddType(enum_descriptor->full_name(), enum_type);
  }
  return absl::OkStatus();
}

void ReferenceDriver::AddTable(const std::string& table_name,
                               const TestTable& table) {
  const Value& array_value = table.table_as_value;
  ZETASQL_CHECK(array_value.type()->IsArray()) << table_name << " "
                                       << array_value.DebugString(true);
  auto element_type = array_value.type()->AsArray()->element_type();
  SimpleTable* simple_table = nullptr;

  if (!table.options.is_value_table()) {
    // Non-value tables are represented as arrays of structs.
    const StructType* row_type = element_type->AsStruct();
    std::vector<SimpleTable::NameAndAnnotatedType> columns;
    const std::vector<const AnnotationMap*>& column_annotations =
        table.options.column_annotations();
    ZETASQL_CHECK(column_annotations.empty() ||
          column_annotations.size() == row_type->num_fields());
    columns.reserve(row_type->num_fields());
    for (int i = 0; i < row_type->num_fields(); i++) {
      columns.push_back(
          {row_type->field(i).name,
           {row_type->field(i).type,
            column_annotations.empty() ? nullptr : column_annotations[i]}});
    }
    simple_table = new SimpleTable(table_name, columns);
  } else {
    // We got a value table. Create a table with a single column named "value".
    simple_table = new SimpleTable(table_name, {{"value", element_type}});
    simple_table->set_is_value_table(true);
  }
  if (!table.options.userid_column().empty()) {
    ZETASQL_CHECK_OK(simple_table->SetAnonymizationInfo(table.options.userid_column()));
  }
  catalog_->AddOwnedTable(simple_table);

  TableInfo table_info;
  table_info.table_name = table_name;
  table_info.required_features = table.options.required_features();
  table_info.is_value_table = table.options.is_value_table();
  table_info.array = array_value;
  table_info.table = simple_table;

  tables_.push_back(table_info);
}

absl::Status ReferenceDriver::CreateDatabase(const TestDatabase& test_db) {
  catalog_ =
      absl::make_unique<SimpleCatalog>("root_catalog", type_factory_.get());
  tables_.clear();
  // Prepare proto importer.
  if (test_db.runs_as_test) {
    proto_source_tree_ = CreateProtoSourceTree();
  } else {
    proto_source_tree_ = absl::make_unique<ProtoSourceTree>("");
  }
  proto_error_collector_ = absl::make_unique<ProtoErrorCollector>(&errors_);
  importer_ = absl::make_unique<google::protobuf::compiler::Importer>(
      proto_source_tree_.get(), proto_error_collector_.get());
  // Load protos and enums.
  ZETASQL_RETURN_IF_ERROR(LoadProtoEnumTypes(test_db.proto_files, test_db.proto_names,
                                     test_db.enum_names));
  // Add tables to the catalog.
  for (const auto& t : test_db.tables) {
    const std::string& table_name = t.first;
    const TestTable& test_table = t.second;
    AddTable(table_name, test_table);
  }
  // Add functions to the catalog.
  function_cache_->SetLanguageOptions(language_options_, catalog_.get());
  return absl::OkStatus();
}

absl::Status ReferenceDriver::SetStatementEvaluationTimeout(
    absl::Duration timeout) {
  ZETASQL_RET_CHECK_GE(timeout, absl::ZeroDuration());
  if (absl::GetFlag(FLAGS_reference_driver_query_eval_timeout_sec) > 0 &&
      timeout > absl::Seconds(absl::GetFlag(
                    FLAGS_reference_driver_query_eval_timeout_sec))) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "timeout value " << absl::ToInt64Seconds(timeout)
           << "sec is greater than reference_driver_query_eval_timeout_sec "
           << absl::GetFlag(FLAGS_reference_driver_query_eval_timeout_sec)
           << "secs.";
  }
  statement_evaluation_timeout_ = timeout;
  return absl::OkStatus();
}

void ReferenceDriver::SetLanguageOptions(const LanguageOptions& options) {
  language_options_ = options;
  if (catalog_ != nullptr) {
    function_cache_->SetLanguageOptions(language_options_, catalog_.get());
  }
}

absl::StatusOr<AnalyzerOptions> ReferenceDriver::GetAnalyzerOptions(
    const std::map<std::string, Value>& parameters,
    bool* uses_unsupported_type) const {
  AnalyzerOptions analyzer_options(language_options_);
  analyzer_options.set_enabled_rewrites(absl::GetFlag(FLAGS_rewrites));
  analyzer_options.set_error_message_mode(
      ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
  analyzer_options.set_default_time_zone(default_time_zone_);

  for (const auto& p : parameters) {
    if (!p.second.type()->IsSupportedType(language_options_)) {
      // AnalyzerOptions will not let us add this parameter. Signal the caller
      // that the error is due to use of an unsupported type.
      *uses_unsupported_type = true;
    }
    ZETASQL_RETURN_IF_ERROR(analyzer_options.AddQueryParameter(
        p.first, p.second.type()));  // Parameter names are case-insensitive.
  }
  return analyzer_options;
}

namespace {
// Creates a catalog that includes all symbols in <catalog>, plus script
// variables.
absl::StatusOr<std::unique_ptr<Catalog>> AugmentCatalogForScriptVariables(
    Catalog* catalog, TypeFactory* type_factory,
    const VariableMap& script_variables,
    std::unique_ptr<SimpleCatalog>* internal_catalog) {
  auto variables_catalog =
      absl::make_unique<SimpleCatalog>("script_variables", type_factory);
  for (const std::pair<const IdString, Value>& variable : script_variables) {
    std::unique_ptr<SimpleConstant> constant;
    ZETASQL_RETURN_IF_ERROR(SimpleConstant::Create({variable.first.ToString()},
                                           variable.second, &constant));
    variables_catalog->AddOwnedConstant(std::move(constant));
  }

  std::unique_ptr<MultiCatalog> combined_catalog;
  ZETASQL_RETURN_IF_ERROR(MultiCatalog::Create("combined_catalog",
                                       {variables_catalog.get(), catalog},
                                       &combined_catalog));
  *internal_catalog = std::move(variables_catalog);

  return std::unique_ptr<Catalog>(std::move(combined_catalog));
}

// Executes a CREATE DDL statement, checking for IF NOT EXISTS/OR REPLACE to
// decide whether to create the object, overwrite it, skip it, or give an error.
//
// If the object does need to be added to the catalog, updates <map> and returns
// true. If the creation is skipped, does not update <map> and returns false.
// Returns an error if the object already exists and neither OR REPLACE nor
// IF NOT EXISTS are present.
template <typename Elem>
absl::StatusOr<bool> ApplyCreateDdl(
    const ResolvedCreateStatement* create_stmt,
    absl::flat_hash_map<std::vector<std::string>, Elem>& map, Elem elem,
    absl::string_view object_type) {
  std::vector<std::string> name_lower = create_stmt->name_path();
  for (std::string& part : name_lower) {
    part = absl::AsciiStrToLower(part);
  }

  bool already_exists = map.contains(name_lower);
  switch (create_stmt->create_mode()) {
    case ResolvedCreateTableStmtBase::CREATE_DEFAULT:
      if (already_exists) {
        return zetasql_base::InvalidArgumentErrorBuilder()
               << object_type << " "
               << absl::StrJoin(create_stmt->name_path(), ".")
               << " already exists";
      }
      map[name_lower] = std::move(elem);
      return true;
    case ResolvedCreateTableStmtBase::CREATE_IF_NOT_EXISTS:
      if (already_exists) {
        return false;
      }
      map[name_lower] = std::move(elem);
      return true;
    case ResolvedCreateTableStmtBase::CREATE_OR_REPLACE:
      map[name_lower] = std::move(elem);
      return true;
    default:
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Unexpected create mode: "
             << ResolvedCreateStatementEnums::CreateMode_Name(
                    create_stmt->create_mode());
  }
}

}  // namespace

absl::StatusOr<Value> ReferenceDriver::ExecuteStatementForReferenceDriver(
    const std::string& sql, const std::map<std::string, Value>& parameters,
    const ExecuteStatementOptions& options, TypeFactory* type_factory,
    bool* is_deterministic_output, bool* uses_unsupported_type,
    TestDatabase* database, std::string* created_table_name) {
  ZETASQL_ASSIGN_OR_RETURN(AnalyzerOptions analyzer_options,
                   GetAnalyzerOptions(parameters, uses_unsupported_type));
  return ExecuteStatementForReferenceDriverInternal(
      sql, analyzer_options, parameters, /*script_variables=*/{},
      /*system_variables=*/{}, options, type_factory, is_deterministic_output,
      uses_unsupported_type, database, created_table_name);
}

namespace {

absl::Status SetAnnotationMapFromResolvedColumnAnnotations(
    const ResolvedColumnAnnotations* column_annotations,
    AnnotationMap* annotation_map) {
  if (column_annotations == nullptr) {
    return absl::OkStatus();
  }
  if (column_annotations->collation_name() != nullptr) {
    const ResolvedExpr* collation_name = column_annotations->collation_name();
    ZETASQL_RET_CHECK(collation_name->node_kind() == RESOLVED_LITERAL);
    ZETASQL_RET_CHECK(collation_name->type()->IsString());
    std::string collation =
        collation_name->GetAs<ResolvedLiteral>()->value().string_value();
    annotation_map->SetAnnotation<CollationAnnotation>(
        SimpleValue::String(collation));
  }
  if (annotation_map->IsArrayMap()) {
    if (column_annotations->child_list_size() == 1) {
      ZETASQL_RETURN_IF_ERROR(SetAnnotationMapFromResolvedColumnAnnotations(
          column_annotations->child_list(0),
          annotation_map->AsArrayMap()->mutable_element()));
    }
  } else if (annotation_map->IsStructMap()) {
    StructAnnotationMap* struct_map = annotation_map->AsStructMap();
    if (column_annotations->child_list_size() > 0 &&
        column_annotations->child_list_size() <= struct_map->num_fields()) {
      for (int i = 0; i < column_annotations->child_list_size(); i++) {
        ZETASQL_RETURN_IF_ERROR(SetAnnotationMapFromResolvedColumnAnnotations(
            column_annotations->child_list(i), struct_map->mutable_field(i)));
      }
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<Value>
ReferenceDriver::ExecuteStatementForReferenceDriverInternal(
    const std::string& sql, const AnalyzerOptions& analyzer_options,
    const std::map<std::string, Value>& parameters,
    const VariableMap& script_variables,
    const SystemVariableValuesMap& system_variables,
    const ExecuteStatementOptions& options, TypeFactory* type_factory,
    bool* is_deterministic_output, bool* uses_unsupported_type,
    TestDatabase* database, std::string* created_table_name) {
  ZETASQL_CHECK(is_deterministic_output != nullptr);
  ZETASQL_CHECK(uses_unsupported_type != nullptr);
  *uses_unsupported_type = false;
  ZETASQL_CHECK(catalog_ != nullptr) << "Call CreateDatabase() first";

  std::unique_ptr<SimpleCatalog> internal_catalog;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<Catalog> catalog,
      AugmentCatalogForScriptVariables(catalog_.get(), type_factory,
                                       script_variables, &internal_catalog));

  std::unique_ptr<const AnalyzerOutput> analyzed;
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(sql, analyzer_options, catalog.get(),
                                   type_factory, &analyzed));
  if (analyzed->analyzer_output_properties().IsRelevant(
          REWRITE_ANONYMIZATION)) {
    ZETASQL_ASSIGN_OR_RETURN(analyzed,
                     RewriteForAnonymization(analyzed, analyzer_options,
                                             catalog.get(), type_factory));
  }

  if (analyzed->resolved_statement()->node_kind() ==
      RESOLVED_CREATE_PROCEDURE_STMT) {
    // This statement is executed directly in the reference driver, rather than
    // through the algebrizer/evaluator.
    const ResolvedCreateProcedureStmt* create_procedure_stmt =
        analyzed->resolved_statement()->GetAs<ResolvedCreateProcedureStmt>();
    std::string name = absl::StrJoin(create_procedure_stmt->name_path(), ".");
    ZETASQL_ASSIGN_OR_RETURN(
        bool created,
        ApplyCreateDdl(create_procedure_stmt, procedures_,
                       absl::make_unique<ProcedureDefinition>(
                           name, create_procedure_stmt->signature(),
                           create_procedure_stmt->argument_name_list(),
                           create_procedure_stmt->procedure_body()),
                       "Procedure"));
    if (created) {
      return Value::String(
          absl::StrCat("Procedure ", name, " created successfully"));
    } else {
      return Value::String(
          absl::StrCat("Skipped creation of procedure ", name));
    }
  }

  // Don't proceed if any columns referenced within the query have types not
  // supported by the language options.
  std::vector<const ResolvedNode*> column_refs;
  analyzed->resolved_statement()->GetDescendantsWithKinds({RESOLVED_COLUMN_REF},
                                                          &column_refs);
  for (const ResolvedNode* node : column_refs) {
    const ResolvedColumnRef* column_ref = node->GetAs<ResolvedColumnRef>();
    if (!column_ref->type()->IsSupportedType(language_options_)) {
      *uses_unsupported_type = true;
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Query references column with unsupported type: "
             << column_ref->type()->DebugString();
    }
  }

  AlgebrizerOptions algebrizer_options;
  algebrizer_options.use_arrays_for_tables = true;

  std::unique_ptr<ValueExpr> algebrized_tree;
  Parameters algebrizer_parameters(ParameterMap{});
  ParameterMap column_map;
  SystemVariablesAlgebrizerMap algebrizer_system_variables;
  ZETASQL_RETURN_IF_ERROR(Algebrizer::AlgebrizeStatement(
      analyzer_options.language(), algebrizer_options, type_factory,
      analyzed->resolved_statement(), &algebrized_tree, &algebrizer_parameters,
      &column_map, &algebrizer_system_variables));
  ZETASQL_VLOG(1) << "Algebrized tree:\n"
          << algebrized_tree->DebugString(true /* verbose */);
  ZETASQL_RET_CHECK(column_map.empty());

  if (!algebrized_tree->output_type()->IsSupportedType(language_options_)) {
    *uses_unsupported_type = true;
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Query produces result with unsupported type: "
           << algebrized_tree->output_type()->DebugString();
  }

  EvaluationOptions evaluation_options;
  evaluation_options.emulate_primary_keys =
      (options.primary_key_mode == PrimaryKeyMode::FIRST_COLUMN_IS_PRIMARY_KEY);
  evaluation_options.scramble_undefined_orderings = true;
  evaluation_options.always_use_stable_sort = true;
  evaluation_options.max_value_byte_size = std::numeric_limits<int64_t>::max();
  evaluation_options.max_intermediate_byte_size =
      std::numeric_limits<int64_t>::max();

  EvaluationContext context(evaluation_options);
  context.SetDefaultTimeZone(default_time_zone_);
  context.SetLanguageOptions(analyzer_options.language());

  for (const TableInfo& table_info : tables_) {
    bool has_all_required_features = true;
    for (const LanguageFeature required_feature :
         table_info.required_features) {
      if (!analyzer_options.language().LanguageFeatureEnabled(
              required_feature)) {
        has_all_required_features = false;
        break;
      }
    }
    if (has_all_required_features) {
      ZETASQL_RETURN_IF_ERROR(context.AddTableAsArray(
          table_info.table_name, table_info.is_value_table, table_info.array,
          analyzer_options.language()));
    }
  }
  if (statement_evaluation_timeout_ > absl::ZeroDuration()) {
    context.SetStatementEvaluationDeadlineFromNow(
        statement_evaluation_timeout_);
  }

  std::vector<VariableId> param_variables;
  param_variables.reserve(parameters.size());
  std::vector<Value> param_values;
  param_values.reserve(parameters.size());
  for (const auto& p : parameters) {
    // Set the parameter if it appears in the statement, ignore it otherwise.
    // Note that it is ok if some parameters are not referenced.
    const ParameterMap& parameter_map =
        algebrizer_parameters.named_parameters();
    auto it = parameter_map.find(absl::AsciiStrToLower(p.first));
    if (it != parameter_map.end() && it->second.is_valid()) {
      param_variables.push_back(it->second);
      param_values.push_back(p.second);
      ZETASQL_VLOG(1) << "Parameter @" << p.first << " (variable " << it->second
              << "): " << p.second.FullDebugString();
    }
  }
  const TupleSchema params_schema(param_variables);
  const TupleData params_data =
      CreateTupleDataFromValues(std::move(param_values));

  std::vector<VariableId> system_var_ids;
  std::vector<Value> system_var_values;
  system_var_ids.reserve(algebrizer_system_variables.size());
  for (const auto& sys_var : algebrizer_system_variables) {
    system_var_ids.push_back(sys_var.second);
    system_var_values.push_back(system_variables.at(sys_var.first));
  }
  const TupleSchema system_vars_schema(system_var_ids);
  const TupleData system_vars_data =
      CreateTupleDataFromValues(std::move(system_var_values));

  ZETASQL_RETURN_IF_ERROR(algebrized_tree->SetSchemasForEvaluation(
      {&params_schema, &system_vars_schema}));

  TupleSlot result;
  absl::Status status;
  if (!algebrized_tree->EvalSimple({&params_data, &system_vars_data}, &context,
                                   &result, &status)) {
    return status;
  }
  const Value& output = result.value();

  const Type* output_type = output.type();
  switch (analyzed->resolved_statement()->node_kind()) {
    case RESOLVED_QUERY_STMT: {
      ZETASQL_RET_CHECK(output_type->IsArray());
      break;
    }
    case RESOLVED_DELETE_STMT:
    case RESOLVED_UPDATE_STMT:
    case RESOLVED_INSERT_STMT:
    case RESOLVED_MERGE_STMT: {
      ZETASQL_RET_CHECK(output_type->IsStruct());
      const StructType* output_struct_type = output_type->AsStruct();

      int expect_num_fields = output_struct_type->num_fields();
      if (analyzed->resolved_statement()->node_kind() == RESOLVED_MERGE_STMT) {
        ZETASQL_RET_CHECK_EQ(expect_num_fields, 2);
      } else {
        ZETASQL_RET_CHECK(expect_num_fields == 2 || expect_num_fields == 3);
      }

      const StructField& field1 = output_struct_type->field(0);
      ZETASQL_RET_CHECK_EQ(kDMLOutputNumRowsModifiedColumnName, field1.name);
      ZETASQL_RET_CHECK(field1.type->IsInt64());

      const StructField& field2 = output_struct_type->field(1);
      ZETASQL_RET_CHECK_EQ(kDMLOutputAllRowsColumnName, field2.name);
      ZETASQL_RET_CHECK(field2.type->IsArray());

      if (expect_num_fields == 3) {
        const StructField& field3 = output_struct_type->field(2);
        ZETASQL_RET_CHECK_EQ(kDMLOutputReturningColumnName, field3.name);
        ZETASQL_RET_CHECK(field3.type->IsArray());
      }
      break;
    }
    case RESOLVED_CREATE_TABLE_AS_SELECT_STMT: {
      // Insert the table into the database
      ZETASQL_CHECK(database != nullptr);
      const ResolvedCreateTableAsSelectStmt* create_table =
          analyzed->resolved_statement()
              ->GetAs<ResolvedCreateTableAsSelectStmt>();
      TestTable new_table;
      new_table.table_as_value = output;
      new_table.options.set_is_value_table(create_table->is_value_table());

      std::vector<const AnnotationMap*> column_annotations;
      bool all_empty_annotations = true;
      for (const auto& column_definition :
           create_table->column_definition_list()) {
        std::unique_ptr<AnnotationMap> new_map =
            AnnotationMap::Create(column_definition->type());
        ZETASQL_RETURN_IF_ERROR(SetAnnotationMapFromResolvedColumnAnnotations(
            column_definition->annotations(), new_map.get()));
        if (new_map->Empty()) {
          column_annotations.push_back(nullptr);
        } else {
          all_empty_annotations = false;
          ZETASQL_ASSIGN_OR_RETURN(const AnnotationMap* owned,
                           type_factory->TakeOwnership(std::move(new_map)));
          column_annotations.push_back(owned);
        }
      }
      if (!all_empty_annotations) {
        new_table.options.set_column_annotations(std::move(column_annotations));
      }

      for (const auto& option : create_table->option_list()) {
        if (option->name() == "userid_column") {
          if (option->value()->node_kind() != RESOLVED_LITERAL ||
              !option->value()->type()->IsString()) {
            return absl::InvalidArgumentError(
                "userid_column option must be a STRING literal");
          }
          new_table.options.set_userid_column(option->value()
                                                  ->GetAs<ResolvedLiteral>()
                                                  ->value()
                                                  .string_value());
        } else {
          return zetasql_base::InvalidArgumentErrorBuilder()
                 << "Unsupported CREATE TABLE option: " << option->name();
        }
      }
      std::string table_name = absl::StrJoin(create_table->name_path(), ".");
      if (created_table_name != nullptr) {
        *created_table_name = table_name;
      }
      bool already_exists = zetasql_base::ContainsKey(database->tables, table_name);
      switch (create_table->create_mode()) {
        case ResolvedCreateTableStmtBase::CREATE_DEFAULT:
          if (already_exists) {
            return zetasql_base::InvalidArgumentErrorBuilder()
                   << "Table " << table_name << " already exists";
          } else {
            database->tables[table_name] = new_table;
          }
          break;
        case ResolvedCreateTableStmtBase::CREATE_IF_NOT_EXISTS:
          if (!already_exists) {
            database->tables[table_name] = new_table;
          }
          break;
        case ResolvedCreateTableStmtBase::CREATE_OR_REPLACE:
          database->tables[table_name] = new_table;
          break;
        default:
          return zetasql_base::InvalidArgumentErrorBuilder()
                 << "Unexpected create mode: "
                 << ResolvedCreateStatementEnums::CreateMode_Name(
                        create_table->create_mode());
      }
      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected statement type: "
                       << ResolvedNodeKind_Name(
                              analyzed->resolved_statement()->node_kind());
      break;
  }

  *is_deterministic_output = context.IsDeterministicOutput();

  return output;
}

// StatementEvaluator implementation for compliance tests with the reference
// driver. We use the reference driver to evaluate statements and the default
// StatementEvaluator for everything else.
class ReferenceDriverStatementEvaluator : public StatementEvaluatorImpl {
 public:
  ReferenceDriverStatementEvaluator(
      const AnalyzerOptions& initial_analyzer_options, ScriptResult* result,
      const std::map<std::string, Value>* parameters,
      const ReferenceDriver::ExecuteStatementOptions& options,
      TypeFactory* type_factory, ReferenceDriver* driver,
      const EvaluatorOptions& evaluator_options)
      : StatementEvaluatorImpl(initial_analyzer_options, evaluator_options,
                               *parameters, type_factory, driver->catalog(),
                               &evaluator_callback_),
        evaluator_callback_(/*bytes_per_iterator=*/100),
        result_(result),
        parameters_(parameters),
        options_(options),
        type_factory_(type_factory),
        driver_(driver) {}

  // Override ExecuteStatement() to ensure that statement results exactly match
  // up what would be produced by a standalone-statement compliance test.
  absl::Status ExecuteStatement(const ScriptExecutor& executor,
                                const ScriptSegment& segment) override;

  absl::StatusOr<std::unique_ptr<ProcedureDefinition>> LoadProcedure(
      const ScriptExecutor& executor, const absl::Span<const std::string>& path,
      const int64_t num_arguments) override {
    std::vector<std::string> name_lower;
    name_lower.reserve(path.size());
    for (const std::string& part : path) {
      name_lower.push_back(absl::AsciiStrToLower(part));
    }

    auto it = driver_->procedures_.find(name_lower);
    if (it == driver_->procedures_.end()) {
      return zetasql_base::NotFoundErrorBuilder()
             << "Procedure " << absl::StrJoin(path, ".") << " not found";
    }
    return absl::make_unique<ProcedureDefinition>(*it->second);
  }

  // TODO: Currently, this is only set to true if a statement uses an
  // unsupported type, and fails to detect cases where a script variable or
  // expression uses an unsupported type.
  bool uses_unsupported_type() const { return uses_unsupported_type_; }

 private:
  StatementEvaluatorCallback evaluator_callback_;
  ScriptResult* result_;
  const std::map<std::string, Value>* parameters_;
  ReferenceDriver::ExecuteStatementOptions options_;
  TypeFactory* type_factory_;
  ReferenceDriver* driver_;
  bool uses_unsupported_type_ = false;
};

absl::Status ReferenceDriverStatementEvaluator::ExecuteStatement(
    const ScriptExecutor& executor, const ScriptSegment& segment) {
  bool stmt_uses_unsupported_type;
  ParseLocationTranslator translator(segment.script());
  absl::StatusOr<std::pair<int, int>> line_and_column =
      translator.GetLineAndColumnAfterTabExpansion(segment.range().start());
  StatementResult result;
  result.procedure_name = executor.GetCurrentProcedureName();
  result.line = line_and_column.ok() ? line_and_column->first : 0;
  result.column = line_and_column.ok() ? line_and_column->second : 0;
  bool is_deterministic_output_unused;

  AnalyzerOptions analyzer_options = initial_analyzer_options();

  // Always execute statements using ERROR_MESSAGE_WITH_PAYLOAD; we need the
  // line and column numbers to be stored explicitly - not embedded in a string
  // - so that we can convert the error locations to be relative to the script,
  // rather than just the single line.
  ErrorMessageMode orig_error_message_mode =
      analyzer_options.error_message_mode();
  analyzer_options.set_error_message_mode(ERROR_MESSAGE_WITH_PAYLOAD);

  ZETASQL_RETURN_IF_ERROR(executor.UpdateAnalyzerOptions(analyzer_options));

  result.result = driver_->ExecuteStatementForReferenceDriverInternal(
      std::string(segment.GetSegmentText()), analyzer_options, *parameters_,
      executor.GetCurrentVariables(), executor.GetKnownSystemVariables(),
      options_, type_factory_, &is_deterministic_output_unused,
      &stmt_uses_unsupported_type,
      /*database=*/nullptr, /*created_table_name=*/nullptr);
  if (!result.result.status().ok()) {
    result.result = MaybeUpdateErrorFromPayload(
        orig_error_message_mode, segment.script(),
        absl::Status(zetasql_base::StatusBuilder(result.result.status())
                         .With(ConvertLocalErrorToScriptError(segment))));
  }
  uses_unsupported_type_ |= stmt_uses_unsupported_type;

  result_->statement_results.push_back(std::move(result));
  absl::Status status = result_->statement_results.back().result.status();
  if (!status.ok() && status.code() != absl::StatusCode::kInternal) {
    // Mark this error as handleable
    internal::AttachPayload(&status, ScriptException());
  }
  return status;
}

namespace {
absl::Status ExecuteScriptInternal(ScriptExecutor* executor) {
  while (!executor->IsComplete()) {
    ZETASQL_RETURN_IF_ERROR(executor->ExecuteNext());
  }
  return absl::OkStatus();
}
}  // namespace

absl::StatusOr<ScriptResult> ReferenceDriver::ExecuteScriptForReferenceDriver(
    const std::string& sql, const std::map<std::string, Value>& parameters,
    const ExecuteStatementOptions& options, TypeFactory* type_factory,
    bool* uses_unsupported_type) {
  ScriptResult result;
  ZETASQL_RETURN_IF_ERROR(ExecuteScriptForReferenceDriverInternal(
      sql, parameters, options, type_factory, uses_unsupported_type, &result));
  return result;
}

absl::Status ReferenceDriver::ExecuteScriptForReferenceDriverInternal(
    const std::string& sql, const std::map<std::string, Value>& parameters,
    const ExecuteStatementOptions& options, TypeFactory* type_factory,
    bool* uses_unsupported_type, ScriptResult* result) {
  procedures_.clear();  // Clear procedures leftover from previous script.
  ZETASQL_ASSIGN_OR_RETURN(AnalyzerOptions analyzer_options,
                   GetAnalyzerOptions(parameters, uses_unsupported_type));
  ScriptExecutorOptions script_executor_options;
  script_executor_options.PopulateFromAnalyzerOptions(analyzer_options);
  EvaluatorOptions evaluator_options;
  evaluator_options.type_factory = type_factory;
  evaluator_options.clock = zetasql_base::Clock::RealClock();
  ReferenceDriverStatementEvaluator evaluator(
      analyzer_options, result, &parameters, options, type_factory, this,
      evaluator_options);

  // Make table data set up in the [prepare_database] section accessible in
  // evaluation of expressions/queries, which go through the evaluator, rather
  // than ExecuteStatementForReferenceDriver().
  for (const TableInfo& table : tables_) {
    std::vector<std::vector<Value>> data;
    data.reserve(table.array.num_elements());
    for (int i = 0; i < table.array.num_elements(); ++i) {
      data.push_back(table.array.element(i).fields());
    }
    table.table->SetContents(data);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ScriptExecutor> executor,
      ScriptExecutor::Create(sql, script_executor_options, &evaluator));
  absl::Status status = ExecuteScriptInternal(executor.get());
  *uses_unsupported_type = evaluator.uses_unsupported_type();
  ZETASQL_RETURN_IF_ERROR(status);
  return absl::OkStatus();
}

const absl::TimeZone ReferenceDriver::GetDefaultTimeZone() const {
  return default_time_zone_;
}

absl::Status ReferenceDriver::SetDefaultTimeZone(const std::string& time_zone) {
  return zetasql::functions::MakeTimeZone(time_zone, &default_time_zone_);
}

}  // namespace zetasql
