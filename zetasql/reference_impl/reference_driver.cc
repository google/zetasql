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

#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/evaluator_registration_utils.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/compliance/test_util.h"
#include "zetasql/compliance/type_helpers.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/algebrizer.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/functions/register_all.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/parameters.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/testing/test_value.h"
#include <cstdint>
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
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

ABSL_FLAG(bool, reference_impl_enable_optional_rewrites, false,
          "If true, enables all default rewrites in the reference "
          "implementation. By default (false), rewrites are disabled for "
          "features that are implemented natively in the reference "
          "implementation. This flag allows RQG tests to be run against either "
          "the native or rewritten reference implementation.");

namespace zetasql {

ReferenceDriver::ReferenceDriver()
    : type_factory_(new TypeFactory),
      default_time_zone_(GetDefaultDefaultTimeZone()),
      statement_evaluation_timeout_(absl::Seconds(
          absl::GetFlag(FLAGS_reference_driver_query_eval_timeout_sec))) {
  language_options_.EnableMaximumLanguageFeatures();
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
    std::vector<SimpleTable::NameAndType> columns;
    columns.reserve(row_type->num_fields());
    for (int i = 0; i < row_type->num_fields(); i++) {
      columns.push_back({row_type->field(i).name, row_type->field(i).type});
    }
    simple_table = new SimpleTable(table_name, columns);
  } else {
    // We got a value table. Create a table with a single column named "value".
    simple_table = new SimpleTable(table_name, {{"value", element_type}});
    simple_table->set_is_value_table(true);
  }
  catalog_->AddOwnedTable(simple_table);

  TableInfo table_info;
  table_info.table_name = table_name;
  table_info.required_features = table.options.required_features();
  table_info.is_value_table = table.options.is_value_table();
  table_info.array = array_value;

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
  catalog_->AddZetaSQLFunctions(language_options_);
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
    // Reinitialize the set of visible functions according to options.
    catalog_->ClearFunctions();
    catalog_->AddZetaSQLFunctions(language_options_);
  }
}

zetasql_base::StatusOr<Value> ReferenceDriver::ExecuteStatementForReferenceDriver(
    const std::string& sql, const std::map<std::string, Value>& parameters,
    const ExecuteStatementOptions& options, TypeFactory* type_factory,
    bool* is_deterministic_output, bool* uses_unsupported_type) {
  ZETASQL_CHECK(is_deterministic_output != nullptr);
  ZETASQL_CHECK(uses_unsupported_type != nullptr);
  *uses_unsupported_type = false;
  ZETASQL_CHECK(catalog_ != nullptr) << "Call CreateDatabase() first";

  AnalyzerOptions analyzer_options(language_options_);
  if (!absl::GetFlag(FLAGS_reference_impl_enable_optional_rewrites)) {
    analyzer_options.enable_rewrite(REWRITE_FLATTEN, false);
  }
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

  std::unique_ptr<const AnalyzerOutput> analyzed;
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(sql, analyzer_options, catalog_.get(),
                                   type_factory, &analyzed));

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
  SystemVariablesAlgebrizerMap system_variables;
  ZETASQL_RETURN_IF_ERROR(Algebrizer::AlgebrizeStatement(
      analyzer_options.language(), algebrizer_options, type_factory,
      analyzed->resolved_statement(), &algebrized_tree, &algebrizer_parameters,
      &column_map, &system_variables));
  ZETASQL_VLOG(1) << "Algebrized tree:\n"
          << algebrized_tree->DebugString(true /* verbose */);
  ZETASQL_RET_CHECK(column_map.empty());
  ZETASQL_RET_CHECK(system_variables.empty());

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
  const TupleData params_data = CreateTupleDataFromValues(param_values);
  ZETASQL_RETURN_IF_ERROR(algebrized_tree->SetSchemasForEvaluation({&params_schema}));

  TupleSlot result;
  absl::Status status;
  if (!algebrized_tree->EvalSimple({&params_data}, &context, &result,
                                   &status)) {
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
      ZETASQL_RET_CHECK_EQ(2, output_struct_type->num_fields());

      const StructField& field1 = output_struct_type->field(0);
      ZETASQL_RET_CHECK_EQ(kDMLOutputNumRowsModifiedColumnName, field1.name);
      ZETASQL_RET_CHECK(field1.type->IsInt64());

      const StructField& field2 = output_struct_type->field(1);
      ZETASQL_RET_CHECK_EQ(kDMLOutputAllRowsColumnName, field2.name);
      ZETASQL_RET_CHECK(field2.type->IsArray());
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

const absl::TimeZone ReferenceDriver::GetDefaultTimeZone() const {
  return default_time_zone_;
}

absl::Status ReferenceDriver::SetDefaultTimeZone(const std::string& time_zone) {
  return zetasql::functions::MakeTimeZone(time_zone, &default_time_zone_);
}

}  // namespace zetasql
