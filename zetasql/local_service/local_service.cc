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

#include "zetasql/local_service/local_service.h"

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/proto_helper.h"
#include "zetasql/local_service/local_service.pb.h"
#include "zetasql/local_service/state.h"
#include "zetasql/proto/simple_catalog.pb.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/function.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_formatter.h"
#include "zetasql/public/table_from_proto.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/sql_builder.h"
#include "absl/base/thread_annotations.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace local_service {

using google::protobuf::RepeatedPtrField;

namespace {

absl::Status RepeatedParametersToMap(
    const RepeatedPtrField<EvaluateRequest::Parameter>& params,
    const QueryParametersMap& types, ParameterValueMap* map) {
  for (const auto& param : params) {
    std::string name = absl::AsciiStrToLower(param.name());
    const Type* type = zetasql_base::FindPtrOrNull(types, name);
    ZETASQL_RET_CHECK(type != nullptr) << "Type not found for '" << name << "'";
    auto result = Value::Deserialize(param.value(), type);
    ZETASQL_RETURN_IF_ERROR(result.status());
    (*map)[name] = result.value();
  }

  return absl::OkStatus();
}

// Populate the existing pools into the map with existing indices, to make sure
// the serialized type will use the same indices.
void PopulateExistingPoolsToFileDescriptorSetMap(
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    FileDescriptorSetMap* file_descriptor_set_map) {
  file_descriptor_set_map->clear();

  for (int i = 0; i < pools.size(); ++i) {
    std::unique_ptr<Type::FileDescriptorEntry>& entry =
        (*file_descriptor_set_map)[pools[i]];
    ZETASQL_CHECK_EQ(entry.get(), nullptr);
    entry = absl::make_unique<Type::FileDescriptorEntry>();
    entry->descriptor_set_index = i;
  }

  ZETASQL_CHECK_EQ(pools.size(), file_descriptor_set_map->size());
}

absl::Status SerializeTypeUsingExistingPools(
    const Type* type, const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeProto* type_proto) {
  FileDescriptorSetMap file_descriptor_set_map;
  PopulateExistingPoolsToFileDescriptorSetMap(pools, &file_descriptor_set_map);

  ZETASQL_RETURN_IF_ERROR(type->SerializeToProtoAndDistinctFileDescriptors(
      type_proto, &file_descriptor_set_map));

  ZETASQL_RET_CHECK_EQ(pools.size(), file_descriptor_set_map.size())
      << type->DebugString(true)
      << " uses unknown DescriptorPool, this shouldn't happen.";
  return absl::OkStatus();
}

}  // namespace

// This class is thread-safe.
class BaseSavedState : public GenericState {
 protected:
  BaseSavedState() : GenericState(), initialized_(false) {}

  absl::Status Init(const RepeatedPtrField<google::protobuf::FileDescriptorSet>& fdsets) {
    absl::MutexLock lock(&mutex_);
    ZETASQL_CHECK(!initialized_);

    // In case Init() failed half way and called again.
    pools_.clear();
    const_pools_.clear();

    const int num_pools = fdsets.size();
    pools_.reserve(num_pools);
    const_pools_.reserve(num_pools);

    for (const auto& file_descirptor_set : fdsets) {
      std::unique_ptr<google::protobuf::DescriptorPool> pool(
          new google::protobuf::DescriptorPool());
      ZETASQL_RETURN_IF_ERROR(
          AddFileDescriptorSetToPool(&file_descirptor_set, pool.get()));
      const_pools_.emplace_back(pool.get());
      pools_.emplace_back(pool.release());
    }

    return absl::OkStatus();
  }

 public:
  TypeFactory* GetTypeFactory() {
    absl::MutexLock lock(&mutex_);
    ZETASQL_CHECK(initialized_);

    return &factory_;
  }

  const std::vector<const google::protobuf::DescriptorPool*>& GetDescriptorPools() {
    absl::MutexLock lock(&mutex_);
    ZETASQL_CHECK(initialized_);

    return const_pools_;
  }

 protected:
  absl::Mutex mutex_;

  bool initialized_ ABSL_GUARDED_BY(mutex_);

  TypeFactory factory_ ABSL_GUARDED_BY(mutex_);

  std::vector<std::unique_ptr<google::protobuf::DescriptorPool>> pools_
      ABSL_GUARDED_BY(mutex_);
  std::vector<const google::protobuf::DescriptorPool*> const_pools_
      ABSL_GUARDED_BY(mutex_);
};

class PreparedExpressionState : public BaseSavedState {
 public:
  PreparedExpressionState() : BaseSavedState() {}
  PreparedExpressionState(const PreparedExpressionState&) = delete;
  PreparedExpressionState& operator=(const PreparedExpressionState&) = delete;

  absl::Status InitAndDeserializeOptions(
      const std::string& sql,
      const RepeatedPtrField<google::protobuf::FileDescriptorSet>& fdsets,
      const AnalyzerOptionsProto& proto) {
    ZETASQL_RETURN_IF_ERROR(BaseSavedState::Init(fdsets));

    absl::MutexLock lock(&mutex_);
    ZETASQL_RETURN_IF_ERROR(AnalyzerOptions::Deserialize(proto, const_pools_, &factory_,
                                                 &options_));
    zetasql::EvaluatorOptions evaluator_options;
    evaluator_options.type_factory = &factory_;
    evaluator_options.default_time_zone = options_.default_time_zone();
    exp_ = absl::make_unique<PreparedExpression>(sql, evaluator_options);
    initialized_ = true;
    return absl::OkStatus();
  }

  PreparedExpression* GetPreparedExpression() {
    absl::MutexLock lock(&mutex_);
    ZETASQL_CHECK(initialized_);

    return exp_.get();
  }

  const AnalyzerOptions& GetAnalyzerOptions() {
    absl::MutexLock lock(&mutex_);
    ZETASQL_CHECK(initialized_);

    return options_;
  }

 private:
  std::unique_ptr<PreparedExpression> exp_ ABSL_GUARDED_BY(mutex_);
  AnalyzerOptions options_ ABSL_GUARDED_BY(mutex_);
};

class PreparedExpressionPool : public SharedStatePool<PreparedExpressionState> {
};

class RegisteredCatalogState : public BaseSavedState {
 public:
  RegisteredCatalogState() : BaseSavedState() {}
  RegisteredCatalogState(const RegisteredCatalogState&) = delete;
  RegisteredCatalogState& operator=(const RegisteredCatalogState&) = delete;

  absl::Status Init(const SimpleCatalogProto& proto,
                    const RepeatedPtrField<google::protobuf::FileDescriptorSet>& fdsets) {
    ZETASQL_RETURN_IF_ERROR(BaseSavedState::Init(fdsets));

    absl::MutexLock lock(&mutex_);
    ZETASQL_RETURN_IF_ERROR(SimpleCatalog::Deserialize(proto, const_pools_, &catalog_));
    initialized_ = true;
    return absl::OkStatus();
  }

  SimpleCatalog* GetCatalog() {
    absl::MutexLock lock(&mutex_);
    ZETASQL_CHECK(initialized_);
    return catalog_.get();
  }

 private:
  std::unique_ptr<SimpleCatalog> catalog_ ABSL_GUARDED_BY(mutex_);
};

class RegisteredCatalogPool : public SharedStatePool<RegisteredCatalogState> {};

ZetaSqlLocalServiceImpl::ZetaSqlLocalServiceImpl()
    : registered_catalogs_(new RegisteredCatalogPool()),
      prepared_expressions_(new PreparedExpressionPool()) {}

ZetaSqlLocalServiceImpl::~ZetaSqlLocalServiceImpl() {}

absl::Status ZetaSqlLocalServiceImpl::Prepare(const PrepareRequest& request,
                                                PrepareResponse* response) {
  std::unique_ptr<PreparedExpressionState> state(new PreparedExpressionState());
  ZETASQL_RETURN_IF_ERROR(state->InitAndDeserializeOptions(
      request.sql(), request.file_descriptor_set(), request.options()));

  RegisteredCatalogState* catalog_state = nullptr;
  // Needed to hold the new state because shared_ptr doesn't support release().
  std::unique_ptr<RegisteredCatalogState> new_catalog_state;

  if (request.has_registered_catalog_id()) {
    int64_t id = request.registered_catalog_id();
    std::shared_ptr<RegisteredCatalogState> shared_state =
        registered_catalogs_->Get(id);
    catalog_state = shared_state.get();
    if (catalog_state == nullptr) {
      return MakeSqlError() << "Registered catalog " << id << " unknown.";
    }
  } else if (request.has_simple_catalog()) {
    new_catalog_state = absl::make_unique<RegisteredCatalogState>();
    catalog_state = new_catalog_state.get();
    ZETASQL_RETURN_IF_ERROR(catalog_state->Init(request.simple_catalog(),
                                        request.file_descriptor_set()));
  }

  ZETASQL_RETURN_IF_ERROR(state->GetPreparedExpression()->Prepare(
      state->GetAnalyzerOptions(),
      catalog_state != nullptr ? catalog_state->GetCatalog() : nullptr));
  return RegisterPrepared(state, response->mutable_prepared());
}

absl::Status ZetaSqlLocalServiceImpl::RegisterPrepared(
    std::unique_ptr<PreparedExpressionState>& state, PreparedState* response) {
  PreparedExpression* exp = state->GetPreparedExpression();

  ZETASQL_RETURN_IF_ERROR(SerializeTypeUsingExistingPools(
      exp->output_type(), state->GetDescriptorPools(),
      response->mutable_output_type()));

  int64_t id = prepared_expressions_->Register(state.release());
  ZETASQL_RET_CHECK_NE(-1, id)
      << "Failed to register prepared state, this shouldn't happen.";

  response->set_prepared_expression_id(id);

  auto columns = exp->GetReferencedColumns();
  ZETASQL_RETURN_IF_ERROR(columns.status());
  for (const std::string& column_name : columns.value()) {
    response->add_referenced_columns(column_name);
  }

  auto parameters = exp->GetReferencedParameters();
  ZETASQL_RETURN_IF_ERROR(parameters.status());
  for (const std::string& parameter_name : parameters.value()) {
    response->add_referenced_parameters(parameter_name);
  }

  auto parameter_count = exp->GetPositionalParameterCount();
  ZETASQL_RETURN_IF_ERROR(parameter_count.status());
  response->set_positional_parameter_count(parameter_count.value());

  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::Unprepare(int64_t id) {
  if (prepared_expressions_->Delete(id)) {
    return absl::OkStatus();
  }
  return MakeSqlError() << "Unknown prepared expression ID: " << id;
}

absl::Status ZetaSqlLocalServiceImpl::Evaluate(const EvaluateRequest& request,
                                                 EvaluateResponse* response) {
  bool prepared = request.has_prepared_expression_id();
  std::shared_ptr<PreparedExpressionState> shared_state;
  // Needed to hold the new state because shared_ptr doesn't support release().
  std::unique_ptr<PreparedExpressionState> new_state;
  PreparedExpressionState* state;

  if (prepared) {
    int64_t id = request.prepared_expression_id();
    shared_state = prepared_expressions_->Get(id);
    state = shared_state.get();
    if (state == nullptr) {
      return MakeSqlError() << "Prepared expression " << id << " unknown.";
    }
  } else {
    new_state = absl::make_unique<PreparedExpressionState>();
    state = new_state.get();
    ZETASQL_RETURN_IF_ERROR(state->InitAndDeserializeOptions(
        request.sql(), request.file_descriptor_set(), request.options()));
  }

  const absl::Status result = EvaluateImpl(request, state, response);

  if (!prepared && result.ok()) {
    ZETASQL_RETURN_IF_ERROR(RegisterPrepared(new_state, response->mutable_prepared()));
  }

  return result;
}

absl::Status ZetaSqlLocalServiceImpl::EvaluateImpl(
    const EvaluateRequest& request, PreparedExpressionState* state,
    EvaluateResponse* response) {
  const AnalyzerOptions& analyzer_options = state->GetAnalyzerOptions();

  ParameterValueMap columns, params;
  ZETASQL_RETURN_IF_ERROR(RepeatedParametersToMap(
      request.columns(), analyzer_options.expression_columns(), &columns));
  ZETASQL_RETURN_IF_ERROR(RepeatedParametersToMap(
      request.params(), analyzer_options.query_parameters(), &params));

  auto result = state->GetPreparedExpression()->Execute(columns, params);
  ZETASQL_RETURN_IF_ERROR(result.status());

  const Value& value = result.value();
  ZETASQL_RETURN_IF_ERROR(value.Serialize(response->mutable_value()));

  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::GetTableFromProto(
    const TableFromProtoRequest& request, SimpleTableProto* response) {
  TypeFactory factory;
  google::protobuf::DescriptorPool pool;
  ZETASQL_RETURN_IF_ERROR(
      AddFileDescriptorSetToPool(&request.file_descriptor_set(), &pool));
  const google::protobuf::Descriptor* proto_descr =
      pool.FindMessageTypeByName(request.proto().proto_name());
  if (proto_descr == nullptr) {
    return ::zetasql_base::UnknownErrorBuilder()
           << "Proto type name not found: " << request.proto().proto_name();
  }
  if (proto_descr->file()->name() != request.proto().proto_file_name()) {
    return ::zetasql_base::UnknownErrorBuilder()
           << "Proto " << request.proto().proto_name() << " found in "
           << proto_descr->file()->name() << ", not "
           << request.proto().proto_file_name() << " as specified.";
  }
  TableFromProto table(proto_descr->name());
  ZETASQL_RETURN_IF_ERROR(table.Init(proto_descr, &factory));
  FileDescriptorSetMap file_descriptor_set_map;
  ZETASQL_RETURN_IF_ERROR(table.Serialize(&file_descriptor_set_map, response));
  if (!file_descriptor_set_map.empty()) {
    ZETASQL_RET_CHECK_EQ(1, file_descriptor_set_map.size())
        << "Table from proto " << proto_descr->full_name()
        << " uses unknown DescriptorPool, this shouldn't happen.";
    ZETASQL_RET_CHECK_EQ(0, file_descriptor_set_map.at(&pool)->descriptor_set_index)
        << "Table from proto " << proto_descr->full_name()
        << " uses unknown DescriptorPool, this shouldn't happen.";
  }
  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::Analyze(const AnalyzeRequest& request,
                                                AnalyzeResponse* response) {
  RegisteredCatalogState* catalog_state;
  // Needed to hold the new state because shared_ptr doesn't support release().
  std::unique_ptr<RegisteredCatalogState> new_catalog_state;

  if (request.has_registered_catalog_id()) {
    int64_t id = request.registered_catalog_id();
    std::shared_ptr<RegisteredCatalogState> shared_state =
        registered_catalogs_->Get(id);
    catalog_state = shared_state.get();
    if (catalog_state == nullptr) {
      return MakeSqlError() << "Registered catalog " << id << " unknown.";
    }
  } else {
    new_catalog_state = absl::make_unique<RegisteredCatalogState>();
    catalog_state = new_catalog_state.get();
    ZETASQL_RETURN_IF_ERROR(catalog_state->Init(request.simple_catalog(),
                                        request.file_descriptor_set()));
  }

  if (request.has_sql_expression()) {
    return AnalyzeExpressionImpl(request, catalog_state, response);
  } else {
    return AnalyzeImpl(request, catalog_state, response);
  }
}

absl::Status ZetaSqlLocalServiceImpl::AnalyzeImpl(
    const AnalyzeRequest& request, RegisteredCatalogState* catalog_state,
    AnalyzeResponse* response) {
  AnalyzerOptions options;
  ZETASQL_RETURN_IF_ERROR(AnalyzerOptions::Deserialize(
      request.options(), catalog_state->GetDescriptorPools(),
      catalog_state->GetTypeFactory(), &options));

  if (!(request.has_sql_statement() || request.has_parse_resume_location())) {
    return ::zetasql_base::UnknownErrorBuilder()
           << "Unrecognized AnalyzeRequest target " << request.target_case();
  }

  TypeFactory factory;
  std::unique_ptr<const AnalyzerOutput> output;

  if (request.has_sql_statement()) {
    const std::string& sql = request.sql_statement();

    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(
        sql, options, catalog_state->GetCatalog(), &factory, &output));

    ZETASQL_RETURN_IF_ERROR(
        SerializeResolvedOutput(output.get(), sql, response, catalog_state));
  } else if (request.has_parse_resume_location()) {
    bool at_end_of_input;
    ParseResumeLocation location =
        ParseResumeLocation::FromProto(request.parse_resume_location());
    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeNextStatement(
        &location, options, catalog_state->GetCatalog(), &factory, &output,
        &at_end_of_input));

    ZETASQL_RETURN_IF_ERROR(SerializeResolvedOutput(output.get(), location.input(),
                                            response, catalog_state));
    response->set_resume_byte_position(location.byte_position());
  }
  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::AnalyzeExpressionImpl(
    const AnalyzeRequest& request, RegisteredCatalogState* catalog_state,
    AnalyzeResponse* response) {
  AnalyzerOptions options;
  ZETASQL_RETURN_IF_ERROR(AnalyzerOptions::Deserialize(
      request.options(), catalog_state->GetDescriptorPools(),
      catalog_state->GetTypeFactory(), &options));

  if (request.has_sql_expression()) {
    std::unique_ptr<const AnalyzerOutput> output;
    TypeFactory factory;

    const std::string& sql = request.sql_expression();

    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeExpression(
        sql, options, catalog_state->GetCatalog(), &factory, &output));

    ZETASQL_RETURN_IF_ERROR(
        SerializeResolvedOutput(output.get(), sql, response, catalog_state));
  }
  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::BuildSql(const BuildSqlRequest& request,
                                                 BuildSqlResponse* response) {
  RegisteredCatalogState* catalog_state;
  // Needed to hold the new state because shared_ptr doesn't support release().
  std::unique_ptr<RegisteredCatalogState> new_catalog_state;

  if (request.has_registered_catalog_id()) {
    int64_t id = request.registered_catalog_id();
    std::shared_ptr<RegisteredCatalogState> shared_state =
        registered_catalogs_->Get(id);
    catalog_state = shared_state.get();
    if (catalog_state == nullptr) {
      return MakeSqlError() << "Registered catalog " << id << " unknown.";
    }
  } else {
    new_catalog_state = absl::make_unique<RegisteredCatalogState>();
    catalog_state = new_catalog_state.get();
    ZETASQL_RETURN_IF_ERROR(catalog_state->Init(request.simple_catalog(),
                                        request.file_descriptor_set()));
  }
  IdStringPool string_pool;
  ResolvedNode::RestoreParams restore_params(
      catalog_state->GetDescriptorPools(), catalog_state->GetCatalog(),
      catalog_state->GetTypeFactory(), &string_pool);

  std::unique_ptr<ResolvedNode> ast;
  if (request.has_resolved_statement()) {
    ast = std::move(ResolvedStatement::RestoreFrom(request.resolved_statement(),
                                                   restore_params)
                        .value());
  } else if (request.has_resolved_expression()) {
    ast = std::move(
        ResolvedExpr::RestoreFrom(request.resolved_expression(), restore_params)
            .value());
  } else {
    return absl::OkStatus();
  }

  zetasql::SQLBuilder sql_builder;
  ZETASQL_CHECK_OK(ast->Accept(&sql_builder));
  response->set_sql(sql_builder.sql());
  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::ExtractTableNamesFromStatement(
    const ExtractTableNamesFromStatementRequest& request,
    ExtractTableNamesFromStatementResponse* response) {
  LanguageOptions language_options = request.has_options()
                                         ? LanguageOptions(request.options())
                                         : LanguageOptions();

  zetasql::TableNamesSet table_names;
  if (request.allow_script()) {
    ZETASQL_RETURN_IF_ERROR(zetasql::ExtractTableNamesFromScript(
        request.sql_statement(), zetasql::AnalyzerOptions(language_options),
        &table_names));
  } else {
    ZETASQL_RETURN_IF_ERROR(zetasql::ExtractTableNamesFromStatement(
        request.sql_statement(), zetasql::AnalyzerOptions(language_options),
        &table_names));
  }
  for (const std::vector<std::string>& table_name : table_names) {
    ExtractTableNamesFromStatementResponse_TableName* table_name_field =
        response->add_table_name();
    for (const std::string& name_segment : table_name) {
      table_name_field->add_table_name_segment(name_segment);
    }
  }
  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::ExtractTableNamesFromNextStatement(
    const ExtractTableNamesFromNextStatementRequest& request,
    ExtractTableNamesFromNextStatementResponse* response) {
  ParseResumeLocation location =
      ParseResumeLocation::FromProto(request.parse_resume_location());

  LanguageOptions language_options = request.has_options() ?
      LanguageOptions(request.options()) :
      LanguageOptions();

  bool at_end_of_input;
  zetasql::TableNamesSet table_names;
  ZETASQL_RETURN_IF_ERROR(zetasql::ExtractTableNamesFromNextStatement(
      &location, zetasql::AnalyzerOptions(language_options), &table_names,
      &at_end_of_input));

  for (const std::vector<std::string>& table_name : table_names) {
    ExtractTableNamesFromNextStatementResponse_TableName* table_name_field =
        response->add_table_name();
    for (const std::string& name_segment : table_name) {
      table_name_field->add_table_name_segment(name_segment);
    }
  }

  response->set_resume_byte_position(location.byte_position());

  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::SerializeResolvedOutput(
    const AnalyzerOutput* output, absl::string_view statement,
    AnalyzeResponse* response, RegisteredCatalogState* state) {
  const std::vector<const google::protobuf::DescriptorPool*>& pools =
      state->GetDescriptorPools();
  FileDescriptorSetMap file_descriptor_set_map;
  PopulateExistingPoolsToFileDescriptorSetMap(pools, &file_descriptor_set_map);

  if (output->resolved_statement() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(output->resolved_statement()->SaveTo(
        &file_descriptor_set_map, response->mutable_resolved_statement()));
  } else {
    ZETASQL_RETURN_IF_ERROR(output->resolved_expr()->SaveTo(
        &file_descriptor_set_map, response->mutable_resolved_expression()));
  }

  // If the file_descriptor_set_map contains more descriptor pools than those
  // passed in the request, the additonal one must be the generated descriptor
  // pool. The reason is that some built-in functions use the DatetimePart
  // enum whose descriptor comes from the generated pool.
  // TODO: Describe the descriptor pool passing contract in detail
  // with a doc, and put a link here.
  if (file_descriptor_set_map.size() != pools.size()) {
    ZETASQL_RET_CHECK_EQ(file_descriptor_set_map.size(), pools.size() + 1)
        << "Analyzer result of " << statement
        << " uses unknown DescriptorPool, this shouldn't happen.";
    const auto& entry =
        file_descriptor_set_map.at(google::protobuf::DescriptorPool::generated_pool());
    ZETASQL_RET_CHECK_NE(entry.get(), nullptr)
        << "Analyzer result of " << statement
        << " uses unknown DescriptorPool, this shouldn't happen.";
    ZETASQL_RET_CHECK_EQ(entry->descriptor_set_index, pools.size())
        << "Analyzer result of " << statement
        << " uses unknown DescriptorPool, this shouldn't happen.";
  }

  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::FormatSql(
    const FormatSqlRequest& request, FormatSqlResponse* response) {
  return ::zetasql::FormatSql(request.sql(), response->mutable_sql());
}

absl::Status ZetaSqlLocalServiceImpl::RegisterCatalog(
    const RegisterCatalogRequest& request, RegisterResponse* response) {
  std::unique_ptr<RegisteredCatalogState> state(new RegisteredCatalogState());
  ZETASQL_RETURN_IF_ERROR(
      state->Init(request.simple_catalog(), request.file_descriptor_set()));

  int64_t id = registered_catalogs_->Register(state.release());
  ZETASQL_RET_CHECK_NE(-1, id) << "Failed to register catalog, this shouldn't happen.";

  response->set_registered_id(id);

  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::UnregisterCatalog(int64_t id) {
  if (registered_catalogs_->Delete(id)) {
    return absl::OkStatus();
  }
  return MakeSqlError() << "Unknown catalog ID: " << id;
}

absl::Status ZetaSqlLocalServiceImpl::GetBuiltinFunctions(
    const ZetaSQLBuiltinFunctionOptionsProto& proto,
    GetBuiltinFunctionsResponse* resp) {
  TypeFactory factory;
  std::map<std::string, std::unique_ptr<Function>> functions;
  ZetaSQLBuiltinFunctionOptions options(proto);

  zetasql::GetZetaSQLFunctions(&factory, options, &functions);

  FileDescriptorSetMap map;
  for (const auto& function : functions) {
    ZETASQL_RETURN_IF_ERROR(function.second->Serialize(&map, resp->add_function()));
  }

  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::GetLanguageOptions(
    const LanguageOptionsRequest& request, LanguageOptionsProto* response) {
  zetasql::LanguageOptions options;
  if (request.has_maximum_features() && request.maximum_features()) {
    options.EnableMaximumLanguageFeatures();
  }
  if (request.has_language_version()) {
    options.SetLanguageVersion(request.language_version());
  }
  options.Serialize(response);
  return absl::OkStatus();
}

absl::Status ZetaSqlLocalServiceImpl::GetAnalyzerOptions(
    const AnalyzerOptionsRequest& request, AnalyzerOptionsProto* response) {
  zetasql::AnalyzerOptions options;
  FileDescriptorSetMap unused_map;
  return options.Serialize(&unused_map, response);
}

size_t ZetaSqlLocalServiceImpl::NumSavedPreparedExpression() const {
  return prepared_expressions_->NumSavedStates();
}

}  // namespace local_service
}  // namespace zetasql
