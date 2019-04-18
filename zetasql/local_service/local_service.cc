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
#include "zetasql/local_service/state.h"
#include "zetasql/proto/simple_catalog.pb.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/table_from_proto.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace local_service {

using google::protobuf::RepeatedPtrField;

namespace {

// Populate the existing pools into the map with existing indices, to make sure
// the serialized type will use the same indices.
void PopulateExistingPoolsToFileDescriptorSetMap(
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    FileDescriptorSetMap* file_descriptor_set_map) {
  file_descriptor_set_map->clear();

  for (int i = 0; i < pools.size(); ++i) {
    std::unique_ptr<Type::FileDescriptorEntry>& entry =
        (*file_descriptor_set_map)[pools[i]];
    CHECK_EQ(entry.get(), nullptr);
    entry = absl::make_unique<Type::FileDescriptorEntry>();
    entry->descriptor_set_index = i;
  }

  CHECK_EQ(pools.size(), file_descriptor_set_map->size());
}

}  // namespace

// This class is thread-safe.
class BaseSavedState : public GenericState {
 protected:
  BaseSavedState() : GenericState(), initialized_(false) {}

  zetasql_base::Status Init(const RepeatedPtrField<google::protobuf::FileDescriptorSet>& fdsets) {
    absl::MutexLock lock(&mutex_);
    CHECK(!initialized_);

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

    return ::zetasql_base::OkStatus();
  }

  zetasql_base::Status static MergeFileDescriptorSetsToPools(
      const RepeatedPtrField<google::protobuf::FileDescriptorSet>& fdsets,
      std::vector<std::unique_ptr<google::protobuf::DescriptorPool>>* pools,
      std::vector<const google::protobuf::DescriptorPool*>* const_pools) {
    const int original_num_pools = pools->size();
    const int num_pools = std::max(fdsets.size(), original_num_pools);
    pools->reserve(num_pools);
    const_pools->reserve(num_pools);

    int i = 0;
    for (const auto& file_descirptor_set : fdsets) {
      if (i < original_num_pools) {
        ZETASQL_RETURN_IF_ERROR(
            AddFileDescriptorSetToPool(
                &file_descirptor_set, (*pools)[i].get()));
      } else {
        std::unique_ptr<google::protobuf::DescriptorPool> pool(
                    new google::protobuf::DescriptorPool());
        ZETASQL_RETURN_IF_ERROR(
            AddFileDescriptorSetToPool(&file_descirptor_set, pool.get()));
        const_pools->emplace_back(pool.get());
        pools->emplace_back(pool.release());
      }
      i++;
    }

    return ::zetasql_base::OkStatus();
  }

 public:
  TypeFactory* GetTypeFactory() {
    absl::MutexLock lock(&mutex_);
    CHECK(initialized_);

    return &factory_;
  }

  const std::vector<const google::protobuf::DescriptorPool*>& GetDescriptorPools() {
    absl::MutexLock lock(&mutex_);
    CHECK(initialized_);

    return const_pools_;
  }

 protected:
  absl::Mutex mutex_;

  bool initialized_ GUARDED_BY(mutex_);

  TypeFactory factory_ GUARDED_BY(mutex_);

  std::vector<std::unique_ptr<google::protobuf::DescriptorPool>> pools_
      GUARDED_BY(mutex_);
  std::vector<const google::protobuf::DescriptorPool*> const_pools_ GUARDED_BY(mutex_);
};

class RegisteredCatalogState : public BaseSavedState {
 public:
  RegisteredCatalogState() : BaseSavedState() {}
  RegisteredCatalogState(const RegisteredCatalogState&) = delete;
  RegisteredCatalogState& operator=(const RegisteredCatalogState&) = delete;

  zetasql_base::Status Init(const SimpleCatalogProto& proto,
                    const RepeatedPtrField<google::protobuf::FileDescriptorSet>& fdsets) {
    ZETASQL_RETURN_IF_ERROR(BaseSavedState::Init(fdsets));

    absl::MutexLock lock(&mutex_);
    ZETASQL_RETURN_IF_ERROR(SimpleCatalog::Deserialize(proto, const_pools_, &catalog_));
    initialized_ = true;
    return ::zetasql_base::OkStatus();
  }

  SimpleCatalog* GetCatalog() {
    absl::MutexLock lock(&mutex_);
    CHECK(initialized_);
    return catalog_.get();
  }

  zetasql_base::Status AddSimpleTable(const AddSimpleTableRequest& request) {
    absl::MutexLock lock(&mutex_);
    std::unique_ptr<SimpleTable> table;
    ZETASQL_RETURN_IF_ERROR(
        MergeFileDescriptorSetsToPools(request.file_descriptor_set(),
                                       &pools_, &const_pools_));
    ZETASQL_RETURN_IF_ERROR(SimpleTable::Deserialize(request.table(),
                                             const_pools_,
                                             &factory_, &table));
    catalog_->AddOwnedTable(table.release());
    return ::zetasql_base::OkStatus();
  }

 private:
  std::unique_ptr<SimpleCatalog> catalog_ GUARDED_BY(mutex_);
};

class RegisteredCatalogPool : public SharedStatePool<RegisteredCatalogState> {};

class RegisteredParseResumeLocationState : public GenericState {
 public:
  explicit RegisteredParseResumeLocationState(
      const ParseResumeLocationProto& proto)
      : GenericState(),
        parse_resume_location_(ParseResumeLocation::FromProto(proto)) {}

  RegisteredParseResumeLocationState(const RegisteredParseResumeLocationState&)
      = delete;
  RegisteredParseResumeLocationState& operator=(
      const RegisteredParseResumeLocationState&) = delete;

  // Get a pointer to the registered ParseResumeLocation object,
  // which will be locked for the life duration of mutex_lock.
  // Caller must no longer use the pointer after the mutex_lock
  // goes out of scope.
  ParseResumeLocation* GetParseResumeLocation(
      std::unique_ptr<absl::MutexLock>* mutex_lock) {
    *mutex_lock = absl::make_unique<absl::MutexLock>(&mutex_);
    return &parse_resume_location_;
  }

 private:
  ParseResumeLocation parse_resume_location_ GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

class RegisteredParseResumeLocationPool
    : public SharedStatePool<RegisteredParseResumeLocationState> {};

ZetaSqlLocalServiceImpl::ZetaSqlLocalServiceImpl()
    : registered_catalogs_(new RegisteredCatalogPool()),
      registered_parse_resume_locations_(
          new RegisteredParseResumeLocationPool()) {}

ZetaSqlLocalServiceImpl::~ZetaSqlLocalServiceImpl() {}

zetasql_base::Status ZetaSqlLocalServiceImpl::GetTableFromProto(
    const TableFromProtoRequest& request, SimpleTableProto* response) {
  TypeFactory factory;
  google::protobuf::DescriptorPool pool;
  ZETASQL_RETURN_IF_ERROR(
      AddFileDescriptorSetToPool(&request.file_descriptor_set(), &pool));
  const google::protobuf::Descriptor* proto_descr =
      pool.FindMessageTypeByName(request.proto().proto_name());
  if (proto_descr == nullptr) {
    return ::zetasql_base::UnknownErrorBuilder(ZETASQL_LOC) << "Proto type name not found: "
                                                << request.proto().proto_name();
  }
  if (proto_descr->file()->name() != request.proto().proto_file_name()) {
    return ::zetasql_base::UnknownErrorBuilder(ZETASQL_LOC)
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
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::Analyze(const AnalyzeRequest& request,
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

  RegisteredParseResumeLocationState* parse_resume_location_state;
  std::unique_ptr<RegisteredParseResumeLocationState>
      owned_parse_resume_location_state;
  ParseResumeLocation* location = nullptr;
  std::unique_ptr<absl::MutexLock> lock;

  if (request.has_registered_parse_resume_location()) {
    int64_t id = request.registered_parse_resume_location().registered_id();
    std::shared_ptr<RegisteredParseResumeLocationState> shared_state =
        registered_parse_resume_locations_->Get(id);
    parse_resume_location_state = shared_state.get();
    if (parse_resume_location_state == nullptr) {
      return MakeSqlError() << "Registered parse resume location " << id
                            << " unknown.";
    }
    location = parse_resume_location_state->GetParseResumeLocation(&lock);
    location->set_byte_position(
        request.registered_parse_resume_location().byte_position());
  } else if (request.has_parse_resume_location()) {
    owned_parse_resume_location_state =
        absl::make_unique<RegisteredParseResumeLocationState>(

            request.parse_resume_location());
    parse_resume_location_state = owned_parse_resume_location_state.get();
    location = parse_resume_location_state->GetParseResumeLocation(&lock);
  }
  return AnalyzeImpl(request, catalog_state, location, response);
}

zetasql_base::Status ZetaSqlLocalServiceImpl::AnalyzeImpl(
    const AnalyzeRequest& request, RegisteredCatalogState* catalog_state,
    ParseResumeLocation* location, AnalyzeResponse* response) {
  AnalyzerOptions options;
  ZETASQL_RETURN_IF_ERROR(AnalyzerOptions::Deserialize(
      request.options(), catalog_state->GetDescriptorPools(),
      catalog_state->GetTypeFactory(), &options));

  if (!(request.has_sql_statement() || location != nullptr)) {
    return ::zetasql_base::UnknownErrorBuilder(ZETASQL_LOC)
           << "Unrecognized AnalyzeRequest target " << request.target_case();
  }

  std::unique_ptr<const AnalyzerOutput> output;
  TypeFactory factory;

  if (request.has_sql_statement()) {
    const std::string& sql = request.sql_statement();

    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(
        sql, options, catalog_state->GetCatalog(), &factory, &output));

    ZETASQL_RETURN_IF_ERROR(
        SerializeResolvedStatement(output.get(), sql, response, catalog_state));
  } else if (location != nullptr) {
    bool at_end_of_input;
    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeNextStatement(
        location, options, catalog_state->GetCatalog(), &factory, &output,
        &at_end_of_input));

    ZETASQL_RETURN_IF_ERROR(SerializeResolvedStatement(
        output.get(), location->input(), response, catalog_state));
    response->set_resume_byte_position(location->byte_position());
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::ExtractTableNamesFromStatement(
    const ExtractTableNamesFromStatementRequest& request,
    ExtractTableNamesFromStatementResponse* response) {
  zetasql::TableNamesSet table_names;
  ZETASQL_RETURN_IF_ERROR(zetasql::ExtractTableNamesFromStatement(
      request.sql_statement(), zetasql::AnalyzerOptions{}, &table_names));
  for (std::vector<std::string> table_name : table_names) {
    ExtractTableNamesFromStatementResponse_TableName* table_name_field =
        response->add_table_name();
    for (std::string name_segment : table_name) {
      table_name_field->add_table_name_segment(name_segment);
    }
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::ExtractTableNamesFromNextStatement(
    const ExtractTableNamesFromNextStatementRequest& request,
    ExtractTableNamesFromNextStatementResponse* response) {
  auto parse_resume_location_state =
    absl::make_unique<RegisteredParseResumeLocationState>(
    request.parse_resume_location());
  std::unique_ptr<absl::MutexLock> lock;
  ParseResumeLocation* location =
      parse_resume_location_state->GetParseResumeLocation(&lock);

  LanguageOptions language_options = request.has_options() ?
      LanguageOptions(request.options()) :
      LanguageOptions();

  bool at_end_of_input;
  zetasql::TableNamesSet table_names;
  ZETASQL_RETURN_IF_ERROR(zetasql::ExtractTableNamesFromNextStatement(
      location,
      zetasql::AnalyzerOptions(language_options),
      &table_names,
      &at_end_of_input
  ));

  for (const std::vector<std::string>& table_name : table_names) {
    ExtractTableNamesFromNextStatementResponse_TableName* table_name_field =
        response->add_table_name();
    for (const std::string& name_segment : table_name) {
      table_name_field->add_table_name_segment(name_segment);
    }
  }

  response->set_resume_byte_position(location->byte_position());

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::SerializeResolvedStatement(
    const AnalyzerOutput* output, absl::string_view statement,
    AnalyzeResponse* response, RegisteredCatalogState* state) {
  const std::vector<const google::protobuf::DescriptorPool*>& pools =
      state->GetDescriptorPools();
  FileDescriptorSetMap file_descriptor_set_map;
  PopulateExistingPoolsToFileDescriptorSetMap(pools, &file_descriptor_set_map);

  ZETASQL_RETURN_IF_ERROR(output->resolved_statement()->SaveTo(
      &file_descriptor_set_map, response->mutable_resolved_statement()));

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

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::RegisterCatalog(
    const RegisterCatalogRequest& request, RegisterResponse* response) {
  std::unique_ptr<RegisteredCatalogState> state(new RegisteredCatalogState());
  ZETASQL_RETURN_IF_ERROR(
      state->Init(request.simple_catalog(), request.file_descriptor_set()));

  int64_t id = registered_catalogs_->Register(state.release());
  ZETASQL_RET_CHECK_NE(-1, id) << "Failed to register catalog, this shouldn't happen.";

  response->set_registered_id(id);

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::UnregisterCatalog(int64_t id) {
  if (registered_catalogs_->Delete(id)) {
    return ::zetasql_base::OkStatus();
  }
  return MakeSqlError() << "Unknown catalog ID: " << id;
}

zetasql_base::Status ZetaSqlLocalServiceImpl::RegisterParseResumeLocation(
    const ParseResumeLocationProto& location, RegisterResponse* response) {
  int64_t id = registered_parse_resume_locations_->Register(
      new RegisteredParseResumeLocationState(location));
  ZETASQL_RET_CHECK_NE(-1, id)
      << "Failed to register ParseResumeLocation, this shouldn't happen.";

  response->set_registered_id(id);

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::UnregisterParseResumeLocation(
    int64_t id) {
  if (registered_parse_resume_locations_->Delete(id)) {
    return ::zetasql_base::OkStatus();
  }
  return MakeSqlError() << "Unknown ParseResumeLocation ID: " << id;
}

zetasql_base::Status ZetaSqlLocalServiceImpl::GetBuiltinFunctions(
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

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::AddSimpleTable(
    const AddSimpleTableRequest& request) {
  int64_t id = request.registered_catalog_id();
  std::shared_ptr<RegisteredCatalogState> shared_state =
      registered_catalogs_->Get(id);
  if (shared_state == nullptr) {
    return MakeSqlError() << "Unknown catalog ID: " << id;
  }
  RegisteredCatalogState* state = shared_state.get();
  ZETASQL_RETURN_IF_ERROR(state->AddSimpleTable(request));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ZetaSqlLocalServiceImpl::GetLanguageOptions(
    const LanguageOptionsRequest& request, LanguageOptionsProto* response) {
  zetasql::LanguageOptions options;
  if (request.has_maximum_features() && request.maximum_features()) {
    options.EnableMaximumLanguageFeatures();
  }
  if (request.has_language_version()) {
    options.SetLanguageVersion(request.language_version());
  }
  options.Serialize(response);
  return ::zetasql_base::OkStatus();
}

}  // namespace local_service
}  // namespace zetasql
