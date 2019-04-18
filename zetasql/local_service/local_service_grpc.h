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

#ifndef ZETASQL_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_
#define ZETASQL_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_

#include "zetasql/local_service/local_service.grpc.pb.h"
#include "zetasql/local_service/local_service.h"
#include "zetasql/local_service/local_service.pb.h"
#include "zetasql/proto/options.pb.h"
#include "zetasql/public/parse_resume_location.pb.h"
#include "zetasql/public/simple_table.pb.h"

namespace zetasql {
namespace local_service {

// Implementation of ZetaSqlLocalService Grpc RPC service.
class ZetaSqlLocalServiceGrpcImpl
    : public ZetaSqlLocalService::Service {
 public:

  grpc::Status GetTableFromProto(grpc::ServerContext* context,
                                 const TableFromProtoRequest* req,
                                 SimpleTableProto* resp) override;

  grpc::Status Analyze(grpc::ServerContext* context, const AnalyzeRequest* req,
                       AnalyzeResponse* resp) override;

  grpc::Status ExtractTableNamesFromStatement(
      grpc::ServerContext* context,
      const ExtractTableNamesFromStatementRequest* req,
      ExtractTableNamesFromStatementResponse* resp) override;

  grpc::Status ExtractTableNamesFromNextStatement(
      grpc::ServerContext* context,
      const ExtractTableNamesFromNextStatementRequest* req,
      ExtractTableNamesFromNextStatementResponse* resp) override;

  grpc::Status RegisterCatalog(grpc::ServerContext* context,
                               const RegisterCatalogRequest* req,
                               RegisterResponse* resp) override;

  grpc::Status UnregisterCatalog(grpc::ServerContext* context,
                                 const UnregisterRequest* req,
                                 google::protobuf::Empty* unused) override;

  grpc::Status RegisterParseResumeLocation(
      grpc::ServerContext* context,
      const ParseResumeLocationProto* parse_resume_location,
      RegisterResponse* resp) override;

  grpc::Status UnregisterParseResumeLocation(
      grpc::ServerContext* context, const UnregisterRequest* req,
      google::protobuf::Empty* unused) override;

  grpc::Status GetBuiltinFunctions(
      grpc::ServerContext* context,
      const ZetaSQLBuiltinFunctionOptionsProto* options,
      GetBuiltinFunctionsResponse* resp) override;

  grpc::Status AddSimpleTable(grpc::ServerContext* context,
                              const AddSimpleTableRequest* req,
                              google::protobuf::Empty* unused) override;

  grpc::Status GetLanguageOptions(grpc::ServerContext* context,
                                  const LanguageOptionsRequest* req,
                                  LanguageOptionsProto* resp) override;

 private:
  ZetaSqlLocalServiceImpl service_;
};

}  // namespace local_service
}  // namespace zetasql

#endif  // ZETASQL_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_
