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

#include "zetasql/local_service/local_service_grpc.h"

#include "zetasql/base/status.h"

namespace zetasql {
namespace local_service {

namespace {

grpc::Status ToGrpcStatus(absl::Status status) {
  if (status.ok()) {
    return grpc::Status();
  }
  grpc::StatusCode grpc_code;
  switch (status.code()) {
    case absl::StatusCode::kCancelled:
      grpc_code = grpc::CANCELLED;
      break;
    case absl::StatusCode::kInvalidArgument:
      grpc_code = grpc::INVALID_ARGUMENT;
      break;
    case absl::StatusCode::kDeadlineExceeded:
      grpc_code = grpc::DEADLINE_EXCEEDED;
      break;
    case absl::StatusCode::kNotFound:
      grpc_code = grpc::NOT_FOUND;
      break;
    case absl::StatusCode::kAlreadyExists:
      grpc_code = grpc::ALREADY_EXISTS;
      break;
    case absl::StatusCode::kPermissionDenied:
      grpc_code = grpc::PERMISSION_DENIED;
      break;
    case absl::StatusCode::kResourceExhausted:
      grpc_code = grpc::RESOURCE_EXHAUSTED;
      break;
    case absl::StatusCode::kFailedPrecondition:
      grpc_code = grpc::FAILED_PRECONDITION;
      break;
    case absl::StatusCode::kAborted:
      grpc_code = grpc::ABORTED;
      break;
    case absl::StatusCode::kOutOfRange:
      grpc_code = grpc::OUT_OF_RANGE;
      break;
    case absl::StatusCode::kUnimplemented:
      grpc_code = grpc::UNIMPLEMENTED;
      break;
    case absl::StatusCode::kInternal:
      grpc_code = grpc::INTERNAL;
      break;
    case absl::StatusCode::kUnavailable:
      grpc_code = grpc::UNAVAILABLE;
      break;
    case absl::StatusCode::kDataLoss:
      grpc_code = grpc::DATA_LOSS;
      break;
    case absl::StatusCode::kUnauthenticated:
      grpc_code = grpc::UNAUTHENTICATED;
      break;
    default:
      grpc_code = grpc::UNKNOWN;
  }
  return grpc::Status(grpc_code, std::string(status.message()), "");
}

}  // namespace

grpc::Status ZetaSqlLocalServiceGrpcImpl::Prepare(
    grpc::ServerContext* context, const PrepareRequest* req,
    PrepareResponse* resp) {
  return ToGrpcStatus(service_.Prepare(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::Unprepare(
    grpc::ServerContext* context, const UnprepareRequest* req,
    google::protobuf::Empty* unused) {
  return ToGrpcStatus(service_.Unprepare(req->prepared_expression_id()));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::Evaluate(
    grpc::ServerContext* context, const EvaluateRequest* req,
    EvaluateResponse* resp) {
  return ToGrpcStatus(service_.Evaluate(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::GetTableFromProto(
    grpc::ServerContext* context, const TableFromProtoRequest* req,
    SimpleTableProto* resp) {
  return ToGrpcStatus(service_.GetTableFromProto(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::Analyze(
    grpc::ServerContext* context, const AnalyzeRequest* req,
    AnalyzeResponse* resp) {
  return ToGrpcStatus(service_.Analyze(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::BuildSql(
    grpc::ServerContext* context, const BuildSqlRequest* req,
    BuildSqlResponse* resp) {
  return ToGrpcStatus(service_.BuildSql(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::ExtractTableNamesFromStatement(
    grpc::ServerContext* context,
    const ExtractTableNamesFromStatementRequest* req,
    ExtractTableNamesFromStatementResponse* resp) {
  return ToGrpcStatus(service_.ExtractTableNamesFromStatement(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::ExtractTableNamesFromNextStatement(
    grpc::ServerContext* context,
    const ExtractTableNamesFromNextStatementRequest* req,
    ExtractTableNamesFromNextStatementResponse* resp) {
  return ToGrpcStatus(service_.ExtractTableNamesFromNextStatement(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::FormatSql(
    grpc::ServerContext* context, const FormatSqlRequest* req,
    FormatSqlResponse* resp) {
  return ToGrpcStatus(service_.FormatSql(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::RegisterCatalog(
    grpc::ServerContext* context, const RegisterCatalogRequest* req,
    RegisterResponse* resp) {
  return ToGrpcStatus(service_.RegisterCatalog(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::UnregisterCatalog(
    grpc::ServerContext* context, const UnregisterRequest* req,
    google::protobuf::Empty* unused) {
  return ToGrpcStatus(service_.UnregisterCatalog(req->registered_id()));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::RegisterParseResumeLocation(
    grpc::ServerContext* context,
    const ParseResumeLocationProto* parse_resume_location,
    RegisterResponse* resp) {
  return ToGrpcStatus(
      service_.RegisterParseResumeLocation(*parse_resume_location, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::UnregisterParseResumeLocation(
    grpc::ServerContext* context, const UnregisterRequest* req,
    google::protobuf::Empty* unused) {
  return ToGrpcStatus(
      service_.UnregisterParseResumeLocation(req->registered_id()));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::GetBuiltinFunctions(
    grpc::ServerContext* context,
    const ZetaSQLBuiltinFunctionOptionsProto* options,
    GetBuiltinFunctionsResponse* resp) {
  return ToGrpcStatus(service_.GetBuiltinFunctions(*options, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::AddSimpleTable(
    grpc::ServerContext* context, const AddSimpleTableRequest* req,
    google::protobuf::Empty* unused) {
  return ToGrpcStatus(service_.AddSimpleTable(*req));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::GetLanguageOptions(
    grpc::ServerContext* context, const LanguageOptionsRequest* req,
    LanguageOptionsProto* resp) {
  return ToGrpcStatus(service_.GetLanguageOptions(*req, resp));
}

grpc::Status ZetaSqlLocalServiceGrpcImpl::GetParseTokens(grpc::ServerContext *context,
                                                         const GetParseTokensRequest *req,
                                                         GetParseTokensResponse *resp) {
  return ToGrpcStatus(service_.GetParseTokens(*req, resp));
}

}  // namespace local_service
}  // namespace zetasql
