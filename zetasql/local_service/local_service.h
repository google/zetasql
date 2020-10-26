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

#ifndef ZETASQL_LOCAL_SERVICE_LOCAL_SERVICE_H_
#define ZETASQL_LOCAL_SERVICE_LOCAL_SERVICE_H_

#include <stddef.h>
#include <memory>

#include "zetasql/local_service/local_service.pb.h"
#include "zetasql/proto/options.pb.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/parse_resume_location.pb.h"
#include <cstdint>
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace local_service {

class PreparedExpressionPool;
class PreparedExpressionState;
class RegisteredCatalogPool;
class RegisteredCatalogState;
class RegisteredParseResumeLocationPool;

// Implementation of ZetaSqlLocalService RPC service.
class ZetaSqlLocalServiceImpl {
 public:
  ZetaSqlLocalServiceImpl();
  ZetaSqlLocalServiceImpl(const ZetaSqlLocalServiceImpl&) = delete;
  ZetaSqlLocalServiceImpl& operator=(const ZetaSqlLocalServiceImpl&) =
      delete;
  ~ZetaSqlLocalServiceImpl();

  absl::Status Prepare(const PrepareRequest& request,
                       PrepareResponse* response);

  absl::Status Unprepare(int64_t id);

  absl::Status Evaluate(const EvaluateRequest& request,
                        EvaluateResponse* response);

  absl::Status EvaluateImpl(const EvaluateRequest& request,
                            PreparedExpressionState* state,
                            EvaluateResponse* response);

  absl::Status GetTableFromProto(const TableFromProtoRequest& request,
                                 SimpleTableProto* response);

  absl::Status GetBuiltinFunctions(
      const ZetaSQLBuiltinFunctionOptionsProto& proto,
      GetBuiltinFunctionsResponse* resp);

  absl::Status AddSimpleTable(const AddSimpleTableRequest& request);

  absl::Status Analyze(const AnalyzeRequest& request,
                       AnalyzeResponse* response);

  absl::Status AnalyzeImpl(const AnalyzeRequest& request,
                           RegisteredCatalogState* catalog_state,
                           ParseResumeLocation* location,
                           AnalyzeResponse* response);

  absl::Status AnalyzeExpressionImpl(const AnalyzeRequest& request,
                                     RegisteredCatalogState* catalog_state,
                                     AnalyzeResponse* response);

  absl::Status BuildSql(const BuildSqlRequest& request,
                        BuildSqlResponse* response);

  // Note, this also can handle scripts.
  absl::Status ExtractTableNamesFromStatement(
      const ExtractTableNamesFromStatementRequest& request,
      ExtractTableNamesFromStatementResponse* response);

  // Note, this does not handle scripts.
  absl::Status ExtractTableNamesFromNextStatement(
      const ExtractTableNamesFromNextStatementRequest& request,
      ExtractTableNamesFromNextStatementResponse* response);

  absl::Status SerializeResolvedOutput(const AnalyzerOutput* output,
                                       absl::string_view statement,
                                       AnalyzeResponse* response,
                                       RegisteredCatalogState* state);

  absl::Status FormatSql(const FormatSqlRequest& request,
                         FormatSqlResponse* response);

  absl::Status RegisterCatalog(const RegisterCatalogRequest& request,
                               RegisterResponse* response);

  absl::Status UnregisterCatalog(int64_t id);

  absl::Status RegisterParseResumeLocation(
      const ParseResumeLocationProto& location, RegisterResponse* response);

  absl::Status UnregisterParseResumeLocation(int64_t id);

  absl::Status GetLanguageOptions(const LanguageOptionsRequest& request,
                                  LanguageOptionsProto* response);

  absl::Status GetAnalyzerOptions(const AnalyzerOptionsRequest& request,
                                  AnalyzerOptionsProto* response);

 private:
  std::unique_ptr<RegisteredCatalogPool> registered_catalogs_;
  std::unique_ptr<PreparedExpressionPool> prepared_expressions_;
  std::unique_ptr<RegisteredParseResumeLocationPool>
      registered_parse_resume_locations_;

  absl::Status RegisterPrepared(std::unique_ptr<PreparedExpressionState>& state,
                                PreparedState* response);

  // For testing.
  size_t NumSavedPreparedExpression() const;

  friend class ZetaSqlLocalServiceImplTest;
};

}  // namespace local_service
}  // namespace zetasql

#endif  // ZETASQL_LOCAL_SERVICE_LOCAL_SERVICE_H_
