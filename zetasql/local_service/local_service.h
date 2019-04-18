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

  zetasql_base::Status GetTableFromProto(const TableFromProtoRequest& request,
                                 SimpleTableProto* response);

  zetasql_base::Status GetBuiltinFunctions(
      const ZetaSQLBuiltinFunctionOptionsProto& proto,
      GetBuiltinFunctionsResponse* resp);

  zetasql_base::Status AddSimpleTable(const AddSimpleTableRequest& request);

  zetasql_base::Status Analyze(const AnalyzeRequest& request,
                       AnalyzeResponse* response);

  zetasql_base::Status AnalyzeImpl(const AnalyzeRequest& request,
                           RegisteredCatalogState* catalog_state,
                           ParseResumeLocation* location,
                           AnalyzeResponse* response);

  zetasql_base::Status ExtractTableNamesFromStatement(
      const ExtractTableNamesFromStatementRequest& request,
      ExtractTableNamesFromStatementResponse* response);

  zetasql_base::Status ExtractTableNamesFromNextStatement(
      const ExtractTableNamesFromNextStatementRequest& request,
      ExtractTableNamesFromNextStatementResponse* response);

  zetasql_base::Status SerializeResolvedStatement(const AnalyzerOutput* output,
                                          absl::string_view statement,
                                          AnalyzeResponse* response,
                                          RegisteredCatalogState* state);

  zetasql_base::Status RegisterCatalog(const RegisterCatalogRequest& request,
                               RegisterResponse* response);

  zetasql_base::Status UnregisterCatalog(int64_t id);

  zetasql_base::Status RegisterParseResumeLocation(
      const ParseResumeLocationProto& location, RegisterResponse* response);

  zetasql_base::Status UnregisterParseResumeLocation(int64_t id);

  zetasql_base::Status GetLanguageOptions(const LanguageOptionsRequest& request,
                                  LanguageOptionsProto* response);

 private:
  std::unique_ptr<RegisteredCatalogPool> registered_catalogs_;
  std::unique_ptr<RegisteredParseResumeLocationPool>
      registered_parse_resume_locations_;

  friend class ZetaSqlLocalServiceImplTest;
};

}  // namespace local_service
}  // namespace zetasql

#endif  // ZETASQL_LOCAL_SERVICE_LOCAL_SERVICE_H_
