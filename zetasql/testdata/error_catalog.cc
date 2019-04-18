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

#include "zetasql/testdata/error_catalog.h"

#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

class Constant;
class Function;
class Procedure;
class TableValuedFunction;
class Type;

zetasql_base::Status ErrorCatalog::Create(
      ::zetasql_base::StatusCode code,
      std::unique_ptr<ErrorCatalog>* error_catalog) {
  ZETASQL_RET_CHECK_NE(::zetasql_base::OK, code);
  error_catalog->reset(new ErrorCatalog(code));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ErrorCatalog::FindTable(
    const absl::Span<const std::string>& path, const Table** table,
    const FindOptions& options) {
  return ::zetasql_base::StatusBuilder(error_code_, ZETASQL_LOC) << "FindTable error";
}

zetasql_base::Status ErrorCatalog::FindFunction(
    const absl::Span<const std::string>& path, const Function** function,
    const FindOptions& options) {
  return ::zetasql_base::StatusBuilder(error_code_, ZETASQL_LOC) << "FindFunction error";
}

zetasql_base::Status ErrorCatalog::FindTableValuedFunction(
    const absl::Span<const std::string>& path, const TableValuedFunction** function,
    const FindOptions& options) {
  return ::zetasql_base::StatusBuilder(error_code_, ZETASQL_LOC)
      << "FindTableValuedFunction error";
}

zetasql_base::Status ErrorCatalog::FindProcedure(const absl::Span<const std::string>& path,
                                         const Procedure** procedure,
                                         const FindOptions& options) {
  return ::zetasql_base::StatusBuilder(error_code_, ZETASQL_LOC) << "FindProcedure error";
}

zetasql_base::Status ErrorCatalog::FindType(const absl::Span<const std::string>& path,
                                    const Type** type,
                                    const FindOptions& options) {
  return ::zetasql_base::StatusBuilder(error_code_, ZETASQL_LOC) << "FindType error";
}

zetasql_base::Status ErrorCatalog::FindConstantWithPathPrefix(
    const absl::Span<const std::string> path, int* num_names_consumed,
    const Constant** constant, const FindOptions& options) {
  return ::zetasql_base::StatusBuilder(error_code_, ZETASQL_LOC) << "FindConstant error";
}

}  // namespace zetasql
