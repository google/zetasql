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

#ifndef ZETASQL_PUBLIC_REMOTE_TVF_FACTORY_H_
#define ZETASQL_PUBLIC_REMOTE_TVF_FACTORY_H_

#include <memory>

#include "zetasql/public/catalog.h"
#include "zetasql/public/module_details.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/base/nullability.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Interface for engine callbacks that create a remote table valued function.
class RemoteTvfFactory {
 public:
  virtual ~RemoteTvfFactory() = default;
  // Creates a remote table valued function given the resolved statement and
  // module details. `module_resolution_catalog` is the catalog that was used
  // to resolve the module that contains the TVF. Since it contains everything
  // resolved from the module, it is not recommended to use this catalog
  // directly as it gives access to the internal details of the module.
  // Implementations should use a zetasql::CatalogWrapper to filter out only
  // what is needed for the TVF.
  virtual absl::StatusOr<std::unique_ptr<TableValuedFunction>> CreateRemoteTVF(
      const ResolvedCreateTableFunctionStmt& stmt, const ModuleDetails& details,
      zetasql::Catalog* /*absl_nonnull*/ module_resolution_catalog) = 0;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_REMOTE_TVF_FACTORY_H_
