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

#ifndef ZETASQL_ANALYZER_REWRITERS_ANONYMIZATION_HELPER_H_
#define ZETASQL_ANALYZER_REWRITERS_ANONYMIZATION_HELPER_H_

#include <memory>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/statusor.h"

namespace zetasql {

absl::StatusOr<std::unique_ptr<const ResolvedNode>> RewriteHelper(
    const ResolvedNode& tree, AnalyzerOptions options,
    ColumnFactory& column_factory, Catalog& catalog, TypeFactory& type_factory);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_ANONYMIZATION_HELPER_H_
