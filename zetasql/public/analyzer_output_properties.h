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

#ifndef ZETASQL_PUBLIC_ANALYZER_OUTPUT_PROPERTIES_H_
#define ZETASQL_PUBLIC_ANALYZER_OUTPUT_PROPERTIES_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/container/flat_hash_map.h"

namespace zetasql {

struct AnalyzerOutputProperties {
  // True if a ResolvedFlatten AST node was generated in the analyzer output.
  bool has_flatten = false;

  // Indicates if anonymization was found during analysis.  When resolving
  // queries, anonymization is found if an AnonymizedAggregateScan is
  // created.  When resolving expressions, anonymization is found if an
  // AnonymizedAggregateScan is created (for a subquery expression) or an
  // anonymized aggregate function call is present.
  bool has_anonymization = false;

  // A map from ResolvedTableScan to ResolvedAnonymizedAggregateScan.
  absl::flat_hash_map<const ResolvedTableScan*,
                      const ResolvedAnonymizedAggregateScan*>
      resolved_table_scan_to_anonymized_aggregate_scan_map;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANALYZER_OUTPUT_PROPERTIES_H_
