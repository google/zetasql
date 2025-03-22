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

#ifndef ZETASQL_ANALYZER_REWRITERS_PRIVACY_APPROX_COUNT_DISTINCT_UTILITY_H_
#define ZETASQL_ANALYZER_REWRITERS_PRIVACY_APPROX_COUNT_DISTINCT_UTILITY_H_

#include <memory>

#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/proto/anon_output_with_report.pb.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/functions/differential_privacy.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace zetasql {
namespace differential_privacy {
namespace approx_count_distinct_utility {

// Scans the aggregate list of `node` for `approx_count_distinct` calls. If a
// specific call is identified to count unique privacy ids, then that call is
// replaced by the following simpler call that counts distinct privacy ids:
// (DP_)SUM(1, constribution_bounds_per_group =>(0,1)).
absl::StatusOr<std::unique_ptr<ResolvedDifferentialPrivacyAggregateScan>>
ReplaceApproxCountDistinctOfPrivacyIdCallsBySimplerAggregations(
    std::unique_ptr<ResolvedDifferentialPrivacyAggregateScan> node,
    const ResolvedExpr* uid_expr, Resolver* resolver,
    TypeFactory* type_factory);

// Returns true if `aggregate_list` contains a DP approx_count_distinct call.
// False otherwise.
absl::StatusOr<bool> HasApproxCountDistinctAggregation(
    absl::Span<const std::unique_ptr<const ResolvedComputedColumnBase>>
        aggregate_list);

// Performs the rewrites specified in
// (broken link).
absl::StatusOr<std::unique_ptr<ResolvedScan>>
PerformApproxCountDistinctRewrites(
    std::unique_ptr<ResolvedDifferentialPrivacyAggregateScan> dp_scan,
    const ResolvedExpr* noisy_count_distinct_privacy_ids_expr,
    bool has_approx_count_distinct, Resolver* resolver,
    ColumnFactory* allocator, AnalyzerOptions* analyzer_options,
    Catalog* catalog, TypeFactory* type_factory);

// Performs the rewrites specified in
// (broken link).
absl::StatusOr<std::unique_ptr<ResolvedScan>>
PerformApproxCountDistinctRewrites(
    std::unique_ptr<ResolvedAnonymizedAggregateScan> dp_scan,
    const ResolvedExpr* noisy_count_distinct_privacy_ids_expr,
    bool has_approx_count_distinct, Resolver* resolver,
    ColumnFactory* allocator, AnalyzerOptions* analyzer_options,
    Catalog* catalog, TypeFactory* type_factory);

}  // namespace approx_count_distinct_utility
}  // namespace differential_privacy
}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_PRIVACY_APPROX_COUNT_DISTINCT_UTILITY_H_
