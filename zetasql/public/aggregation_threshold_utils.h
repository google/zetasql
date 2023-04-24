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

#ifndef ZETASQL_PUBLIC_AGGREGATION_THRESHOLD_UTILS_H_
#define ZETASQL_PUBLIC_AGGREGATION_THRESHOLD_UTILS_H_

#include <string>

#include "zetasql/public/analyzer_options.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/case.h"

namespace zetasql {
// Returns map of allowed options for ResolvedAggregationThresholdAggregateScan
// node. Make sure to update ResolvedAggregationThresholdAggregateScan when
// function name changes.
const absl::flat_hash_map<std::string, AllowedOptionProperties,
                          zetasql_base::StringViewCaseHash,
                          zetasql_base::StringViewCaseEqual>&
GetAllowedAggregationThresholdOptions();

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_AGGREGATION_THRESHOLD_UTILS_H_
