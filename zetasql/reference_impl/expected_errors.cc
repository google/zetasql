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

#include "zetasql/reference_impl/expected_errors.h"

#include <string>
#include <utility>
#include <vector>

#include "zetasql/compliance/matchers.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"

namespace zetasql {

std::unique_ptr<MatcherCollection<absl::Status>> ReferenceExpectedErrorMatcher(
    std::string matcher_name) {
  std::vector<std::unique_ptr<MatcherBase<absl::Status>>> error_matchers;
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Unsupported built-in function: (st_accum|st_askml|st_buffer|"
      "st_bufferwithtolerance|st_geogfromkml|st_simplify|st_unaryunion)"));
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kResourceExhausted,
      "The statement has been aborted because the statement deadline (.+) was "
      "exceeded\\."));
  // TABLESAMPLE is not supported by the reference implementation.
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument, "TABLESAMPLE not supported"));
  // The reference implementation does not support KMS and AEAD envelope
  // encryption functions since they depend on an external service.
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Unsupported built-in function: kms.*"));
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Unsupported built-in function: aead\\.envelope.*"));
  // The reference implementation does not support KEYS.KEYSET_CHAIN function.
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Unsupported built-in function: keys\\.keyset_chain"));
  // b/111212209
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Checking the presence of scalar field .* is not supported for proto3"));
  // The RQG can produce assignments to repeated proto values that contain
  // NULL.
  error_matchers.emplace_back(absl::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kOutOfRange,
      "Cannot store a NULL element in repeated proto field"));
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Unsupported built-in function: \\$(?:safe_)?proto_map_at_key"));
  // b/160778032
  error_matchers.emplace_back(absl::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kInternal,
      "functions::IsValidDate(decoded_date_value) Invalid date"));
  // b/173659202
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Cannot execute a nested (DELETE|INSERT|UPDATE) statement on a "
      "NULL array value"));
  // b/182819630
  error_matchers.emplace_back(absl::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kOutOfRange,
      "Attempted to modify an array element with multiple nested UPDATE "
      "statements"));

  // Moved over from the PrepareQuery expected errors as these occur in the
  // normal random query tests as well.
  // TODO: Figure out if it's possible to avoid generating invalid
  // PIVOT queries in RQG.
  error_matchers.emplace_back(
      new StatusRegexMatcher(absl::StatusCode::kUnimplemented,
                             "as (?:a )?PIVOT expression is not supported"));

  // TODO: Pivot does a rewrite which can create multiple columns
  // with the same id?
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInternal,
      "Resolved AST validation failed: ZETASQL_RET_CHECK failure .* Duplicate column "
      "id.*"));

  // TODO: RQG does not include
  // zetasql.functions.DateTimestampPart in the set of protos that the query
  // needs to know about.
  error_matchers.emplace_back(absl::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Type not found: `zetasql.functions.DateTimestampPart`"));

  // TODO: SQLBuilder does not take the fact that DateTimestampPart
  // names can only appear in this form in particular locations.
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      ".*(Unrecognized name|Function not found): "
      "(YEAR|MONTH|DAY|DAYOFWEEK|DAYOFYEAR|QUARTER|HOUR|MINUTE|SECOND|"
      "MILLISECOND|MICROSECOND|NANOSECOND|DATE|WEEK|DATETIME|TIME"
      "ISOWEEK|ISOYEAR).*"));

  // TODO: RQG should not generate DISTINCT with
  // PERCENTILE_CONT/DISC.
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Aggregate function PERCENTILE_(CONT|DISC) does not support "
      "DISTINCT in arguments"));
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "DISTINCT is not allowed for analytic function percentile_(cont|disc)"));

  return absl::make_unique<MatcherCollection<absl::Status>>(
      matcher_name, std::move(error_matchers));
}

std::unique_ptr<MatcherCollection<absl::Status>>
ReferenceInvalidInputErrorMatcher(std::string matcher_name) {
  std::vector<std::unique_ptr<MatcherBase<absl::Status>>> error_matchers;

  // Errors from relational_op.cc
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Limit requires non-null count and offset"));
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Limit requires non-negative count and offset"));
  error_matchers.emplace_back(absl::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange, "Enumerate requires non-null count"));

  return absl::make_unique<MatcherCollection<absl::Status>>(
      matcher_name, std::move(error_matchers));
}

}  // namespace zetasql
