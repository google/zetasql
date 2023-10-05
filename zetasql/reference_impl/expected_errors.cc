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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/compliance/matchers.h"
#include "absl/status/status.h"

namespace zetasql {

std::unique_ptr<MatcherCollection<absl::Status>> ReferenceExpectedErrorMatcher(
    std::string matcher_name) {
  std::vector<std::unique_ptr<MatcherBase<absl::Status>>> error_matchers;
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kUnimplemented,
      "Unsupported built-in function: (st_accum|st_askml|st_buffer|"
      "st_bufferwithtolerance|st_geogfromkml|st_simplify|st_unaryunion)"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kResourceExhausted,
      "The statement has been aborted because the statement deadline (.+) was "
      "exceeded\\."));
  // TABLESAMPLE is not supported by the reference implementation.
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument, "TABLESAMPLE not supported"));
  // The reference implementation does not support KMS and AEAD envelope
  // encryption functions since they depend on an external service.
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kUnimplemented,
      "Unsupported built-in function: kms.*"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kUnimplemented,
      "Unsupported built-in function: aead\\.envelope.*"));
  // The reference implementation does not support KEYS.KEYSET_CHAIN function.
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kUnimplemented,
      "Unsupported built-in function: keys\\.keyset_chain"));
  // b/111212209
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Checking the presence of scalar field .* is not supported for proto3"));
  // The RQG can produce assignments to repeated proto values that contain
  // NULL.
  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kOutOfRange,
      "Cannot store a NULL element in repeated proto field"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kUnimplemented,
      "Unsupported built-in function: \\$(?:safe_)?proto_map_at_key"));
  // b/160778032
  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kInternal,
      "functions::IsValidDate(decoded_date_value) Invalid date"));
  // b/173659202
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Cannot execute a nested (DELETE|INSERT|UPDATE) statement on a "
      "NULL array value"));
  // b/182819630
  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
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

  // TODO: RQG should not generate proto expressions for protos in
  // zetasql.functions.* as they are often "special" (e.g. only allowed as
  // arguments to particular functions during analysis). However, at the moment
  // we do generate these things.
  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kInvalidArgument,
      "Type not found: `zetasql.functions."));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      ".*(Unrecognized name|Function not found): "
      "(YEAR|MONTH|DAY|DAYOFWEEK|DAYOFYEAR|QUARTER|HOUR|MINUTE|SECOND|"
      "MILLISECOND|MICROSECOND|NANOSECOND|DATE|WEEK|DATETIME|TIME"
      "ISOWEEK|ISOYEAR|NFC|NFKC|NFD|NFKD|FIRST|LAST).*"));

  // TODO: Certain expressions have a built-in proto type that
  // isn't necessarily in the catalog. Right now, the STRUCT builder includes
  // that built-in type from google.protobuf. in the STRUCT declaration, e.x.
  // STRUCT< Field_1 `google.protobuf.Int64Value`, ... > (TO_PROTO(...),...) The
  // standard catalog setups do not include these types by default, so this
  // leads to a "Type not found: 'google.protobuf.Int64Value'" error. For now,
  // we allowlist this case. We should either figure out how to support this or
  // avoid generating it.
  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kInvalidArgument, "Type not found: `google.protobuf."));
  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kInvalidArgument, "Type not found: `google.type."));

  // D3A_* calls can be generated in a way that returns an invalid argument
  // from the evaluator/executor.
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kInvalidArgument,
      ".*Unexpected aggregator type:.*Expected D3A.*"));

  // RQG can generate various casts / proto accesses that will create floating
  // point errors.
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange, "Floating point error in function.*"));

  // Aggregate values can overflow in large queries. However, this might not be
  // the behavior other engines would expect.
  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kResourceExhausted,
      "Aggregate values are limited to "));

  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kResourceExhausted, "Arrays are limited to "));

  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kResourceExhausted,
      "Cannot construct array Value larger than "));

  error_matchers.emplace_back(std::make_unique<StatusSubstringMatcher>(
      absl::StatusCode::kResourceExhausted,
      "Out of memory for MemoryAccountant"));

  // Expected errors for JSON_OBJECT.
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Invalid input to JSON_OBJECT: The (keys|values) array cannot be NULL"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Invalid input to JSON_OBJECT: A key cannot be NULL"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Invalid input to JSON_OBJECT: The number of keys and values must "
      "match"));

  // Expected errors for COSINE_DISTANCE, EUCLIDEAN_DISTANCE, EDIT_DISTANCE.
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange, "Array length mismatch:"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Cannot compute.*distance against zero vector"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange,
      "Duplicate index.*found in the input array"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange, "NULL array element"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange, "NULL struct field"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange, "max_distance must be non-negative"));
  error_matchers.emplace_back(std::make_unique<StatusRegexMatcher>(
      absl::StatusCode::kOutOfRange, "EDIT_DISTANCE.*invalid UTF8 string"));

  return std::make_unique<MatcherCollection<absl::Status>>(
      matcher_name, std::move(error_matchers));
}

}  // namespace zetasql
