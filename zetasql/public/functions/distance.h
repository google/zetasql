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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_DISTANCE_H_
#define ZETASQL_PUBLIC_FUNCTIONS_DISTANCE_H_

// This code contains implementations for the *_DISTANCE functions.

#include <optional>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"

namespace zetasql {
namespace functions {

// Implementation of:
// COSINE_DISTANCE(ARRAY<DOUBLE>, ARRAY<DOUBLE>) -> DOUBLE
absl::StatusOr<Value> CosineDistanceDense(Value vector1, Value vector2);

// Implementation of:
// COSINE_DISTANCE(ARRAY<STRUCT<INT64, DOUBLE>>, ARRAY<STRUCT<INT64,
// DOUBLE>>) -> DOUBLE
absl::StatusOr<Value> CosineDistanceSparseInt64Key(Value vector1,
                                                   Value vector2);

// Implementation of:
// COSINE_DISTANCE(ARRAY<STRUCT<STRING, DOUBLE>>, ARRAY<STRUCT<STRING,
// DOUBLE>>) -> DOUBLE
absl::StatusOr<Value> CosineDistanceSparseStringKey(Value vector1,
                                                    Value vector2);

// Implementation of:
// COSINE_DISTANCE(ARRAY<DOUBLE>, ARRAY<DOUBLE>) -> DOUBLE
absl::StatusOr<Value> EuclideanDistanceDense(Value vector1, Value vector2);

// Implementation of:
// COSINE_DISTANCE(ARRAY<STRUCT<INT64, DOUBLE>>, ARRAY<STRUCT<INT64,
// DOUBLE>>) -> DOUBLE
absl::StatusOr<Value> EuclideanDistanceSparseInt64Key(Value vector1,
                                                      Value vector2);

// Implementation of:
// COSINE_DISTANCE(ARRAY<STRUCT<STRING, DOUBLE>>, ARRAY<STRUCT<STRING,
// DOUBLE>>) -> DOUBLE
absl::StatusOr<Value> EuclideanDistanceSparseStringKey(Value vector1,
                                                       Value vector2);

// Implementation of:
// EDIT_DISTANCE(STRING, STRING) -> INT64
// EDIT_DISTANCE(BYTES, BYTES) -> INT64
absl::StatusOr<Value> EditDistance(Value v1, Value v2,
                                   std::optional<Value> max_distance);

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_DISTANCE_H_
