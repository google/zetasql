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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_BITCAST_H_
#define ZETASQL_PUBLIC_FUNCTIONS_BITCAST_H_

// This file implements basic bitcast operations. The following functions
// are defined:
//
//   bool BitCast(TIN in1, TOUT *out, absl::Status* error);
//
// Here TIN and TOUT can be one of the following types: int32_t, int64_t, uint32_t,
// uint64_t.

#include "absl/base/casts.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

template <typename TIN, typename TOUT>
bool BitCast(TIN in, TOUT *out, absl::Status* error) {
  *out = absl::bit_cast<TOUT>(in);
  return true;
}

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_BITCAST_H_
