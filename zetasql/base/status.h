//
// Copyright 2018 ZetaSQL Authors
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

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUS_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUS_H_

#include "absl/status/status.h"  

// This is better than CHECK((val).ok()) because the embedded
// error string gets printed by the CHECK_EQ.
#define ZETASQL_CHECK_OK(val) CHECK_EQ(::absl::OkStatus(), (val))
#define ZETASQL_DCHECK_OK(val) DCHECK_EQ(::absl::OkStatus(), (val))
#define ZETASQL_ZETASQL_CHECK_OK(val) DCHECK_EQ(::absl::OkStatus(), (val))

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUS_H_
