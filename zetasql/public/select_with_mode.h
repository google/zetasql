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

#ifndef ZETASQL_PUBLIC_SELECT_WITH_MODE_H_
#define ZETASQL_PUBLIC_SELECT_WITH_MODE_H_

namespace zetasql {

// This enum describes the select mode that is introduced by SELECT WITH
// <identifier> clause.
enum class SelectWithMode {
  // Represents regular SELECT query.
  NONE,
  // Represents SELECT WITH ANONYMIZATION query - Differential Privacy query.
  // Although ANONYMIZATION and DIFFERENTIAL_PRIVACY are semantically similar in
  // most ways, there are a couple of differences e.g. list of valid options are
  // different and so we need to be able to distinguish between them.
  ANONYMIZATION,
  // Represents SELECT WITH DIFFERENTIAL_PRIVACY query - Differential Privacy
  // query. See comment above for why we have ANONYMIZATION and
  // DIFFERENTIAL_PRIVACY.
  DIFFERENTIAL_PRIVACY,
  // Represents SELECT WITH AGGREGATION_THRESHOLD query.
  AGGREGATION_THRESHOLD,
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_SELECT_WITH_MODE_H_
