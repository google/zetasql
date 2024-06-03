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

#include "zetasql/analyzer/rewriters/privacy/privacy_utility.h"

#include "zetasql/common/errors.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {
zetasql_base::StatusBuilder MakeSqlErrorAtNode(const ResolvedNode& node) {
  zetasql_base::StatusBuilder builder = MakeSqlError();
  const auto* parse_location = node.GetParseLocationRangeOrNULL();
  if (parse_location != nullptr) {
    builder.AttachPayload(parse_location->start().ToInternalErrorLocation());
  }
  return builder;
}
}  // namespace zetasql
