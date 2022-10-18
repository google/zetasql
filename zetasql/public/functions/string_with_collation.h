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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_STRING_WITH_COLLATION_H_
#define ZETASQL_PUBLIC_FUNCTIONS_STRING_WITH_COLLATION_H_

#include <string>
#include <vector>

#include "zetasql/public/collator.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

// REPLACE(COLLATOR, STRING, STRING, STRING) -> STRING
// Returns true if the method executed successfully, false if an error was
// encountered.
bool ReplaceUtf8WithCollation(const ZetaSqlCollator& collator,
                              absl::string_view str, absl::string_view oldsub,
                              absl::string_view newsub, std::string* out,
                              absl::Status* status);

// SPLIT(COLLATOR, STRING, STRING) -> ARRAY<STRING>
// Returns true if the method executed successfully, false if an error was
// encountered.
bool SplitUtf8WithCollation(const ZetaSqlCollator& collator,
                            absl::string_view str, absl::string_view delimiter,
                            std::vector<absl::string_view>* out,
                            absl::Status* status);

// INSTR(COLLATOR, STRING, STRING, [INT, [INT]]) -> INT
// Returns true if the method executed successfully, false if an error was
// encountered.
bool StrPosOccurrenceUtf8WithCollation(const ZetaSqlCollator& collator,
                                       absl::string_view str,
                                       absl::string_view substr, int64_t pos,
                                       int64_t occurrence, int64_t* out,
                                       absl::Status* status);

// STRPOS(COLLATOR, STRING, STRING) -> INT64
inline bool StrposUtf8WithCollation(const ZetaSqlCollator& collator,
                                    absl::string_view str,
                                    absl::string_view substr, int64_t* out,
                                    absl::Status* status) {
  return StrPosOccurrenceUtf8WithCollation(collator, str, substr, /*pos=*/1,
                                           /*occurrence=*/1, out, status);
}

// STARTS_WITH(COLLATOR, STRING, STRING) -> BOOL
// Returns true if the method executed successfully, false if an error was
// encountered.
bool StartsWithUtf8WithCollation(const ZetaSqlCollator& collator,
                                 absl::string_view str,
                                 absl::string_view substr, bool* out,
                                 absl::Status* status);

// ENDS_WITH(COLLATOR, STRING, STRING) -> BOOL
// Returns true if the method executed successfully, false if an error was
// encountered.
bool EndsWithUtf8WithCollation(const ZetaSqlCollator& collator,
                               absl::string_view str, absl::string_view substr,
                               bool* out, absl::Status* status);

// Returns true when the <text> matches the <pattern> with collation specified
// in the <collator>. The <pattern> can have '%' specifiers which represent 0 or
// more characters. This function searches the <pattern> using ICU StringSearch
// API and follows the rules specified by the collation.
absl::StatusOr<bool> LikeWithUtf8WithCollation(
    absl::string_view text, absl::string_view pattern,
    const ZetaSqlCollator& collator);
}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_STRING_WITH_COLLATION_H_
