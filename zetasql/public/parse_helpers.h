//
// Copyright 2019 ZetaSQL Authors
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

#ifndef ZETASQL_PUBLIC_PARSE_HELPERS_H_
#define ZETASQL_PUBLIC_PARSE_HELPERS_H_

#include <string>

#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {

struct ParseResumeLocation;

// Parses the <sql> statement and returns OK if the statement is valid
// ZetaSQL syntax.  Otherwise returns the parser error, formatted according
// to <error_message_mode>.  Scripting statements are not supported.
zetasql_base::Status IsValidStatementSyntax(absl::string_view sql,
                                    ErrorMessageMode error_message_mode);

// Similar to the previous, but checks the validity of the next statement
// starting from <resume_location>.  If the syntax is valid then returns OK
// and sets <at_end_of_input> and updates <resume_location> to indicate the
// start of the next statement (if not at end of input).
//
// This method can be invoked iteratively on a multi-statement std::string to
// validate that all the statements in the std::string are valid syntax.
zetasql_base::Status IsValidNextStatementSyntax(
    ParseResumeLocation* resume_location, ErrorMessageMode error_message_mode,
    bool* at_end_of_input);

// Parse the first few keywords from <input> (ignoring whitespace, comments
// and hints) to determine what kind of statement it is (if it is valid).
// Scripting statements are not supported.
//
// Returns a ResolvedNodeKind enum for one of the subclasses of
// ResolvedStatement.  e.g. RESOLVED_QUERY_STMT or RESOLVED_EXPLAIN_STMT.
//
// If <input> cannot be any known statement type, returns RESOLVED_LITERAL
// (a non-statement) as a sentinel.
ResolvedNodeKind GetStatementKind(const std::string& input);

// Converts ASTNodeKind to ResolvedNodeKind.
ResolvedNodeKind GetStatementKind(ASTNodeKind node_kind);

// Same as GetStatementKind, but determines the statement kind for the next
// statement starting from <resume_location>.
ResolvedNodeKind GetNextStatementKind(
    const ParseResumeLocation& resume_location);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_PARSE_HELPERS_H_
