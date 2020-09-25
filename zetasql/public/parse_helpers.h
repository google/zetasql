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

#ifndef ZETASQL_PUBLIC_PARSE_HELPERS_H_
#define ZETASQL_PUBLIC_PARSE_HELPERS_H_

#include <string>

#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {

struct ParseResumeLocation;

// Parses the <sql> statement and returns OK if the statement is valid
// ZetaSQL syntax.  Otherwise returns the parser error, formatted according
// to <error_message_mode>.  Scripting statements are not supported.
//
// TODO: Take LanguageOptions as input, which controls some parser
// behavior.
absl::Status IsValidStatementSyntax(absl::string_view sql,
                                    ErrorMessageMode error_message_mode);

// Similar to the previous, but checks the validity of the next statement
// starting from <resume_location>.  If the syntax is valid then returns OK
// and sets <at_end_of_input> and updates <resume_location> to indicate the
// start of the next statement (if not at end of input).
//
// This method can be invoked iteratively on a multi-statement string to
// validate that all the statements in the string are valid syntax.
absl::Status IsValidNextStatementSyntax(
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
//
// TODO: Add LanguageOptions to this, which controls some
// parser behaviors.
ResolvedNodeKind GetStatementKind(const std::string& input);

// Converts ASTNodeKind to ResolvedNodeKind.
ResolvedNodeKind GetStatementKind(ASTNodeKind node_kind);

// Same as GetStatementKind, but determines the statement kind for the next
// statement starting from <resume_location>.
//
// TODO: Make LanguageOptions non-optional, since they control some
// parser behaviors.
ResolvedNodeKind GetNextStatementKind(
    const ParseResumeLocation& resume_location,
    const LanguageOptions& language_options = LanguageOptions());

// Properties of a statement, currently including the statement kind, the
// statement category (DDL, DML, etc.), statement level hints, and whether
// the statement creates a TEMP object.
//
// Note that script statements are not currently supported.
struct StatementProperties {
  enum StatementCategory {
    // Statement category unknown, for cases where we cannot determine the
    // statement type (e.g., invalid statement syntax)
    UNKNOWN = 0,  // Statement category unknown
    SELECT = 1,   // SELECT (including WITH)
    DML = 2,      // INSERT, UPDATE, DELETE, MERGE
    DDL = 3,      // CREATE (including CTAS), DROP, ALTER
    // The OTHER category currently contains a wide range of statement
    // kinds that do not clearly fit into the categories above, and may
    // be broken down into more categories in the future.
    OTHER = 4
  };

  // There is no UNKNOWN or similar ResolvedNodeKind, so default to
  // RESOLVED_LITERAL (which is also what GetStatementKind() defaults to for
  // unrecognized statement kinds).
  ResolvedNodeKind node_kind = RESOLVED_LITERAL;
  StatementCategory statement_category = StatementCategory::UNKNOWN;

  // Only applies if <statement_type> is DDL (and the statement kind is
  // RESOLVED_CREATE_*).
  bool is_create_temporary_object = false;

  // Statement level hints, mapping from hint name to hint expression.
  // The mapping is on the actual SQL text indicated by the ParseLocation
  // of the left and right sides of the hint assignment.  For example, for
  // the following statement level hint:
  //
  //   @{ a.b = concat("a", "b")} SELECT...
  //
  // The mapping is from 'a.b' -> 'concat("a", "b")'.
  absl::flat_hash_map<std::string, std::string> statement_level_hints;
};

// Returns the statement properties, including ResolvedNodeKind, statement
// type (SELECT, DDL, DML, etc.), and others.  Currently fetches properties
// that do not require parsing the entire statement.  Can be extended to
// return additional properties over time, as needed.
//
// <language_options> impacts parsing behavior.
//
// Returns OK for invalid syntax, with an UNKNOWN statement node kind.  Only
// returns internal errors.
absl::Status GetStatementProperties(const std::string& input,
                                    const LanguageOptions& language_options,
                                    StatementProperties* statement_properties);

// Same as GetStatementProperties, but determines the statement properties for
// the next statement starting from <resume_location>.
absl::Status GetNextStatementProperties(
    const ParseResumeLocation& resume_location,
    const LanguageOptions& language_options,
    StatementProperties* statement_properties);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_PARSE_HELPERS_H_
