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

#ifndef ZETASQL_PARSER_PARSE_TREE_ERRORS_H_
#define ZETASQL_PARSER_PARSE_TREE_ERRORS_H_

// This header creates common parse-related error status factory functions for
// ZetaSQL. The factory functions return zetasql_base::StatusBuilder so that the error
// message can be constructed using << operators.
//
// We use util::INVALID_ARGUMENT for all SQL parsing and analysis errors.
// We use util::OUT_OF_RANGE for all runtime evaluation errors.
//
// ZetaSQL uses ErrorLocation and InternalErrorLocation as payloads on
// Status.  ErrorLocations are exposed through the public ZetaSQL interfaces,
// while InternalErrorLocations are used internally.  InternalErrorLocations
// are converted to ErrorLocations before returning them through public apis.
//
// ErrorLocations/InternalErrorLocations can contain a list of ErrorSources.
// An ErrorSource identifies a source error which caused the current error.
// The ErrorSource list is ordered, identifying a chain of dependencies
// between errors (i.e., error1 caused error2, which caused error3, etc.).
//
// These helpers are defined as follows:
//
//   // Return an error with code util::INVALID_ARGUMENT, and add an
//   // InternalErrorLocation payload pointing at ast_node's start location.
//   return MakeSqlErrorAt(ast_node) << "Message";
//
//   // Same as previous, but points at <ast_node>'s local location rather than
//   // the start of its leftmost child.  For 'abc=def', this returns a pointer
//   // to '=' rather than to 'a'.
//   return MakeSqlErrorAtLocalNode(ast_node) << "Message";
//
//   // Same as previous two, but decide whether to use the local location
//   // or the leftmost location depending on <use_local_node>.
//   return MakeSqlErrorAtNode(ast_node, use_local_node) << "Message";
//
// See also zetasql/common/errors.h for non parser-dependent error helpers.

#include <string>
#include <vector>

#include "zetasql/common/errors.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// Returns a ParseLocationPoint pointing at an ASTNode's start location.
//
// If <include_leftmost_child>, then for infix operators where the ASTNode's
// position points at the middle token, this function tries to extract the
// leftmost position of a child node instead.  For example, for "abc=def",
// this will point at 'a' rather than '='.
ParseLocationPoint GetErrorLocationPoint(const ASTNode* ast_node,
                                         bool include_leftmost_child);

// Returns MakeSqlError() annotated with the error location for <ast_node>.
// See GetErrorLocationPoint() for the meaning of <include_leftmost_child>.
inline ::zetasql_base::StatusBuilder MakeSqlErrorAtNode(const ASTNode* ast_node,
                                                bool include_leftmost_child) {
  return MakeSqlError().Attach(
      GetErrorLocationPoint(ast_node, include_leftmost_child)
          .ToInternalErrorLocation());
}

// Returns MakeSqlError() annotated with the error location given by the
// leftmost child of <ast_node>. See GetErrorLocationPoint() for the meaning
// of "leftmost child".
inline ::zetasql_base::StatusBuilder MakeSqlErrorAt(const ASTNode* ast_node) {
  return MakeSqlErrorAtNode(ast_node, /*include_leftmost_child=*/true);
}

// Returns MakeSqlError() annotated with the error location for <ast_node>
// itself. In contrast with MakeSqlErrorAt(), this does not use the error
// location for the leftmost child. I.e., for infix operators like "a = b" the
// error location will point at the operator "=", not at the start of the entire
// expression.
inline ::zetasql_base::StatusBuilder MakeSqlErrorAtLocalNode(const ASTNode* ast_node) {
  return MakeSqlErrorAtNode(ast_node, /*include_leftmost_child=*/false);
}

// This is a variant of ZETASQL_RETURN_IF_ERROR from status_macros.h that also
// converts errors to SQL errors and adds a location, overriding the existing
// location if one existed.
//
// This is used to add locations onto errors that propagate from parts of the
// system that don't have locations, like TypeFactory.
#define RETURN_SQL_ERROR_AT_IF_ERROR(ast_node, expr)                         \
  do {                                                                       \
    /* Using _status below to avoid capture problems if expr is "status". */ \
    const absl::Status _status = (expr);                                   \
    if (ABSL_PREDICT_FALSE(!_status.ok())) {                                 \
      return MakeSqlErrorAt((ast_node)) << _status.message();                \
    }                                                                        \
  } while (0)

// Returns a team policy that attaches a parse location to an error status
// without changing the error message or code. If the error status already has a
// location, that will be overridden by this one.
//
// Example:
//
//  ZETASQL_RETURN_IF_ERROR(DoAThing()).With(LocationOverride(ast_node));
//
inline auto LocationOverride(const ASTNode* node) {
  return [node](zetasql_base::StatusBuilder error) -> zetasql_base::StatusBuilder {
    return error.Attach(
        GetErrorLocationPoint(node, /*include_leftmost_child=*/true)
            .ToInternalErrorLocation());
  };
}

// If <status> is an error, returns a absl::Status with the
// InternalErrorLocation of the ErrorLocationPoint from <ast_node> attached.
// Otherwise, just returns <status>.
//
// Can be used in
//   ZETASQL_RETURN_IF_ERROR(StatusWithInternalErrorLocation(SomeFunction(),
//                                                   ast_location));
// to add ErrorLocations into error return paths that don't have locations.
absl::Status StatusWithInternalErrorLocation(
    const absl::Status& status, const ASTNode* ast_node,
    bool include_leftmost_child = true);

// Makes a new Status from <code> and <message> with an external ErrorLocation.
absl::Status MakeStatusWithErrorLocation(absl::StatusCode code,
                                         absl::string_view message,
                                         const std::string& filename,
                                         const std::string& query,
                                         const ASTNode* ast_node,
                                         bool include_leftmost_child = true);

// Returns an InternalErrorLocation pointing at ASTNode's start location,
// including the given <filename> (if present).
// See GetErrorLocationPoint() for the meaning of <include_leftmost_child>.
InternalErrorLocation MakeInternalErrorLocation(
    const ASTNode* ast_node, absl::string_view filename = "",
    bool include_leftmost_child = true);

// Creates and returns a new Status based on <ast_location> and
// <error_message>.  The returned Status has an InternalErrorLocation
// payload derived from <ast_location>, and that InternalErrorLocation
// payload contains a new ErrorSource that wraps the <input_status>
// based on <error_source_mode>.
absl::Status WrapNestedErrorStatus(const ASTNode* ast_location,
                                   const std::string& error_message,
                                   const absl::Status& input_status,
                                   ErrorMessageMode error_source_mode);

}  // namespace zetasql

#endif  // ZETASQL_PARSER_PARSE_TREE_ERRORS_H_
