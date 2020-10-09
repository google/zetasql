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

#include "zetasql/parser/parse_tree_errors.h"

#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// Sometimes we make an error pointing at an expression like 1+2 and since
// the Plus is the root AST node, the error points at the '+' character.
// This traverses left-most children of a node and updates <location> with
// the earliest position it finds.
static void FindMinimalErrorLocation(const ASTNode* parent_node,
                                     ParseLocationPoint* error_location) {
  // TODO This still won't find the real start character for
  // a parenthesized expression like (1+2).  It will point at the '1'.
  const ASTNode* node = parent_node;
  while (node->num_children() > 0) {
    node = node->child(0);
    ZETASQL_DCHECK(node != nullptr);
    if (node->GetParseLocationRange().start() < *error_location) {
      *error_location = node->GetParseLocationRange().start();
    }
  }
}

ParseLocationPoint GetErrorLocationPoint(const ASTNode* ast_node,
                                         bool include_leftmost_child) {
  // For extra safety, try not to crash while constructing errors.
  ZETASQL_DCHECK(ast_node != nullptr);
  if (ast_node != nullptr) {
    ParseLocationPoint error_location =
        ast_node->GetParseLocationRange().start();
    if (include_leftmost_child) {
      FindMinimalErrorLocation(ast_node, &error_location);
    }
    return error_location;
  }
  return {};
}

absl::Status StatusWithInternalErrorLocation(const absl::Status& status,
                                             const ASTNode* ast_node,
                                             bool include_leftmost_child) {
  if (status.ok()) return status;

  return StatusWithInternalErrorLocation(
      status, GetErrorLocationPoint(ast_node, include_leftmost_child));
}

absl::Status MakeStatusWithErrorLocation(absl::StatusCode code,
                                         absl::string_view message,
                                         const std::string& filename,
                                         const std::string& query,
                                         const ASTNode* ast_node,
                                         bool include_leftmost_child) {
  const absl::Status status =
      zetasql_base::StatusBuilder(code).Attach(
          MakeInternalErrorLocation(ast_node, filename, include_leftmost_child))
      << message;
  return ConvertInternalErrorLocationToExternal(status, query);
}

InternalErrorLocation MakeInternalErrorLocation(
    const ASTNode* ast_node, absl::string_view filename,
    bool include_leftmost_child) {
  InternalErrorLocation internal_error_location =
      GetErrorLocationPoint(
          ast_node, include_leftmost_child).ToInternalErrorLocation();
  if (!filename.empty()) {
    internal_error_location.set_filename(std::string(filename));
  }
  return internal_error_location;
}

absl::Status WrapNestedErrorStatus(const ASTNode* ast_location,
                                   const std::string& error_message,
                                   const absl::Status& input_status,
                                   ErrorMessageMode error_source_mode) {
  zetasql_base::StatusBuilder error_status_builder =
      absl::IsInternal(input_status) ? zetasql_base::StatusBuilder(input_status)
                                     : MakeSqlError();
  return error_status_builder.Attach(
             SetErrorSourcesFromStatus(MakeInternalErrorLocation(ast_location),
                                       input_status, error_source_mode))
         << error_message;
}

}  // namespace zetasql
