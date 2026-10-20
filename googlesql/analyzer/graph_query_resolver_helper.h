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

#ifndef GOOGLESQL_ANALYZER_GRAPH_QUERY_RESOLVER_HELPER_H_
#define GOOGLESQL_ANALYZER_GRAPH_QUERY_RESOLVER_HELPER_H_

#include <memory>

#include "googlesql/analyzer/name_scope.h"
#include "googlesql/parser/parse_tree.h"

namespace googlesql {

// With support for quantified path patterns, the output name list on
// resolving any graph/path/element pattern will always contain 2 lists:
// 1) <singleton_name_list> contains all the singleton variables (variables
// that are not quantified).
// 2) <group_name_list> contains all the group variables (variables that are
// quantified).
//
// Note that whether a variable is treated as a singleton or group variable
// is context specific. For example:
// (
//  ((a) -[e]-> (b <elem_scope>) WHERE <inner_scope>){1, 3}
//  WHERE <outer_scope>
// )
// - <a>, <e>, <b> are singleton variables in the scope of <inner_scope>
// - <b> is a singleton in <elem_scope>;
// - However they are all group variables in the scope of <outer_scope>.
struct GraphTableNamedVariables {
  // Describes the context which outputs GraphTableNamedVariables.
  const ASTNode* ast_node;
  // Contains all the singleton variables for this 'ast_node'
  std::shared_ptr<NameList> singleton_name_list = std::make_shared<NameList>();
  // Contains all the group variables for this 'ast_node'
  std::shared_ptr<NameList> group_name_list = std::make_shared<NameList>();

  // Used during the resolution of a CALL subquery, to keep the correlated
  // variables from the outside scope. The subquery can have multiple linear
  // operators, so these correlated names travel all along.
  // Always nullptr (not just an empty list) unless we're inside the body of
  // of a CALL.
  //
  // INVARIANT: Names in the other lists are not allowed to conflict with
  // names in this list, unlike outer names which would simply be shadowed.
  // i.e., this list is always disjoint with the other lists.
  std::shared_ptr<NameList> correlated_name_list = nullptr;
};

// Context passed during GQL query resolution to track statement state and
// syntax hints.
struct GqlQueryContext {
  // True if this is the first statement in a top-level graph query.
  bool is_first_statement_in_graph_query = false;
  // True if this query is nested inside a GRAPH_TABLE subquery or a GQL
  // subquery (for example, CALL {...} or EXISTS {...}).
  bool is_nested = false;
  // True if this query appears inside a GQL set operation.
  bool is_set_op = false;
};

// Represents the output of most ResolveGraph/Gql* statements.
template <typename NodeType>
struct ResolvedGraphWithNameList {
  // The output resolved node produced by the call.
  std::unique_ptr<NodeType> resolved_node;
  // The output graph namelists produced by the call.
  GraphTableNamedVariables graph_name_lists;
};

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_GRAPH_QUERY_RESOLVER_HELPER_H_
