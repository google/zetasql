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

#ifndef ZETASQL_RESOLVED_AST_NODE_SOURCES_H_
#define ZETASQL_RESOLVED_AST_NODE_SOURCES_H_

// `node_source` is a field of ResolvedScan that can be populated by the
// resolver, rewriters, or the random query generator to record why a node was
// added. The SQLBuilder may also use `node_source` to influence which query
// patterns to generate.

namespace zetasql {

// When the resolver needs to add a ProjectScan for set operations with
// CORRESPONDING (to select or reorder columns, or to pad NULL columns), it will
// populate the `node_source` of the added ProjectScan with this constant.
// SQLBuilder will try to generate a query using CORRESPONDING, expecting this
// ProjectScan is only used to match columns.
constexpr char kNodeSourceResolverSetOperationCorresponding[] =
    "resolver_set_operation_corresponding";

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_NODE_SOURCES_H_
