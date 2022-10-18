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

#ifndef ZETASQL_PUBLIC_ANNOTATION_COLLATION_H_
#define ZETASQL_PUBLIC_ANNOTATION_COLLATION_H_

#include "zetasql/parser/ast_node.h"
#include "zetasql/public/annotation/default_annotation_spec.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/resolved_ast/resolved_ast.h"

namespace zetasql {

// Collation allows users to specify alternative rules for comparing strings.
// This class defines rules to propagate collation annotation for ResolvedAst
// nodes. Check comments on each method for propagation behavior on each kind of
// node.
class CollationAnnotation : public DefaultAnnotationSpec {
 public:
  CollationAnnotation() {}

  static int GetId() { return static_cast<int>(AnnotationKind::kCollation); }

  int Id() const override { return GetId(); }

  // Determines whether collation should be propagated to the function's result,
  // as defined by the function call's signature.  If appropriate, propagates
  // collation from the function argument(s) to <result_annotation_map>.
  // Currently only supports default collation propagation, which returns an
  // error if multiple arguments have different collation, or returns the
  // common collation otherwise.
  absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) override;

  // Returns false when <map> is nullptr or CollationAnnotation is not
  // present in <map> or any of its nested AnnotationMaps.
  static bool ExistsIn(const AnnotationMap* map) {
    return map != nullptr && map->Has<CollationAnnotation>();
  }

  // Validates that all collations present on function arguments (if any) are
  // consistent, and returns that collation.  Only function arguments which have
  // the option argument_collation_mode matching the <collation_mode_mask> are
  // considered.  Returns nullptr to indicate no (non-default) collation. Throws
  // an error if function arguments have different collations.  If non-null,
  // <error_location> is used for error messages.
  static absl::StatusOr<const AnnotationMap*> GetCollationFromFunctionArguments(
      const ASTNode* error_location,
      const ResolvedFunctionCallBase& function_call,
      FunctionEnums::ArgumentCollationMode collation_mode_mask);

  // Resolves the collation for ORDER BY item.
  static absl::Status ResolveCollationForResolvedOrderByItem(
      ResolvedOrderByItem* resolved_order_by_item);

  // Throws error if any function argument has collation annotation.
  static absl::Status RejectsCollationOnFunctionArguments(
      const ResolvedFunctionCallBase& function_call);

  // Similar to the implementation in the parent class, except it's not an error
  // if 'in' is NULL when 'out' has our annotation.
  absl::Status ScalarMergeIfCompatible(const AnnotationMap* in,
                                       AnnotationMap& out) const override;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANNOTATION_COLLATION_H_
