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

#include "zetasql/public/type.h"
#include "zetasql/public/types/annotation.h"

namespace zetasql {

// Collation allows users to specify alternative rules for comparing strings.
// This class defines rules to propagate collation annotation for ResolvedAst
// nodes. Check comments on each method for propagation behavior on each kind of
// node.
class CollationAnnotation : public AnnotationSpec {
 public:
  CollationAnnotation() {}

  static int GetId() { return static_cast<int>(AnnotationKind::kCollation); }

  int Id() const override { return GetId(); }

  // TODO: implement propagation rules for functions.
  absl::Status CheckAndPropagateForFunctionCall(
      const ResolvedFunctionCall& function_call,
      AnnotationMap* result_annotation_map) override {
    ZETASQL_RET_CHECK_FAIL() << "Not implemented";
  }

  // Replicates collation from <column_ref>.column to <result_annotation_map>.
  absl::Status CheckAndPropagateForColumnRef(
      const ResolvedColumnRef& column_ref,
      AnnotationMap* result_annotation_map) override;

  // Replicates collation from the referenced struct field to
  // <result_annotation_map>.
  absl::Status CheckAndPropagateForGetStructField(
      const ResolvedGetStructField& get_struct_field,
      AnnotationMap* result_annotation_map) override;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANNOTATION_COLLATION_H_
