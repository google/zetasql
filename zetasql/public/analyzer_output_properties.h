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

#ifndef ZETASQL_PUBLIC_ANALYZER_OUTPUT_PROPERTIES_H_
#define ZETASQL_PUBLIC_ANALYZER_OUTPUT_PROPERTIES_H_

#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/target_syntax.h"
#include "absl/base/attributes.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"

namespace zetasql {

class AnalyzerOutputProperties {
 public:
  // TODO: Remove when external references drop to zero.
  ABSL_DEPRECATED(
      "Client code should consider this struct internal. "
      "It doesn't mean what you think it means.")
  bool has_anonymization = false;  // NOLINT

  // Marks the given `rewrite` as being applicable to the resolved AST.
  ABSL_DEPRECATED(
      "REWRITE_ANONYMIZATION is the only rewrite still checked through this "
      "mechanism.")
  void MarkRelevant(ResolvedASTRewrite rewrite) {
    relevant_rewrites_.insert(rewrite);
    if (rewrite == REWRITE_ANONYMIZATION) {
      has_anonymization = true;
    }
  }

  // Returns true if the rewrite was marked relevant by the resolver.
  ABSL_DEPRECATED(
      "REWRITE_ANONYMIZATION is the only rewrite still checked through this "
      "mechanism.")
  bool IsRelevant(ResolvedASTRewrite rewrite) const {
    return relevant_rewrites_.contains(rewrite);
  }

 private:
  // Defined in zetasql/common/internal_analyzer_output_properties.h.
  friend class InternalAnalyzerOutputProperties;

  absl::btree_set<ResolvedASTRewrite> relevant_rewrites_;
  TargetSyntaxMap target_syntax_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANALYZER_OUTPUT_PROPERTIES_H_
