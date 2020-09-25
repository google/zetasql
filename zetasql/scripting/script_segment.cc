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

#include "zetasql/scripting/script_segment.h"

#include <cmath>
#include <cstdlib>
#include <stack>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/parse_location.h"

namespace zetasql {
namespace {
// Workaround for b/113538338: For some expression types, such as binary
// operations, node->GetParseLocationRange() returns only a subset of the range,
// where the start position is the operator position, rather than the actual
// expression start.
//
// To get the full range of an expression, we need calculate the union of all
// ParseLocationRange's of all subtrees.
ParseLocationRange GetFullParseLocationRange(const ASTNode* node) {
  if (!node->IsExpression()) {
    // b/113538338 applies only for expressions.  Other node types, such
    // as statements, we can just return the parse location range directly,
    // and avoid the traversal.
    return node->GetParseLocationRange();
  }

  ParseLocationRange full_range = node->GetParseLocationRange();
  std::stack<const ASTNode*> stack;
  stack.push(node);

  while (!stack.empty()) {
    const ASTNode* child_node = stack.top();
    stack.pop();

    ParseLocationRange child_range = child_node->GetParseLocationRange();
    full_range.set_start(std::min(full_range.start(), child_range.start()));
    full_range.set_end(std::max(full_range.end(), child_range.end()));

    for (int i = 0; i < child_node->num_children(); i++) {
      stack.push(child_node->child(i));
    }
  }
  return full_range;
}
}  // namespace

ScriptSegment::ScriptSegment(absl::string_view script, const ASTNode* node)
    : script_(script), node_(node), range_(GetFullParseLocationRange(node)) {}

ScriptSegment ScriptSegment::FromASTNode(absl::string_view script,
                                         const ASTNode* node) {
  return ScriptSegment(script, node);
}

absl::string_view ScriptSegment::GetSegmentText() const {
  return script_.substr(
      range_.start().GetByteOffset(),
      range_.end().GetByteOffset() - range_.start().GetByteOffset());
}

std::ostream& operator<<(std::ostream& os, const ScriptSegment& segment) {
  return os << "ScriptSegment: range " << segment.range() << " of script "
            << segment.script();
}

}  // namespace zetasql
