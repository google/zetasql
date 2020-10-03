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

#ifndef ZETASQL_SCRIPTING_BREAK_CONTINUE_CONTEXT_H_
#define ZETASQL_SCRIPTING_BREAK_CONTINUE_CONTEXT_H_

#include <vector>

#include "zetasql/parser/parse_tree.h"

namespace zetasql {

// Describes contextual information about a BREAK or CONTINUE statement,
// including which loop it will continue or break out of, and which begin/end
// blocks are exited when the BREAK/CONTINUE statement executes.
class BreakContinueContext {
 public:
  BreakContinueContext() = default;
  BreakContinueContext(const ASTLoopStatement* enclosing_loop,
                       std::vector<const ASTBeginEndBlock*> blocks_to_exit)
      : enclosing_loop_(enclosing_loop),
        blocks_to_exit_(std::move(blocks_to_exit)) {}
  BreakContinueContext(const BreakContinueContext& context) = default;
  BreakContinueContext(BreakContinueContext&& context) = default;
  BreakContinueContext& operator=(const BreakContinueContext& context) =
      default;
  BreakContinueContext& operator=(BreakContinueContext&& context) = default;

  // The loop that the statement should continue or break out of.
  const ASTLoopStatement* enclosing_loop() const { return enclosing_loop_; }

  // Blocks which must be exited when jumping from the current break/continue
  // statement to the end of the loop.  All variables declared in these blocks
  // will go out of scope when the break/continue statement is executed.
  const std::vector<const ASTBeginEndBlock*>& blocks_to_exit() const {
    return blocks_to_exit_;
  }

 private:
  const ASTLoopStatement* enclosing_loop_ = nullptr;
  std::vector<const ASTBeginEndBlock*> blocks_to_exit_;
};
}  // namespace zetasql

#endif  // ZETASQL_SCRIPTING_BREAK_CONTINUE_CONTEXT_H_
