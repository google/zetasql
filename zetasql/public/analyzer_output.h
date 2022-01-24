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

#ifndef ZETASQL_PUBLIC_ANALYZER_OUTPUT_H_
#define ZETASQL_PUBLIC_ANALYZER_OUTPUT_H_

#include <memory>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"

namespace zetasql {
class AnalyzerOutput {
 public:
  AnalyzerOutput(
      std::shared_ptr<IdStringPool> id_string_pool,
      std::shared_ptr<zetasql_base::UnsafeArena> arena,
      std::unique_ptr<const ResolvedStatement> statement,
      const AnalyzerOutputProperties& analyzer_output_properties,
      std::unique_ptr<ParserOutput> parser_output,
      const std::vector<absl::Status>& deprecation_warnings,
      const QueryParametersMap& undeclared_parameters,
      const std::vector<const Type*>& undeclared_positional_parameters,
      int max_column_id);
  AnalyzerOutput(
      std::shared_ptr<IdStringPool> id_string_pool,
      std::shared_ptr<zetasql_base::UnsafeArena> arena,
      std::unique_ptr<const ResolvedExpr> expr,
      const AnalyzerOutputProperties& analyzer_output_properties,
      std::unique_ptr<ParserOutput> parser_output,
      const std::vector<absl::Status>& deprecation_warnings,
      const QueryParametersMap& undeclared_parameters,
      const std::vector<const Type*>& undeclared_positional_parameters,
      int max_column_id);
  AnalyzerOutput(const AnalyzerOutput&) = delete;
  AnalyzerOutput& operator=(const AnalyzerOutput&) = delete;
  ~AnalyzerOutput();

  // Present for output from AnalyzeStatement.
  // IdStrings in this resolved AST are allocated in the IdStringPool attached
  // to this AnalyzerOutput, and copies of those IdStrings will be valid only
  // if the IdStringPool is still alive.
  const ResolvedStatement* resolved_statement() const {
    return statement_.get();
  }

  // Present for output from AnalyzeExpression.
  // IdStrings in this resolved AST are allocated in the IdStringPool attached
  // to this AnalyzerOutput, and copies of those IdStrings will be valid only
  // if the IdStringPool is still alive.
  const ResolvedExpr* resolved_expr() const { return expr_.get(); }

  // These are warnings for use of deprecated features.
  // The statuses will have code INVALID_ARGUMENT and will include a location,
  // when possible. They will also have DeprecationWarning protos attached to
  // them.
  // If there are multiple warnings with the same error message and
  // DeprecationWarning::Kind, they will be deduplicated..
  const std::vector<absl::Status>& deprecation_warnings() const {
    return deprecation_warnings_;
  }
  void set_deprecation_warnings(const std::vector<absl::Status>& warnings) {
    deprecation_warnings_ = warnings;
  }

  // Returns the undeclared query parameters found in the query and their
  // inferred types. If none are present, returns an empty set.
  const QueryParametersMap& undeclared_parameters() const {
    return undeclared_parameters_;
  }

  // Returns undeclared positional parameters found the query and their inferred
  // types. The index in the vector corresponds with the position of the
  // undeclared parameter--for example, the first element in the vector is the
  // type of the undeclared parameter at position 1 and so on.
  const std::vector<const Type*>& undeclared_positional_parameters() const {
    return undeclared_positional_parameters_;
  }

  // Returns the IdStringPool that stores IdStrings allocated for the
  // resolved AST.  This was propagated from AnalyzerOptions.
  std::shared_ptr<IdStringPool> id_string_pool() const {
    return id_string_pool_;
  }

  // Returns the arena() that was propagated from AnalyzerOptions. This contains
  // some or all of the resolved AST and parse tree.
  std::shared_ptr<zetasql_base::UnsafeArena> arena() const { return arena_; }

  const AnalyzerOutputProperties& analyzer_output_properties() const {
    return analyzer_output_properties_;
  }

  // Returns the maximum column id that has been allocated.
  // Column ids above this number are unused.
  int max_column_id() const { return max_column_id_; }

 private:
  friend class AnalyzerOutputMutator;

  // This IdStringPool and arena must be kept alive for the Resolved trees below
  // to be valid.
  std::shared_ptr<IdStringPool> id_string_pool_;
  std::shared_ptr<zetasql_base::UnsafeArena> arena_;

  std::unique_ptr<const ResolvedStatement> statement_;
  std::unique_ptr<const ResolvedExpr> expr_;

  AnalyzerOutputProperties analyzer_output_properties_;

  // AnalyzerOutput can (but is not guaranteed to) take ownership of the parser
  // output so deleting the parser AST can be deferred.  Deleting the parser
  // AST is expensive.  This allows engines to defer AnalyzerOutput cleanup
  // until after critical-path work is done.  May be NULL.
  std::unique_ptr<ParserOutput> parser_output_;

  std::vector<absl::Status> deprecation_warnings_;

  QueryParametersMap undeclared_parameters_;
  std::vector<const Type*> undeclared_positional_parameters_;
  int max_column_id_;
};
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANALYZER_OUTPUT_H_
