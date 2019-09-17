//
// Copyright 2019 ZetaSQL Authors
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

#ifndef ZETASQL_SCRIPTING_PARSED_SCRIPT_H_
#define ZETASQL_SCRIPTING_PARSED_SCRIPT_H_

#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/scripting/break_continue_context.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "zetasql/base/status.h"

// Flag which controls the maximum supported nesting of script statements within
// a ZetaSQL script.
ABSL_DECLARE_FLAG(int, zetasql_scripting_max_nesting_level);

namespace zetasql {

class ParsedScript {
 public:
  // Maps an ASTStatement pointer to the child index of that statement, relative
  // to its parent.  For each statement s, s->parent()->child(map[s]) == s.
  using StatementIndexMap = absl::flat_hash_map<const ASTStatement*, int>;

  // Mapping of each break/continue statement to a BreakContinueContext
  // structure.  All node pointers within each BreakContinueContext are
  // owend by <parser_output_>.
  using BreakContinueMap =
      absl::flat_hash_map<const ASTStatement*, BreakContinueContext>;

  // Mapping of variable name to ASTType.
  using VariableTypeMap =
      absl::flat_hash_map<IdString, const ASTType*, IdStringCaseHash,
                          IdStringCaseEqualFunc>;

  // Mapping of argument name to zetasql Type.
  using ArgumentTypeMap =
      absl::flat_hash_map<IdString, const Type*, IdStringCaseHash,
                          IdStringCaseEqualFunc>;

  // Performs preliminary analysis on the parse tree for a zetasql script
  // before execution.  Currently, this includes the following:
  // - Verify that variable declarations occur only at the start of a block,
  //    or the start of the entire script.
  // - Verify that a variable is not declared which shadows a routine argument
  //    or another variable of the same name in either the same scope, or any
  //    enclosing scope.
  // - Verify that BREAK and CONTINUE statements occur only inside of a loop.
  // - Obtain a mapping associating each statement to the index of that
  //    statement in the statement list of its parent node.
  // - Obtain a mapping associating each BREAK and CONTINUE statement with a
  //    pointer to the enclosing loop, plus a list of blocks whose variables
  //    must go out of scope when the corresponding BREAK/CONTINUE statement
  //    executes.
  //
  // All results are stored in the returned ParsedScript object.
  static zetasql_base::StatusOr<std::unique_ptr<ParsedScript>> Create(
      absl::string_view script_string, const ParserOptions& parser_options,
      ErrorMessageMode error_message_mode);

  // Similar to the above function, but uses an existing, externally-owned
  // AST instead of parsing the script.  <ast_script> must be kept alive for
  // the lifetime of the returned ParsedScript.
  static zetasql_base::StatusOr<std::unique_ptr<ParsedScript>> Create(
      absl::string_view script_string, const ASTScript* ast_script,
      ErrorMessageMode error_message_mode);

  // Similar to above function, but also passes arguments for a routine, i.e. a
  // function or stored procedure whose body is a script.
  static zetasql_base::StatusOr<std::unique_ptr<ParsedScript>> CreateForRoutine(
      absl::string_view script_string, const ParserOptions& parser_options,
      ErrorMessageMode error_message_mode, ArgumentTypeMap routine_arguments);

  const ASTScript* script() const { return ast_script_; }
  absl::string_view script_text() const { return script_string_; }
  ErrorMessageMode error_message_mode() const { return error_message_mode_; }
  const ArgumentTypeMap& routine_arguments() const {
    return routine_arguments_;
  }

  const StatementIndexMap& statement_index_map() const {
    return statement_index_map_;
  }

  const BreakContinueMap& break_continue_map() const {
    return break_continue_map_;
  }

  // Returns the statement in the script which starts at the given position,
  // or nullptr if no such statement exists.
  const ASTStatement* FindStatementFromPosition(
      const ParseLocationPoint& start_pos) const;

  // Returns a map of all variables in scope immediately prior to the execution
  // of <next_statement>.
  zetasql_base::StatusOr<VariableTypeMap> GetVariablesInScopeAtStatement(
      const ASTStatement* next_statement) const;

 private:
  static zetasql_base::StatusOr<std::unique_ptr<ParsedScript>> CreateInternal(
      absl::string_view script_string, const ParserOptions& parser_options,
      ErrorMessageMode error_message_mode, ArgumentTypeMap routine_arguments);

  // <script_string> is owned externally.  Takes ownership of <parser_output>.
  ParsedScript(absl::string_view script_string, const ASTScript* ast_script,
               std::unique_ptr<ParserOutput> parser_output,
               ErrorMessageMode error_message_mode,
               ArgumentTypeMap routine_arguments);

  // Called from Create() to walk the parse tree and perform non-trivial work
  // to initialize fields.
  zetasql_base::Status GatherInformationAndRunChecks();

  // Helper function called by GatherInformationAndRunChecks().  Returns errors
  // with an InternalErrorLocation, which the caller then converts to an
  // external ErrorLocation.
  zetasql_base::Status GatherInformationAndRunChecksInternal();

  // Controls the lifetime of the script AST, if it is owned by us.  Otherwise,
  // nullptr.
  std::unique_ptr<ParserOutput> parser_output_;

  // Points to the script AST.  This is either parsed_output_->script(), if
  // we own the script, or an externally-owned AST if parser_output_ is nullptr.
  const ASTScript* ast_script_;

  // The text of the script.  Externally owned.
  absl::string_view script_string_;

  // How to report error messages in GatherInformationAndRunChecks().
  ErrorMessageMode error_message_mode_;

  // Map associating each statement in the AST with the index of the statement
  // relative to its parent.  We maintain the invariant that:
  //   statement->parent()->child(map_statement_index_[statement]) == statement.
  // for all statements <statement> in the parse tree.
  //
  // <parser_output_> owns the lifetime of all ASTStatement objects in the map.
  StatementIndexMap statement_index_map_;

  // Map associating each BREAK/CONTINUE statement in the script with
  // information needed by the script executor to execute it.
  //
  // <parser_output_> owns the lifetime of all objects in the map.
  BreakContinueMap break_continue_map_;

  // Routine arguments existing from the beginning the script.
  ArgumentTypeMap routine_arguments_;
};

}  // namespace zetasql

#endif  // ZETASQL_SCRIPTING_PARSED_SCRIPT_H_
