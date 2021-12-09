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

#ifndef ZETASQL_SCRIPTING_SCRIPT_EXECUTOR_IMPL_H_
#define ZETASQL_SCRIPTING_SCRIPT_EXECUTOR_IMPL_H_

#include <cstdint>
#include <utility>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/scripting/control_flow_graph.h"
#include "zetasql/scripting/error_helpers.h"
#include "zetasql/scripting/parsed_script.h"
#include "zetasql/scripting/script_executor.h"
#include "zetasql/scripting/script_executor_state.pb.h"
#include "zetasql/scripting/stack_frame.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "zetasql/base/flat_set.h"
#include "zetasql/base/status.h"

namespace zetasql {

using VariableSizesMap =
    absl::flat_hash_map<IdString, int64_t, IdStringCaseHash,
                        IdStringCaseEqualFunc>;
using OutputArgumentMap =
    absl::flat_hash_map<IdString, IdString, IdStringCaseHash,
                        IdStringCaseEqualFunc>;

class ScriptExecutorImpl;

// Implements the rules of control flow for ZetaSQL scripting.
// ControlFlowProcessor understands the semantics of all the control-flow
// statements (IF, WHILE, RETURN, etc.), but does not actually store information
// about the progress of a script.  This allows ControlFlowProcessor to be
// completely stateless, with all statement management relegated to the callback
// (ScriptExecutorImpl).
class ControlFlowProcessor {
 public:
  explicit ControlFlowProcessor(ScriptExecutorImpl* callback,
                                bool dry_run)
      : callback_(callback), dry_run_(dry_run) {}

  // Assumes that the current statement has just completed (except for
  // control-flow changes).  Advances control to the next statement that will
  // run.  If the current statement is a control-flow statement (BREAK,
  // CONTINUE, RETURN), control is transferred to the appropriate target, except
  // in dry-run mode, where these statements are considered nops.
  //
  // <status> indicates whether non-control-flow-related operations associated
  // with the current statement succeeded, along with context if it failed:
  //   - If <status> is OK, control is transferred to the next statement
  //     normally, and the returned status is OK.
  //   - If <status> has a ScriptException payload and a matching handler
  //     exists, control is transferred to the handler, and the returned status
  //     is OK.
  //   - If <status> does NOT contain a ScriptException payload, or the
  //     exception is unhandled, the current statement is not modified and the
  //     returned status is <status>.
  absl::Status AdvancePastCurrentStatement(const absl::Status& status);

  // Assumes that a condition has just finished evaluating for an IF or WHILE
  // statement.  Advances control to the next statement that will run, given
  // the condition result.
  //
  // If <condition_value> has a failed status, control will transfer to an
  // exception handler instead, if one exists.  See
  // AdvancePastCurrentStatement() for a detailed discussion on how statues are
  // handled.
  absl::Status AdvancePastCurrentCondition(
      const absl::StatusOr<bool>& condition_value);

  // If <status> indicates a handleable exception, transfers control to the
  // handler.  Otherwise, leaves the current location unmodified.
  absl::Status MaybeDispatchException(const absl::Status& status);

  // Rethrows a given ScriptException, without modifying the exception to
  // represent the current stack trace.
  //
  // Returns OK if the rethrown exception is handled, after transferring control
  // to the handler.  Returns an error status with <exception> as its payload
  // if unhandled.
  absl::Status RethrowException(const ScriptException& exception);

  // Assumes that the current statement is an ASTBeginEndBlock.  Enters the
  // block and transfers control to the first statement in the block.
  absl::Status EnterBlock();

 private:
  // Advances to the next node in the graph along the path identified by
  // <edge_kind>, which cannot be kException.
  absl::Status AdvanceInternal(ControlFlowEdge::Kind edge_kind);

  // Returns the control-flow node representing the current location in the
  // current stack frame, or nullptr if none exists.
  const ControlFlowNode* GetCurrentControlFlowNode() const;

  // If an exception is thrown at the current location, returns true if the
  // exception would be handled (including handlers up the call stack),
  // false otherwise.
  absl::StatusOr<bool> CheckIfExceptionHandled() const;

  // Advances to the next statement, given a pending exception.  This function
  // should be called only when the exception is handled (that is,
  // CheckIfExceptionHandled() previously returned true).
  absl::Status DispatchException(const ScriptException& exception);

  // Executes the side effects in a control-flow edge.  This includes destroying
  // out-of-scope variables and entering/exiting exception handlers.
  //
  // <exception> represents the current pending exception, and should be set
  // only for edges of kind kException.
  absl::Status ExecuteSideEffects(
      const ControlFlowEdge& edge,
      const absl::optional<ScriptException>& exception);

  // Updates the current position of the script, given the execution of a given
  // control-flow edge.  Returns true if exiting a procedure, indicating that
  // we need to follow up by advancing past the CALL statement.  Returns false
  // if control remains within the current stack frame and control-flow
  // processing is complete.
  absl::StatusOr<bool> UpdateCurrentLocation(const ControlFlowEdge& edge);

 private:
  ScriptExecutorImpl* callback() { return callback_; }
  const ScriptExecutorImpl* callback() const { return callback_; }

  const StackFrame& GetLeafFrame() const;

  ScriptExecutorImpl* const callback_;
  bool dry_run_;
};

// Implementation of ScriptExecutor to execute a ZetaSQL script.
//
// At the moment, only SQL statements are supported.  This class will be
// extended over time to include all the functionality described in
// (broken link).
class ScriptExecutorImpl : public ScriptExecutor {
 public:
  // The caller maintains ownership of <script>, <evaluator>, and <ast_script>,
  // and must keep them alive for the lifetime of the returned ScriptExecutor.
  //
  // If <ast_script> is nullptr, Create() will parse the script, and the
  // returned ScriptExecutor will own the AST.
  static absl::StatusOr<std::unique_ptr<ScriptExecutor>> Create(
      absl::string_view script, const ASTScript* ast_script,
      const ScriptExecutorOptions& options, StatementEvaluator* evaluator);

  absl::Status DestroyVariables(
      const std::set<std::string>& variables);
  absl::StatusOr<const ASTStatement*> ExitProcedure(
      bool normal_return);
  absl::StatusOr<ScriptException> SetupNewException(
      const absl::Status& status);
  absl::Status EnterExceptionHandler(const ScriptException& exception);
  absl::Status ExitExceptionHandler();
  absl::Status ExitForLoop();

  bool IsComplete() const override;
  absl::Status ExecuteNext() override;
  absl::string_view GetScriptText() const override;
  const ControlFlowNode* GetCurrentNode() const override;
  std::string DebugString(bool verbose) const override;
  absl::StatusOr<std::vector<StackFrameTrace>> StackTrace() const override;
  absl::string_view GetCurrentStackFrameName() const override;
  absl::string_view GetCurrentProcedureName() const override;
  absl::TimeZone time_zone() const override;
  int64_t stack_depth() const override { return callstack_.size(); }
  absl::Status UpdateAnalyzerOptions(
      AnalyzerOptions& analyzer_options) const override;
  const std::optional<absl::variant<ParameterValueList, ParameterValueMap>>&
  GetCurrentParameterValues() const override {
    return callstack_.back().parameters();
  }
  absl::Status UpdateAnalyzerOptionParameters(AnalyzerOptions* options) const;

  const VariableMap& GetCurrentVariables() const override {
    return callstack_.back().variables();
  }
  const VariableTypeParametersMap& GetCurrentVariableTypeParameters() const {
    return callstack_.back().variable_type_params();
  }
  const ParsedScript::StringSet GetCurrentNamedParameters() const {
    return callstack_.back().parsed_script()->GetNamedParameters(
        callstack_.back().parsed_script()->script()->GetParseLocationRange());
  }
  const std::pair<int64_t, int64_t> GetCurrentPositionalParameters() const {
    return callstack_.back().parsed_script()->GetPositionalParameters(
        callstack_.back().parsed_script()->script()->GetParseLocationRange());
  }
  const SystemVariableValuesMap& GetKnownSystemVariables() const override {
    return system_variables_;
  }
  const StackFrame* const GetCurrentStackFrame() const override {
    return IsComplete() ? nullptr : &callstack_.back();
  }
  absl::StatusOr<bool> DefaultAssignSystemVariable(
      const ASTSystemVariableAssignment* ast_assignment,
      const Value& value) override;

  absl::StatusOr<ScriptExecutorStateProto> GetState() const override;
  absl::Status SetState(const ScriptExecutorStateProto& state) override;

 private:
  // Keeps context information of one frame on call stack. See each field for
  // detail.
  class StackFrameImpl : public StackFrame {
   public:
    StackFrameImpl(std::unique_ptr<const ParsedScript> parsed_script,
                   const ControlFlowNode* current_node)
        : parsed_script_(std::move(parsed_script)),
          current_node_(current_node) {}

    StackFrameImpl(
        std::unique_ptr<const ParsedScript> parsed_script,
        const ControlFlowNode* current_node, VariableMap variables,
        VariableSizesMap variable_sizes,
        VariableTypeParametersMap variable_type_params,
        std::unique_ptr<ProcedureDefinition> procedure_definition,
        std::unique_ptr<IdStringPool> id_string_pool,
        std::optional<absl::variant<ParameterValueList, ParameterValueMap>>
            parameters,
        std::vector<std::unique_ptr<EvaluatorTableIterator>>
            for_loop_stack)
        : parsed_script_(std::move(parsed_script)),
          current_node_(current_node),
          variables_(std::move(variables)),
          variable_sizes_(std::move(variable_sizes)),
          variable_type_params_(variable_type_params),
          procedure_definition_(std::move(procedure_definition)),
          id_string_pool_(std::move(id_string_pool)),
          parameters_(std::move(parameters)),
          for_loop_stack_(std::move(for_loop_stack)) {}

    const ParsedScript* parsed_script() const override {
      return parsed_script_.get();
    }
    bool is_dynamic_sql() const override {
      return procedure_definition_ != nullptr &&
             procedure_definition_->is_dynamic_sql();
    }
    const ControlFlowNode* current_node() const override {
      return current_node_;
    }

    const VariableMap& variables() const { return variables_; }
    const VariableSizesMap& variable_sizes() const { return variable_sizes_; }
    const VariableTypeParametersMap& variable_type_params() const {
      return variable_type_params_;
    }
    const ProcedureDefinition* procedure_definition() const {
      return procedure_definition_.get();
    }
    const std::optional<absl::variant<ParameterValueList, ParameterValueMap>>&
    parameters() const {
      return parameters_;
    }
    const std::vector<std::unique_ptr<EvaluatorTableIterator>>&
    for_loop_stack() const {
      return for_loop_stack_;
    }

    void SetCurrentNode(const ControlFlowNode* node) { current_node_ = node; }
    std::unique_ptr<const ParsedScript> ReleaseParsedScript() {
      return std::move(parsed_script_);
    }
    VariableMap* mutable_variables() { return &variables_; }
    VariableSizesMap* mutable_variable_sizes() { return &variable_sizes_; }
    VariableTypeParametersMap* mutable_variable_type_params() {
      return &variable_type_params_;
    }

    std::vector<std::unique_ptr<EvaluatorTableIterator>>*
    mutable_for_loop_stack() {
      return &for_loop_stack_;
    }

   private:
    // Holds the parse tree and input text of the main script or a running
    // procedure.
    std::unique_ptr<const ParsedScript> parsed_script_;

    // Always points to the node that is currently executing or about to be
    // executed:
    // - For non-leaf frames, points to a CALL statement
    // - When preparing to evaluate an IF or WHILE condition, points to the
    //     IF/WHILE statement
    // - When preparing to evaluate an ELSEIF condition, points to the relevant
    //     ELSEIF clause
    // - The root frame remains on the stack after script execution is complete.
    //     When this happens, the current node is nullptr.
    //
    // All node pointers are owned by <parsed_script_>.
    const ControlFlowNode* current_node_;

    // Vector storing the list of variable values. Procedure arguments are
    // treated as variables and stored in the VariableMap.
    VariableMap variables_;

    // Mapping of variables to their sizes.
    VariableSizesMap variable_sizes_;

    // Mapping of variables to their type parameters. Only variables which
    // contain type parameters are present in this map.
    VariableTypeParametersMap variable_type_params_;

    // Holds definition of procedure of current stack frame. Null if current
    // stack frame is the main script.
    std::unique_ptr<ProcedureDefinition> procedure_definition_;

    // Used to allocate IdString for procedure parameter name.
    // Null if current stack frame is the main script.
    std::unique_ptr<IdStringPool> id_string_pool_;

    // Parameters to use in this stack frame
    std::optional<absl::variant<ParameterValueList, ParameterValueMap>>
        parameters_;

    // Stack of EvaluatorTableIterators used by nested FOR loops.
    // Innermost loop's iterator is last.
    std::vector<std::unique_ptr<EvaluatorTableIterator>> for_loop_stack_;
  };

  ScriptExecutorImpl(const ScriptExecutorOptions& options,
                     std::unique_ptr<const ParsedScript> parsed_script,
                     StatementEvaluator* evaluator);
  ScriptExecutorImpl(const ScriptExecutorImpl&) = delete;
  ScriptExecutorImpl& operator=(const ScriptExecutorImpl&) = delete;

  absl::Status ExecuteNextImpl();

  // Evaluates the given expression as a condition.
  //
  // Return true if the expression successfully evaluates to TRUE, or false if
  // the expression successfully evaluates to FALSE or NULL.  Returns an error
  // if the expression fails to evaluate, or has a type other than BOOL.
  absl::StatusOr<bool> EvaluateCondition(const ASTExpression* condition) const;

  // Assumes that <current_statement_> is an ASTCaseStatement, then evaluates
  // and stores the expression if it exists. Moves on to the following
  // ASTWhenThenClause.
  absl::Status ExecuteCaseStatement();

  // Assumes that <current_statement_> is an ASTWhenThenClause. Enters the
  // clause if condition evaluates to true, or, if the parent ASTCaseStatement
  // contains an expression, to a value equivalent to the expression.
  absl::StatusOr<bool> EvaluateWhenThenClause();

  // Assumes that <current_statement_> is an ASTWhileStatement and evaluates the
  // condition.  Then, sets <current_statement_> to the next statement to
  // execute, given the result of the condition.
  absl::Status ExecuteWhileCondition();

  // Generate a struct instance of struct_type from iterator's current row.
  // Deduce struct_type from row values if struct_type == nullptr.
  absl::StatusOr<Value> GenerateStructValueFromRow(
      TypeFactory* type_factory,
      const EvaluatorTableIterator& iterator,
      const StructType* struct_type) const;

  // Assumes that <current_statement_> is an ASTForInStatement and either
  // starts a new loop or advances an on-going loop, by fetching the next row
  // from query results. Then, sets <current_statement_> to the next statement
  // to execute, depending on whether it reached the last row.
  absl::Status ExecuteForInStatement();

  // Assumes that <current_statement_> is an ASTBeginEndBlock.  Sets up
  // a new scope for variables in the block, and transfers control to the
  // first statement in the block (or the statement following the block,
  // if the block is empty).
  absl::Status BeginBlockExecution();

  // Executes current_statement_, which must be a ASTVariableDeclaration.
  absl::Status ExecuteVariableDeclaration();

  // Executes an ASTSingleAssignment statement (e.g. SET x = 5).
  absl::Status ExecuteSingleAssignment();

  // Executes an ASTAssignmentFromStruct statement (e.g. SET (x, y) = (5, 6)).
  absl::Status ExecuteAssignmentFromStruct();

  // Loads procedure definition and setup a new StackFrame to execute the
  // procedure.
  absl::Status ExecuteCallStatement();

  // Loads parameters specified in an EXECUTE IMMEDIATE USING clause
  absl::StatusOr<
      std::optional<absl::variant<ParameterValueList, ParameterValueMap>>>
  EvaluateDynamicParams(const ASTExecuteUsingClause* using_clause,
                        VariableSizesMap* variable_sizes_map);

  // Completes execution of an EXECUTE IMMEDIATE statement containing an INTO
  // clause. This should only be called once a dynamic SQL stack frame has been
  // setup.
  absl::Status ExecuteDynamicIntoStatement(
      const ASTExecuteIntoClause* into_clause);

  // Executes an EXECUTE IMMEDIATE statement by setting up a new StackFrame
  absl::Status ExecuteDynamicStatement();

  // Executes a RAISE statement.  This function is used both for re-raising an
  // existing exception and creating a new exception.
  absl::Status ExecuteRaiseStatement(const ASTRaiseStatement* stmt);

  // Executes an assignment to a system variable.
  absl::Status ExecuteSystemVariableAssignment(
      const ASTSystemVariableAssignment* stmt);

  // Sets @@time_zone to the specified value.  Uses <location> for error
  // messages if <timezone_value> is invalid.
  absl::Status SetTimezone(const Value& timezone_value,
                           const ASTNode* location);

  // Executes a RAISE statement to fire a new exception.
  absl::Status RaiseNewException(const ASTRaiseStatement* stmt);

  // Executes a RAISE statement to rethrow the exception currently being
  // handled.  Must be in an exception handler.
  absl::Status ReraiseCaughtException(const ASTRaiseStatement* stmt);

  // Evaluates procedure arguments and verifies the arguments against the
  // procedure signature.  On output, sets <variables> and <variable_sizes>
  // to the procedure argument values and their sizes.
  absl::Status CheckAndEvaluateProcedureArguments(
      const ASTCallStatement* call_statement,
      const ProcedureDefinition& procedure_definition,
      IdStringPool* id_string_pool, VariableMap* variables,
      VariableSizesMap* variable_sizes);

  // Return a string describing each active variable and its value.
  // Variables are sorted in order of innermost to outermost scope, and in
  // alphabetical order within each scope.
  std::string VariablesDebugString() const;

  // Return a string describing each active parameter by name, sorted
  // alphabetically.
  std::string ParameterDebugString() const;

  // Return a string showing point of execution on each level of the call stack.
  // If <verbose> is true, print current statement as well.
  std::string CallStackDebugString(bool verbose) const;

  // Returns a string to describe a stack trace.  This is shared between
  // DebugString() output and the @@error.formatted_stack_trace system variable.
  // If <verbose> is true, print current statement as well.
  std::string StackTraceString(absl::Span<StackFrameTrace> stack_frame_traces,
                               bool verbose) const;

  // Evaluates <expr>, converting the result to <type>.  If the value is not
  // of the correct type, returns an error annoated with the parse location of
  // <expr>.  <context> describes the context of the expression for purposes
  // of error messages (e.g. "IF condition").  <for_assignment>, if true,
  // allows additional conversion cases, such as int64_t->int32_t and
  // uint64_t->uint32_t.
  absl::StatusOr<Value> EvaluateExpression(const ASTExpression* expr,
                                           const Type* target_type) const;

  // Returns an error at the location of <node>, indicating that this script is
  // over its size limit.
  absl::Status MakeNoSizeRemainingError(
      const ASTNode* node, int64_t total_size_limit,
      const std::vector<StackFrameImpl>& callstack);

  // Returns an error at the location of <node>, indicating that <var_name> is
  // too large.
  static absl::Status MakeVariableIsTooLargeError(
      const ASTNode* node, const IdString& var_name,
      int64_t per_variable_size_limit);

  // Copies OUT/INOUT argument value from <argument_map> to <frame_return_to>.
  absl::Status AssignOutArguments(
      const ProcedureDefinition& procedure_definition,
      const VariableMap& argument_map, StackFrameImpl* frame_return_to);

  // Exits from procedure <procedure_exit_from>. Copies OUT/INOUT argument value
  // from <frame_exit_from> to <frame_return_to>.
  absl::Status ExitFromProcedure(const ProcedureDefinition& procedure_exit_from,
                                 StackFrameImpl* frame_exit_from,
                                 StackFrameImpl* frame_return_to,
                                 bool normal_return);

  absl::Status ValidateVariablesOnSetState(
      const ControlFlowNode * next_cfg_node,
      const VariableMap& new_variables,
      const ParsedScript& parsed_script) const;

  const ParsedScript* CurrentScript() const {
    return callstack_.back().parsed_script();
  }

  // Returns the parent stack frame if it exists, or nullptr otherwise
  StackFrameImpl* GetMutableParentStackFrame() {
    return callstack_.size() >= 2 ? &callstack_[callstack_.size() - 2]
                                  : nullptr;
  }

  VariableMap* MutableCurrentVariables() {
    return callstack_.back().mutable_variables();
  }

  // Returns a pointer to value of variable <variable_name>. Returns an SQL
  // error on <node> if the variable name is not found.
  absl::StatusOr<Value*> MutableVariableValue(const ASTIdentifier* node) {
    VariableMap* variables = MutableCurrentVariables();
    auto it = variables->find(node->GetAsIdString());
    if (it == variables->end()) {
      return MakeUndeclaredVariableError(node);
    }
    return &(it->second);
  }

  const VariableSizesMap& CurrentVariableSizes() {
    return callstack_.back().variable_sizes();
  }

  VariableSizesMap* MutableCurrentVariableSizes() {
    return callstack_.back().mutable_variable_sizes();
  }

  const VariableTypeParametersMap& CurrentVariableTypeParameters() {
    return callstack_.back().variable_type_params();
  }

  VariableTypeParametersMap* MutableCurrentVariableTypeParameters() {
    return callstack_.back().mutable_variable_type_params();
  }

  const ProcedureDefinition* CurrentProcedure() const {
    return callstack_.back().procedure_definition();
  }

  // Helper function for resetting variable sizes after a call to SetState().
  absl::Status ResetVariableSizes(const ASTNode* node,
                                  const VariableMap& new_variables,
                                  VariableSizesMap* variable_sizes);

  // Helper function for resetting iterator sizes after a call to SetState().
  absl::Status ResetIteratorSizes(
      const ASTNode* node,
      const std::vector<std::unique_ptr<EvaluatorTableIterator>>& iterator_vec);

  absl::Status UpdateAndCheckMemorySize(const ASTNode* node,
                                        int64_t current_memory_size,
                                        int64_t previous_memory_size);

  absl::Status UpdateAndCheckVariableSize(const ASTNode* node,
                                          const IdString& var_name,
                                          int64_t variable_size,
                                          VariableSizesMap* variable_sizes_map);

  absl::Status OnVariablesValueChangedWithoutSizeCheck(
      bool notify_evaluator, const StackFrame& var_declaration_stack_frame,
      const ASTNode* current_node,
      const std::vector<StatementEvaluator::VariableChange>& variable_changes);

  absl::Status OnVariablesValueChangedWithSizeCheck(
      bool notify_evaluator, const StackFrame& var_declaration_stack_frame,
      const ASTNode* current_node,
      const std::vector<StatementEvaluator::VariableChange>& variable_changes,
      VariableSizesMap* variable_sizes_map);

  absl::StatusOr<int64_t> GetIteratorMemoryUsage(
      const EvaluatorTableIterator* iterator) {
    if (iterator == nullptr) {
      return 0;
    }
    return evaluator_->GetIteratorMemoryUsage(*iterator);
  }

  // Returns true if <from_type> is coercible to <to_type>
  bool CoercesTo(const Type* from_type, const Type* to_type) const;

  absl::StatusOr<Value> CastValueToType(const Value& from_value,
                                        const Type* to_type) const;

  // Resets execution to the beginning of the script.  Discards all state
  // regarding the progress of the script.
  absl::Status Reset();

  void SetSystemVariablesForPendingException();

  // Returns the text for @@error.statement_text, referring to the current
  // location.
  absl::StatusOr<std::string> GenerateStatementTextSystemVariable() const;
  absl::Status GenerateStackTraceSystemVariable(
      ScriptException::Internal* proto);

  // Returns the type of @@error.stack_trace.
  absl::StatusOr<const ArrayType*> GetStackTraceSystemVariableType();

  // Returns the value of @@error.stack_trace.
  absl::StatusOr<Value> GetStackTraceSystemVariable(
      const ScriptException::Internal& internal_exception);

  // Returns the value of @@error.formatted_stack_trace
  Value GetFormattedStackTrace(
      const ScriptException::Internal& internal_exception);

  // Unwraps NamedArguments, if necessary.
  ScriptSegment SegmentForScalarExpression(const ASTExpression* expr) const;

  absl::StatusOr<Value> EvaluateProcedureArgument(
      absl::string_view argument_name,
      const FunctionArgumentType& function_argument_type,
      const ASTExpression* argument_expr);

  // Assumes that the current statement has just completed (except for
  // control-flow changes).  Advances control to the next statement that will
  // run.  If the current statement is a control-flow statement (BREAK,
  // CONTINUE, RETURN), control is transferred to the appropriate target, except
  // in dry-run mode, where these statements are considered nops.
  //
  // <status> indicates whether non-control-flow-related operations associated
  // with the current statement succeeded, along with context if it failed:
  //   - If <status> is OK, control is transferred to the next statement
  //     normally, and the returned status is OK.
  //   - If <status> has a ScriptException payload and a matching handler
  //     exists, control is transferred to the handler, and the returned status
  //     is OK.
  //   - If <status> does NOT contain a ScriptException payload, or the
  //     exception is unhandled, the current statement is not modified and the
  //     returned status is <status>.
  absl::Status AdvancePastCurrentStatement(const absl::Status& status);

  // Assumes that a condition has just finished evaluating for an IF or WHILE
  // statement.  Advances control to the next statement that will run, given
  // the condition result.
  //
  // If <condition_value> has a failed status, control will transfer to an
  // exception handler instead, if one exists.  See
  // AdvancePastCurrentStatement() for a detailed discussion on how statues are
  // handled.
  absl::Status AdvancePastCurrentCondition(
      const absl::StatusOr<bool>& condition_value);

  // If <status> indicates a handleable exception, transfers control to the
  // handler.  Otherwise, leaves the current location unmodified.
  absl::Status MaybeDispatchException(const absl::Status& status);

  // Rethrows a given ScriptException, without modifying the exception to
  // represent the current stack trace.
  //
  // Returns OK if the rethrown exception is handled, after transferring control
  // to the handler.  Returns an error status with <exception> as its payload
  // if unhandled.
  absl::Status RethrowException(const ScriptException& exception);

  // Assumes that the current statement is an ASTBeginEndBlock.  Enters the
  // block and transfers control to the first statement in the block.
  absl::Status EnterBlock();

  // Advances to the next node in the graph along the path identified by
  // <edge_kind>, which cannot be kException.
  absl::Status AdvanceInternal(ControlFlowEdge::Kind edge_kind);

  // If an exception is thrown at the current location, returns true if the
  // exception would be handled (including handlers up the call stack),
  // false otherwise.
  absl::StatusOr<bool> CheckIfExceptionHandled() const;

  // Advances to the next statement, given a pending exception.  This function
  // should be called only when the exception is handled (that is,
  // CheckIfExceptionHandled() previously returned true).
  absl::Status DispatchException(const ScriptException& exception);

  // Executes the side effects in a control-flow edge.  This includes destroying
  // out-of-scope variables and entering/exiting exception handlers.
  //
  // <exception> represents the current pending exception, and should be set
  // only for edges of kind kException.
  absl::Status ExecuteSideEffects(
      const ControlFlowEdge& edge,
      const absl::optional<ScriptException>& exception);

  // Updates the current position of the script, given the execution of a given
  // control-flow edge.  Returns true if exiting a procedure, indicating that
  // we need to follow up by advancing past the CALL statement.  Returns false
  // if control remains within the current stack frame and control-flow
  // processing is complete.
  absl::StatusOr<bool> UpdateCurrentLocation(const ControlFlowEdge& edge);

  ParserOptions GetParserOptions() const;

  // Sets the predefined variables used by scripts. The predefined script
  // variables can only be set to the main script stack frame. Which means it
  // should be called right after the ScriptExecutor gets created if the call is
  // needed.
  absl::Status SetPredefinedVariables(
      const VariableWithTypeParameterMap& variables);

  VariableSet GetPredefinedVariableNames() const override {
    return predefined_variable_names_;
  }

  const ScriptExecutorOptions options_;

  // Current timezone; initially set from ScriptExecutorOptions; adjusts as the
  // script executes when @@time_zone gets set.
  absl::TimeZone time_zone_;

  // Engine-owned evaluator for executing individual statements and
  // expressions.  Lifetime is owned by the caller of Create(), and must remain
  // alive as long as the ScriptExecutor is alive.
  StatementEvaluator* const evaluator_;

  // Keeps track of point of execution and local variable value. A new
  // StackFrame is pushed on top of stack when ScriptExecutorImpl is created
  // with a script or when a procedure is called during execution.
  // Main script should always be the bottom frame of the stack, therefore call
  // stack can never be empty.
  std::vector<StackFrameImpl> callstack_;

  // Total memory usage through a script. Includes variable sizes and memory
  // used by iterators. This count persists across call stacks.
  int64_t total_memory_usage_;

  // Controls the lifetimes of zetasql types of variable values deserialized
  // from a ScriptExecutorStateProto.
  google::protobuf::DescriptorPool descriptor_pool_;
  TypeFactory* type_factory_;

  // If no TypeFactory is passed in from ScriptExecutorOptions, one is created
  // and held here.
  std::unique_ptr<TypeFactory> type_factory_holder_;

  // System variables whose values are known to the script executor.
  SystemVariableValuesMap system_variables_;

  // Stack of pending exceptions.  Innermost exception being handled is last.
  std::vector<ScriptException> pending_exceptions_;

  // IdStringPool used for the names of exception variables.
  IdStringPool exception_variables_string_pool_;

  // Features triggered by current script.
  zetasql_base::flat_set<ScriptExecutorStateProto::ScriptFeature> triggered_features_;

  // Sql feature usage proto used for tracking script feature usage
  ScriptExecutorStateProto::ScriptFeatureUsage sql_feature_usage_;

  // Type of @@error.stack_trace.
  // Lazily initialized in GetStackTraceSystemVariableType() to
  //   ARRAY<STRUCT<line INT64, column INT64, filename STRING, location STRING>>
  //
  // once set, it is owned by <type_factory_>.
  const ArrayType* stack_trace_system_variable_type_ = nullptr;

  // The predefined script variables that exist outside of the script scope.
  // This is needed to make sure the variable validation passes for the
  // predefined script variables.
  VariableSet predefined_variable_names_;

  // Index of the current CASE statement's WHEN branch that should be
  // entered. If no branch meets condition, set to -1.
  int64_t case_stmt_true_branch_index_ = -1;
  // Tracker index of current WHEN branch being executed.
  int64_t case_stmt_current_branch_index_ = -1;
};

}  // namespace zetasql

#endif  // ZETASQL_SCRIPTING_SCRIPT_EXECUTOR_IMPL_H_
