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

#include "zetasql/scripting/script_executor_impl.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <stack>
#include <tuple>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_decls.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/scripting/error_helpers.h"
#include "zetasql/scripting/parsed_script.h"
#include "zetasql/scripting/script_exception.pb.h"
#include "zetasql/scripting/script_executor.h"
#include "zetasql/scripting/script_segment.h"
#include "zetasql/scripting/serialization_helpers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "re2/re2.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/status_payload.h"

namespace zetasql {
namespace {

using StackFrameProto = ScriptExecutorStateProto::StackFrame;
using ProcedureDefinitionProto = ScriptExecutorStateProto::ProcedureDefinition;
using StackFrameTrace = ScriptExecutor::StackFrameTrace;
using VariableChange = StatementEvaluator::VariableChange;

constexpr char kTimeZoneSystemVarName[] = "time_zone";

// If <constraint_status> is an internal error or OK, <constraint_status> is
// returned. Otherwise, a user-facing error is returned by making a
// ScriptExceptionError.
absl::Status CheckConstraintStatus(const absl::Status& constraint_status,
                                   const ASTNode* error_node) {
  if (constraint_status.ok() || absl::IsInternal(constraint_status)) {
    return constraint_status;
  }
  return MakeScriptExceptionAt(error_node) << constraint_status.message();
}

ParsedScript::QueryParameters GetQueryParameters(
    const std::optional<absl::variant<ParameterValueList, ParameterValueMap>>&
        params) {
  if (!params.has_value()) {
    return absl::nullopt;
  }
  if (absl::holds_alternative<ParameterValueMap>(*params)) {
    ParsedScript::StringSet parameter_names;
    for (const auto& p : absl::get<ParameterValueMap>(*params)) {
      parameter_names.insert(p.first);
    }
    return parameter_names;
  } else {
    return static_cast<int64_t>(absl::get<ParameterValueList>(*params).size());
  }
}
}  // namespace

absl::StatusOr<std::unique_ptr<ScriptExecutor>> ScriptExecutorImpl::Create(
    absl::string_view script, const ASTScript* ast_script,
    const ScriptExecutorOptions& options, StatementEvaluator* evaluator) {
  ZETASQL_RET_CHECK_GE(options.maximum_stack_depth(), 1) << absl::Substitute(
      "Maximum stack depth must be at least 1, $0 was provided",
      options.maximum_stack_depth());
  ErrorMessageMode error_message_mode = options.error_message_mode();

  std::unique_ptr<const ParsedScript> parsed_script;
  if (ast_script != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(parsed_script, ParsedScript::Create(
                                        script, ast_script, error_message_mode,
                                        options.script_variables()));
  } else {
    ParserOptions parser_options;
    parser_options.set_language_options(&options.language_options());
    ZETASQL_ASSIGN_OR_RETURN(
        parsed_script,
        ParsedScript::Create(script, parser_options, error_message_mode,
                             options.script_variables()));
  }
  if (!options.dry_run()) {
    ZETASQL_RETURN_IF_ERROR(
        parsed_script->CheckQueryParameters(options.query_parameters()));
  }
  std::unique_ptr<ScriptExecutorImpl> script_executor(
      new ScriptExecutorImpl(options, std::move(parsed_script), evaluator));
  ZETASQL_RETURN_IF_ERROR(
      script_executor->SetPredefinedVariables(options.script_variables()));
  return absl::WrapUnique<ScriptExecutor>(script_executor.release());
}

ScriptExecutorImpl::ScriptExecutorImpl(
    const ScriptExecutorOptions& options,
    std::unique_ptr<const ParsedScript> parsed_script,
    StatementEvaluator* evaluator)
    : options_(options),
      evaluator_(evaluator),
      total_memory_usage_(0),
      type_factory_(options.type_factory()) {
  const ControlFlowNode* start_node =
      parsed_script->control_flow_graph().start_node();
  callstack_.emplace_back(StackFrameImpl(std::move(parsed_script), start_node));
  if (type_factory_ == nullptr) {
    type_factory_holder_ = absl::make_unique<TypeFactory>();
    type_factory_ = type_factory_holder_.get();
  }
  SetSystemVariablesForPendingException();
  system_variables_.emplace(std::vector<std::string>{kTimeZoneSystemVarName},
                            Value::String(options.default_time_zone().name()));
  time_zone_ = options.default_time_zone();
}

void ScriptExecutorImpl::SetSystemVariablesForPendingException() {
  const ScriptException& exception = (pending_exceptions_.empty())
                                         ? ScriptException::default_instance()
                                         : pending_exceptions_.back();

  if (exception.has_message()) {
    system_variables_[{"error", "message"}] =
        Value::String(exception.message());
  } else {
    system_variables_[{"error", "message"}] = Value::NullString();
  }
  if (exception.internal().has_statement_text()) {
    system_variables_[{"error", "statement_text"}] =
        Value::String(exception.internal().statement_text());
  } else {
    system_variables_[{"error", "statement_text"}] = Value::NullString();
  }
  if (!exception.internal().stack_trace().empty()) {
    absl::StatusOr<Value> stack_trace_value =
        GetStackTraceSystemVariable(exception.internal());
    if (stack_trace_value.ok()) {
      system_variables_[{"error", "stack_trace"}] = stack_trace_value.value();
    } else {
      ZETASQL_LOG(ERROR) << "Unable to get value for @@error.stack_trace: "
                 << stack_trace_value.status();
    }
    system_variables_[{"error", "formatted_stack_trace"}] =
        GetFormattedStackTrace(exception.internal());
  } else {
    absl::StatusOr<const Type*> stack_trace_type =
        GetStackTraceSystemVariableType();
    if (stack_trace_type.ok()) {
      system_variables_[{"error", "stack_trace"}] =
          Value::Null(stack_trace_type.value());
    } else {
      ZETASQL_LOG(ERROR) << "Unable to get type for @@error.stack_trace: "
                 << stack_trace_type.status();
    }
    system_variables_[{"error", "formatted_stack_trace"}] = Value::NullString();
  }
}

Value ScriptExecutorImpl::GetFormattedStackTrace(
    const ScriptException::Internal& internal_exception) {
  std::vector<StackFrameTrace> frames;
  for (const auto& frame_proto : internal_exception.stack_trace()) {
    StackFrameTrace* frame = &frames.emplace_back();
    frame->start_line = static_cast<int>(frame_proto.line());
    frame->start_column = static_cast<int>(frame_proto.column());
    frame->procedure_name = frame_proto.location();
  }
  return Value::StringValue(
      StackTraceString(absl::MakeSpan(frames), /*verbose=*/false));
}

absl::StatusOr<Value> ScriptExecutorImpl::GetStackTraceSystemVariable(
    const ScriptException::Internal& internal_exception) {
  ZETASQL_ASSIGN_OR_RETURN(const ArrayType* type, GetStackTraceSystemVariableType());
  std::vector<Value> frame_values;
  for (const auto& frame : internal_exception.stack_trace()) {
    frame_values.push_back(
        Value::Struct(type->element_type()->AsStruct(),
                      {Value::Int64(frame.line()), Value::Int64(frame.column()),
                       Value::NullString(),  // filename
                       frame.has_location() ? Value::String(frame.location())
                                            : Value::NullString()}));
  }

  return Value::Array(type, frame_values);
}

absl::StatusOr<const ArrayType*>
ScriptExecutorImpl::GetStackTraceSystemVariableType() {
  if (stack_trace_system_variable_type_ == nullptr) {
    const StructType* stack_frame_type;
    ZETASQL_RETURN_IF_ERROR(type_factory_->MakeStructType(
        {StructField("line", type_factory_->get_int64()),
         StructField("column", type_factory_->get_int64()),
         StructField("filename", type_factory_->get_string()),
         StructField("location", type_factory_->get_string())},
        &stack_frame_type));

    ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(
        stack_frame_type, &stack_trace_system_variable_type_));
  }
  return stack_trace_system_variable_type_;
}

absl::Status ScriptExecutorImpl::GenerateStackTraceSystemVariable(
    ScriptException::Internal* proto) {
  for (auto it = callstack_.rbegin(); it != callstack_.rend(); ++it) {
    const auto& frame = *it;
    ScriptException::StackTraceFrame* frame_proto = proto->add_stack_trace();
    ZETASQL_RET_CHECK_NE(frame.current_node(), nullptr);
    ParseLocationTranslator translator(frame.parsed_script()->script_text());
    std::pair<int, int> line_and_column;
    ZETASQL_ASSIGN_OR_RETURN(
        line_and_column,
        translator.GetLineAndColumnAfterTabExpansion(
            frame.current_node()->ast_node()->GetParseLocationRange().start()));
    frame_proto->set_line(line_and_column.first);
    frame_proto->set_column(line_and_column.second);
    if (frame.procedure_definition() != nullptr) {
      frame_proto->set_location(frame.procedure_definition()->name());
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string>
ScriptExecutorImpl::GenerateStatementTextSystemVariable() const {
  const ASTNode* current_node = callstack_.back().current_node()->ast_node();
  ZETASQL_RET_CHECK_NE(current_node, nullptr);
  switch (current_node->node_kind()) {
    case AST_IF_STATEMENT:
    case AST_WHILE_STATEMENT:
    case AST_FOR_IN_STATEMENT: {
      // Show truncated statement with condition only.  Showing every statement
      // in the body would be too much.
      ParseLocationRange range;
      range.set_start(current_node->GetParseLocationRange().start());
      if (current_node->node_kind() == AST_IF_STATEMENT) {
        range.set_end(current_node->GetAsOrDie<ASTIfStatement>()
                      ->condition()
                      ->GetParseLocationRange()
                      .end());
      } else if (current_node->node_kind() == AST_WHILE_STATEMENT) {
        range.set_end(current_node->GetAsOrDie<ASTWhileStatement>()
                      ->condition()
                      ->GetParseLocationRange()
                      .end());
      } else if (current_node->node_kind() == AST_FOR_IN_STATEMENT) {
        range.set_end(current_node->GetAsOrDie<ASTForInStatement>()
                      ->query()
                      ->GetParseLocationRange()
                      .end());
      } else {
        return zetasql_base::InternalErrorBuilder() << "Unexpected node kind.";
      }
      return absl::StrCat(
          GetScriptText().substr(
              range.start().GetByteOffset(),
              range.end().GetByteOffset() - range.start().GetByteOffset()),
          "...");
    }
    default:
      // Show the entire statement
      return std::string(
          ScriptSegment::FromASTNode(GetScriptText(), current_node)
              .GetSegmentText());
  }
}

absl::StatusOr<ScriptException> ScriptExecutorImpl::SetupNewException(
    const absl::Status& status) {
  ZETASQL_DCHECK(!status.ok() && internal::HasPayloadWithType<ScriptException>(status));
  ZETASQL_DCHECK(!options_.dry_run());

  // Should not get here when rethrowing a prior exception.
  const ASTNode* curr_node = callstack_.back().current_node()->ast_node();
  ZETASQL_DCHECK(curr_node->node_kind() != AST_RAISE_STATEMENT ||
         !curr_node->GetAsOrDie<ASTRaiseStatement>()->is_rethrow());

  ScriptException exception = internal::GetPayload<ScriptException>(status);
  if (!exception.has_message()) {
    // Use the absl::Status message as the default
    exception.set_message(std::string(status.message()));
  }

  if (exception.has_internal()) {
    return zetasql_base::InternalErrorBuilder()
           << "Engines should not set ScriptException::internal field when "
              "raising an exception"
           << exception.DebugString();
  }

  ZETASQL_ASSIGN_OR_RETURN(*exception.mutable_internal()->mutable_statement_text(),
                   GenerateStatementTextSystemVariable());
  ZETASQL_RETURN_IF_ERROR(
      GenerateStackTraceSystemVariable(exception.mutable_internal()));
  return exception;
}

absl::Status ScriptExecutorImpl::EnterExceptionHandler(
    const ScriptException& exception) {
  ZETASQL_DCHECK(!options_.dry_run());
  sql_feature_usage_.set_exception(sql_feature_usage_.exception() + 1);
  triggered_features_.insert(ScriptExecutorStateProto::EXCEPTION_CAUGHT);
  pending_exceptions_.push_back(exception);
  SetSystemVariablesForPendingException();

  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ExitExceptionHandler() {
  ZETASQL_DCHECK(!options_.dry_run());
  ZETASQL_RET_CHECK(!pending_exceptions_.empty());
  pending_exceptions_.pop_back();
  SetSystemVariablesForPendingException();
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ExitForLoop() {
  ZETASQL_RET_CHECK(!options_.dry_run());
  ZETASQL_RET_CHECK(!callstack_.back().mutable_for_loop_stack()->empty());
  ZETASQL_ASSIGN_OR_RETURN(int64_t iterator_memory,
                   GetIteratorMemoryUsage(
                       callstack_.back().for_loop_stack().back().get()));
  ZETASQL_RETURN_IF_ERROR(UpdateAndCheckMemorySize(
      GetCurrentNode()->ast_node(),
      /*current_memory_size=*/ 0,
      iterator_memory));
  callstack_.back().mutable_for_loop_stack()->pop_back();
  return absl::OkStatus();
}

absl::StatusOr<const ASTStatement*> ScriptExecutorImpl::ExitProcedure(
    bool normal_return) {
  if (callstack_.size() > 1) {
    ZETASQL_RETURN_IF_ERROR(ExitFromProcedure(*CurrentProcedure(), &callstack_.back(),
                                      GetMutableParentStackFrame(),
                                      normal_return));
    callstack_.pop_back();

    const ASTNode* current_node = callstack_.back().current_node()->ast_node();
    ZETASQL_RET_CHECK(current_node != nullptr);
    ZETASQL_RET_CHECK(current_node->node_kind() == AST_CALL_STATEMENT ||
              current_node->node_kind() == AST_EXECUTE_IMMEDIATE_STATEMENT)
        << "Expected call or execute immediate statement; got "
        << current_node->GetNodeKindString();
    return current_node->GetAsOrDie<ASTStatement>();
  } else {
    // End of top-level script
    callstack_.back().SetCurrentNode(nullptr);
    return nullptr;
  }
}

bool ScriptExecutorImpl::IsComplete() const {
  return callstack_.back().current_node() == nullptr ||
         callstack_.back().current_node()->ast_node() == nullptr;
}

absl::Status ScriptExecutorImpl::ExecuteNext() {
  absl::Status status = ExecuteNextImpl();
  return ConvertInternalErrorLocationAndAdjustErrorString(
      options_.error_message_mode(), CurrentScript()->script_text(), status);
}

absl::Status ScriptExecutorImpl::ExecuteNextImpl() {
  ZETASQL_RET_CHECK(!IsComplete()) << "Script already completed";
  const ASTNode* curr_ast_node = callstack_.back().current_node()->ast_node();
  switch (curr_ast_node->node_kind()) {
    case AST_BEGIN_END_BLOCK:
      ZETASQL_RETURN_IF_ERROR(EnterBlock());
      break;
    case AST_BREAK_STATEMENT:
    case AST_CONTINUE_STATEMENT:
    case AST_RETURN_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          absl::OkStatus()));
      break;
    case AST_IF_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentCondition(EvaluateCondition(
          curr_ast_node->GetAs<ASTIfStatement>()->condition())));
      break;
    case AST_ELSEIF_CLAUSE:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentCondition(EvaluateCondition(
          curr_ast_node->GetAs<ASTElseifClause>()->condition())));
      break;
    case AST_CASE_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          ExecuteCaseStatement()));
      break;
    case AST_WHEN_THEN_CLAUSE:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentCondition(
          EvaluateWhenThenClause()));
      break;
    case AST_ASSIGNMENT_FROM_STRUCT:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          ExecuteAssignmentFromStruct()));
      break;
    case AST_SINGLE_ASSIGNMENT:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          ExecuteSingleAssignment()));
      break;
    case AST_VARIABLE_DECLARATION:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          ExecuteVariableDeclaration()));
      break;
    case AST_WHILE_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(ExecuteWhileCondition());
      break;
    case AST_REPEAT_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          absl::OkStatus()));
      break;
    case AST_UNTIL_CLAUSE:
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentCondition(EvaluateCondition(
          curr_ast_node->GetAs<ASTUntilClause>()->condition())));
      break;
    case AST_FOR_IN_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(MaybeDispatchException(ExecuteForInStatement()));
      break;
    case AST_CALL_STATEMENT:
      sql_feature_usage_.set_call_stmt(sql_feature_usage_.call_stmt() + 1);
      triggered_features_.insert(ScriptExecutorStateProto::CALL_STATEMENT);
      ZETASQL_RETURN_IF_ERROR(MaybeDispatchException(
          ExecuteCallStatement()));
      break;
    case AST_RAISE_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(ExecuteRaiseStatement(
          curr_ast_node->GetAsOrDie<ASTRaiseStatement>()));
      break;
    case AST_SYSTEM_VARIABLE_ASSIGNMENT:
      ZETASQL_RETURN_IF_ERROR(
          AdvancePastCurrentStatement(ExecuteSystemVariableAssignment(
              curr_ast_node->GetAsOrDie<ASTSystemVariableAssignment>())));
      break;
    case AST_EXECUTE_IMMEDIATE_STATEMENT:
      sql_feature_usage_.set_execute_immediate_stmt(
          sql_feature_usage_.execute_immediate_stmt() + 1);
      triggered_features_.insert(
          ScriptExecutorStateProto::EXECUTE_IMMEDIATE_STATEMENT);
      ZETASQL_RETURN_IF_ERROR(MaybeDispatchException(
          ExecuteDynamicStatement()));
      break;
    default:
      if (!curr_ast_node->IsSqlStatement()) {
        return absl::UnimplementedError(
            absl::StrCat("Statement kind not implemented: ",
                         curr_ast_node->GetNodeKindString()));
      }
      ScriptSegment current_statement_segment = ScriptSegment::FromASTNode(
          CurrentScript()->script_text(), curr_ast_node);
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          evaluator_->ExecuteStatement(*this, current_statement_segment)));
  }

  return absl::OkStatus();
}

absl::StatusOr<bool> ScriptExecutorImpl::DefaultAssignSystemVariable(
    const ASTSystemVariableAssignment* ast_assignment, const Value& value) {
  // @@time_zone can be assigned to; all other executor-owned system variables
  // are read-only
  std::vector<std::string> var_name =
      ast_assignment->system_variable()->path()->ToIdentifierVector();
  if (var_name == std::vector<std::string>{kTimeZoneSystemVarName}) {
    ZETASQL_RETURN_IF_ERROR(SetTimezone(value, ast_assignment->expression()));
    return true;
  }

  if (zetasql_base::ContainsKey(system_variables_, var_name)) {
    return AssignmentToReadOnlySystemVariable(ast_assignment);
  }

  // Don't know about this variable; defer to the engine.
  return false;
}

namespace {
absl::StatusOr<absl::TimeZone> ParseTimezone(absl::string_view input) {
  static constexpr LazyRE2 kUtcOffsetPattern = {R"(UTC([+-])(\d{1,2}))"};
  absl::string_view utc_offset_sign;
  absl::string_view utc_offset_number_str;
  if (RE2::FullMatch(input, *kUtcOffsetPattern, &utc_offset_sign,
                     &utc_offset_number_str)) {
    int utc_offset_number;
    if (absl::SimpleAtoi(utc_offset_number_str, &utc_offset_number) &&
        utc_offset_number >= 0 && utc_offset_number < 24) {
      if (utc_offset_sign == "-") {
        return absl::FixedTimeZone(-3600 * utc_offset_number);
      } else {
        return absl::FixedTimeZone(3600 * utc_offset_number);
      }
    }
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid UTC offset: " << input;
  }
  absl::TimeZone timezone;
  if (absl::LoadTimeZone(input, &timezone)) {
    return timezone;
  }
  return zetasql_base::InvalidArgumentErrorBuilder() << "Invalid timezone: " << input;
}
}  // namespace

absl::Status ScriptExecutorImpl::SetTimezone(const Value& timezone_value,
                                             const ASTNode* location) {
  if (timezone_value.is_null()) {
    return MakeScriptExceptionAt(location) << "Invalid timezone: NULL";
  }
  ZETASQL_ASSIGN_OR_RETURN(time_zone_, ParseTimezone(timezone_value.string_value()),
                   MakeScriptExceptionAt(location)
                       << absl::Status(_).message());

  // Save the timezone in both the analyzer options and in the system variable
  // map for when "@@time_zone" is evaluated directly.
  system_variables_[{kTimeZoneSystemVarName}] = timezone_value;
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ExecuteSystemVariableAssignment(
    const ASTSystemVariableAssignment* stmt) {
  std::vector<std::string> var_name =
      stmt->system_variable()->path()->ToIdentifierVector();
  // Try ScriptExecutor-owned system variables.
  auto it_scriptexecutor = system_variables_.find(var_name);
  if (it_scriptexecutor != system_variables_.end()) {
    ZETASQL_ASSIGN_OR_RETURN(Value new_value,
                     EvaluateExpression(stmt->expression(),
                                        it_scriptexecutor->second.type()));
    return evaluator_->AssignSystemVariable(this, stmt, new_value);
  }

  // Now, try engine-owned system variables, which appear in the options.
  auto it_engine = options_.engine_owned_system_variables().find(var_name);
  if (it_engine != options_.engine_owned_system_variables().end()) {
    ZETASQL_ASSIGN_OR_RETURN(Value new_value,
                     EvaluateExpression(stmt->expression(), it_engine->second));
    return evaluator_->AssignSystemVariable(this, stmt, new_value);
  }

  // System variable is neither ScriptExecutor-owned nor engine-owned.
  return MakeUnknownSystemVariableError(stmt->system_variable());
}

absl::Status ScriptExecutorImpl::ExecuteRaiseStatement(
    const ASTRaiseStatement* stmt) {
  if (stmt->is_rethrow()) {
    return ReraiseCaughtException(stmt);
  } else {
    return RaiseNewException(stmt);
  }
}

absl::Status ScriptExecutorImpl::RaiseNewException(
    const ASTRaiseStatement* stmt) {
  absl::StatusOr<Value> status_or_message_value =
      EvaluateExpression(stmt->message(), type_factory_->get_string());
  if (!status_or_message_value.ok()) {
    return AdvancePastCurrentStatement(
        status_or_message_value.status());
  }
  const Value& message_value = status_or_message_value.value();
  ZETASQL_RET_CHECK_EQ(message_value.type_kind(), TypeKind::TYPE_STRING);
  if (options_.dry_run()) {
    // In dry runs, we don't want the presence of a RAISE statement to fail
    // validation, even though it will always throw an exception.
    // Just accept the current statement and terminate (since we don't have
    // anything to come next).
    ZETASQL_RETURN_IF_ERROR(ExitProcedure(/*normal_return=*/false).status());
    return absl::OkStatus();
  }
  std::string message =
      message_value.is_null() ? "NULL" : message_value.string_value();
  return AdvancePastCurrentStatement(
      MakeScriptExceptionAt(stmt) << message);
}

absl::Status ScriptExecutorImpl::ReraiseCaughtException(
    const ASTRaiseStatement* stmt) {
  if (options_.dry_run()) {
    // In dry runs, we don't want the presence of a RAISE statement to fail
    // validation, even though it will always throw an exception.
    // Just accept the current statement and terminate (since we don't have
    // anything to come next).
    ZETASQL_RETURN_IF_ERROR(ExitProcedure(/*normal_return=*/false).status());
    return absl::OkStatus();
  }
  // It should not be possible to get here without a pending exception, since
  // ParsedScript already validated that RAISE statements do not appear outside
  // of an exception handler.
  ZETASQL_RET_CHECK(!pending_exceptions_.empty());

  // Copy the exception first, rather than pass a reference to it on the
  // pending_exceptions_ stack, to prevent it from being deleted underneath us
  // as we unwind out of the exception handler.
  ScriptException exception_copy(pending_exceptions_.back());
  return RethrowException(exception_copy);
}

absl::Status ScriptExecutorImpl::UpdateAndCheckMemorySize(
    const ASTNode* node, int64_t current_memory_size,
    int64_t previous_memory_size) {
  total_memory_usage_ += current_memory_size - previous_memory_size;
  if (total_memory_usage_ >
      options_.variable_size_limit_options().total_memory_limit()) {
    return MakeNoSizeRemainingError(
        node, options_.variable_size_limit_options().total_memory_limit(),
        callstack_);
  }
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::UpdateAndCheckVariableSize(
    const ASTNode* node, const IdString& var_name, int64_t variable_size,
    VariableSizesMap* variable_sizes_map) {
  int64_t& previous_variable_size = (*variable_sizes_map)[var_name];
  total_memory_usage_ += variable_size - previous_variable_size;
  if (variable_size >
      options_.variable_size_limit_options().per_variable_size_limit()) {
    return MakeVariableIsTooLargeError(
        node, var_name,
        options_.variable_size_limit_options().per_variable_size_limit());
  }
  (*variable_sizes_map)[var_name] = variable_size;

  if (total_memory_usage_ >
      options_.variable_size_limit_options().total_memory_limit()) {
    return MakeNoSizeRemainingError(
        node,
        options_.variable_size_limit_options().total_memory_limit(),
        callstack_);
  }

  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::OnVariablesValueChangedWithoutSizeCheck(
    bool notify_evaluator, const StackFrame& var_declaration_stack_frame,
    const ASTNode* current_node,
    const std::vector<VariableChange>& variable_changes) {
  if (variable_changes.empty() ||!notify_evaluator) {
    return absl::OkStatus();
  }
  return evaluator_->OnVariablesChanged(
      *this, current_node, var_declaration_stack_frame, variable_changes);
}

absl::Status ScriptExecutorImpl::OnVariablesValueChangedWithSizeCheck(
    bool notify_evaluator, const StackFrame& var_declaration_stack_frame,
    const ASTNode* current_node,
    const std::vector<VariableChange>& variable_changes,
    VariableSizesMap* variable_sizes_map) {
  if (variable_changes.empty()) {
    return absl::OkStatus();
  }
  for (auto& var : variable_changes) {
    ZETASQL_RETURN_IF_ERROR(UpdateAndCheckVariableSize(current_node, var.var_name,
                                               var.value.physical_byte_size(),
                                               variable_sizes_map));
  }
  return OnVariablesValueChangedWithoutSizeCheck(
      notify_evaluator, var_declaration_stack_frame, current_node,
      variable_changes);
}

// static
absl::Status ScriptExecutorImpl::MakeVariableIsTooLargeError(
    const ASTNode* node, const IdString& var_name,
    int64_t per_variable_size_limit) {
  zetasql_base::StatusBuilder status = MakeScriptExceptionAt(node)
                               << "Script variable " << var_name.ToString()
                               << " exceeded the size limit of "
                               << per_variable_size_limit << " bytes";
  return status;
}


absl::Status ScriptExecutorImpl::MakeNoSizeRemainingError(
    const ASTNode* node, int64_t total_size_limit,
    const std::vector<StackFrameImpl>& callstack) {
  return MakeScriptExceptionAt(node)
     << "Script exceeded the memory limit of "
     << total_size_limit << "bytes";
}

absl::Status ScriptExecutorImpl::ExecuteSingleAssignment() {
  auto assignment = callstack_.back()
                        .current_node()
                        ->ast_node()
                        ->GetAs<ASTSingleAssignment>();
  IdString var_name = assignment->variable()->GetAsIdString();

  ZETASQL_ASSIGN_OR_RETURN(Value * value, MutableVariableValue(assignment->variable()));
  ZETASQL_ASSIGN_OR_RETURN(*value,
                   EvaluateExpression(assignment->expression(), value->type()));

  // If type parameters exist for the variable, apply type parameter constraints
  // to the variable.
  TypeParameters type_param;
  auto it = GetCurrentVariableTypeParameters().find(var_name);
  if (it != GetCurrentVariableTypeParameters().end()) {
    type_param = it->second;
    ZETASQL_RETURN_IF_ERROR(CheckConstraintStatus(
        evaluator_->ApplyTypeParameterConstraints(type_param, value),
        assignment->expression()));
  }

  VariableChange var = {var_name, *value, type_param};
  ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithSizeCheck(
      /*notify_evaluator=*/true, callstack_.back(), assignment, {var},
      MutableCurrentVariableSizes()));
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ExecuteAssignmentFromStruct() {
  auto assignment = callstack_.back()
                        .current_node()
                        ->ast_node()
                        ->GetAs<ASTAssignmentFromStruct>();
  // Build up a list of variables to assign, and make sure the same variable is
  // not being assigned to more than once.
  absl::flat_hash_set<IdString, IdStringCaseHash, IdStringCaseEqualFunc>
      variables_to_assign;
  for (const ASTIdentifier* var : assignment->variables()->identifier_list()) {
    if (!zetasql_base::InsertIfNotPresent(&variables_to_assign, var->GetAsIdString())) {
      return MakeScriptExceptionAt(var)
             << "Assigning multiple values to variable: " << var->GetAsString();
    }
  }

  // Obtain the expected type for a struct expression which has one field per
  // variable being assigned to.
  TypeFactory type_factory;

  std::vector<StructType::StructField> fields;
  for (const ASTIdentifier* var : assignment->variables()->identifier_list()) {
    ZETASQL_ASSIGN_OR_RETURN(Value * value, MutableVariableValue(var));
    fields.emplace_back("" /*unnamed field*/, value->type());
  }

  const Type* struct_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeStructType(fields, &struct_type));

  // Evaluate the expression and coerce it to the expected type
  ZETASQL_ASSIGN_OR_RETURN(
      Value struct_value,
      EvaluateExpression(assignment->struct_expression(), struct_type));

  // Assign each variable to the appropriate field.
  ZETASQL_RET_CHECK_EQ(struct_value.type()->AsStruct()->num_fields(),
               assignment->variables()->identifier_list().size())
      << "Wrong number of fields; type conversion should have failed in "
         "EvaluateExpression()";

  std::vector<VariableChange> variable_changes;
  for (int i = 0; i < struct_value.type()->AsStruct()->num_fields(); i++) {
    const IdString& var_name =
        assignment->variables()->identifier_list()[i]->GetAsIdString();
    Value& var_value = MutableCurrentVariables()->at(var_name);
    if (struct_value.is_null()) {
      var_value = Value::Null(var_value.type());
    } else {
      var_value = struct_value.field(i);
    }

    // If type parameters exist for the variable, apply type parameter
    // constraints to the variable.
    TypeParameters type_param;
    auto it = GetCurrentVariableTypeParameters().find(var_name);
    if (it != GetCurrentVariableTypeParameters().end()) {
      type_param = it->second;
      ZETASQL_RETURN_IF_ERROR(CheckConstraintStatus(
          evaluator_->ApplyTypeParameterConstraints(type_param, &var_value),
          assignment->struct_expression()->child(i)));
    }
    ZETASQL_RETURN_IF_ERROR(UpdateAndCheckVariableSize(assignment, var_name,
                                               var_value.physical_byte_size(),
                                               MutableCurrentVariableSizes()));
    VariableChange var = {var_name, var_value, type_param};
    variable_changes.emplace_back(var);
  }
  ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithoutSizeCheck(
      /*notify_evaluator=*/true, callstack_.back(), assignment,
      variable_changes));
  return absl::OkStatus();
}

namespace {
absl::Status ThrowErrorIfASTTypeHasCollation(const ASTType* type) {
  if (type == nullptr) {
    return absl::OkStatus();
  }

  if (type->collate() != nullptr) {
    return MakeScriptExceptionAt(type->collate())
           << "Type with collation name is not supported";
  }

  switch (type->node_kind()) {
    case AST_SIMPLE_TYPE:
      return absl::OkStatus();
    case AST_ARRAY_TYPE: {
      return ThrowErrorIfASTTypeHasCollation(
          type->GetAsOrDie<ASTArrayType>()->element_type());
    }
    case AST_STRUCT_TYPE: {
      for (auto struct_field :
           type->GetAsOrDie<ASTStructType>()->struct_fields()) {
        ZETASQL_RETURN_IF_ERROR(ThrowErrorIfASTTypeHasCollation(struct_field->type()));
      }
      return absl::OkStatus();
    }
    default:
      // This will never happen since <type> cannot be of other node kinds.
      ZETASQL_RET_CHECK_FAIL() << type->DebugString();
      break;
  }
  return absl::OkStatus();
}
}  // namespace

absl::Status ScriptExecutorImpl::ExecuteVariableDeclaration() {
  auto declaration = callstack_.back()
                         .current_node()
                         ->ast_node()
                         ->GetAs<ASTVariableDeclaration>();

  ZETASQL_RETURN_IF_ERROR(ThrowErrorIfASTTypeHasCollation(declaration->type()));

  TypeWithParameters type_with_params;
  if (declaration->type() != nullptr) {
    auto segment = ScriptSegment::FromASTNode(CurrentScript()->script_text(),
                                              declaration->type());
    ZETASQL_ASSIGN_OR_RETURN(type_with_params,
                     evaluator_->ResolveTypeName(*this, segment));

    ZETASQL_RETURN_IF_ERROR(type_with_params.type->ValidateResolvedTypeParameters(
        type_with_params.type_params, LanguageOptions().product_mode()));
    if (!evaluator_->IsSupportedVariableType(type_with_params)) {
      return MakeScriptExceptionAt(declaration->type())
             << "Unsupported variable type: " << segment.GetSegmentText();
    }
  }
  Value default_value;
  if (declaration->default_value() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(default_value,
                     EvaluateExpression(declaration->default_value(),
                                        type_with_params.type));

    // If the variable's type was inferred from the default value expression,
    // make sure the type is supported as a variable type.
    if (type_with_params.type == nullptr &&
        !evaluator_->IsSupportedVariableType(
            {default_value.type(), TypeParameters()})) {
      return MakeScriptExceptionAt(declaration->default_value())
             << "Unsupported variable type: "
             << default_value.type()->TypeName(
                    options_.language_options().product_mode());
    }
  } else {
    // The grammar forbids a DECLARE statement missing both a type and a default
    // value expression, so if we don't have a default value, we must, for sure,
    // have a type.
    ZETASQL_RET_CHECK_NE(type_with_params.type, nullptr)
        << "DECLARE statement without either type or default value specified: "
        << declaration->DebugString();
    default_value = Value::Null(type_with_params.type);
  }

  std::vector<VariableChange> variable_changes;
  for (const ASTIdentifier* var_id :
       declaration->variable_list()->identifier_list()) {
    ZETASQL_RETURN_IF_ERROR(
        CheckConstraintStatus(evaluator_->ApplyTypeParameterConstraints(
                                  type_with_params.type_params, &default_value),
                              declaration->default_value()));
    zetasql_base::InsertOrUpdate(MutableCurrentVariables(), var_id->GetAsIdString(),
                        default_value);
    if (!type_with_params.type_params.IsEmpty()) {
      zetasql_base::InsertOrUpdate(MutableCurrentVariableTypeParameters(),
                          var_id->GetAsIdString(),
                          type_with_params.type_params);
    }
    ZETASQL_RETURN_IF_ERROR(UpdateAndCheckVariableSize(
        declaration, var_id->GetAsIdString(),
        default_value.physical_byte_size(), MutableCurrentVariableSizes()));
    VariableChange var = {var_id->GetAsIdString(), default_value,
                          type_with_params.type_params};
    variable_changes.emplace_back(var);
  }

  ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithoutSizeCheck(
      /*notify_evaluator=*/true, callstack_.back(), declaration,
      variable_changes));

  return absl::OkStatus();
}

absl::StatusOr<Value> ScriptExecutorImpl::EvaluateExpression(
    const ASTExpression* expr, const Type* target_type) const {
  ScriptSegment segment = SegmentForScalarExpression(expr);
  ZETASQL_ASSIGN_OR_RETURN(Value value, evaluator_->EvaluateScalarExpression(
                                    *this, segment, target_type));
  return value;
}

absl::StatusOr<bool> ScriptExecutorImpl::EvaluateCondition(
    const ASTExpression* condition) const {
  ZETASQL_ASSIGN_OR_RETURN(Value value,
                   EvaluateExpression(condition, types::BoolType()));

  // Per (broken link), a NULL condition result is equivalent to false.
  return value.is_null() ? false : value.bool_value();
}

absl::Status ScriptExecutorImpl::ExecuteWhileCondition() {
  const ASTWhileStatement* const while_stmt =
      callstack_.back().current_node()->ast_node()->GetAs<ASTWhileStatement>();
  if (while_stmt->condition() != nullptr) {
    return AdvancePastCurrentCondition(
        EvaluateCondition(while_stmt->condition()));
  } else {
    // A while statement without a condition is a LOOP...END LOOP construct;
    // advance unconditionally.
    return AdvancePastCurrentStatement(
        absl::OkStatus());
  }
}

absl::Status ScriptExecutorImpl::ExecuteCaseStatement() {
  const ASTCaseStatement* const case_stmt =
      callstack_.back().current_node()->ast_node()->GetAs<ASTCaseStatement>();
  if (case_stmt->expression() == nullptr) {
    return absl::OkStatus();
  }

  // Evaluate CASE expression and WHEN conditions, fetch the index of the WHEN
  // branch that should be entered.
  std::vector<ScriptSegment> conditions;
  auto when_clauses = case_stmt->when_then_clauses()->when_then_clauses();
  conditions.reserve(when_clauses.length());
  for (int i = 0; i < when_clauses.length(); i++) {
    conditions.push_back(SegmentForScalarExpression(
        when_clauses.at(i)->condition()));
  }
  ZETASQL_ASSIGN_OR_RETURN(
      case_stmt_true_branch_index_,
      evaluator_->EvaluateCaseExpression(
          SegmentForScalarExpression(case_stmt->expression()),
          conditions, *this));
  // Reset WHEN branch tracker index.
  case_stmt_current_branch_index_ = -1;

  return absl::OkStatus();
}

absl::StatusOr<bool> ScriptExecutorImpl::EvaluateWhenThenClause() {
  const ASTWhenThenClause* const when_clause =
      callstack_.back().current_node()->ast_node()->GetAs<ASTWhenThenClause>();
  if (when_clause->case_stmt()->expression() == nullptr) {
    return EvaluateCondition(when_clause->condition());
  }

  case_stmt_current_branch_index_++;
  return case_stmt_current_branch_index_ == case_stmt_true_branch_index_;
}

absl::StatusOr<Value> ScriptExecutorImpl::GenerateStructValueFromRow(
    TypeFactory* type_factory,
    const EvaluatorTableIterator& iterator,
    const StructType* struct_type) const {
  // If struct_type is nullptr, construct from iterator.
  if (struct_type == nullptr) {
    // Gather field from each column to build struct type.
    std::vector<StructType::StructField> fields;
    fields.reserve(iterator.NumColumns());
    for (int col = 0; col < iterator.NumColumns(); col++) {
      fields.emplace_back(iterator.GetColumnName(col),
                          iterator.GetColumnType(col));
    }
    ZETASQL_RETURN_IF_ERROR(type_factory->MakeStructType(fields, &struct_type));
  }
  std::vector<Value> values;
  values.reserve(iterator.NumColumns());
  for (int col = 0; col < iterator.NumColumns(); col++) {
    values.push_back(iterator.GetValue(col));
  }
  return Value::Struct(struct_type, values);
}

absl::Status ScriptExecutorImpl::ExecuteForInStatement() {
  const ControlFlowNode * cfg_node = callstack_.back().current_node();
  const ASTForInStatement* for_stmt =
      cfg_node->ast_node()->GetAs<ASTForInStatement>();
  std::vector<std::unique_ptr<EvaluatorTableIterator>>* for_loop_stack =
      callstack_.back().mutable_for_loop_stack();

  if (cfg_node->kind() == ControlFlowNode::Kind::kForInitial) {
    // Begin new loop
    ScriptSegment query_segment = ScriptSegment::FromASTNode(
        CurrentScript()->script_text(),
        for_stmt->query());
    // Placeholder value in case function exits early with error.
    for_loop_stack->push_back(nullptr);
    ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<EvaluatorTableIterator> iterator,
      evaluator_->ExecuteQueryWithResult(*this, query_segment));

    // If next row exists, generates value for row and assigns to variable.
    if (iterator->NextRow()) {
      TypeFactory type_factory;
      ZETASQL_ASSIGN_OR_RETURN(Value variable_value,
                       GenerateStructValueFromRow(&type_factory,
                                                  *iterator,
                                                  /* struct_type */ nullptr));
      ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(MutableCurrentVariables(),
                                        for_stmt->variable()->GetAsIdString(),
                                        variable_value));
      VariableChange var = {for_stmt->variable()->GetAsIdString(),
                            variable_value, TypeParameters()};
      ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithSizeCheck(
          /*notify_evaluator=*/true, callstack_.back(),
          callstack_.back().current_node()->ast_node(), {var},
          MutableCurrentVariableSizes()));

      for_loop_stack->pop_back();
      for_loop_stack->push_back(std::move(iterator));
      ZETASQL_ASSIGN_OR_RETURN(
          int64_t iterator_memory,
          GetIteratorMemoryUsage(for_loop_stack->back().get()));
      ZETASQL_RETURN_IF_ERROR(UpdateAndCheckMemorySize(
          for_stmt, iterator_memory, /*previous_memory_size=*/ 0));

      return AdvancePastCurrentCondition(true);
    }

    // NextRow() returned false, so check status before advancing.
    if (!iterator->Status().ok()) {
      return MakeScriptExceptionAt(for_stmt) << iterator->Status();
    }
    for_loop_stack->pop_back();
    for_loop_stack->push_back(std::move(iterator));
    ZETASQL_ASSIGN_OR_RETURN(
        int64_t iterator_memory,
        GetIteratorMemoryUsage(for_loop_stack->back().get()));
    ZETASQL_RETURN_IF_ERROR(UpdateAndCheckMemorySize(
        for_stmt, iterator_memory, /*previous_memory_size=*/ 0));

    return AdvancePastCurrentCondition(false);
  } else if (cfg_node->kind() == ControlFlowNode::Kind::kForAdvance) {
    // Advance running loop
    ZETASQL_RET_CHECK(!for_loop_stack->empty());
    EvaluatorTableIterator * iterator = for_loop_stack->back().get();
    ZETASQL_ASSIGN_OR_RETURN(int64_t previous_iterator_memory,
                     GetIteratorMemoryUsage(iterator));

    // If next row exists, store the row as a struct.
    if (iterator->NextRow()) {
      Value* variable_value =
          zetasql_base::FindOrNull(*MutableCurrentVariables(),
                          for_stmt->variable()->GetAsIdString());
      ZETASQL_RET_CHECK(variable_value != nullptr)
          << "Variable "
          << for_stmt->variable()->GetAsIdString().ToStringView()
          << " should be in scope, but cannot be found";

      TypeFactory type_factory;
      ZETASQL_ASSIGN_OR_RETURN(*variable_value, GenerateStructValueFromRow(
          &type_factory, *iterator, variable_value->type()->AsStruct()));

      VariableChange var = {for_stmt->variable()->GetAsIdString(),
                            *variable_value, TypeParameters()};
      ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithSizeCheck(
          /*notify_evaluator=*/true, callstack_.back(),
          callstack_.back().current_node()->ast_node(), {var},
          MutableCurrentVariableSizes()));

      ZETASQL_ASSIGN_OR_RETURN(int64_t iterator_memory,
                       GetIteratorMemoryUsage(iterator));
      ZETASQL_RETURN_IF_ERROR(UpdateAndCheckMemorySize(
          for_stmt, iterator_memory, previous_iterator_memory));

      return AdvancePastCurrentCondition(true);
    }
    // NextRow() returned false, so check status before advancing.
    if (!iterator->Status().ok()) {
      return MakeScriptExceptionAt(for_stmt) << iterator->Status();
    }
    ZETASQL_ASSIGN_OR_RETURN(int64_t iterator_memory,
                     GetIteratorMemoryUsage(iterator));
    ZETASQL_RETURN_IF_ERROR(UpdateAndCheckMemorySize(
        for_stmt, iterator_memory, previous_iterator_memory));

    return AdvancePastCurrentCondition(false);
  }
  // Should never reach here.
  return zetasql_base::InternalErrorBuilder()
      << "Control Flow nodes tied to ASTForInStatement should have"
      << "Kind::kForInitial or Kind::kForAdvance";
}

// Given <call_statement> calling a procedure defined <procedure_definition>,
// verifies if passed-in OUT/INOUT argument is valid. Returns a map from
// <passed-in variable name> to <argument_name>.
absl::StatusOr<OutputArgumentMap> VerifyOutputArgumentsAndBuildMap(
    const ProcedureDefinition& procedure_definition,
    const ASTCallStatement* call_statement, IdStringPool* id_string_pool) {
  OutputArgumentMap output_argument_map;
  for (int i = 0; i < procedure_definition.signature().NumRequiredArguments();
       i++) {
    const ASTTVFArgument* ast_tvf_argument = call_statement->arguments().at(i);
    // TODO: support table-typed arguments.
    if (ast_tvf_argument->table_clause() || ast_tvf_argument->model_clause()) {
      return MakeScriptExceptionAt(ast_tvf_argument)
             << (ast_tvf_argument->table_clause() ? "Table" : "Model")
             << " typed argument is not supported";
    }
    const ASTExpression* argument_expr = ast_tvf_argument->expr();
    FunctionEnums::ProcedureArgumentMode argument_mode =
        procedure_definition.signature()
            .argument(i)
            .options()
            .procedure_argument_mode();
    if (argument_mode == FunctionEnums::INOUT ||
        argument_mode == FunctionEnums::OUT) {
      if (argument_expr->node_kind() != AST_PATH_EXPRESSION ||
          argument_expr->GetAsOrDie<ASTPathExpression>()->num_names() != 1) {
        return MakeScriptExceptionAt(argument_expr)
               << "Procedure OUT/INOUT argument must be a variable";
      }
      const auto* path_expr = argument_expr->GetAsOrDie<ASTPathExpression>();
      IdString variable_name = path_expr->name(0)->GetAsIdString();
      if (output_argument_map.find(variable_name) !=
          output_argument_map.end()) {
        return MakeScriptExceptionAt(argument_expr)
               << "A variable cannot be passed in multiple times as "
                  "OUT/INOUT arguments: "
               << path_expr->ToIdentifierPathString();
      }
      output_argument_map[variable_name] =
          id_string_pool->Make(procedure_definition.argument_name_list()[i]);
    }
  }

  return output_argument_map;
}

absl::StatusOr<Value> ScriptExecutorImpl::EvaluateProcedureArgument(
    absl::string_view argument_name,
    const FunctionArgumentType& function_argument_type,
    const ASTExpression* argument_expr) {
  const Type* argument_type = function_argument_type.type();
  const bool is_any_type_argument = (function_argument_type.kind() ==
                                     SignatureArgumentKind::ARG_TYPE_ARBITRARY);
  ZETASQL_RET_CHECK(function_argument_type.kind() ==
                    SignatureArgumentKind::ARG_TYPE_FIXED &&
                argument_type != nullptr ||
            is_any_type_argument && argument_type == nullptr)
      << "Procedure arguments have either a concrete type or nullptr type in "
         "case of ANY TYPE argument";
  FunctionEnums::ProcedureArgumentMode argument_mode =
      function_argument_type.options().procedure_argument_mode();
  switch (argument_mode) {
    case FunctionEnums::NOT_SET:
    case FunctionEnums::IN: {
      // TODO: Extend EvaluateExpression to evaluate all
      // expressions with one request.
      return EvaluateExpression(argument_expr, argument_type);
    } break;
    case FunctionEnums::INOUT:
    case FunctionEnums::OUT: {
      const auto* path_expr = argument_expr->GetAsOrDie<ASTPathExpression>();
      IdString variable_name = path_expr->first_name()->GetAsIdString();
      ZETASQL_ASSIGN_OR_RETURN(Value * variable_value,
                       MutableVariableValue(path_expr->first_name()));
      const Type* variable_type = variable_value->type();
      if (!is_any_type_argument && !CoercesTo(argument_type, variable_type)) {
        return MakeScriptExceptionAt(argument_expr) << absl::Substitute(
                   "Variable $0 cannot be passed as output argument $1, for "
                   "argument type $2 cannot be coerced to variable type $3 "
                   "when procedure exits",
                   variable_name.ToStringView(), argument_name,
                   argument_type->ShortTypeName(
                       LanguageOptions().product_mode()),
                   variable_type->ShortTypeName(
                       LanguageOptions().product_mode()));
      }
      if (argument_mode == FunctionEnums::INOUT) {
        // Coerce pass-in variable value to argument type.
        return EvaluateExpression(argument_expr, argument_type);
      } else {
        // OUT argument always has initial value of NULL.
        return Value::Null(is_any_type_argument ? variable_type
                                                : argument_type);
      }
    }
  }
}

absl::Status ScriptExecutorImpl::CheckAndEvaluateProcedureArguments(
    const ASTCallStatement* call_statement,
    const ProcedureDefinition& procedure_definition,
    IdStringPool* id_string_pool, VariableMap* variables,
    VariableSizesMap* variable_sizes) {
  int number_of_defined_arguments =
      procedure_definition.signature().NumRequiredArguments();
  size_t number_of_defined_argument_names =
      procedure_definition.argument_name_list().size();
  ZETASQL_RET_CHECK_EQ(number_of_defined_arguments, number_of_defined_argument_names)
      << absl::Substitute(
             "Procedure $0 has $1 argument types but $2 argument names",
             call_statement->procedure_name()->ToIdentifierPathString(),
             number_of_defined_arguments, number_of_defined_argument_names);
  ZETASQL_RETURN_IF_ERROR(
      CheckProcedureArgumentCount(call_statement, number_of_defined_arguments));

  ZETASQL_ASSIGN_OR_RETURN(OutputArgumentMap unused_output_argument_map,
                   VerifyOutputArgumentsAndBuildMap(
                       procedure_definition, call_statement, id_string_pool));

  // Procedures may be called with named arguments. In this case, any positional
  // arguments should appear before any named arguments. Named arguments may
  // appear out of order, but it is an error to repeat or omit one.
  absl::flat_hash_set<absl::string_view, zetasql_base::StringViewCaseHash,
                      zetasql_base::StringViewCaseEqual>
      remaining_args;
  for (int i = 0; i < number_of_defined_arguments; i++) {
    const FunctionArgumentType& function_argument_type =
        procedure_definition.signature().argument(i);
    remaining_args.insert(function_argument_type.argument_name());
  }

  bool allow_positional_args = true;
  for (int i = 0; i < number_of_defined_arguments; i++) {
    const FunctionArgumentType& function_argument_type =
        procedure_definition.signature().argument(i);
    const ASTExpression* expr = call_statement->arguments().at(i)->expr();
    if (expr->Is<ASTNamedArgument>()) {
      allow_positional_args = false;
    } else if (!allow_positional_args) {
      return MakeScriptExceptionAt(expr)
             << "Positional arguments must appear before named arguments";
    }

    IdString argument_name =
        allow_positional_args
            ? id_string_pool->Make(procedure_definition.argument_name_list()[i])
            : expr->GetAs<ASTNamedArgument>()->name()->GetAsIdString();

    if (remaining_args.erase(argument_name.ToStringView()) == 0) {
      // We differentiate between duplicate names and unrecognized names in
      // order to give more specific error messages.
      absl::flat_hash_set<absl::string_view, zetasql_base::StringViewCaseHash,
                          zetasql_base::StringViewCaseEqual>
          all_args;
      for (int j = 0; j < number_of_defined_arguments; j++) {
        const FunctionArgumentType& function_argument_type =
            procedure_definition.signature().argument(j);
        all_args.insert(function_argument_type.argument_name());
      }

      if (all_args.find(argument_name.ToStringView()) != all_args.end()) {
        return MakeScriptExceptionAt(expr->GetAs<ASTNamedArgument>()->name())
               << "Duplicate procedure argument: "
               << argument_name.ToStringView();
      }
      return MakeScriptExceptionAt(expr->GetAs<ASTNamedArgument>()->name())
             << "Unrecognized procedure argument: "
             << argument_name.ToStringView();
    }

    ZETASQL_ASSIGN_OR_RETURN(Value argument_value,
                     EvaluateProcedureArgument(argument_name.ToStringView(),
                                               function_argument_type, expr));
    VariableChange var = {argument_name, argument_value, TypeParameters()};
    ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithSizeCheck(
        /*notify_evaluator=*/false, callstack_.back(), call_statement, {var},
        variable_sizes));
    (*variables)[argument_name] = std::move(argument_value);
  }

  ZETASQL_RET_CHECK(remaining_args.empty());
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ExecuteCallStatement() {
  const ASTCallStatement* call_statement = callstack_.back()
                                               .current_node()
                                               ->ast_node()
                                               ->GetAsOrDie<ASTCallStatement>();
  const ASTPathExpression* path_node = call_statement->procedure_name();
  if (callstack_.size() >= options_.maximum_stack_depth()) {
    return MakeScriptExceptionAt(call_statement) << absl::Substitute(
               "Out of stack space due to deeply-nested procedure call to $0",
               path_node->ToIdentifierPathString());
  }
  absl::StatusOr<std::unique_ptr<ProcedureDefinition>> status_or_definition =
      evaluator_->LoadProcedure(*this, path_node->ToIdentifierVector(),
                                call_statement->arguments().size());
  const absl::Status& status = status_or_definition.status();
  if (!status.ok()) {
    if (absl::IsNotFound(status)) {
      return MakeScriptExceptionAt(path_node)
             << "Procedure is not found: "
             << path_node->ToIdentifierPathString();
    }
    if (absl::IsInternal(status)) {
      return status;
    }
    // Attach other loading errors with procedure name.
    return MakeScriptExceptionAt(path_node) << status.message();
  }
  std::unique_ptr<ProcedureDefinition> procedure_definition =
      std::move(status_or_definition).value();
  // If the procedure_definition is null it indicates that we cannot get
  // information about the procedure. If so, we should only verify that
  // the arguments passed into CALL are valid expressions before continuing
  // execution.
  if (procedure_definition == nullptr) {
    ZETASQL_RET_CHECK(options_.dry_run());
    for (const zetasql::ASTTVFArgument* arg : call_statement->arguments()) {
      ZETASQL_RETURN_IF_ERROR(EvaluateExpression(arg->expr(), nullptr).status());
    }
    return AdvancePastCurrentStatement(
        absl::OkStatus());
  }
  auto id_string_pool = absl::make_unique<IdStringPool>();
  VariableMap variables;
  VariableSizesMap variable_sizes;
  ZETASQL_RETURN_IF_ERROR(CheckAndEvaluateProcedureArguments(
      call_statement, *procedure_definition, id_string_pool.get(), &variables,
      &variable_sizes));

  // If procedure is native, pass arguments prepared above to StatementEvaluator
  // to execute.
  if (procedure_definition->native_function()) {
    // We don't support dry-running native procedures, in part because we don't
    // know what the arguments would be when the procedure is run for real.
    if (!options_.dry_run()) {
      VariableMap& procedure_arguments = variables;
      ZETASQL_RETURN_IF_ERROR(
          procedure_definition->native_function()(&procedure_arguments))
          .With([path_node](zetasql_base::StatusBuilder builder) {
            // Attach location to error thrown by native procedure.
            if (!builder.ok()) {
              builder.Attach(GetErrorLocationPoint(
                                 path_node, /*include_leftmost_child*/ true)
                                 .ToInternalErrorLocation());
            }
            return builder;
          });
      // TODO: in current way, internal state of native procedure
      // cannot be saved. Because ScriptExecutor always resume from CALL
      // statement and will try to re-evaluate arguments, there is no guarantee
      // the arguments will be the same. A stackframe will have to be formed
      // here in order to persist a state of a running native procedure.
      ZETASQL_RETURN_IF_ERROR(AssignOutArguments(
          *procedure_definition, procedure_arguments, &callstack_.back()));
    }

    return AdvancePastCurrentStatement(
        absl::OkStatus());
  }
  // TODO: Report correct error when procedure body failed to
  // parse.
  ParsedScript::ArgumentTypeMap arguments_map;
  for (const auto& variable : variables) {
    arguments_map[variable.first] = variable.second.type();
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ParsedScript> parsed_script,
                   ParsedScript::CreateForRoutine(
                       procedure_definition->body(), GetParserOptions(),
                       options_.error_message_mode(), arguments_map));
  const ControlFlowNode* start_node =
      parsed_script->control_flow_graph().start_node();
  // For empty procedure, we don't have to create a stack frame, only assign
  // NULL to OUT arguments.
  if (start_node->ast_node() == nullptr) {
    ZETASQL_RETURN_IF_ERROR(AssignOutArguments(*procedure_definition.get(), variables,
                                       &callstack_.back()));
    ZETASQL_RETURN_IF_ERROR(
        AdvancePastCurrentStatement(absl::OkStatus()));
    return absl::OkStatus();
  }

  if (options_.dry_run()) {
    // In dry run mode, we skip procedures that have a SQL body; after enough
    // validation to confirm that we would successfully reach at least the
    // start of the procedure, we just move on to the next statement.
    ZETASQL_RETURN_IF_ERROR(
        AdvancePastCurrentStatement(absl::OkStatus()));
  } else {
    callstack_.emplace_back(StackFrameImpl(
        std::move(parsed_script), start_node, std::move(variables),
        std::move(variable_sizes), {}, std::move(procedure_definition),
        std::move(id_string_pool),
        /*parameters=*/{},
        /*for_loop_stack=*/{}));
  }
  return absl::OkStatus();
}

absl::TimeZone ScriptExecutorImpl::time_zone() const { return time_zone_; }

absl::Status ScriptExecutorImpl::UpdateAnalyzerOptions(
    AnalyzerOptions& analyzer_options) const {
  analyzer_options.set_default_time_zone(time_zone_);
  for (const auto& system_variable : system_variables_) {
    ZETASQL_RETURN_IF_ERROR(analyzer_options.AddSystemVariable(
        system_variable.first, system_variable.second.type()))
        << "Failed to add system variable "
        << absl::StrJoin(system_variable.first, ".") << "("
        << system_variable.second.type()->DebugString() << ")";
  }
  return UpdateAnalyzerOptionParameters(&analyzer_options);
}

absl::Status ScriptExecutorImpl::UpdateAnalyzerOptionParameters(
    AnalyzerOptions* options) const {
  if (GetCurrentParameterValues().has_value()) {
    options->clear_query_parameters();
    options->clear_positional_query_parameters();
    if (absl::holds_alternative<ParameterValueList>(
            *GetCurrentParameterValues())) {
      options->set_parameter_mode(PARAMETER_POSITIONAL);
      for (const auto& value :
           absl::get<ParameterValueList>(*GetCurrentParameterValues())) {
        ZETASQL_RETURN_IF_ERROR(options->AddPositionalQueryParameter(value.type()));
      }
    } else {
      options->set_parameter_mode(PARAMETER_NAMED);
      for (const auto& [name, value] :
           absl::get<ParameterValueMap>(*GetCurrentParameterValues())) {
        ZETASQL_RETURN_IF_ERROR(options->AddQueryParameter(name, value.type()));
      }
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<
    std::optional<absl::variant<ParameterValueList, ParameterValueMap>>>
ScriptExecutorImpl::EvaluateDynamicParams(
    const ASTExecuteUsingClause* using_clause,
    VariableSizesMap* variable_sizes_map) {
  // TODO: Refactor this into parsed_script.cc
  const ASTNode* curr_ast_node = callstack_.back().current_node()->ast_node();
  std::optional<absl::variant<ParameterValueList, ParameterValueMap>>
      stack_params;
  if (using_clause == nullptr) {
    return stack_params;
  }
  for (const ASTExecuteUsingArgument* using_arg : using_clause->arguments()) {
    if (!stack_params.has_value()) {
      if (using_arg->alias() != nullptr) {
        stack_params = ParameterValueMap();
      } else {
        stack_params = ParameterValueList();
      }
    }

    ScriptSegment param_statement_segment =
        SegmentForScalarExpression(using_arg->expression());
    ZETASQL_ASSIGN_OR_RETURN(Value param_value, evaluator_->EvaluateScalarExpression(
                                            *this, param_statement_segment,
                                            /*target_type=*/nullptr));
    if (absl::holds_alternative<ParameterValueMap>(*stack_params)) {
      if (using_arg->alias() == nullptr) {
        return MakeScriptExceptionAt(curr_ast_node)
               << "EXECUTE IMMEDIATE USING parameters must be all positional "
                  "or all named.";
      }
      VariableChange var = {using_arg->alias()->GetAsIdString(), param_value,
                            TypeParameters()};
      ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithSizeCheck(
          /*notify_evaluator=*/true, callstack_.back(),
          using_arg->expression(), {var}, variable_sizes_map));
      std::string param_name =
          absl::AsciiStrToLower(using_arg->alias()->GetAsString());
      // Params must be all unique (map::insert will return true in its second
      // tuple position if the insert was a new element)
      if (!absl::get<ParameterValueMap>(*stack_params)
               .insert({param_name, param_value})
               .second) {
        return MakeScriptExceptionAt(using_arg->alias())
               << "EXECUTE IMMEDIATE USING parameters must have unique "
                  "names, "
               << "found duplicate name: " << using_arg->alias()->GetAsString();
      }
    } else {
      if (using_arg->alias() != nullptr) {
        return MakeScriptExceptionAt(curr_ast_node)
               << "EXECUTE IMMEDIATE USING parameters must be all positional "
                  "or all named.";
      }
      // TODO: Use better name when reporting positional parameter
      // size error
      VariableChange var = {IdString::MakeGlobal("positional parameter"),
                            param_value, TypeParameters()};
      ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithSizeCheck(
          /*notify_evaluator=*/true, callstack_.back(),
          using_arg->expression(), {var}, variable_sizes_map));
      absl::get<ParameterValueList>(*stack_params).push_back(param_value);
    }
  }

  return stack_params;
}

absl::Status ScriptExecutorImpl::ExecuteDynamicIntoStatement(
    const ASTExecuteIntoClause* into_clause) {
  const ASTNode* curr_ast_node = callstack_.back().current_node()->ast_node();
  if (curr_ast_node->node_kind() != AST_QUERY_STATEMENT) {
    return MakeScriptExceptionAt(curr_ast_node)
           << "EXECUTE IMMEDIATE with INTO clause must use a query "
              "statement";
  }
  ScriptSegment select_statement_segment =
      ScriptSegment::FromASTNode(CurrentScript()->script_text(), curr_ast_node);

  auto into_ids = into_clause->identifiers()->identifier_list();
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<EvaluatorTableIterator> iterator,
      evaluator_->ExecuteQueryWithResult(*this, select_statement_segment));
  StackFrameImpl* parent_frame = GetMutableParentStackFrame();
  VariableMap* variables = parent_frame->mutable_variables();
  VariableSizesMap* variable_sizes_map = parent_frame->mutable_variable_sizes();
  VariableTypeParametersMap variable_type_params =
      parent_frame->variable_type_params();
  bool has_row = iterator->NextRow();
  ZETASQL_RETURN_IF_ERROR(iterator->Status());
  if (iterator->NumColumns() != into_ids.size()) {
    return MakeScriptExceptionAt(curr_ast_node)
           << "Variables returned by Dynamic SQL do not match number of "
           << "variables in INTO clause, expected " << into_ids.size()
           << " columns, but got " << iterator->NumColumns();
  }
  for (int i = 0; i < into_ids.size(); ++i) {
    // Before calling this, INTO variables should have been validated,
    // but we'll defensively check again.
    auto it = variables->find(into_ids[i]->GetAsIdString());
    ZETASQL_RET_CHECK(it != variables->end());
    Value* value = &it->second;
    if (has_row) {
      Coercer coercer(type_factory_, time_zone_, &options_.language_options());
      SignatureMatchResult unused;
      if (coercer.AssignableTo(InputArgumentType(iterator->GetValue(i).type()),
                               value->type(),
                               /*is_explicit=*/false, &unused)) {
        ZETASQL_ASSIGN_OR_RETURN(*value,
                         CastValue(iterator->GetValue(i), time_zone_,
                                   options_.language_options(), value->type()));
      } else {
        // Because we're currently in the dynamic SQL stack frame, we cannot
        // reference the AST Node of the original variable ID, so just indicate
        // variable name in the error.
        return MakeScriptExceptionAt(curr_ast_node)
               << "Value returned by Dynamic SQL has type "
               << iterator->GetValue(i).type()->TypeName(
                      ProductMode::PRODUCT_EXTERNAL)
               << " which is not assignable to variable "
               << into_ids[i]->GetAsIdString() << " of type "
               << value->type()->TypeName(ProductMode::PRODUCT_EXTERNAL);
      }
    } else {
      *value = Value::Null(value->type());
    }

    // If type parameters exist for the variable, apply type parameter
    // constraints to the variable.
    TypeParameters type_params;
    auto param_it = variable_type_params.find(into_ids[i]->GetAsIdString());
    if (param_it != variable_type_params.end()) {
      type_params = param_it->second;
      ZETASQL_RETURN_IF_ERROR(CheckConstraintStatus(
          evaluator_->ApplyTypeParameterConstraints(type_params, value),
          curr_ast_node));
    }
    // Use the previous stack frame since the variables for INTO statement need
    // to be defined at least one level up.
    ZETASQL_RET_CHECK(callstack_.size() >= 2);
    VariableChange var = {into_ids[i]->GetAsIdString(), *value, type_params};
    absl::Status variable_size_result = OnVariablesValueChangedWithSizeCheck(
        /*notify_evaluator=*/true, callstack_[callstack_.size() - 2],
        into_ids[i], {var}, variable_sizes_map);
    if (!variable_size_result.ok()) {
      // We don't want to advance past the current statement here, as otherwise
      // the script will report an error past the DynamicSQL statement.
      // However we also don't want to report the error in the DynamicSQL frame
      // itself, as the assignment is happening in the parent frame.
      // Thus we'll pop the callstack to return to the parent frame but not
      // advance any statements before returning the error.
      callstack_.pop_back();
      return variable_size_result;
    }
  }
  ZETASQL_RETURN_IF_ERROR(iterator->Status());
  if (iterator->NextRow()) {
    return MakeScriptExceptionAt(curr_ast_node)
           << "DynamicSQL may only return 0 or 1 rows with INTO clause";
  }
  return AdvancePastCurrentStatement(absl::OkStatus());
}

absl::Status ScriptExecutorImpl::ExecuteDynamicStatement() {
  const ASTExecuteImmediateStatement* execute_immediate_statement =
      callstack_.back()
          .current_node()
          ->ast_node()
          ->GetAsOrDie<ASTExecuteImmediateStatement>();
  if (callstack_.size() >= options_.maximum_stack_depth()) {
    return MakeScriptExceptionAt(execute_immediate_statement)
           << "Out of stack space due to deeply-nested execute immediate call";
  }
  if (execute_immediate_statement->into_clause() != nullptr) {
    auto into_ids = execute_immediate_statement->into_clause()
                        ->identifiers()
                        ->identifier_list();
    for (auto id : into_ids) {
      if (!GetCurrentVariables().contains(id->GetAsIdString())) {
        return MakeUndeclaredVariableError(id);
      }
    }
  }
  ScriptSegment sql_statement_segment =
      SegmentForScalarExpression(execute_immediate_statement->sql());
  ZETASQL_ASSIGN_OR_RETURN(Value sql_value,
                   evaluator_->EvaluateScalarExpression(
                       *this, sql_statement_segment, types::StringType()));
  if (sql_value.is_null()) {
    // During dry runs EvaluateScalarExpression will return null values for
    // complex expressions, thus we won't validate any further in dry run
    if (options_.dry_run()) {
      ZETASQL_RETURN_IF_ERROR(AdvancePastCurrentStatement(
          absl::OkStatus()));
      return absl::OkStatus();
    }
    // NULL dynamic SQL not allowed
    return MakeScriptExceptionAt(execute_immediate_statement->sql())
           << "EXECUTE IMMEDIATE sql string cannot be NULL";
  }

  auto id_string_pool = absl::make_unique<IdStringPool>();
  VariableSizesMap variable_sizes;

  ZETASQL_ASSIGN_OR_RETURN(
      auto stack_params,
      EvaluateDynamicParams(execute_immediate_statement->using_clause(),
                            &variable_sizes));
  if (execute_immediate_statement->into_clause() != nullptr) {
    auto into_ids = execute_immediate_statement->into_clause()
                        ->identifiers()
                        ->identifier_list();
    absl::flat_hash_set<IdString, IdStringCaseHash, IdStringCaseEqualFunc>
        unique_into_ids;
    for (int i = 0; i < into_ids.size(); ++i) {
      if (unique_into_ids.contains(into_ids[i]->GetAsIdString())) {
        return MakeScriptExceptionAt(into_ids[i])
               << "EXECUTE IMMEDIATE with INTO clause cannot assign to the "
               << "same variable multiple times";
      }
      unique_into_ids.insert(into_ids[i]->GetAsIdString());
    }
  }

  FunctionSignature signature(
      /*result_type=*/FunctionArgumentType(
          SignatureArgumentKind::ARG_TYPE_VOID),
      /*arguments=*/{}, /*context_ptr=*/nullptr);
  // This value only exists within this function call, do not pass references to
  // it to other objects. Instead use ProcedureDefinition which creates an owned
  // copy.
  std::string sql_string = sql_value.string_value();
  auto procedure_definition =
      absl::make_unique<ProcedureDefinition>(signature, sql_string);
  absl::StatusOr<std::unique_ptr<ParsedScript>> parsed_script_or_error =
      ParsedScript::Create(
          procedure_definition->body(), GetParserOptions(),
          options_.error_message_mode(),
          /*predefined_variable_names=*/options_.script_variables());
  if (!parsed_script_or_error.ok()) {
    return MakeScriptExceptionAt(execute_immediate_statement->sql())
           << "Invalid EXECUTE IMMEDIATE sql string `" << sql_string << "`, "
           << parsed_script_or_error.status().message() << "\n";
  }
  std::unique_ptr<ParsedScript> parsed_script =
      std::move(parsed_script_or_error.value());

  if (!options_.dry_run()) {
    ZETASQL_RETURN_IF_ERROR(
        parsed_script->CheckQueryParameters(GetQueryParameters(stack_params)));
  }

  const ControlFlowNode* start_node =
      parsed_script->control_flow_graph().start_node();
  if (start_node->ast_node() == nullptr) {
    // empty dynamic SQL not allowed
    return MakeScriptExceptionAt(execute_immediate_statement->sql())
           << "Empty EXECUTE IMMEDIATE sql string cannot be executed";
  }
  if (parsed_script->script()->statement_list().size() != 1) {
    // Only a single SQL statement is allowed
    return MakeScriptExceptionAt(execute_immediate_statement->sql())
           << "EXECUTE IMMEDIATE sql string must contain exactly one statement";
  }

  // Allow the evaluator control over which types of statements are
  // permitted in dynamic SQL.
  const ASTStatement* dynamic_statement =
      parsed_script->script()->statement_list()[0];
  if (!evaluator_->IsAllowedAsDynamicSql(dynamic_statement)) {
    return MakeScriptExceptionAt(execute_immediate_statement->sql())
           << "SQL created by EXECUTE IMMEDIATE contains unsupported statement "
              "type: "
           << dynamic_statement->GetNodeKindString();
  }

  if (options_.dry_run()) {
    ZETASQL_RETURN_IF_ERROR(
        AdvancePastCurrentStatement(absl::OkStatus()));
  } else {
    // Variable_sizes must be included to update the stack frame memory with
    // query parameter variable sizes.
    callstack_.emplace_back(StackFrameImpl(
        std::move(parsed_script), start_node, {}, std::move(variable_sizes), {},
        std::move(procedure_definition), std::move(id_string_pool),
        stack_params, /*for_loop_stack=*/{}));

    if (execute_immediate_statement->into_clause() != nullptr) {
      auto into_ids = execute_immediate_statement->into_clause()
                          ->identifiers()
                          ->identifier_list();
      StackFrameImpl* parent_frame = GetMutableParentStackFrame();
      const VariableMap& variables = parent_frame->variables();
      for (int i = 0; i < into_ids.size(); ++i) {
        auto it = variables.find(into_ids[i]->GetAsIdString());
        ZETASQL_RET_CHECK(it != variables.end());
      }
      absl::Status into_result = ExecuteDynamicIntoStatement(
          execute_immediate_statement->into_clause());
      return into_result;
    }
  }

  return absl::OkStatus();
}

absl::string_view ScriptExecutorImpl::GetScriptText() const {
  return CurrentScript()->script_text();
}

const ControlFlowNode* ScriptExecutorImpl::GetCurrentNode() const {
  return callstack_.back().current_node();
}

absl::Status ScriptExecutorImpl::ValidateVariablesOnSetState(
    const ControlFlowNode * next_cfg_node,
    const VariableMap& new_variables,
    const ParsedScript& parsed_script) const {
  ParsedScript::ArgumentTypeMap arguments = parsed_script.routine_arguments();
  ParsedScript::VariableCreationMap variables_in_scope;
  ZETASQL_ASSIGN_OR_RETURN(variables_in_scope,
                   parsed_script.GetVariablesInScopeAtNode(next_cfg_node));

  for (const auto& pair : new_variables) {
    const IdString& var_name = pair.first;
    const Value& new_variable_value = pair.second;
    // <var_type> is either from resolving a local variable declaration or
    // from argument type.
    const Type* type_at_creation = nullptr;
    if (variables_in_scope.contains(var_name)) {
      const ASTScriptStatement* stmt =
          zetasql_base::EraseKeyReturnValuePtr(&variables_in_scope, var_name);
      if (stmt->node_kind() == AST_VARIABLE_DECLARATION) {
        const ASTVariableDeclaration* decl =
            stmt->GetAs<ASTVariableDeclaration>();
        if (decl != nullptr && decl->type() != nullptr) {
          ZETASQL_ASSIGN_OR_RETURN(
              TypeWithParameters type_with_params,
              evaluator_->ResolveTypeName(
                  *this, ScriptSegment::FromASTNode(parsed_script.script_text(),
                                                    decl->type())));
          type_at_creation = type_with_params.type;
          ZETASQL_RET_CHECK_OK(type_at_creation->ValidateResolvedTypeParameters(
              type_with_params.type_params, LanguageOptions().product_mode()));
        }
      } else if (stmt->node_kind() == AST_FOR_IN_STATEMENT) {
        // Variable created in FOR...IN statement is always a struct type.
        ZETASQL_RET_CHECK(new_variable_value.type()->IsStruct());
        zetasql_base::EraseKeyReturnValuePtr(&variables_in_scope, var_name);
      }
    } else if (arguments.contains(var_name)) {
      type_at_creation = zetasql_base::EraseKeyReturnValuePtr(&arguments, var_name);
    } else if (!predefined_variable_names_.contains(var_name)) {
      ZETASQL_RET_CHECK_FAIL() << "ScriptExecutorImpl::SetState(): Variable "
                       << var_name.ToString() << " not in scope";
    }
    // Skip check when we don't know the type of the variable up front. This
    // applies for variables which automatically infer their types from the
    // default value expression, or for struct-type variables whose fields
    // are determined by a query result, or for ANY TYPE procedure
    // arguments, or the predefined variables declared outside of the script.
    if (type_at_creation != nullptr) {
      ZETASQL_RET_CHECK(type_at_creation->Equivalent(new_variable_value.type()))
          << "ScriptExecutorImpl::SetState(): Variable " << var_name.ToString()
          << " does not match its declared type; expected "
          << type_at_creation->DebugString() << ", found "
          << new_variable_value.type()->DebugString();
    }
  }
  ZETASQL_RET_CHECK(arguments.empty() && variables_in_scope.empty())
      << "ScriptExecutorImpl::SetState(): Variable "
      << (variables_in_scope.empty()
              ? arguments.begin()->first.ToString()
              : variables_in_scope.begin()->first.ToString())
      << " is in scope, but no value is assigned.";
  return absl::OkStatus();
}

ParserOptions ScriptExecutorImpl::GetParserOptions() const {
  ParserOptions parser_options;
  parser_options.set_language_options(&options_.language_options());
  return parser_options;
}

absl::Status ScriptExecutorImpl::SetState(
    const ScriptExecutorStateProto& state) {
  ZETASQL_RETURN_IF_ERROR(Reset());
  if (state.callstack().empty()) {
    // For now, ScriptExecutor may not have a state with an empty callstack.
    // Setting an empty callstack effectively resets it.
    // TODO: treat empty callstack as end of script execution.
    return absl::OkStatus();
  }

  system_variables_[{kTimeZoneSystemVarName}] = Value::String(state.timezone());
  absl::StatusOr<absl::TimeZone> timezone = ParseTimezone(state.timezone());
  if (timezone.ok()) {
    time_zone_ = timezone.value();
  } else {
    // Unable to load timezone from state proto - fall back to default time
    // zone.  This can happen if we are restoring from an older version of
    // ScriptExecutorStateProto, which did not persist timezone values.
    ZETASQL_LOG_IF(WARNING, !state.timezone().empty())
        << "Unable to load timezone '" << state.timezone()
        << "' from state proto; using default timezone instead: "
        << options_.default_time_zone().name();
    time_zone_ = options_.default_time_zone();
  }
  case_stmt_true_branch_index_ = state.case_stmt_true_branch_index();
  case_stmt_current_branch_index_ = state.case_stmt_current_branch_index();

  // Reset sql feature usage to the proto states sql feature usage
  sql_feature_usage_ = state.sql_feature_usage();

  std::vector<const google::protobuf::DescriptorPool*> pools;
  std::vector<StackFrameImpl> new_callstack;
  for (const StackFrameProto& stack_frame_state : state.callstack()) {
    ZETASQL_RET_CHECK_LT(new_callstack.size(), options_.maximum_stack_depth())
        << "Out of stack space while restoring callstack";
    auto id_string_pool = absl::make_unique<IdStringPool>();

    VariableMap new_variables;
    VariableTypeParametersMap new_variable_type_params;
    ZETASQL_RETURN_IF_ERROR(
        DeserializeVariableProto(stack_frame_state.variables(), &new_variables,
                                 &new_variable_type_params, &descriptor_pool_,
                                 id_string_pool.get(), type_factory_));

    pending_exceptions_.clear();
    for (const ScriptException& exception : state.pending_exceptions()) {
      pending_exceptions_.push_back(exception);
    }
    triggered_features_.clear();

    for (int triggered_feature : state.triggered_features()) {
      triggered_features_.insert(
          static_cast<ScriptExecutorStateProto::ScriptFeature>(
              triggered_feature));
    }
    SetSystemVariablesForPendingException();

    std::unique_ptr<ProcedureDefinition> procedure_definition;
    std::unique_ptr<const ParsedScript> parsed_script;
    if (stack_frame_state.has_procedure_definition()) {
      ZETASQL_RET_CHECK_GT(new_callstack.size(), 0)
          << "Main script may not have procedure definition";
      ZETASQL_ASSIGN_OR_RETURN(
          procedure_definition,
          DeserializeProcedureDefinitionProto(
              stack_frame_state.procedure_definition(), pools, type_factory_));
      ZETASQL_RET_CHECK_EQ(procedure_definition->argument_name_list().size(),
                   procedure_definition->signature().arguments().size());
      ParsedScript::ArgumentTypeMap arguments_map;
      for (int i = 0; i < procedure_definition->argument_name_list().size();
           i++) {
        const std::string& argument_name =
            procedure_definition->argument_name_list().at(i);
        arguments_map[id_string_pool->Make(argument_name)] =
            procedure_definition->signature().arguments().at(i).type();
      }
      ZETASQL_ASSIGN_OR_RETURN(parsed_script,
                       ParsedScript::CreateForRoutine(
                           procedure_definition->body(), GetParserOptions(),
                           options_.error_message_mode(), arguments_map));
    } else {
      ZETASQL_RET_CHECK_EQ(new_callstack.size(), 0)
          << "Procedure definition is missing";
      // Move ParsedScript for main script from old callstack to new one.
      ZETASQL_RET_CHECK_GE(callstack_.size(), 1);
      parsed_script = callstack_.front().ReleaseParsedScript();
    }

    std::vector<std::unique_ptr<EvaluatorTableIterator>> for_loop_stack;
    // Temporarily add the current StackFrame to the back of callstack_ so that
    // calls to this->GetCurrentStack() returns the current parsed_script.
    // A non-nullptr cfg_node is used at StackFrame initialization to
    // differentiate from a script that is complete.
    const ControlFlowNode * unused_cfg_node =
        parsed_script->control_flow_graph().start_node();
    callstack_.emplace_back(StackFrameImpl(std::move(parsed_script),
                                           unused_cfg_node));
    for (const google::protobuf::Any& msg
             : stack_frame_state.for_loop_stack()) {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<EvaluatorTableIterator> for_loop_iterator,
          evaluator_->DeserializeToIterator(
              msg, *this,
              *callstack_.back().parsed_script()));
      for_loop_stack.push_back(std::move(for_loop_iterator));
    }
    parsed_script = callstack_.back().ReleaseParsedScript();
    callstack_.pop_back();

    const ASTNode* next_node = nullptr;
    const ControlFlowNode* next_cfg_node = nullptr;
    VariableSizesMap new_variable_sizes;
    // <next_node> and <next_cfg_node> will be nullptr if script is complete.
    if (stack_frame_state.has_current_location_byte_offset()) {
      ParseLocationPoint new_position = ParseLocationPoint::FromByteOffset(
          stack_frame_state.current_location_byte_offset());
      ZETASQL_ASSIGN_OR_RETURN(next_node,
                       parsed_script->FindScriptNodeFromPosition(new_position));
      ZETASQL_RET_CHECK_NE(next_node, nullptr)
          << "SetState(): position " << new_position.GetByteOffset()
          << " has no associated script node.  Script:\n"
          << parsed_script->script_text();

      next_cfg_node = parsed_script->control_flow_graph().GetControlFlowNode(
          next_node, static_cast<ControlFlowNode::Kind>(
              stack_frame_state.control_flow_node_kind()));
      ZETASQL_RET_CHECK_NE(next_cfg_node, nullptr)
          << "Deserialized AST node has no associated control flow node";

      ZETASQL_RETURN_IF_ERROR(
          ValidateVariablesOnSetState(
              next_cfg_node, new_variables, *parsed_script))
          << state.DebugString();
      ZETASQL_RETURN_IF_ERROR(
          ResetVariableSizes(next_node, new_variables, &new_variable_sizes));
      ZETASQL_RETURN_IF_ERROR(
          ResetIteratorSizes(next_node, for_loop_stack));
    }

    absl::optional<absl::variant<ParameterValueList, ParameterValueMap>>
        new_parameters;
    ZETASQL_RETURN_IF_ERROR(DeserializeParametersProto(
        stack_frame_state.parameters(), &new_parameters, &descriptor_pool_,
        id_string_pool.get(), type_factory_));
    new_callstack.emplace_back(StackFrameImpl(
        std::move(parsed_script), next_cfg_node, std::move(new_variables),
        std::move(new_variable_sizes), std::move(new_variable_type_params),
        std::move(procedure_definition), std::move(id_string_pool),
        new_parameters, std::move(for_loop_stack)));
  }
  callstack_ = std::move(new_callstack);
  return absl::OkStatus();
}

absl::StatusOr<ScriptExecutorStateProto> ScriptExecutorImpl::GetState() const {
  ScriptExecutorStateProto state_proto;
  FileDescriptorSetMap file_descriptor_set_map;
  for (const StackFrameImpl& stack_frame : callstack_) {
    StackFrameProto* stack_frame_proto = state_proto.add_callstack();
    if (stack_frame.procedure_definition() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(SerializeProcedureDefinitionProto(
          *stack_frame.procedure_definition(),
          stack_frame_proto->mutable_procedure_definition(),
          &file_descriptor_set_map));
    }
    ZETASQL_RETURN_IF_ERROR(SerializeVariableProto(
        stack_frame.variables(), stack_frame.variable_type_params(),
        stack_frame_proto->mutable_variables()));
    if (stack_frame.parameters()) {
      ZETASQL_RETURN_IF_ERROR(SerializeParametersProto(
          *stack_frame.parameters(), stack_frame_proto->mutable_parameters()));
    }
    if (!IsComplete()) {
      stack_frame_proto->set_current_location_byte_offset(
          stack_frame.current_node()
              ->ast_node()
              ->GetParseLocationRange()
              .start()
              .GetByteOffset());
      stack_frame_proto->set_control_flow_node_kind(
          static_cast<int>(stack_frame.current_node()->kind()));
    }
    for (const std::unique_ptr<EvaluatorTableIterator>& it
             : stack_frame.for_loop_stack()) {
      if (it != nullptr) {
        ZETASQL_RETURN_IF_ERROR(evaluator_->SerializeIterator(
            *it, *stack_frame_proto->add_for_loop_stack()));
      }
    }
  }
  for (const ScriptException& exception : pending_exceptions_) {
    *state_proto.add_pending_exceptions() = exception;
  }
  state_proto.mutable_sql_feature_usage()->MergeFrom(sql_feature_usage_);
  for (ScriptExecutorStateProto::ScriptFeature triggered_feature :
       triggered_features_) {
    state_proto.add_triggered_features(triggered_feature);
  }
  state_proto.set_case_stmt_true_branch_index(case_stmt_true_branch_index_);
  state_proto.set_case_stmt_current_branch_index(
      case_stmt_current_branch_index_);
  state_proto.set_timezone(
      system_variables_.at({kTimeZoneSystemVarName}).string_value());
  return state_proto;
}

absl::Status ScriptExecutorImpl::ResetVariableSizes(
    const ASTNode* node, const VariableMap& new_variables,
    VariableSizesMap* variable_sizes) {
  variable_sizes->clear();
  for (auto itr = new_variables.begin(); itr != new_variables.end(); ++itr) {
    int64_t variable_size = itr->second.physical_byte_size();
    ZETASQL_RETURN_IF_ERROR(UpdateAndCheckVariableSize(node, itr->first, variable_size,
                                               variable_sizes));
  }
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ResetIteratorSizes(
    const ASTNode* node,
    const std::vector<std::unique_ptr<EvaluatorTableIterator>>& iterator_vec) {
  for (const std::unique_ptr<EvaluatorTableIterator>& iterator : iterator_vec) {
    ZETASQL_ASSIGN_OR_RETURN(int64_t iterator_memory,
                     GetIteratorMemoryUsage(iterator.get()));
    ZETASQL_RETURN_IF_ERROR(UpdateAndCheckMemorySize(
        node, iterator_memory, /*previous_memory_size=*/ 0));
  }
  return absl::OkStatus();
}

std::string ScriptExecutorImpl::CallStackDebugString(bool verbose) const {
  absl::StatusOr<std::vector<StackFrameTrace>> status_or_stack_trace =
      StackTrace();
  if (!status_or_stack_trace.ok()) {
    return absl::StrCat("Error fetching stack trace: ",
                        status_or_stack_trace.status().message());
  }
  std::vector<StackFrameTrace> stack_frame_traces =
      std::move(status_or_stack_trace).value();
  return StackTraceString(absl::MakeSpan(stack_frame_traces), verbose);
}

std::string ScriptExecutorImpl::StackTraceString(
    absl::Span<StackFrameTrace> stack_frame_traces, bool verbose) const {
  if (stack_frame_traces.empty()) {
    return "[Script complete]\n";
  }
  std::string stack_trace_debug_string;
  for (const StackFrameTrace& stack_frame_trace : stack_frame_traces) {
    absl::StrAppend(
        &stack_trace_debug_string,
        absl::Substitute("At $0[$1:$2]", stack_frame_trace.procedure_name,
                         stack_frame_trace.start_line,
                         stack_frame_trace.start_column));
    if (verbose) {
      constexpr static int kNumStatementCharsForDisplay = 40;
      std::string statement(stack_frame_trace.current_statement);
      absl::StrReplaceAll({{"\n", " "}}, &statement);
      absl::RemoveExtraAsciiWhitespace(&statement);
      // Output statement to debug string up to <kKeptStatementLengthInChar>
      // characters.
      if (statement.length() > kNumStatementCharsForDisplay) {
        statement = absl::StrCat(
            statement.substr(0, kNumStatementCharsForDisplay), "...");
      }
      absl::StrAppend(&stack_trace_debug_string, " ", statement);
    }
    absl::StrAppend(&stack_trace_debug_string, "\n");
  }
  return stack_trace_debug_string;
}

std::string ScriptExecutorImpl::VariablesDebugString() const {
  std::string debug_string;
  std::string intent = "  ";

  // Display variables in alphabetical order so that tests which rely upon the
  // result of this function are determanistic.
  std::vector<std::pair<IdString, Value>> sorted_variables(
      GetCurrentVariables().begin(), GetCurrentVariables().end());
  std::sort(sorted_variables.begin(), sorted_variables.end(),
            [](const std::pair<IdString, Value>& v1,
               const std::pair<IdString, Value>& v2) {
              return v1.first.CaseLessThan(v2.first);
            });
  for (const std::pair<IdString, Value>& variable : sorted_variables) {
    // Add type parameters to the variable debug string if they exist.
    std::string type_name;
    auto it = GetCurrentVariableTypeParameters().find(variable.first);
    if (it != GetCurrentVariableTypeParameters().end() &&
        !it->second.IsEmpty()) {
      type_name = variable.second.type()
                      ->TypeNameWithParameters(it->second,
                                               LanguageOptions().product_mode())
                      .value();
    } else {
      type_name = variable.second.type()->DebugString();
    }
    absl::StrAppend(&debug_string, intent, type_name, " ",
                    variable.first.ToStringView(), " = ",
                    variable.second.DebugString(), "\n");
  }

  return debug_string;
}

std::string ScriptExecutorImpl::ParameterDebugString() const {
  std::string debug_string;
  std::string indent = "  ";

  // Parameters are already returned in alphabetical order.
  ParsedScript::StringSet named_parameters = GetCurrentNamedParameters();
  for (absl::string_view name : named_parameters) {
    absl::StrAppend(&debug_string, indent, "parameter @", name, "\n");
  }

  std::pair<int64_t, int64_t> positional_parameters =
      GetCurrentPositionalParameters();
  if (positional_parameters.second > 0) {
    absl::StrAppend(&debug_string, indent, positional_parameters.second,
                    " parameters starting at index ",
                    positional_parameters.first, "\n");
  }

  return debug_string;
}

std::string ScriptExecutorImpl::DebugString(bool verbose) const {
  std::string debug_string = CallStackDebugString(verbose);
  absl::StrAppend(&debug_string, VariablesDebugString());
  absl::StrAppend(&debug_string, ParameterDebugString());
  return debug_string;
}

absl::string_view ScriptExecutorImpl::GetCurrentProcedureName() const {
  for (auto itr = callstack_.rbegin(); itr != callstack_.rend(); ++itr) {
    if (itr->procedure_definition() != nullptr && !itr->is_dynamic_sql()) {
      return itr->procedure_definition()->name();
    }
  }
  return "";
}

absl::string_view ScriptExecutorImpl::GetCurrentStackFrameName() const {
  if (callstack_.back().procedure_definition() != nullptr) {
    return callstack_.back().procedure_definition()->name();
  } else {
    return "";
  }
}

absl::StatusOr<std::vector<StackFrameTrace>> ScriptExecutorImpl::StackTrace()
    const {
  std::vector<StackFrameTrace> stack_trace;
  if (IsComplete()) {
    return stack_trace;
  }
  for (auto iter = callstack_.rbegin(); iter != callstack_.rend(); iter++) {
    const StackFrameImpl& stack_frame = *iter;
    ParseLocationTranslator translator(
        stack_frame.parsed_script()->script_text());
    ParseLocationRange cur_stmt_range =
        stack_frame.current_node()->ast_node()->GetParseLocationRange();
    std::pair<int, int> start_line_and_column, end_line_and_column;
    ZETASQL_ASSIGN_OR_RETURN(
        start_line_and_column,
        translator.GetLineAndColumnAfterTabExpansion(cur_stmt_range.start()));
    ZETASQL_ASSIGN_OR_RETURN(
        end_line_and_column,
        translator.GetLineAndColumnAfterTabExpansion(cur_stmt_range.end()));
    std::string procedure_name =
        stack_frame.procedure_definition()
            ? stack_frame.procedure_definition()->name()
            : "";
    absl::string_view statement =
        stack_frame.parsed_script()->script_text().substr(
            cur_stmt_range.start().GetByteOffset(),
            cur_stmt_range.end().GetByteOffset() -
                cur_stmt_range.start().GetByteOffset());
    stack_trace.emplace_back(StackFrameTrace{
        start_line_and_column.first, start_line_and_column.second,
        end_line_and_column.first, end_line_and_column.second,
        std::move(procedure_name), statement});
  }
  return stack_trace;
}

absl::Status ScriptExecutorImpl::Reset() {
  // Leave only main script on call stack
  if (callstack_.size() > 1) {
    callstack_.erase(callstack_.begin() + 1, callstack_.end());
  }
  callstack_.back().SetCurrentNode(
      CurrentScript()->control_flow_graph().start_node());
  MutableCurrentVariables()->clear();
  MutableCurrentVariableSizes()->clear();
  MutableCurrentVariableTypeParameters()->clear();
  pending_exceptions_.clear();
  total_memory_usage_ = 0;
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::DestroyVariables(
    const std::set<std::string>& variables) {
  IdStringPool pool;
  for (const std::string& var_name : variables) {
    IdString var_name_id_string = pool.Make(var_name);
    // The variable might not exist if its DECLARE statement did not execute
    // due to an exception.  Just skip non-existent variables.
    if (MutableCurrentVariables()->erase(var_name_id_string)) {
      total_memory_usage_ -= CurrentVariableSizes().at(var_name_id_string);
      ZETASQL_RET_CHECK_EQ(MutableCurrentVariableSizes()->erase(var_name_id_string), 1);
      ZETASQL_RET_CHECK_GE(total_memory_usage_, 0)
          << "Total size should never be negative";

      auto it =
          MutableCurrentVariableTypeParameters()->find(var_name_id_string);
      if (it != MutableCurrentVariableTypeParameters()->end()) {
        MutableCurrentVariableTypeParameters()->erase(it);
      }
    }
  }
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::AssignOutArguments(
    const ProcedureDefinition& procedure_definition,
    const VariableMap& argument_map, StackFrameImpl* frame_return_to) {
  if (procedure_definition.is_dynamic_sql()) {
    return absl::OkStatus();
  }
  IdStringPool id_string_pool;
  ZETASQL_ASSIGN_OR_RETURN(
      OutputArgumentMap output_argument_map,
      VerifyOutputArgumentsAndBuildMap(procedure_definition,
                                       frame_return_to->current_node()
                                           ->ast_node()
                                           ->GetAsOrDie<ASTCallStatement>(),
                                       &id_string_pool));
  // Assign OUT/INOUT argument to <frame_return_to>.
  for (const auto& pair : output_argument_map) {
    IdString passed_in_variable = pair.first;
    IdString out_argument = pair.second;
    auto to_value_iter =
        frame_return_to->mutable_variables()->find(passed_in_variable);
    ZETASQL_RET_CHECK(to_value_iter != frame_return_to->variables().end())
        << passed_in_variable << " is not found.";
    auto from_value_iter = argument_map.find(out_argument);
    ZETASQL_RET_CHECK(from_value_iter != argument_map.end())
        << out_argument << " is not found.";
    ZETASQL_ASSIGN_OR_RETURN(
        Value to_value,
        CastValueToType(from_value_iter->second, to_value_iter->second.type()));

    // Use the previous stack frame since the variables for OUT arguments need
    // to be defined at least one level up or at the root level.
    int64_t var_declaration_stack =
        callstack_.size() >= 2 ? callstack_.size() - 2 : 0;
    VariableChange var = {passed_in_variable, to_value, TypeParameters()};
    ZETASQL_RETURN_IF_ERROR(OnVariablesValueChangedWithSizeCheck(
        /*notify_evaluator=*/true, callstack_[var_declaration_stack],
        frame_return_to->current_node()->ast_node(), {var},
        frame_return_to->mutable_variable_sizes()));
    to_value_iter->second = to_value;
  }
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ExitFromProcedure(
    const ProcedureDefinition& procedure_exit_from,
    StackFrameImpl* frame_exit_from, StackFrameImpl* frame_return_to,
    bool normal_return) {
  // Remove variable sizes from <frame_exit_from>
  ZETASQL_RET_CHECK_NE(frame_exit_from, nullptr);
  ZETASQL_RET_CHECK_NE(frame_return_to, nullptr);
  for (const auto& pair : frame_exit_from->variable_sizes()) {
    int64_t variable_size = pair.second;
    total_memory_usage_ -= variable_size;
    ZETASQL_DCHECK_GE(total_memory_usage_, 0) << "Total size should never be negative";
  }

  if (normal_return) {
    return AssignOutArguments(procedure_exit_from, frame_exit_from->variables(),
                              frame_return_to);
  }
  return absl::OkStatus();
}

bool ScriptExecutorImpl::CoercesTo(const Type* from_type,
                                   const Type* to_type) const {
  TypeFactory type_factory;
  SignatureMatchResult unused;
  Coercer coercer(&type_factory, time_zone_, &options_.language_options());
  return coercer.CoercesTo(InputArgumentType(from_type), to_type,
                           /*is_explicit=*/false, &unused);
}

absl::StatusOr<Value> ScriptExecutorImpl::CastValueToType(
    const Value& from_value, const Type* to_type) const {
  return CastValue(from_value, time_zone_, options_.language_options(),
                   to_type);
}

ScriptSegment ScriptExecutorImpl::SegmentForScalarExpression(
    const ASTExpression* expr) const {
  if (expr->Is<ASTNamedArgument>()) {
    expr = expr->GetAs<ASTNamedArgument>()->expr();
  }
  return ScriptSegment::FromASTNode(CurrentScript()->script_text(), expr);
}

absl::StatusOr<bool> ScriptExecutorImpl::CheckIfExceptionHandled() const {
  for (auto frame_it = callstack_.rbegin();
       frame_it != callstack_.rend(); ++frame_it) {
    const StackFrame& stack_frame = *frame_it;
    const ControlFlowNode* cfg_node = stack_frame.current_node();
    ZETASQL_RET_CHECK(cfg_node != nullptr);

    auto edge_it =
        cfg_node->successors().find(ControlFlowEdge::Kind::kException);
    if (edge_it != cfg_node->successors().end()) {
      // Exception is handled
      return true;
    }
  }
  // Exception is unhandled.
  return false;
}

absl::Status ScriptExecutorImpl::DispatchException(
    const ScriptException& exception) {
  for (;;) {
    const ControlFlowNode* cfg_node = callstack_.back().current_node();
    auto it = cfg_node->successors().find(ControlFlowEdge::Kind::kException);
    if (it == cfg_node->successors().end()) {
      // Exception handler lies up the callstack. Exit the procedure and keep
      // going.
      ZETASQL_ASSIGN_OR_RETURN(const ASTStatement* call_stmt,
                       ExitProcedure(/*normal_return=*/false));
      ZETASQL_RET_CHECK_NE(call_stmt, nullptr)
          << "Unhandled exception - DispatchException() should not have been "
             "called";
      continue;
    }
    const ControlFlowEdge& edge = *it->second;
    ZETASQL_RETURN_IF_ERROR(ExecuteSideEffects(edge, exception));
    ZETASQL_ASSIGN_OR_RETURN(bool keep_going, UpdateCurrentLocation(edge));
    if (keep_going) {
      // The exception has been handled, what's left is just a normal step-out
      // of CALL statements.
      ZETASQL_RETURN_IF_ERROR(AdvanceInternal(ControlFlowEdge::Kind::kNormal));
    }
    break;
  }
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::ExecuteSideEffects(
    const ControlFlowEdge& edge,
    const absl::optional<ScriptException>& exception) {
  ControlFlowEdge::SideEffects side_effects = edge.ComputeSideEffects();
  if (!side_effects.destroyed_variables.empty()) {
    ZETASQL_RETURN_IF_ERROR(
        DestroyVariables(side_effects.destroyed_variables));
  }
  for (int i = 0; i < side_effects.num_exception_handlers_exited; ++i) {
    ZETASQL_RETURN_IF_ERROR(ExitExceptionHandler());
  }
  for (int i = 0; i < side_effects.num_for_loops_exited; ++i) {
    ZETASQL_RETURN_IF_ERROR(ExitForLoop());
  }
  if (side_effects.exception_handler_entered) {
    ZETASQL_RET_CHECK(exception.has_value());
    ZETASQL_RETURN_IF_ERROR(EnterExceptionHandler(exception.value()));
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> ScriptExecutorImpl::UpdateCurrentLocation(
    const ControlFlowEdge& edge) {
  if (edge.successor()->ast_node() == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(const ASTStatement* call_stmt,
                     ExitProcedure(/*normal_return=*/true));
    if (call_stmt != nullptr) {
      return true;
    }
  } else {
    callstack_.back().SetCurrentNode(edge.successor());
  }
  return false;
}

absl::Status ScriptExecutorImpl::AdvanceInternal(
    ControlFlowEdge::Kind edge_kind) {
  ZETASQL_RET_CHECK(edge_kind != ControlFlowEdge::Kind::kException)
      << "AdvanceInternal() should not be used for exception edges; use "
         "DispatchException() instead.";
  bool keep_going;
  do {
    const ControlFlowNode* cfg_node = callstack_.back().current_node();
    auto it = cfg_node->successors().find(edge_kind);
    if (it == cfg_node->successors().end()) {
      ZETASQL_RET_CHECK_FAIL() << "Statement " << cfg_node->DebugString()
                       << "has no successor of kind "
                       << ControlFlowEdgeKindString(edge_kind)
                       << "\nControl flow graph:\n"
                       << cfg_node->graph()->DebugString();
    }
    const ControlFlowEdge& edge = *it->second;
    if (options_.dry_run()) {
      // Terminate validation if we see a back edge; in dry runs, we don't want
      // to validate the same statement more than once. This check also avoids
      // looping forever when dry-running a LOOP statement.
      if (edge.successor()->ast_node() != nullptr &&
          edge.predecessor()->ast_node() != nullptr &&
          edge.successor()
                  ->ast_node()
                  ->GetParseLocationRange()
                  .start()
                  .GetByteOffset() <= edge.predecessor()
                                          ->ast_node()
                                          ->GetParseLocationRange()
                                          .start()
                                          .GetByteOffset()) {
        return ExitProcedure(true).status();
      }
    }
    ZETASQL_RETURN_IF_ERROR(ExecuteSideEffects(edge, absl::nullopt));
    ZETASQL_ASSIGN_OR_RETURN(keep_going, UpdateCurrentLocation(edge));

    // Even if we had a condition initially, the next iteration of the loop
    // (stepping past the CALL statement) is unconditional.
    edge_kind = ControlFlowEdge::Kind::kNormal;
  } while (keep_going);
  return absl::OkStatus();
}

absl::Status ScriptExecutorImpl::AdvancePastCurrentStatement(
    const absl::Status& status) {
  if (!status.ok()) {
    return MaybeDispatchException(status);
  }
  return AdvanceInternal(ControlFlowEdge::Kind::kNormal);
}

absl::Status ScriptExecutorImpl::AdvancePastCurrentCondition(
    const absl::StatusOr<bool>& condition_value) {
  if (!condition_value.ok()) {
    return MaybeDispatchException(condition_value.status());
  }

  // In dry runs, any conditional branching terminates validation of the script.
  if (options_.dry_run()) {
    ZETASQL_RETURN_IF_ERROR(ExitProcedure(true).status());
    return absl::OkStatus();
  }

  return AdvanceInternal(*condition_value
                             ? ControlFlowEdge::Kind::kTrueCondition
                             : ControlFlowEdge::Kind::kFalseCondition);
}

absl::Status ScriptExecutorImpl::MaybeDispatchException(
    const absl::Status& status) {
  // In dry runs, any exception indicates that the script has failed validation.
  // We do *not* dispatch to the exception handler, nor do we support validating
  // statements inside of an exception handler.
  if (status.ok() || !internal::HasPayloadWithType<ScriptException>(status) ||
      options_.dry_run()) {
    return status;
  }

  ZETASQL_ASSIGN_OR_RETURN(bool handled, CheckIfExceptionHandled());
  if (handled) {
    ZETASQL_ASSIGN_OR_RETURN(ScriptException exception,
                     SetupNewException(status));
    return DispatchException(exception);
  }
  return status;
}

absl::Status ScriptExecutorImpl::RethrowException(
    const ScriptException& exception) {
  ZETASQL_ASSIGN_OR_RETURN(bool handled, CheckIfExceptionHandled());
  if (handled) {
    return DispatchException(exception);
  }
  return MakeScriptException(exception) << exception.message();
}

absl::Status ScriptExecutorImpl::EnterBlock() {
  return AdvanceInternal(ControlFlowEdge::Kind::kNormal);
}

absl::Status ScriptExecutorImpl::SetPredefinedVariables(
    const VariableWithTypeParameterMap& variables) {
  if (variables.empty()) {
    return absl::OkStatus();
  }

  // Make sure the current stack frame is the main script.
  ZETASQL_RET_CHECK_EQ(callstack_.size(), 1)
      << "Can't set variables to a non-main script stack frame.";
  // Make sure the current node is the start node.
  const StackFrameImpl& current_frame = callstack_.front();
  const ControlFlowNode* start_cfg_node =
      current_frame.parsed_script()->control_flow_graph().start_node();
  ZETASQL_RET_CHECK_EQ(start_cfg_node, current_frame.current_node())
      << "Can't set variables if the script is already in progress.";

  VariableMap new_variables;
  VariableTypeParametersMap new_variable_type_params;
  predefined_variable_names_.clear();
  for (const auto& it : variables) {
    predefined_variable_names_.insert(it.first);
    new_variables.insert_or_assign(it.first, it.second.value);
    new_variable_type_params.insert_or_assign(it.first, it.second.type_params);
  }
  VariableSizesMap new_variable_sizes;
  ZETASQL_RETURN_IF_ERROR(ResetVariableSizes(start_cfg_node->ast_node(), new_variables,
                                     &new_variable_sizes));

  *MutableCurrentVariables() = std::move(new_variables);
  *MutableCurrentVariableSizes() = std::move(new_variable_sizes);
  *MutableCurrentVariableTypeParameters() = std::move(new_variable_type_params);

  return absl::OkStatus();
}
}  // namespace zetasql
