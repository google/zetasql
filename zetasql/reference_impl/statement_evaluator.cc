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

#include "zetasql/reference_impl/statement_evaluator.h"

#include <cstdint>
#include <memory>

#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/multi_catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/reference_impl/evaluator_table_iterator.pb.h"
#include "zetasql/reference_impl/type_helpers.h"
#include "zetasql/reference_impl/type_parameter_constraints.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/scripting/error_helpers.h"
#include "zetasql/scripting/script_exception.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_payload.h"

namespace zetasql {

namespace {
// Populates <filter_parameters> and only includes values from <parameters> that
// exist in <range>.
void FilterParameters(
    absl::variant<ParameterValueList, ParameterValueMap> parameters,
    const ParsedScript* parsed_script, const ParseLocationRange& range,
    absl::variant<ParameterValueList, ParameterValueMap>* filtered_parameters) {
  if (absl::holds_alternative<ParameterValueList>(parameters)) {
    const ParameterValueList& current_params =
        absl::get<ParameterValueList>(parameters);
    std::pair<int64_t, int64_t> pos_params =
        parsed_script->GetPositionalParameters(range);
    int64_t start = pos_params.first;
    int64_t end = start + pos_params.second;
    ParameterValueList filtered;
    for (int64_t i = start; i < current_params.size() && i < end; i++) {
      filtered.push_back(current_params[i]);
    }
    *filtered_parameters = filtered;
  } else {
    const ParameterValueMap& current_params =
        absl::get<ParameterValueMap>(parameters);
    ParsedScript::StringSet named_params =
        parsed_script->GetNamedParameters(range);
    ParameterValueMap filtered;
    for (absl::string_view name : named_params) {
      std::string lower_name = absl::AsciiStrToLower(name);
      auto itr = current_params.find(lower_name);
      if (itr != current_params.end()) {
        filtered.insert({itr->first, itr->second});
      }
    }
    *filtered_parameters = filtered;
  }
}

// This class stores the following data which are used
// to serialize/deserialize the iterator along with the rest of
// ScriptExecutor state:
// - ParseLocationPoint of the query node that invoked the creation of this
//   iterator
// - number of times NextRow() was called
class EvaluatorTableIteratorWrapper : public EvaluatorTableIterator {
 public:
  EvaluatorTableIteratorWrapper(
      std::unique_ptr<PreparedQuery> prepared_query,
      std::unique_ptr<EvaluatorTableIterator> iterator,
      const ParseLocationPoint& location)
      : prepared_query_(std::move(prepared_query)),
        iterator_(std::move(iterator)),
        location_(location) {}

  EvaluatorTableIteratorWrapper(
      const EvaluatorTableIteratorWrapper&) = delete;
  EvaluatorTableIteratorWrapper& operator=(
      const EvaluatorTableIteratorWrapper&) = delete;
  ~EvaluatorTableIteratorWrapper() override {}

  int NumColumns() const override { return iterator_->NumColumns(); }
  absl::Status Status() const override { return iterator_->Status(); }
  absl::Status Cancel() override { return iterator_->Cancel(); }
  void SetDeadline(absl::Time deadline) override {
    iterator_->SetDeadline(deadline);
  }
  std::string GetColumnName(int i) const override {
    return iterator_->GetColumnName(i);
  }
  const Type* GetColumnType(int i) const override {
    return iterator_->GetColumnType(i);
  }
  absl::Status SetColumnFilterMap(
      absl::flat_hash_map<int, std::unique_ptr<ColumnFilter>> filter_map)
      override {
    return iterator_->SetColumnFilterMap(std::move(filter_map));
  }
  absl::Status SetReadTime(absl::Time read_time) override {
    return iterator_->SetReadTime(read_time);
  }
  const Value& GetValue(int i) const override {
    return iterator_->GetValue(i);
  }

  bool NextRow() override {
    next_row_count_++;
    return iterator_->NextRow();
  }

  const ParseLocationPoint& get_location() const { return location_; }
  int get_next_row_count() const { return next_row_count_; }

 private:
  // Set from ExecuteQueryWithResult(), must be kept alive for iterator_
  // to remain valid.
  std::unique_ptr<PreparedQuery> prepared_query_;
  std::unique_ptr<EvaluatorTableIterator> iterator_;
  const ParseLocationPoint location_;
  int next_row_count_ = 0;
};
}  // namespace

const ResolvedStatement*
StatementEvaluatorImpl::StatementEvaluation::resolved_statement() const {
  if (analyzer_output_ == nullptr) {
    return nullptr;
  }
  return analyzer_output_->resolved_statement();
}

const ResolvedExpr*
StatementEvaluatorImpl::ExpressionEvaluation::resolved_expr() const {
  if (analyzer_output_ == nullptr) {
    return nullptr;
  }
  return analyzer_output_->resolved_expr();
}

absl::Status StatementEvaluatorImpl::Evaluation::Evaluate(
    const ScriptExecutor& script_executor, StatementEvaluatorImpl* evaluator,
    const ScriptSegment& segment) {
  ZETASQL_RETURN_IF_ERROR(EvaluateInternal(script_executor, evaluator, segment))
      .With(ConvertLocalErrorToScriptError(segment));
  return absl::OkStatus();
}

absl::Status StatementEvaluatorImpl::Evaluation::EvaluateInternal(
    const ScriptExecutor& script_executor, StatementEvaluatorImpl* evaluator,
    const ScriptSegment& segment) {
  ZETASQL_CHECK(evaluator != nullptr);
  ZETASQL_CHECK(evaluator_ == nullptr) << "StatementEvaluatorImpl::Evaluation::"
                                  "Evaluate() called multiple times.";
  evaluator_ = evaluator;
  AnalyzerOptions analyzer_options = evaluator->initial_analyzer_options_;
  ZETASQL_RETURN_IF_ERROR(script_executor.UpdateAnalyzerOptions(analyzer_options));

  const SystemVariableValuesMap& system_variables =
      script_executor.GetKnownSystemVariables();
  analyzer_options.CreateDefaultArenasIfNotSet();

  // Create a catalog which supports script variables, alongside whatever
  // user-defined symbols have been provided when the StatementEvaluatorImpl
  // object was created.
  SimpleCatalog variables_catalog("script_variables",
                                  evaluator->type_factory());
  std::vector<std::unique_ptr<SimpleConstant>> constants;
  const VariableMap& variables = script_executor.GetCurrentVariables();
  for (const std::pair<const IdString, Value>& variable : variables) {
    std::unique_ptr<SimpleConstant> constant;
    ZETASQL_RETURN_IF_ERROR(SimpleConstant::Create({variable.first.ToString()},
                                           variable.second, &constant));
    variables_catalog.AddConstant(constant.get());
    constants.push_back(std::move(constant));
  }

  std::unique_ptr<MultiCatalog> combined_catalog;
  ZETASQL_RETURN_IF_ERROR(MultiCatalog::Create(
      "combined_catalog", {&variables_catalog, evaluator->catalog_},
      &combined_catalog));

  // Force usage of ERROR_MESSAGE_WITH_PAYLOAD so that the script executor can
  // fill in the context of the error, relative to the entire script.
  analyzer_options.set_error_message_mode(ERROR_MESSAGE_WITH_PAYLOAD);

  EvaluatorOptions evaluator_options = evaluator_->options();
  evaluator_options.default_time_zone = analyzer_options.default_time_zone();

  absl::variant<ParameterValueList, ParameterValueMap> filtered_parameters;
  FilterParameters(script_executor.GetCurrentParameterValues().value_or(
                       evaluator->parameters()),
                   script_executor.GetCurrentStackFrame()->parsed_script(),
                   segment.range(), &filtered_parameters);
  return EvaluateImpl(segment.GetSegmentText(), analyzer_options,
                      combined_catalog.get(), evaluator_options,
                      system_variables, filtered_parameters);
}

absl::Status StatementEvaluatorImpl::StatementEvaluation::Analyze(
    absl::string_view sql, const AnalyzerOptions& analyzer_options,
    Catalog* catalog) {
  ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(sql, analyzer_options, catalog,
                                   type_factory(), &analyzer_output_));
  if (analyzer_output_->analyzer_output_properties().IsRelevant(
          REWRITE_ANONYMIZATION)) {
    ZETASQL_ASSIGN_OR_RETURN(analyzer_output_,
                     RewriteForAnonymization(analyzer_output_, analyzer_options,
                                             catalog, type_factory()));
  }
  return absl::OkStatus();
}

namespace {
// Returns the type of a Value that would contain the table whose values are
// provided in <iterator>.
absl::StatusOr<const ArrayType*> GetTableType(
    TypeFactory* type_factory, const EvaluatorTableIterator* iterator,
    bool is_value_table) {
  if (is_value_table) {
    if (iterator->NumColumns() != 1) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "is_value_table specified, but table has "
             << iterator->NumColumns() << " instead of 1";
    }
    const ArrayType* result;
    ZETASQL_RETURN_IF_ERROR(
        type_factory->MakeArrayType(iterator->GetColumnType(0), &result));
    return result;
  } else {
    std::vector<StructField> fields;
    fields.reserve(iterator->NumColumns());
    for (int i = 0; i < iterator->NumColumns(); ++i) {
      fields.emplace_back(iterator->GetColumnName(i),
                          iterator->GetColumnType(i));
    }
    const StructType* elem_type;
    ZETASQL_RETURN_IF_ERROR(type_factory->MakeStructType(fields, &elem_type));

    const ArrayType* result;
    ZETASQL_RETURN_IF_ERROR(type_factory->MakeArrayType(elem_type, &result));
    return result;
  }
}

bool IsDml(const ResolvedStatement* statement) {
  return statement->node_kind() == RESOLVED_INSERT_STMT ||
         statement->node_kind() == RESOLVED_UPDATE_STMT ||
         statement->node_kind() == RESOLVED_DELETE_STMT ||
         statement->node_kind() == RESOLVED_MERGE_STMT;
}

// Returns a Value which holds the tabledata described in the given iterator:
// - The returned value is an array, with one element per row in the table.
// - If <is_value_table> is true, the table must contain exactly one column.
//      Each element of the array corresponds to a value in the table.
// - Otherwise, each element of the array is a struct, with one field per
//      column.  Each field is the value of the corresponding row/column.
absl::StatusOr<Value> IteratorToValue(TypeFactory* type_factory,
                                      EvaluatorTableIterator* iterator,
                                      bool is_value_table) {
  ZETASQL_ASSIGN_OR_RETURN(const ArrayType* type,
                   GetTableType(type_factory, iterator, is_value_table));
  std::vector<Value> values;
  while (iterator->NextRow()) {
    if (is_value_table) {
      ZETASQL_CHECK_EQ(iterator->NumColumns(), 1);
      values.push_back(iterator->GetValue(0));
      ZETASQL_RET_CHECK(iterator->GetValue(0).is_valid());
    } else {
      std::vector<Value> fields;
      fields.reserve(iterator->NumColumns());
      for (int i = 0; i < iterator->NumColumns(); ++i) {
        ZETASQL_RET_CHECK(iterator->GetValue(i).is_valid());
        fields.push_back(iterator->GetValue(i));
      }
      values.push_back(Value::UnsafeStruct(
          type->AsArray()->element_type()->AsStruct(), std::move(fields)));
    }
  }
  ZETASQL_RETURN_IF_ERROR(iterator->Status());
  return Value::UnsafeArray(type, std::move(values));
}

absl::StatusOr<Value> GetTableContents(const Table* table,
                                       TypeFactory* type_factory) {
  std::vector<int> column_idxs;
  column_idxs.reserve(table->NumColumns());
  for (int i = 0; i < table->NumColumns(); ++i) {
    column_idxs.push_back(i);
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<EvaluatorTableIterator> iter,
                   table->CreateEvaluatorTableIterator(column_idxs));
  return IteratorToValue(type_factory, iter.get(), table->IsValueTable());
}

}  // namespace

absl::StatusOr<int>
StatementEvaluatorImpl::StatementEvaluation::DoDmlSideEffects(
    EvaluatorTableModifyIterator* iterator) {
  // Obtain original table, as a vector of rows
  const SimpleTable* table = iterator->table()->GetAs<SimpleTable>();
  TypeFactory type_factory;
  ZETASQL_ASSIGN_OR_RETURN(Value original_table_value,
                   GetTableContents(table, &type_factory));
  std::vector<std::vector<Value>> rows;
  for (const Value& row : original_table_value.elements()) {
    if (table->IsValueTable()) {
      rows.push_back({row});
    } else {
      rows.push_back(row.fields());
    }
  }
  // Build up indexes to associate each row with its primary key
  if (!table->PrimaryKey().has_value()) {
    return absl::InvalidArgumentError(
        "GetModifiedTable: table must have primary key");
  }
  std::vector<int> primary_key_column_idx = table->PrimaryKey().value();
  absl::flat_hash_map<std::vector<Value>, int> row_idxs_by_key;
  for (int i = 0; i < rows.size(); ++i) {
    std::vector<Value> key;
    key.reserve(primary_key_column_idx.size());
    for (int idx : primary_key_column_idx) {
      key.push_back(rows[i][idx]);
    }
    row_idxs_by_key[key] = i;
  }
  absl::flat_hash_set<int> deleted_rows;

  bool is_insert = iterator->GetOperation() ==
                   EvaluatorTableModifyIterator::Operation::kInsert;
  bool is_update = iterator->GetOperation() ==
                   EvaluatorTableModifyIterator::Operation::kUpdate;
  bool is_delete = iterator->GetOperation() ==
                   EvaluatorTableModifyIterator::Operation::kDelete;
  int num_rows_modified = 0;
  while (iterator->NextRow()) {
    ++num_rows_modified;
    std::vector<Value> key;
    for (int i = 0; i < table->PrimaryKey().value().size(); ++i) {
      key.push_back(iterator->GetOriginalKeyValue(i));
    }
    auto row_it = row_idxs_by_key.find(key);
    bool row_found = row_it != row_idxs_by_key.end();

    if ((is_update || is_delete) && !row_found) {
      return absl::InternalError(
          "Unable to find primary key for row in UPDATE/DELETE operation");
    }
    if (is_insert && row_found) {
      return absl::InternalError("Key already exists for INSERT operation");
    }

    if (is_delete) {
      deleted_rows.insert(row_it->second);
      continue;
    }

    std::vector<Value> columns;
    for (int i = 0; i < table->NumColumns(); ++i) {
      ZETASQL_RET_CHECK(iterator->GetColumnValue(i).is_valid());
      columns.push_back(iterator->GetColumnValue(i));
    }
    if (is_update) {
      rows[row_it->second] = std::move(columns);
    } else {
      rows.push_back(std::move(columns));
    }
  }
  ZETASQL_RETURN_IF_ERROR(iterator->Status());

  if (is_delete) {
    // Final pass to remove rows that have been deleted
    std::vector<std::vector<Value>> remaining_rows;
    remaining_rows.reserve(rows.size() - deleted_rows.size());
    for (int i = 0; i < rows.size(); ++i) {
      if (!deleted_rows.contains(i)) {
        remaining_rows.push_back(std::move(rows[i]));
      }
    }
    ZETASQL_RETURN_IF_ERROR(SetTableContents(table, remaining_rows));
  } else {
    ZETASQL_RETURN_IF_ERROR(SetTableContents(table, rows));
  }
  return num_rows_modified;
}

absl::Status StatementEvaluatorImpl::ExpressionEvaluation::EvaluateImpl(
    absl::string_view sql, const AnalyzerOptions& analyzer_options,
    Catalog* catalog, const EvaluatorOptions& evaluator_options,
    const SystemVariableValuesMap& system_variables,
    absl::variant<ParameterValueList, ParameterValueMap> parameters) {
  ZETASQL_RETURN_IF_ERROR(AnalyzeExpressionForAssignmentToType(
      sql, analyzer_options, catalog, type_factory(), target_type_,
      &analyzer_output_));
  PreparedExpression prepared_expr(resolved_expr(), evaluator_options);
  ZETASQL_RETURN_IF_ERROR(prepared_expr.Prepare(analyzer_options, nullptr));
  if (absl::holds_alternative<ParameterValueList>(parameters)) {
    ZETASQL_ASSIGN_OR_RETURN(
        result_, prepared_expr.ExecuteWithPositionalParams(
                     /*columns=*/{}, absl::get<ParameterValueList>(parameters),
                     system_variables));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        result_, prepared_expr.Execute(
                     /*columns=*/{}, absl::get<ParameterValueMap>(parameters),
                     system_variables));
  }
  return absl::OkStatus();
}

absl::Status StatementEvaluatorImpl::StatementEvaluation::EvaluateImpl(
    absl::string_view sql, const AnalyzerOptions& analyzer_options,
    Catalog* catalog, const EvaluatorOptions& evaluator_options,
    const SystemVariableValuesMap& system_variables,
    absl::variant<ParameterValueList, ParameterValueMap> parameters) {
  ZETASQL_RETURN_IF_ERROR(Analyze(sql, analyzer_options, catalog));

  // Use PreparedQuery to evaluate query statements to minimize dependencies on
  // the evaluator's internals.
  if (resolved_statement()->node_kind() == RESOLVED_QUERY_STMT) {
    const ResolvedQueryStmt* query_stmt =
        resolved_statement()->GetAs<ResolvedQueryStmt>();
    prepared_query_ =
        std::make_unique<PreparedQuery>(query_stmt, evaluator_options);
    ZETASQL_RETURN_IF_ERROR(prepared_query_->Prepare(analyzer_options, nullptr));
    std::unique_ptr<EvaluatorTableIterator> result_iterator;
    if (absl::holds_alternative<ParameterValueMap>(parameters)) {
      const auto& named_params = absl::get<ParameterValueMap>(parameters);
      ZETASQL_ASSIGN_OR_RETURN(result_iterator, prepared_query_->Execute(
                                            named_params, system_variables));
      ZETASQL_ASSIGN_OR_RETURN(table_iterator_, prepared_query_->Execute(
                                            named_params, system_variables));
    } else {
      const auto& positional_params = absl::get<ParameterValueList>(parameters);
      ZETASQL_ASSIGN_OR_RETURN(result_iterator,
                       prepared_query_->ExecuteWithPositionalParams(
                           positional_params, system_variables));
      ZETASQL_ASSIGN_OR_RETURN(table_iterator_,
                       prepared_query_->ExecuteWithPositionalParams(
                           positional_params, system_variables));
    }
    ZETASQL_ASSIGN_OR_RETURN(result_,
                     IteratorToValue(type_factory(), result_iterator.get(),
                                     query_stmt->is_value_table()));
    return absl::OkStatus();
  } else if (IsDml(resolved_statement())) {
    PreparedModify prepared_modify(resolved_statement(), evaluator_options);
    ZETASQL_RETURN_IF_ERROR(prepared_modify.Prepare(analyzer_options, nullptr));
    std::unique_ptr<EvaluatorTableModifyIterator> result_iterator;
    if (absl::holds_alternative<ParameterValueMap>(parameters)) {
      const auto& named_params = absl::get<ParameterValueMap>(parameters);
      ZETASQL_ASSIGN_OR_RETURN(result_iterator,
                       prepared_modify.Execute(named_params, system_variables));
    } else {
      const auto& positional_params = absl::get<ParameterValueList>(parameters);
      ZETASQL_ASSIGN_OR_RETURN(result_iterator,
                       prepared_modify.ExecuteWithPositionalParams(
                           positional_params, system_variables));
    }
    ZETASQL_ASSIGN_OR_RETURN(int num_rows_modified,
                     DoDmlSideEffects(result_iterator.get()));

    // The "result" of a DML operation is a STRUCT with two fields -
    // the number of rows modified, followed by an ArrayValue
    // representing the entire content of the new table.
    ZETASQL_ASSIGN_OR_RETURN(Value new_table, GetTableContents(result_iterator->table(),
                                                       type_factory()));
    const StructType* struct_type;
    ZETASQL_RETURN_IF_ERROR(type_factory()->MakeStructType(
        {{kDMLOutputNumRowsModifiedColumnName, type_factory()->get_int64()},
         {kDMLOutputAllRowsColumnName, new_table.type()}},
        &struct_type));
    result_ = Value::Struct(
        struct_type, {Value::Int64(num_rows_modified), std::move(new_table)});
    return absl::OkStatus();
  }

  // Assignments to script variables and @@timezone are handled internally
  // within ScriptExecutor.  If we see an assignment statement here, that means
  // we're assigning to something else, which is not supported.
  if (resolved_statement()->node_kind() == RESOLVED_ASSIGNMENT_STMT) {
    const ResolvedAssignmentStmt* assign_stmt =
        resolved_statement()->GetAs<ResolvedAssignmentStmt>();
    return MakeEvalError() << "Unsupported system variable for assignment: "
                           << "@@"
                           << absl::StrJoin(
                                  assign_stmt->target()
                                      ->GetAs<ResolvedSystemVariable>()
                                      ->name_path(),
                                  ".");
  }
  return zetasql_base::UnimplementedErrorBuilder()
         << "Statement type not supported by StatementEvaluator: "
         << resolved_statement()->node_kind();
}

namespace {
bool IsHandleableError(const absl::Status& status) {
  // Consider all errors as handleable, except internal errors.
  return !status.ok() && status.code() != absl::StatusCode::kInternal;
}

absl::Status WrapIfHandleable(const absl::Status& status) {
  if (IsHandleableError(status)) {
    absl::Status new_status(status);
    internal::AttachPayload(&new_status, ScriptException());
    return new_status;
  }
  return status;
}

template <class T>
absl::StatusOr<Value> MakeStatusOrValue(const absl::Status& status,
                                        const T& evaluation) {
  if (status.ok()) {
    return evaluation.result();
  }
  return status;
}

}  // namespace

absl::Status StatementEvaluatorImpl::ExecuteStatement(
    const ScriptExecutor& executor, const ScriptSegment& segment) {
  StatementEvaluation evaluation;
  absl::Status status =
      WrapIfHandleable(evaluation.Evaluate(executor, this, segment));
  if (callback_ != nullptr) {
    const absl::StatusOr<Value> status_or_result =
        MakeStatusOrValue(status, evaluation);
    callback_->OnStatementResult(segment, evaluation.resolved_statement(),
                                 status_or_result);
  }
  return status;
}

absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
StatementEvaluatorImpl::ExecuteQueryWithResult(
    const ScriptExecutor& executor, const ScriptSegment& segment) {
  if (segment.node()->node_kind() != AST_QUERY_STATEMENT
      && segment.node()->node_kind() != AST_QUERY) {
    return MakeEvalError()
           << "ExecuteQueryWithResult used on non query node";
  }
  StatementEvaluation evaluation;
  absl::Status status =
      WrapIfHandleable(evaluation.Evaluate(executor, this, segment));
  const absl::StatusOr<Value> status_or_result =
      MakeStatusOrValue(status, evaluation);
  if (callback_ != nullptr) {
    callback_->OnStatementResult(segment, evaluation.resolved_statement(),
                                 status_or_result);
  }
  if (!status.ok()) {
    return status;
  }

  return std::make_unique<EvaluatorTableIteratorWrapper>(
      evaluation.get_prepared_query(),
      evaluation.get_table_iterator(),
      segment.node()->GetParseLocationRange().start());
}

absl::Status StatementEvaluatorImpl::SerializeIterator(
    const EvaluatorTableIterator& iterator,
    google::protobuf::Any& out) {
  ZETASQL_RET_CHECK(iterator.Status().ok());
  auto wrapper =
       dynamic_cast<const EvaluatorTableIteratorWrapper *>(&iterator);
  ZETASQL_RET_CHECK(wrapper != nullptr);
  EvaluatorTableIteratorProto it_msg;
  it_msg.set_location_byte_offset(wrapper->get_location().GetByteOffset());
  it_msg.set_next_row_count(wrapper->get_next_row_count());

  out.PackFrom(it_msg);
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
StatementEvaluatorImpl::DeserializeToIterator(
    const google::protobuf::Any& msg,
    const ScriptExecutor& executor,
    const ParsedScript& parsed_script) {
  EvaluatorTableIteratorProto it_msg;
  msg.UnpackTo(&it_msg);

  ParseLocationPoint query_position = ParseLocationPoint::FromByteOffset(
      it_msg.location_byte_offset());
  ZETASQL_ASSIGN_OR_RETURN(
      const ASTNode* query_node,
      parsed_script.FindScriptNodeFromPosition(query_position));
  ZETASQL_RET_CHECK_NE(query_node, nullptr)
      << "StatementEvaluatorImpl::DeserializeToIterator: position "
      << query_position.GetByteOffset()
      << " has no associated script node.  Script:\n"
      << parsed_script.script_text();

  ScriptSegment segment =
      ScriptSegment::FromASTNode(parsed_script.script_text(),
                                 query_node);
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<EvaluatorTableIterator> it,
                   ExecuteQueryWithResult(executor, segment));

  auto wrapper = std::unique_ptr<EvaluatorTableIteratorWrapper>(
      dynamic_cast<EvaluatorTableIteratorWrapper*>(it.release()));

  for (int i = 0; i < it_msg.next_row_count(); i++) {
    if (!wrapper->NextRow()) {
      ZETASQL_RET_CHECK(wrapper->Status().ok());
    }
  }
  return wrapper;
}

absl::StatusOr<int64_t> StatementEvaluatorImpl::GetIteratorMemoryUsage(
      const EvaluatorTableIterator& iterator) {
  return callback_->get_bytes_per_iterator();
}

absl::StatusOr<int>
StatementEvaluatorImpl::EvaluateCaseExpression(
    const ScriptSegment& case_value,
    const std::vector<ScriptSegment>& when_values,
    const ScriptExecutor& executor) {
  // Generate CASE...WHEN...END expression.
  std::string query_str = absl::StrCat("CASE ", case_value.GetSegmentText());
  // Store byte offset of each expression, so that ErrorLocation can be
  // translated to the corresponding location in the original script text.
  std::vector<size_t> offsets;
  offsets.push_back(query_str.size());
  for (int i = 0; i < when_values.size(); i++) {
    absl::StrAppend(&query_str,
                    " WHEN ", when_values[i].GetSegmentText(), " THEN ", i);
    offsets.push_back(query_str.size());
  }
  absl::StrAppend(&query_str, " ELSE -1 END");
  offsets.push_back(query_str.size());

  ParserOptions parser_options(initial_analyzer_options_.id_string_pool(),
                               initial_analyzer_options_.arena(),
                               &initial_analyzer_options_.language());
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RET_CHECK_OK(ParseExpression(query_str, parser_options, &parser_output));
  ScriptSegment query_segment = ScriptSegment::FromASTNode(
      query_str,
      parser_output->expression());
  auto status_or_index =
      EvaluateScalarExpression(executor, query_segment, nullptr);

  if (!status_or_index.status().ok()) {
    auto status = status_or_index.status();
    if (absl::StrContains(status.message(), "argument types")) {
      status = zetasql_base::InvalidArgumentErrorBuilder()
          << "CASE...WHEN statement values have incompatible types";
      internal::AttachPayload(&status, ScriptException());
    }
    std::pair<int, int> line_and_column;
    ErrorLocation location;
    ParseLocationTranslator translator(executor.GetScriptText());
    ZETASQL_ASSIGN_OR_RETURN(line_and_column,
                     translator.GetLineAndColumnAfterTabExpansion(
                         executor.GetCurrentNode()->ast_node()
                         ->GetParseLocationRange().start()));
    if (internal::HasPayloadWithType<ErrorLocation>(status)) {
      location = internal::GetPayload<ErrorLocation>(status);
      auto it = std::upper_bound(offsets.begin(), offsets.end(),
                                 location.column() - 1);
      int64_t vec_index = it - offsets.begin();
      if (vec_index == 0) {
        ZETASQL_ASSIGN_OR_RETURN(line_and_column,
                         translator.GetLineAndColumnAfterTabExpansion(
                             case_value.range().start()));
      } else if (vec_index <= when_values.size()) {
        ZETASQL_ASSIGN_OR_RETURN(line_and_column,
                         translator.GetLineAndColumnAfterTabExpansion(
                             when_values[vec_index - 1].range().start()));
      }
    }
    location.set_line(line_and_column.first);
    location.set_column(line_and_column.second);
    internal::AttachPayload(&status, location);
    return status;
  }

  ZETASQL_RET_CHECK(status_or_index.value().type()->IsInt64());
  return status_or_index.value().int64_value();
}

absl::StatusOr<Value> StatementEvaluatorImpl::EvaluateScalarExpression(
    const ScriptExecutor& executor, const ScriptSegment& segment,
    const Type* target_type) {
  ExpressionEvaluation evaluation(target_type);
  absl::Status status =
      WrapIfHandleable(evaluation.Evaluate(executor, this, segment));
  const absl::StatusOr<Value> status_or_result =
      MakeStatusOrValue(status, evaluation);
  if (callback_ != nullptr) {
    callback_->OnScalarExpressionResult(segment, evaluation.resolved_expr(),
                                        status_or_result);
  }
  return status_or_result;
}

bool StatementEvaluatorImpl::IsSupportedVariableType(
    const TypeWithParameters& type_with_params) {
  if (callback_ != nullptr) {
    return callback_->IsSupportedVariableType(type_with_params.type);
  }
  return true;
}

absl::Status StatementEvaluatorImpl::ApplyTypeParameterConstraints(
    const TypeParameters& type_params, Value* value) {
  // TODO: Refactor code to store the LanguageOptions in
  // the StatementEvaluator instead of hard-coding PRODUCT_INTERNAL.
  return ApplyConstraints(type_params, PRODUCT_INTERNAL, *value);
}

absl::StatusOr<std::unique_ptr<ProcedureDefinition>>
StatementEvaluatorImpl::LoadProcedure(const ScriptExecutor& executor,
                                      const absl::Span<const std::string>& path,
                                      const int64_t num_arguments) {
  if (callback_ != nullptr) {
    return callback_->LoadProcedure(path);
  }
  return absl::NotFoundError("");
}

namespace {
absl::StatusOr<const Type*> MakeStatusOrType(const ScriptSegment& segment,
                                             const absl::Status& status,
                                             const Type* type) {
  if (status.ok()) {
    return type;
  }
  return ConvertLocalErrorToScriptError(segment)(zetasql_base::StatusBuilder(status));
}
}  // namespace

absl::StatusOr<TypeWithParameters> StatementEvaluatorImpl::ResolveTypeName(
    const ScriptExecutor& executor, const ScriptSegment& segment) {
  AnalyzerOptions analyzer_options = initial_analyzer_options_;
  ZETASQL_RETURN_IF_ERROR(executor.UpdateAnalyzerOptions(analyzer_options));
  analyzer_options.CreateDefaultArenasIfNotSet();

  const Type* type;
  TypeParameters type_params;
  absl::Status status = WrapIfHandleable(
      AnalyzeType(std::string(segment.GetSegmentText()), analyzer_options,
                  catalog_, type_factory_, &type, &type_params));
  TypeWithParameters type_with_params = {type, type_params};
  absl::StatusOr<const Type*> status_or_result =
      MakeStatusOrType(segment, status, type_with_params.type);
  if (callback_ != nullptr) {
    callback_->OnTypeResult(segment, status_or_result);
  }

  ZETASQL_RETURN_IF_ERROR(status_or_result.status());
  return type_with_params;
}

}  // namespace zetasql
