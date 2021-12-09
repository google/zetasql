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

#include "zetasql/public/evaluator_base.h"

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/algebrizer.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/parameters.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/validator.h"
#include "zetasql/base/case.h"
#include "absl/container/node_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

using ExpressionOptions =
    ::zetasql::PreparedExpressionBase::ExpressionOptions;
using QueryOptions = ::zetasql::PreparedQueryBase::QueryOptions;

// Represents either a map of named parameters or a list of positional
// parameters.
class ParameterValues {
 public:
  explicit ParameterValues(const ParameterValueMap* parameter_map)
      : parameters_(parameter_map) {}

  explicit ParameterValues(const ParameterValueList* parameter_list)
      : parameters_(parameter_list) {}

  ParameterValues(const ParameterValues&) = delete;
  ParameterValues& operator=(const ParameterValues&) = delete;

  // Returns whether this represents a collection of named parameters.
  bool is_named() const {
    return absl::holds_alternative<const ParameterValueMap*>(parameters_);
  }

  // Precondition: is_named().
  const ParameterValueMap& named_parameters() const {
    return **absl::get_if<const ParameterValueMap*>(&parameters_);
  }

  // Precondition: !is_named().
  const ParameterValueList& positional_parameters() const {
    return **absl::get_if<const ParameterValueList*>(&parameters_);
  }

 private:
  absl::variant<const ParameterValueMap*, const ParameterValueList*>
      parameters_;
};

// Implements EvaluatorTableModifyIterator by wrapping a vector of updated rows.
//
// Requires the same operation for all rows.
//
// Currently, the original primary key values are extracted from the updated row
// contents, which assumes the primary key columns are not updated. This is
// justified by the fact that we always enable
// FEATURE_DISALLOW_PRIMARY_KEY_UPDATES in CreateEvaluationContext() below.
//
// `deletion_cb` will be called on deletion of an EvaluatorTableModifyIterator.
// This is currently used by Evaluator to detect outliving iterators.
class VectorEvaluatorTableModifyIterator : public EvaluatorTableModifyIterator {
 public:
  VectorEvaluatorTableModifyIterator(const std::vector<Value>& rows,
                                     const Table* table, Operation operation,
                                     const std::function<void()>& deletion_cb)
      : rows_(rows),
        table_(table),
        operation_(operation),
        deletion_cb_(deletion_cb) {}

  ~VectorEvaluatorTableModifyIterator() override { deletion_cb_(); }

  bool NextRow() override { return ++row_idx_ < rows_.size(); }

  const Value& GetColumnValue(int i) const override {
    return operation_ == EvaluatorTableModifyIterator::Operation::kDelete
               ? invalid_value_
               : rows_[row_idx_].field(i);
  }

  absl::Status Status() const override { return absl::OkStatus(); }

  const Value& GetOriginalKeyValue(int i) const override {
    return operation_ == EvaluatorTableModifyIterator::Operation::kInsert
               ? invalid_value_
               : rows_[row_idx_].field(table_->PrimaryKey()->at(i));
  }

  Operation GetOperation() const override { return operation_; }

  const Table* table() const override { return table_; }

 private:
  // The content of the iterator. Each item represents a row update operation.
  const std::vector<Value> rows_;
  // The table to be updated.
  const Table* table_;
  // The type of operation for all rows.
  const Operation operation_;
  // The positional index of the current row in the row vector.
  int row_idx_ = -1;
  // The value to be returned by GetValue() for delete operations.
  const Value invalid_value_ = Value::Invalid();
  // A callback function called by the destructor, currently used by Evalutor to
  // detect outliving iterators.
  const std::function<void()> deletion_cb_;
};

}  // namespace

namespace internal {

class Evaluator {
 public:
  Evaluator(const std::string& sql, bool is_expr,
            const EvaluatorOptions& evaluator_options)
      : sql_(sql), is_expr_(is_expr), evaluator_options_(evaluator_options) {
    MaybeInitTypeFactory();
  }

  Evaluator(const ResolvedExpr* expr, const EvaluatorOptions& evaluator_options)
      : is_expr_(true), expr_(expr), evaluator_options_(evaluator_options) {
    MaybeInitTypeFactory();
  }

  Evaluator(const ResolvedStatement* statement,
            const EvaluatorOptions& evaluator_options)
      : is_expr_(false),
        statement_(statement),
        evaluator_options_(evaluator_options) {
    MaybeInitTypeFactory();
  }

  Evaluator(const Evaluator&) = delete;
  Evaluator& operator=(const Evaluator&) = delete;

  ~Evaluator() {
    ZETASQL_CHECK_EQ(num_live_iterators_, 0)
        << "An iterator returned by PreparedQuery::Execute() cannot outlive "
        << "the PreparedQuery object.";
  }

  absl::Status Prepare(const AnalyzerOptions& options, Catalog* catalog)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock l(&mutex_);
    return PrepareLocked(options, catalog);
  }

  // For expressions, populates 'expression_output_value'. For queries,
  // populates 'query_output_iterator'.
  absl::Status Execute(
      const ExpressionOptions& options, Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator)
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status ExecuteAfterPrepare(
      const ExpressionOptions& options, Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    return ExecuteAfterPrepareLocked(options, expression_output_value,
                                     query_output_iterator);
  }

  absl::Status ExecuteAfterPrepareWithOrderedParams(
      const ExpressionOptions& options, Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    return ExecuteAfterPrepareWithOrderedParamsLocked(
        options, expression_output_value, query_output_iterator);
  }

  absl::StatusOr<std::string> ExplainAfterPrepare() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns NULL if this object is for a query instead of an expression.
  const Type* expression_output_type() const ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the column names referenced in the expression.
  // REQUIRES: Prepare() or Execute() has been called successfully.
  absl::StatusOr<std::vector<std::string>> GetReferencedColumns() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the parameters referenced in the expression.
  // REQUIRES: Prepare() or Execute() has been called successfully.
  absl::StatusOr<std::vector<std::string>> GetReferencedParameters() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the number of positional parameters in the expression.
  // REQUIRES: Prepare() or Execute() has been called successfully.
  absl::StatusOr<int> GetPositionalParameterCount() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Makes an EvaluatorTableModifyIterator from the result of evaluating a
  // DMLValueExpr and its input ResolvedStatement.
  absl::StatusOr<std::unique_ptr<EvaluatorTableModifyIterator>>
  MakeUpdateIterator(const Value& value, const ResolvedStatement* statement);

  // REQUIRES: !is_expr_ and Prepare() has been called successfully.
  const ResolvedStatement* resolved_statement() const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    ZETASQL_CHECK(statement_ != nullptr);
    return statement_;
  }

  // REQUIRES: the statement is ResolvedQueryStmt and Prepare() has been called
  // successfully.
  using NameAndType = PreparedQueryBase::NameAndType;
  const std::vector<NameAndType>& query_output_columns() const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    ZETASQL_CHECK(statement_ != nullptr);
    return output_columns_;
  }

  // For unit tests, it is possible to set a callback that is invoked every time
  // an EvaluationContext is created.
  void SetCreateEvaluationCallbackTestOnly(
      std::function<void(EvaluationContext*)> cb) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock l(&mutex_);
    create_evaluation_context_cb_test_only_ =
        absl::make_unique<std::function<void(EvaluationContext*)>>(cb);
  }

 private:
  // Kind of parameters in the AnalyzerOptions.
  enum ParameterKind {
    QUERY_PARAMETER,
    COLUMN_PARAMETER,
  };

  // Converts 'kind' to string.
  static std::string ParameterKindToString(ParameterKind kind) {
    switch (kind) {
      case QUERY_PARAMETER:
        return "query";
      case COLUMN_PARAMETER:
        return "column";
    }
  }

  // If the EvaluatorOptions don't specify a type factory, use our own.
  void MaybeInitTypeFactory() ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock l(&mutex_);
    if (evaluator_options_.type_factory == nullptr) {
      owned_type_factory_ = absl::make_unique<TypeFactory>();
      evaluator_options_.type_factory = owned_type_factory_.get();
    }
  }

  // Same a Prepare(), but the mutex is already locked.
  absl::Status PrepareLocked(const AnalyzerOptions& options, Catalog* catalog)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Same as ExecuteAfterPrepare(), but the mutex is already locked (possibly
  // with a write lock).
  absl::Status ExecuteAfterPrepareLocked(
      const ExpressionOptions& options, Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      ABSL_SHARED_LOCKS_REQUIRED(mutex_);

  // Same as ExecuteAfterPrepareWithOrderedParams(), but with the mutex already
  // locked.
  absl::Status ExecuteAfterPrepareWithOrderedParamsLocked(
      const ExpressionOptions& options, Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      ABSL_SHARED_LOCKS_REQUIRED(mutex_);

  // Checks if 'parameters_map' specifies valid values for all variables from
  // resolved variable map 'variable_map', and populates 'values' with the
  // corresponding Values in the order they appear when iterating over
  // 'variable_map'. 'kind' is the kind of the parameters.
  absl::Status TranslateParameterValueMapToList(
      const ParameterValueMap& parameters_map, const ParameterMap& variable_map,
      ParameterKind kind, ParameterValueList* variable_values) const
      ABSL_SHARED_LOCKS_REQUIRED(mutex_);

  // Validates the arguments to ExecuteAfterPrepareWithOrderedParams().
  absl::Status ValidateColumns(const ParameterValueList& columns) const
      ABSL_SHARED_LOCKS_REQUIRED(mutex_);
  absl::Status ValidateParameters(const ParameterValueList& parameters) const
      ABSL_SHARED_LOCKS_REQUIRED(mutex_);
  absl::Status ValidateSystemVariables(
      const SystemVariableValuesMap& system_variables) const
      ABSL_SHARED_LOCKS_REQUIRED(mutex_);

  bool is_prepared() const ABSL_SHARED_LOCKS_REQUIRED(mutex_) {
    return is_prepared_;
  }

  bool has_prepare_succeeded() const ABSL_SHARED_LOCKS_REQUIRED(mutex_) {
    if (!is_prepared()) return false;
    return compiled_value_expr_ != nullptr ||
           compiled_relational_op_ != nullptr;
  }

  std::unique_ptr<EvaluationContext> CreateEvaluationContext() const
      ABSL_SHARED_LOCKS_REQUIRED(mutex_) {
    // Construct the EvaluationOptions for the internal evaluation API from the
    // user-provided EvaluatorOptions. These are two different struct types with
    // unfortunately similar names.
    EvaluationOptions evaluation_options;
    evaluation_options.scramble_undefined_orderings =
        evaluator_options_.scramble_undefined_orderings;
    evaluation_options.store_proto_field_value_maps = true;
    evaluation_options.use_top_n_accumulator_when_possible = true;
    evaluation_options.max_value_byte_size =
        evaluator_options_.max_value_byte_size;
    evaluation_options.max_intermediate_byte_size =
        evaluator_options_.max_intermediate_byte_size;
    evaluation_options.return_all_rows_for_dml = false;

    auto context = absl::make_unique<EvaluationContext>(evaluation_options);

    context->SetClockAndClearCurrentTimestamp(evaluator_options_.clock);
    if (evaluator_options_.default_time_zone.has_value()) {
      context->SetDefaultTimeZone(evaluator_options_.default_time_zone.value());
    }
    LanguageOptions language_options{analyzer_options_.language()};
    language_options.EnableLanguageFeature(
        FEATURE_DISALLOW_PRIMARY_KEY_UPDATES);
    context->SetLanguageOptions(std::move(language_options));

    if (create_evaluation_context_cb_test_only_ != nullptr) {
      (*create_evaluation_context_cb_test_only_)(context.get());
    }

    return context;
  }

  void IncrementNumLiveIterators() const {
    absl::MutexLock l(&num_live_iterators_mutex_);
    ++num_live_iterators_;
  }

  void DecrementNumLiveIterators() const {
    absl::MutexLock l(&num_live_iterators_mutex_);
    --num_live_iterators_;
  }

  // The original SQL. Not present if expr_ or statement_ was passed in
  // directly.
  const std::string sql_;
  // True for expressions, false for statements. (Set by the constructor).
  const bool is_expr_;

  // The resolved expression. Only valid if is_expr_ is true, in which case
  // either sql_ or expr_ is passed in to the constructor. If sql_ is passed,
  // this is populated by Prepare.
  const ResolvedExpr* expr_ = nullptr;
  // The resolved statement. Only valid if is_expr_ is false, in which case
  // either sql_ or statement_ is passed in to the constructor. If sql_ is
  // passed, this is populated by Prepare.
  const ResolvedStatement* statement_ = nullptr;

  mutable absl::Mutex mutex_;
  EvaluatorOptions evaluator_options_ ABSL_GUARDED_BY(mutex_);
  AnalyzerOptions analyzer_options_ ABSL_GUARDED_BY(mutex_);
  // map or list of parameters
  Parameters algebrizer_parameters_ ABSL_GUARDED_BY(mutex_);
  // maps to variables
  ParameterMap algebrizer_column_map_ ABSL_GUARDED_BY(mutex_);
  SystemVariablesAlgebrizerMap algebrizer_system_variables_
      ABSL_GUARDED_BY(mutex_);
  bool is_prepared_ ABSL_GUARDED_BY(mutex_) = false;
  std::unique_ptr<TypeFactory> owned_type_factory_ ABSL_GUARDED_BY(mutex_)
      ABSL_PT_GUARDED_BY(mutex_);
  // std::unique_ptr<EvaluationContext> evaluation_context_;
  std::unique_ptr<const AnalyzerOutput> analyzer_output_ ABSL_GUARDED_BY(mutex_)
      ABSL_PT_GUARDED_BY(mutex_);

  // For expressions, Prepare populates compiled_value_expr_. For queries, it
  // populates compiled_relational_op.
  std::unique_ptr<ValueExpr> compiled_value_expr_ ABSL_GUARDED_BY(mutex_)
      ABSL_PT_GUARDED_BY(mutex_);
  std::unique_ptr<RelationalOp> compiled_relational_op_ ABSL_GUARDED_BY(mutex_)
      ABSL_PT_GUARDED_BY(mutex_);

  // Output columns corresponding to `compiled_relational_op_` Only valid if
  // the statement is ResolvedQueryStmt.
  std::vector<NameAndType> output_columns_ ABSL_GUARDED_BY(mutex_);
  // The i-th element corresponds to `output_columns_[i]` in the TupleIterator
  // returned by `compiled_relational_op_`. Only valid if the statement is
  // ResolvedQueryStmt.
  std::vector<VariableId> output_column_variables_ ABSL_GUARDED_BY(mutex_);

  // If catalog is not provided via prepare, we will construct the default
  // 'builtin' catalog automatically and store it here.
  std::unique_ptr<SimpleCatalog> owned_catalog_ ABSL_GUARDED_BY(mutex_);

  mutable absl::Mutex num_live_iterators_mutex_;
  // The number of live iterators corresponding to `compiled_relational_op_` or
  // `complied_value_expr` with type `DMLValueExpr`. Only valid if !is_expr_.
  // Mutable so that it can be modified in ExecuteAfterPrepare(). It is only
  // used for sanity checking that an iterator does not outlive the Evaluator.
  mutable int num_live_iterators_ ABSL_GUARDED_BY(num_live_iterators_mutex_) =
      0;

  // The last EvaluationContext that we created, only for use by unit tests. May
  // be NULL.
  std::unique_ptr<std::function<void(EvaluationContext*)>>
      create_evaluation_context_cb_test_only_ ABSL_GUARDED_BY(mutex_)
          ABSL_PT_GUARDED_BY(mutex_);
};

namespace {
// Anonymous columns get empty names.
static std::string HideInternalName(const std::string& name) {
  return IsInternalAlias(name) ? "" : name;
}
}  // namespace

absl::Status Evaluator::PrepareLocked(const AnalyzerOptions& options,
                                      Catalog* catalog) {
  if (is_prepared()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder() << "Prepare called twice";
  }
  is_prepared_ = true;
  analyzer_options_ = options;
  // TODO: Enable pruning by default. We will need to
  // fix some Table::CreateEvaluatorTableIterator() implementations to
  // respect the input column indexes.
  // analyzer_options_.set_prune_unused_columns(true);

  if (catalog == nullptr && (statement_ == nullptr && expr_ == nullptr)) {
    owned_catalog_ = absl::make_unique<SimpleCatalog>(
        "default_catalog", evaluator_options_.type_factory);
    // Add built-in functions to the catalog, using provided <options>.
    owned_catalog_->AddZetaSQLFunctions(options.language());
    catalog = owned_catalog_.get();
  }

  AlgebrizerOptions algebrizer_options;
  algebrizer_options.consolidate_proto_field_accesses = true;
  algebrizer_options.allow_hash_join = true;
  algebrizer_options.allow_order_by_limit_operator = true;
  algebrizer_options.push_down_filters = true;
  algebrizer_options.inline_with_entries = true;

  if (!is_expr_) {
    if (statement_ == nullptr) {
      ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(sql_, analyzer_options_, catalog,
                                       evaluator_options_.type_factory,
                                       &analyzer_output_));
      statement_ = analyzer_output_->resolved_statement();
    } else {
      // TODO: When we're confident that it's no longer possible to
      // crash the reference implementation, remove this validation step.
      ZETASQL_RETURN_IF_ERROR(
          Validator(options.language()).ValidateResolvedStatement(statement_));
    }

    // Algebrize.
    if (analyzer_options_.parameter_mode() == PARAMETER_POSITIONAL) {
      algebrizer_parameters_.set_named(false);
    }
    ResolvedColumnList output_column_list;
    std::vector<std::string> output_column_names;
    switch (statement_->node_kind()) {
      case RESOLVED_QUERY_STMT: {
        ZETASQL_RETURN_IF_ERROR(Algebrizer::AlgebrizeQueryStatementAsRelation(
            options.language(), algebrizer_options,
            evaluator_options_.type_factory,
            statement_->GetAs<ResolvedQueryStmt>(), &output_column_list,
            &compiled_relational_op_, &output_column_names,
            &output_column_variables_, &algebrizer_parameters_,
            &algebrizer_column_map_, &algebrizer_system_variables_));
        ZETASQL_RET_CHECK_EQ(output_column_list.size(), output_column_names.size());
        for (int i = 0; i < output_column_list.size(); ++i) {
          output_columns_.emplace_back(HideInternalName(output_column_names[i]),
                                       output_column_list[i].type());
        }
        break;
      }
      case RESOLVED_INSERT_STMT:
      case RESOLVED_DELETE_STMT:
      case RESOLVED_UPDATE_STMT: {
        ZETASQL_RETURN_IF_ERROR(Algebrizer::AlgebrizeStatement(
            options.language(), algebrizer_options,
            evaluator_options_.type_factory, statement_, &compiled_value_expr_,
            &algebrizer_parameters_, &algebrizer_column_map_,
            &algebrizer_system_variables_));
        break;
      }
      default:
        return ::zetasql_base::InvalidArgumentErrorBuilder()
               << "Evaluator does not support statement kind: "
               << analyzer_output_->resolved_statement()->node_kind_string();
    }
  } else {
    if (expr_ == nullptr) {
      ZETASQL_RETURN_IF_ERROR(AnalyzeExpression(sql_, analyzer_options_, catalog,
                                        evaluator_options_.type_factory,
                                        &analyzer_output_));
      expr_ = analyzer_output_->resolved_expr();
    } else {
      // TODO: When we're confident that it's no longer possible to
      // crash the reference implementation, remove this validation step.
      ZETASQL_RETURN_IF_ERROR(
          Validator(options.language()).ValidateStandaloneResolvedExpr(expr_));
    }

    // Algebrize.
    if (analyzer_options_.parameter_mode() == PARAMETER_POSITIONAL) {
      algebrizer_parameters_.set_named(false);
    }
    ZETASQL_RETURN_IF_ERROR(Algebrizer::AlgebrizeExpression(
        options.language(), algebrizer_options, evaluator_options_.type_factory,
        expr_, &compiled_value_expr_, &algebrizer_parameters_,
        &algebrizer_column_map_, &algebrizer_system_variables_));
  }

  // Build the TupleSchema for the parameters.
  const int num_params =
      algebrizer_parameters_.is_named()
          ? algebrizer_parameters_.named_parameters().size()
          : algebrizer_parameters_.positional_parameters().size();
  std::vector<VariableId> vars;
  vars.reserve(algebrizer_column_map_.size() + num_params);
  for (const auto& elt : algebrizer_column_map_) {
    vars.push_back(elt.second);
  }
  if (algebrizer_parameters_.is_named()) {
    for (const auto& elt : algebrizer_parameters_.named_parameters()) {
      vars.push_back(elt.second);
    }
  } else {
    const ParameterList& params =
        algebrizer_parameters_.positional_parameters();
    vars.insert(vars.end(), params.begin(), params.end());
  }
  for (const auto& system_variable : algebrizer_system_variables_) {
    vars.push_back(system_variable.second);
  }
  const TupleSchema params_schema(vars);

  // Set the TupleSchema for the parameters.
  if (compiled_relational_op_ != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        compiled_relational_op_->SetSchemasForEvaluation({&params_schema}));
  } else {
    ZETASQL_RETURN_IF_ERROR(
        compiled_value_expr_->SetSchemasForEvaluation({&params_schema}));
  }

  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::string>> Evaluator::GetReferencedColumns()
    const {
  absl::ReaderMutexLock l(&mutex_);
  if (!is_prepared()) {
    return ::zetasql_base::FailedPreconditionErrorBuilder()
           << "Expression/Query has not been prepared";
  }
  if (!has_prepare_succeeded()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid prepared expression/query";
  }
  std::vector<std::string> referenced_columns;
  for (const auto& col_var : algebrizer_column_map_) {
    referenced_columns.push_back(col_var.first);
  }
  return referenced_columns;
}

absl::StatusOr<std::vector<std::string>> Evaluator::GetReferencedParameters()
    const {
  absl::ReaderMutexLock l(&mutex_);
  if (!is_prepared()) {
    return ::zetasql_base::FailedPreconditionErrorBuilder()
           << "Expression/Query has not been prepared";
  }
  if (!has_prepare_succeeded()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid prepared expression/query";
  }
  std::vector<std::string> referenced_parameters;
  if (algebrizer_parameters_.is_named()) {
    for (const auto& param_var : algebrizer_parameters_.named_parameters()) {
      referenced_parameters.push_back(param_var.first);
    }
  }
  return referenced_parameters;
}

absl::StatusOr<int> Evaluator::GetPositionalParameterCount() const {
  absl::ReaderMutexLock l(&mutex_);
  if (!is_prepared()) {
    return ::zetasql_base::FailedPreconditionErrorBuilder()
           << "Expression/Query has not been prepared";
  }
  if (!has_prepare_succeeded()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid prepared expression/query";
  }
  std::vector<int> referenced_positional_parameters;
  if (!algebrizer_parameters_.is_named()) {
    return static_cast<int>(
        algebrizer_parameters_.positional_parameters().size());
  }
  return 0;
}

absl::Status Evaluator::TranslateParameterValueMapToList(
    const ParameterValueMap& parameters_map, const ParameterMap& variable_map,
    ParameterKind kind, ParameterValueList* variable_values) const {
  absl::node_hash_map<std::string, const Value*> normalized_parameters;
  for (const auto& value : parameters_map) {
    normalized_parameters[absl::AsciiStrToLower(value.first)] = &value.second;
  }
  for (const auto& v : variable_map) {
    const std::string& variable_name = v.first;
    const Value* value =
        zetasql_base::FindPtrOrNull(normalized_parameters, variable_name);
    if (value == nullptr) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Incomplete " << ParameterKindToString(kind) << " parameters "
             << v.first;
    }
    variable_values->push_back(*value);
  }

  return absl::OkStatus();
}

absl::Status Evaluator::Execute(
    const ExpressionOptions& options, Value* expression_output_value,
    std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) {
  {
    const ParameterValues parameters =
        options.parameters.has_value()
            ? ParameterValues(&options.parameters.value())
            : ParameterValues(&options.ordered_parameters.value());
    absl::MutexLock l(&mutex_);
    // Call Prepare() implicitly if not done by the user.
    if (!is_prepared()) {
      ZETASQL_RET_CHECK(analyzer_options_.query_parameters().empty() &&
                analyzer_options_.positional_query_parameters().empty() &&
                analyzer_options_.expression_columns().empty() &&
                analyzer_options_.in_scope_expression_column_type() ==
                    nullptr &&
                analyzer_options_.system_variables().empty());

      for (const auto& system_variable : options.system_variables) {
        ZETASQL_RETURN_IF_ERROR(analyzer_options_.AddSystemVariable(
            system_variable.first, system_variable.second.type()));
      }

      if (parameters.is_named()) {
        analyzer_options_.set_parameter_mode(PARAMETER_NAMED);
        for (const auto& p : parameters.named_parameters()) {
          ZETASQL_RETURN_IF_ERROR(analyzer_options_.AddQueryParameter(
              p.first,
              p.second.type()));  // Parameter names are case-insensitive.
        }
      } else {
        analyzer_options_.set_parameter_mode(PARAMETER_POSITIONAL);
        for (const Value& parameter : parameters.positional_parameters()) {
          ZETASQL_RETURN_IF_ERROR(
              analyzer_options_.AddPositionalQueryParameter(parameter.type()));
        }
      }
      for (const auto& p : options.columns.value()) {
        // If we find a column with an empty name, we'll treat it as an
        // anonymous in-scope expression column.
        if (p.first.empty()) {
          ZETASQL_RETURN_IF_ERROR(analyzer_options_.SetInScopeExpressionColumn(
              "" /* name */, p.second.type()));
        } else {
          ZETASQL_RETURN_IF_ERROR(analyzer_options_.AddExpressionColumn(
              p.first, p.second.type()));  // Column names are case-insensitive.
        }
      }
      ZETASQL_RETURN_IF_ERROR(
          PrepareLocked(analyzer_options_, nullptr));  // no custom catalog
    }
  }

  absl::ReaderMutexLock l(&mutex_);
  return ExecuteAfterPrepareLocked(options, expression_output_value,
                                   query_output_iterator);
}

absl::Status Evaluator::ExecuteAfterPrepareLocked(
    const ExpressionOptions& options, Value* expression_output_value,
    std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const {
  if (!has_prepare_succeeded()) {
    // Previous Prepare() failed with an analysis error or Prepare was never
    // called. Returns an error for consistency.
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid prepared expression/query";
  }

  ParameterValueList columns_list;
  ZETASQL_RETURN_IF_ERROR(
      TranslateParameterValueMapToList(*options.columns, algebrizer_column_map_,
                                       COLUMN_PARAMETER, &columns_list));

  const ParameterValues parameters =
      options.parameters.has_value()
          ? ParameterValues(&options.parameters.value())
          : ParameterValues(&options.ordered_parameters.value());
  ParameterValueList parameters_list;
  if (parameters.is_named()) {
    ZETASQL_RETURN_IF_ERROR(TranslateParameterValueMapToList(
        parameters.named_parameters(),
        algebrizer_parameters_.named_parameters(), QUERY_PARAMETER,
        &parameters_list));
  } else {
    parameters_list = parameters.positional_parameters();
  }
  ExpressionOptions new_options = options;
  new_options.ordered_columns = columns_list;
  new_options.ordered_parameters = parameters_list;

  return ExecuteAfterPrepareWithOrderedParamsLocked(
      new_options, expression_output_value, query_output_iterator);
}

absl::StatusOr<std::unique_ptr<EvaluatorTableModifyIterator>>
Evaluator::MakeUpdateIterator(const Value& value,
                              const ResolvedStatement* statement) {
  const Table* table;
  EvaluatorTableModifyIterator::Operation operation;
  switch (statement->node_kind()) {
    case RESOLVED_INSERT_STMT:
      table = statement->GetAs<ResolvedInsertStmt>()->table_scan()->table();
      operation = EvaluatorTableModifyIterator::Operation::kInsert;
      break;
    case RESOLVED_UPDATE_STMT:
      table = statement->GetAs<ResolvedUpdateStmt>()->table_scan()->table();
      operation = EvaluatorTableModifyIterator::Operation::kUpdate;
      break;
    case RESOLVED_DELETE_STMT:
      table = statement->GetAs<ResolvedDeleteStmt>()->table_scan()->table();
      operation = EvaluatorTableModifyIterator::Operation::kDelete;
      break;
    default:
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "MakeUpdateIterator() does not support statement kind: "
             << statement->node_kind_string();
  }

  if (table->IsValueTable()) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "PreparedModify api does not support modifying value tables";
  }

  ZETASQL_RET_CHECK(value.type()->IsStruct());
  ZETASQL_RET_CHECK_EQ(value.num_fields(), 2);
  ZETASQL_RET_CHECK(value.field(1).type()->IsArray());
  IncrementNumLiveIterators();
  return absl::make_unique<VectorEvaluatorTableModifyIterator>(
      value.field(1).elements(), table, operation,
      std::bind(&Evaluator::DecrementNumLiveIterators, this));
}

namespace {
// An EvaluatorTableIterator representation of a TupleIterator.
class TupleIteratorAdaptor : public EvaluatorTableIterator {
 public:
  using NameAndType = PreparedQueryBase::NameAndType;

  // 'tuple_indexes[i]' is in the index in a TupleData returned by 'iter' of the
  // value for 'columns[i]'.
  TupleIteratorAdaptor(const std::vector<NameAndType>& columns,
                       const std::vector<int>& tuple_indexes,
                       const std::function<void()>& deletion_cb,
                       std::unique_ptr<EvaluationContext> context,
                       std::unique_ptr<TupleIterator> iter)
      : columns_(columns),
        tuple_indexes_(tuple_indexes),
        deletion_cb_(deletion_cb),
        context_(std::move(context)),
        iter_(std::move(iter)) {}

  TupleIteratorAdaptor(const TupleIteratorAdaptor&) = delete;
  TupleIteratorAdaptor& operator=(const TupleIteratorAdaptor&) = delete;

  ~TupleIteratorAdaptor() override { deletion_cb_(); }

  int NumColumns() const override { return columns_.size(); }

  std::string GetColumnName(int i) const override { return columns_[i].first; }

  const Type* GetColumnType(int i) const override { return columns_[i].second; }

  bool NextRow() override {
    absl::MutexLock l(&mutex_);
    current_ = iter_->Next();
    called_next_ = true;
    return current_ != nullptr;
  }

  const Value& GetValue(int i) const override {
    absl::ReaderMutexLock l(&mutex_);
    return current_->slot(tuple_indexes_[i]).value();
  }

  absl::Status Status() const override {
    absl::MutexLock l(&mutex_);
    return iter_->Status();
  }

  absl::Status Cancel() override {
    absl::MutexLock l(&mutex_);
    return context_->CancelStatement();
  }

  void SetDeadline(absl::Time deadline) override {
    absl::MutexLock l(&mutex_);
    context_->SetStatementEvaluationDeadline(deadline);
  }

 private:
  const std::vector<NameAndType> columns_;
  const std::vector<int> tuple_indexes_;
  const std::function<void()> deletion_cb_;
  mutable absl::Mutex mutex_;
  std::unique_ptr<EvaluationContext> context_ ABSL_GUARDED_BY(mutex_)
      ABSL_PT_GUARDED_BY(mutex_);
  bool called_next_ ABSL_GUARDED_BY(mutex_) = false;
  std::unique_ptr<TupleIterator> iter_ ABSL_GUARDED_BY(mutex_)
      ABSL_PT_GUARDED_BY(mutex_);
  const TupleData* current_ ABSL_GUARDED_BY(mutex_)
      ABSL_PT_GUARDED_BY(mutex_) = nullptr;
  absl::Status status_ ABSL_GUARDED_BY(mutex_);
};
}  // namespace

absl::Status Evaluator::ExecuteAfterPrepareWithOrderedParamsLocked(
    const ExpressionOptions& options, Value* expression_output_value,
    std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const {
  if (!has_prepare_succeeded()) {
    // Previous Prepare() failed with an analysis error or Prepare was never
    // called. Returns an error for consistency.
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid prepared expression/query";
  }

  const ParameterValueList& columns = options.ordered_columns.value();
  const ParameterValueList& parameters = options.ordered_parameters.value();
  const SystemVariableValuesMap& system_variables = options.system_variables;
  ZETASQL_RETURN_IF_ERROR(ValidateColumns(columns));
  ZETASQL_RETURN_IF_ERROR(ValidateParameters(parameters));
  ZETASQL_RETURN_IF_ERROR(ValidateSystemVariables(system_variables));

  std::unique_ptr<EvaluationContext> context = CreateEvaluationContext();
  context->SetStatementEvaluationDeadline(options.deadline);

  ParameterValueList params;
  params.reserve(columns.size() + parameters.size() + system_variables.size());
  params.insert(params.end(), columns.begin(), columns.end());
  params.insert(params.end(), parameters.begin(), parameters.end());
  for (const auto& algebrizer_sysvar : algebrizer_system_variables_) {
    params.push_back(system_variables.at(algebrizer_sysvar.first));
  }
  const TupleData params_data = CreateTupleDataFromValues(std::move(params));

  if (compiled_relational_op_ != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<TupleIterator> tuple_iter,
        compiled_relational_op_->Eval({&params_data},
                                      /*num_extra_slots=*/0, context.get()));
    std::vector<int> tuple_indexes;
    tuple_indexes.reserve(output_column_variables_.size());
    for (const VariableId& var : output_column_variables_) {
      absl::optional<int> i = tuple_iter->Schema().FindIndexForVariable(var);
      ZETASQL_RET_CHECK(i.has_value()) << var;
      tuple_indexes.push_back(i.value());
    }

    IncrementNumLiveIterators();
    std::function<void()> deletion_cb = [this]() {
      DecrementNumLiveIterators();
    };
    *query_output_iterator = absl::make_unique<TupleIteratorAdaptor>(
        output_columns_, tuple_indexes, deletion_cb, std::move(context),
        std::move(tuple_iter));
  } else {
    ZETASQL_RET_CHECK(compiled_value_expr_ != nullptr);

    TupleSlot result;
    absl::Status status;
    if (!compiled_value_expr_->EvalSimple({&params_data}, context.get(),
                                          &result, &status)) {
      return status;
    }
    *expression_output_value = result.value();
  }

  return absl::OkStatus();
}

absl::StatusOr<std::string> Evaluator::ExplainAfterPrepare() const {
  absl::ReaderMutexLock l(&mutex_);
  ZETASQL_RET_CHECK(is_prepared()) << "Prepare must be called first";
  if (compiled_relational_op_ != nullptr) {
    return compiled_relational_op_->DebugString();
  } else {
    ZETASQL_RET_CHECK(compiled_value_expr_ != nullptr);
    return compiled_value_expr_->DebugString();
  }
}

const Type* Evaluator::expression_output_type() const {
  absl::ReaderMutexLock l(&mutex_);
  ZETASQL_CHECK(is_expr_) << "Only expressions have output types";
  ZETASQL_CHECK(is_prepared()) << "Prepare or Execute must be called first";
  ZETASQL_CHECK(compiled_value_expr_ != nullptr) << "Invalid prepared expression";
  return compiled_value_expr_->output_type();
}

absl::Status Evaluator::ValidateColumns(
    const ParameterValueList& columns) const {
  if (columns.size() != algebrizer_column_map_.size()) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Incorrect number of column parameters. Expected "
           << algebrizer_column_map_.size() << " but found " << columns.size();
  }
  int i = 0;
  for (const auto& entry : algebrizer_column_map_) {
    const Value& value = columns[i];

    const std::string& variable_name = entry.first;
    const Type* expected_type = zetasql_base::FindPtrOrNull(
        analyzer_options_.expression_columns(), variable_name);
    if (expected_type == nullptr &&
        analyzer_options_.lookup_expression_column_callback() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(analyzer_options_.lookup_expression_column_callback()(
          variable_name, &expected_type));
    }
    ZETASQL_RET_CHECK(expected_type != nullptr)
        << "Expected type not found for variable " << variable_name;
    if (!expected_type->Equals(value.type())) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Expected column parameter '" << variable_name
             << "' to be of type " << expected_type->DebugString()
             << " but found " << value.type()->DebugString();
    }

    ++i;
  }

  return absl::OkStatus();
}

absl::Status Evaluator::ValidateSystemVariables(
    const SystemVariableValuesMap& system_variables) const {
  // Make sure <system_variables> is consistent with the analyzer options.
  for (const auto& analyzer_options_sysvar :
       analyzer_options_.system_variables()) {
    const std::vector<std::string>& sysvar_name = analyzer_options_sysvar.first;
    auto it = system_variables.find(sysvar_name);
    if (it == system_variables.end()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "No value provided for system variable "
             << absl::StrJoin(sysvar_name, ".");
    }
    const Type* expected_type = analyzer_options_sysvar.second;
    const Type* actual_type = it->second.type();
    if (!expected_type->Equals(actual_type)) {
      ProductMode product_mode = analyzer_options_.language().product_mode();
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Expected system variable '" << absl::StrJoin(sysvar_name, ".")
             << "' to be of type " << expected_type->TypeName(product_mode)
             << " but found " << actual_type->TypeName(product_mode);
    }
  }

  for (const auto& sysvar : system_variables) {
    const std::vector<std::string>& sysvar_name = sysvar.first;
    auto it = analyzer_options_.system_variables().find(sysvar_name);
    if (it == analyzer_options_.system_variables().end()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Value provided for system variable "
             << absl::StrJoin(sysvar_name, ".")
             << ", which is not in the AnalyzerOptions";
    }
  }

  // Finally, do a sanity check that all algebrized system variables map to
  // a known value.  These checks should never fail unless something is wrong
  // with either the algebrizer, or the above checks.
  for (const auto& sysvar : algebrizer_system_variables_) {
    const std::vector<std::string>& sysvar_name = sysvar.first;
    auto it = system_variables.find(sysvar_name);
    ZETASQL_RET_CHECK(it != system_variables.end())
        << "System variable " << absl::StrJoin(sysvar_name, ".")
        << " exists in algebrizer, but no value provided.";
    const Type* expected_type =
        zetasql_base::FindPtrOrNull(analyzer_options_.system_variables(), sysvar_name);
    ZETASQL_RET_CHECK(expected_type != nullptr)
        << "Expected type not found for variable "
        << absl::StrJoin(sysvar_name, ".");
    ZETASQL_RET_CHECK(it->second.type()->Equals(expected_type))
        << "Type mismatch between analyzer options and value type: "
        << absl::StrJoin(sysvar_name, ".");
  }
  return absl::OkStatus();
}

absl::Status Evaluator::ValidateParameters(
    const ParameterValueList& parameters) const {
  if (algebrizer_parameters_.is_named()) {
    if (parameters.size() != algebrizer_parameters_.named_parameters().size()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Incorrect number of named parameters. Expected "
             << algebrizer_parameters_.named_parameters().size()
             << " but found " << parameters.size();
    }


    const QueryParametersMap *query_parameters =
        &analyzer_options_.query_parameters();
    if (analyzer_options_.allow_undeclared_parameters() &&
        analyzer_output_ != nullptr) {
      query_parameters = &analyzer_output_->undeclared_parameters();
    }
    int i = 0;
    for (const auto& elt : algebrizer_parameters_.named_parameters()) {
      const Value& value = parameters[i];

      const std::string& variable_name = elt.first;
      const Type* expected_type = zetasql_base::FindPtrOrNull(
          *query_parameters, variable_name);
      ZETASQL_RET_CHECK(expected_type != nullptr)
          << "Expected type not found for variable " << variable_name;
      if (!expected_type->Equals(value.type())) {
        return zetasql_base::InvalidArgumentErrorBuilder()
            << "Expected query parameter '" << variable_name
            << "' to be of type " << expected_type->DebugString()
            << " but found " << value.type()->DebugString();
      }

      ++i;
    }
  } else {
    if (parameters.size() <
        algebrizer_parameters_.positional_parameters().size()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Incorrect number of positional parameters. Expected at "
             << "least "
             << algebrizer_parameters_.positional_parameters().size()
             << " but found " << parameters.size();
    }

    const std::vector<const Type*>* positional_query_parameters =
        &analyzer_options_.positional_query_parameters();
    if (analyzer_options_.allow_undeclared_parameters() &&
        analyzer_output_ != nullptr) {
      positional_query_parameters =
          &analyzer_output_->undeclared_positional_parameters();
    }
    ZETASQL_RET_CHECK_GE(positional_query_parameters->size(),
                 algebrizer_parameters_.positional_parameters().size())
        << "Mismatch in number of analyzer parameters versus algebrizer "
        << "parameters";

    for (int i = 0; i < algebrizer_parameters_.positional_parameters().size();
         ++i) {
      const Type* expected_type = (*positional_query_parameters)[i];
      const Type* actual_type = parameters[i].type();
      if (!expected_type->Equals(actual_type)) {
        // Parameter positions are 1-based, so use the correct position in the
        // error message.
        return ::zetasql_base::InvalidArgumentErrorBuilder()
            << "Expected positional parameter " << (i + 1)
            << " to be of type " << expected_type->DebugString()
            << " but found " << actual_type->DebugString();
      }
    }
  }

  return absl::OkStatus();
}

EvaluatorOptions EvaluatorOptionsFromTypeFactory(TypeFactory* type_factory) {
  EvaluatorOptions options;
  options.type_factory = type_factory;
  return options;
}

}  // namespace internal

PreparedExpressionBase::PreparedExpressionBase(const std::string& sql,
                                               TypeFactory* type_factory)
    : PreparedExpressionBase(
          sql, internal::EvaluatorOptionsFromTypeFactory(type_factory)) {}

PreparedExpressionBase::PreparedExpressionBase(const std::string& sql,
                                               const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(sql, /*is_expr=*/true, options)) {}

PreparedExpressionBase::PreparedExpressionBase(const ResolvedExpr* expression,
                                               const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(expression, options)) {}

PreparedExpressionBase::~PreparedExpressionBase() {}

absl::Status PreparedExpressionBase::Prepare(const AnalyzerOptions& options,
                                             Catalog* catalog) {
  return evaluator_->Prepare(options, catalog);
}

absl::StatusOr<std::vector<std::string>>
PreparedExpressionBase::GetReferencedColumns() const {
  return evaluator_->GetReferencedColumns();
}

absl::StatusOr<std::vector<std::string>>
PreparedExpressionBase::GetReferencedParameters() const {
  return evaluator_->GetReferencedParameters();
}

absl::StatusOr<int> PreparedExpressionBase::GetPositionalParameterCount()
    const {
  return evaluator_->GetPositionalParameterCount();
}

namespace {
// Utility function for options struct validation.
absl::Status ValidateExpressionOptions(const ExpressionOptions& options) {
  ZETASQL_RET_CHECK(options.columns.has_value() ^ options.ordered_columns.has_value())
      << "One of the columns fields has to be set, but not both";
  ZETASQL_RET_CHECK(options.parameters.has_value() ^
            options.ordered_parameters.has_value())
      << "One of the parameter fields has to be set, but not both";
  return absl::OkStatus();
}

ExpressionOptions QueryOptionsToExpressionOptions(
    const QueryOptions& query_options) {
  ExpressionOptions expr_options;
  if (query_options.parameters.has_value()) {
    expr_options.parameters = query_options.parameters;
  }
  if (query_options.ordered_parameters.has_value()) {
    expr_options.ordered_parameters = query_options.ordered_parameters;
  }
  expr_options.system_variables = query_options.system_variables;
  return expr_options;
}

// If both the named and positional columns are empty, insert an empty named
// columns map into the options struct. Does the same for the parameters.
void GiveDefaultParameters(ExpressionOptions* options) {
  if (!options->columns.has_value() && !options->ordered_columns.has_value()) {
    options->columns = ParameterValueMap();
  }
  if (!options->parameters.has_value() &&
      !options->ordered_parameters.has_value()) {
    options->parameters = ParameterValueMap();
  }
}
};  // namespace

absl::StatusOr<Value> PreparedExpressionBase::Execute(
    ExpressionOptions options) {
  GiveDefaultParameters(&options);
  ZETASQL_RETURN_IF_ERROR(ValidateExpressionOptions(options));
  ZETASQL_RET_CHECK(!options.ordered_columns.has_value())
      << "`ordered_columns` cannot be set for Execute(). Did you mean to call "
         "ExecuteAfterPrepare()?";
  Value output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->Execute(options, &output,
                                      /*query_output_iterator=*/nullptr));
  return output;
}

absl::StatusOr<Value> PreparedExpressionBase::Execute(
    const ParameterValueMap& columns, const ParameterValueMap& parameters,
    const SystemVariableValuesMap& system_variables) {
  ExpressionOptions options;
  options.columns = columns;
  options.parameters = parameters;
  options.system_variables = system_variables;
  return Execute(std::move(options));
}

absl::StatusOr<Value> PreparedExpressionBase::ExecuteWithPositionalParams(
    const ParameterValueMap& columns,
    const ParameterValueList& positional_parameters,
    const SystemVariableValuesMap& system_variables) {
  ExpressionOptions options;
  options.columns = columns;
  options.ordered_parameters = positional_parameters;
  options.system_variables = system_variables;
  return Execute(std::move(options));
}

absl::StatusOr<Value> PreparedExpressionBase::ExecuteAfterPrepare(
    ExpressionOptions options) const {
  GiveDefaultParameters(&options);
  ZETASQL_RETURN_IF_ERROR(ValidateExpressionOptions(options));
  Value output;
  // Branch into the non-positional execute call if one of columns or parameters
  // is non-positional.
  if (options.columns.has_value()) {
    ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepare(
        options, &output, /*query_output_iterator=*/nullptr));
  } else {
    ZETASQL_RET_CHECK(options.ordered_parameters.has_value())
        << "Expected positional parameters since the columns are positional";
    ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepareWithOrderedParams(
        options, &output, /*query_output_iterator=*/nullptr));
  }
  return output;
}

absl::StatusOr<Value> PreparedExpressionBase::ExecuteAfterPrepare(
    const ParameterValueMap& columns, const ParameterValueMap& parameters,
    const SystemVariableValuesMap& system_variables) const {
  ExpressionOptions options;
  options.columns = columns;
  options.parameters = parameters;
  options.system_variables = system_variables;
  return ExecuteAfterPrepare(std::move(options));
}

absl::StatusOr<Value>
PreparedExpressionBase::ExecuteAfterPrepareWithPositionalParams(
    const ParameterValueMap& columns,
    const ParameterValueList& positional_parameters,
    const SystemVariableValuesMap& system_variables) const {
  ExpressionOptions options;
  options.columns = columns;
  options.ordered_parameters = positional_parameters;
  options.system_variables = system_variables;
  return ExecuteAfterPrepare(std::move(options));
}

absl::StatusOr<Value>
PreparedExpressionBase::ExecuteAfterPrepareWithOrderedParams(
    const ParameterValueList& columns, const ParameterValueList& parameters,
    const SystemVariableValuesMap& system_variables) const {
  ExpressionOptions options;
  options.ordered_columns = columns;
  options.ordered_parameters = parameters;
  options.system_variables = system_variables;
  return ExecuteAfterPrepare(std::move(options));
}

absl::StatusOr<std::string> PreparedExpressionBase::ExplainAfterPrepare()
    const {
  return evaluator_->ExplainAfterPrepare();
}

const Type* PreparedExpressionBase::output_type() const {
  return evaluator_->expression_output_type();
}

PreparedQueryBase::PreparedQueryBase(const std::string& sql,
                                     const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(sql, /*is_expr=*/false, options)) {}

PreparedQueryBase::PreparedQueryBase(const ResolvedQueryStmt* stmt,
                                     const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(stmt, options)) {}

PreparedQueryBase::~PreparedQueryBase() {}

absl::Status PreparedQueryBase::Prepare(const AnalyzerOptions& options,
                                        Catalog* catalog) {
  ZETASQL_RETURN_IF_ERROR(evaluator_->Prepare(options, catalog));
  ZETASQL_RET_CHECK_NE(evaluator_->resolved_statement(), nullptr);
  if (evaluator_->resolved_statement()->node_kind() != RESOLVED_QUERY_STMT) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Statement kind "
           << evaluator_->resolved_statement()->node_kind_string()
           << " does not correspond to a query.";
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::string>>
PreparedQueryBase::GetReferencedParameters() const {
  return evaluator_->GetReferencedParameters();
}

absl::StatusOr<int> PreparedQueryBase::GetPositionalParameterCount() const {
  return evaluator_->GetPositionalParameterCount();
}

absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
PreparedQueryBase::Execute(const QueryOptions& options) {
  std::unique_ptr<EvaluatorTableIterator> output;
  ExpressionOptions expr_options = QueryOptionsToExpressionOptions(options);
  GiveDefaultParameters(&expr_options);
  ZETASQL_RETURN_IF_ERROR(ValidateExpressionOptions(expr_options));
  ZETASQL_RETURN_IF_ERROR(evaluator_->Execute(expr_options,
                                      /*expression_output_value=*/nullptr,
                                      &output));
  return output;
}

absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
PreparedQueryBase::Execute(const ParameterValueMap& parameters,
                           const SystemVariableValuesMap& system_variables) {
  QueryOptions options;
  options.parameters = parameters;
  options.system_variables = system_variables;
  return Execute(options);
}

absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
PreparedQueryBase::ExecuteWithPositionalParams(
    const ParameterValueList& positional_parameters,
    const SystemVariableValuesMap& system_variables) {
  QueryOptions options;
  options.ordered_parameters = positional_parameters;
  options.system_variables = system_variables;
  return Execute(options);
}

absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
PreparedQueryBase::ExecuteAfterPrepare(const QueryOptions& options) const {
  std::unique_ptr<EvaluatorTableIterator> output;
  ExpressionOptions expr_options = QueryOptionsToExpressionOptions(options);
  GiveDefaultParameters(&expr_options);
  if (expr_options.parameters.has_value()) {
    ZETASQL_RETURN_IF_ERROR(ValidateExpressionOptions(expr_options));
    ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepare(
        expr_options,
        /*expression_output_value=*/nullptr, &output));
  } else {
    expr_options.columns.reset();
    expr_options.ordered_columns = ParameterValueList();
    ZETASQL_RETURN_IF_ERROR(ValidateExpressionOptions(expr_options));
    ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepareWithOrderedParams(
        expr_options,
        /*expression_output_value=*/nullptr, &output));
  }
  return output;
}

absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
PreparedQueryBase::ExecuteAfterPrepare(
    const ParameterValueList& parameters,
    const SystemVariableValuesMap& system_variables) const {
  QueryOptions options;
  options.ordered_parameters = parameters;
  options.system_variables = system_variables;
  return ExecuteAfterPrepare(options);
}

absl::StatusOr<std::string> PreparedQueryBase::ExplainAfterPrepare() const {
  return evaluator_->ExplainAfterPrepare();
}

int PreparedQueryBase::num_columns() const {
  return evaluator_->query_output_columns().size();
}

std::string PreparedQueryBase::column_name(int i) const {
  ZETASQL_DCHECK_LT(i, evaluator_->query_output_columns().size());
  return evaluator_->query_output_columns()[i].first;
}

const Type* PreparedQueryBase::column_type(int i) const {
  ZETASQL_DCHECK_LT(i, evaluator_->query_output_columns().size());
  return evaluator_->query_output_columns()[i].second;
}

std::vector<PreparedQueryBase::NameAndType> PreparedQueryBase::GetColumns()
    const {
  return evaluator_->query_output_columns();
}

const ResolvedQueryStmt* PreparedQueryBase::resolved_query_stmt() const {
  return evaluator_->resolved_statement()->GetAs<ResolvedQueryStmt>();
}

void PreparedQueryBase::SetCreateEvaluationCallbackTestOnly(
    std::function<void(EvaluationContext*)> cb) {
  return evaluator_->SetCreateEvaluationCallbackTestOnly(cb);
}

PreparedModifyBase::PreparedModifyBase(const std::string& sql,
                                       const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(sql, /*is_expr=*/false, options)) {}

PreparedModifyBase::PreparedModifyBase(const ResolvedStatement* stmt,
                                       const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(stmt, options)) {}

PreparedModifyBase::~PreparedModifyBase() {}

absl::Status PreparedModifyBase::Prepare(const AnalyzerOptions& options,
                                         Catalog* catalog) {
  ZETASQL_RETURN_IF_ERROR(evaluator_->Prepare(options, catalog));
  ZETASQL_RET_CHECK_NE(evaluator_->resolved_statement(), nullptr);
  switch (evaluator_->resolved_statement()->node_kind()) {
    case RESOLVED_INSERT_STMT:
    case RESOLVED_DELETE_STMT:
    case RESOLVED_UPDATE_STMT:
      return absl::OkStatus();
    default:
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Statement kind "
             << evaluator_->resolved_statement()->node_kind_string()
             << " does not correspond to a DML statement.";
  }
}

absl::StatusOr<std::unique_ptr<EvaluatorTableModifyIterator>>
PreparedModifyBase::Execute(const ParameterValueMap& parameters,
                            const SystemVariableValuesMap& system_variables) {
  ExpressionOptions options;
  options.columns = ParameterValueMap();
  options.parameters = parameters;
  options.system_variables = system_variables;
  Value value;
  ZETASQL_RETURN_IF_ERROR(
      evaluator_->Execute(options, &value, /*query_output_iterator=*/nullptr));
  return evaluator_->MakeUpdateIterator(value, resolved_statement());
}

absl::StatusOr<std::unique_ptr<EvaluatorTableModifyIterator>>
PreparedModifyBase::ExecuteWithPositionalParams(
    const ParameterValueList& positional_parameters,
    const SystemVariableValuesMap& system_variables) {
  ExpressionOptions options;
  options.columns = ParameterValueMap();
  options.ordered_parameters = positional_parameters;
  options.system_variables = system_variables;
  Value value;
  ZETASQL_RETURN_IF_ERROR(
      evaluator_->Execute(options, &value, /*query_output_iterator=*/nullptr));
  return evaluator_->MakeUpdateIterator(value, resolved_statement());
}

absl::StatusOr<std::unique_ptr<EvaluatorTableModifyIterator>>
PreparedModifyBase::ExecuteAfterPrepare(
    const ParameterValueMap& parameters,
    const SystemVariableValuesMap& system_variables) const {
  ExpressionOptions options;
  options.columns = ParameterValueMap();
  options.parameters = parameters;
  options.system_variables = system_variables;
  Value value;
  ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepare(
      options, &value, /*query_output_iterator=*/nullptr));
  return evaluator_->MakeUpdateIterator(value, resolved_statement());
}

absl::StatusOr<std::unique_ptr<EvaluatorTableModifyIterator>>
PreparedModifyBase::ExecuteAfterPrepareWithOrderedParams(
    const ParameterValueList& parameters,
    const SystemVariableValuesMap& system_variables) const {
  ExpressionOptions options;
  options.ordered_columns = ParameterValueList();
  options.ordered_parameters = parameters;
  options.system_variables = system_variables;
  Value value;
  ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepareWithOrderedParams(
      options, &value, /*query_output_iterator=*/nullptr));
  return evaluator_->MakeUpdateIterator(value, resolved_statement());
}

absl::StatusOr<std::string> PreparedModifyBase::ExplainAfterPrepare() const {
  return evaluator_->ExplainAfterPrepare();
}

const ResolvedStatement* PreparedModifyBase::resolved_statement() const {
  return evaluator_->resolved_statement();
}

absl::StatusOr<std::vector<std::string>>
PreparedModifyBase::GetReferencedParameters() const {
  return evaluator_->GetReferencedParameters();
}

absl::StatusOr<int> PreparedModifyBase::GetPositionalParameterCount() const {
  return evaluator_->GetPositionalParameterCount();
}

}  // namespace zetasql
