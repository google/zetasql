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

#include "zetasql/public/evaluator.h"

#include <functional>
#include <unordered_map>
#include <utility>

#include "zetasql/base/logging.h"
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
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

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

}  // namespace

namespace internal {

class Evaluator {
 public:
  Evaluator(const std::string& sql, bool is_query,
            const EvaluatorOptions& evaluator_options)
      : sql_(sql), is_query_(is_query), evaluator_options_(evaluator_options) {
    MaybeInitTypeFactory();
  }

  Evaluator(const ResolvedExpr* expr, const EvaluatorOptions& evaluator_options)
      : is_query_(false), expr_(expr), evaluator_options_(evaluator_options) {
    MaybeInitTypeFactory();
  }

  Evaluator(const ResolvedQueryStmt* query,
            const EvaluatorOptions& evaluator_options)
      : is_query_(true), query_(query), evaluator_options_(evaluator_options) {
    MaybeInitTypeFactory();
  }

  Evaluator(const Evaluator&) = delete;
  Evaluator& operator=(const Evaluator&) = delete;

  ~Evaluator() {
    CHECK_EQ(num_live_iterators_, 0)
        << "An iterator returned by PreparedQuery::Execute() cannot outlive "
        << "the PreparedQuery object.";
  }

  zetasql_base::Status Prepare(const AnalyzerOptions& options, Catalog* catalog)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock l(&mutex_);
    return PrepareLocked(options, catalog);
  }

  // For expressions, populates 'expression_output_value'. For queries,
  // populates 'query_output_iterator'.
  zetasql_base::Status Execute(
      const ParameterValueMap& columns, const ParameterValues& parameters,
      Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator)
      ABSL_LOCKS_EXCLUDED(mutex_);

  zetasql_base::Status ExecuteAfterPrepare(
      const ParameterValueMap& columns, const ParameterValues& parameters,
      Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    return ExecuteAfterPrepareLocked(
        columns, parameters, expression_output_value, query_output_iterator);
  }

  zetasql_base::Status ExecuteAfterPrepareWithOrderedParams(
      const ParameterValueList& columns, const ParameterValueList& parameters,
      Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    return ExecuteAfterPrepareWithOrderedParamsLocked(
        columns, parameters, expression_output_value, query_output_iterator);
  }

  zetasql_base::StatusOr<std::string> ExplainAfterPrepare() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns NULL if this object is for a query instead of an expression.
  const Type* expression_output_type() const ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the column names referenced in the expression.
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<std::vector<std::string>> GetReferencedColumns() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the parameters referenced in the expression.
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<std::vector<std::string>> GetReferencedParameters() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the number of positional parameters in the expression.
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<int> GetPositionalParameterCount() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // REQUIRES: is_query_ and Prepare() has been called successfully.
  const ResolvedQueryStmt* resolved_query_stmt() const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    CHECK(query_ != nullptr);
    return query_;
  }

  // REQUIRES: is_query_ and Prepare() has been called successfully.
  using NameAndType = PreparedQuery::NameAndType;
  const std::vector<NameAndType>& query_output_columns() const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock l(&mutex_);
    CHECK(query_ != nullptr);
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

  // Converts 'kind' to std::string.
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
  zetasql_base::Status PrepareLocked(const AnalyzerOptions& options, Catalog* catalog)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Same as ExecuteAfterPrepare(), but the mutex is already locked (possibly
  // with a write lock).
  zetasql_base::Status ExecuteAfterPrepareLocked(
      const ParameterValueMap& columns, const ParameterValues& parameters,
      Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      SHARED_LOCKS_REQUIRED(mutex_);

  // Same as ExecuteAfterPrepareWithOrderedParams(), but with the mutex already
  // locked.
  zetasql_base::Status ExecuteAfterPrepareWithOrderedParamsLocked(
      const ParameterValueList& columns, const ParameterValueList& parameters,
      Value* expression_output_value,
      std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const
      SHARED_LOCKS_REQUIRED(mutex_);

  // Checks if 'parameters_map' specifies valid values for all variables from
  // resolved variable map 'variable_map', and populates 'values' with the
  // corresponding Values in the order they appear when iterating over
  // 'variable_map'. 'kind' is the kind of the parameters.
  zetasql_base::Status TranslateParameterValueMapToList(
      const ParameterValueMap& parameters_map, const ParameterMap& variable_map,
      ParameterKind kind, ParameterValueList* variable_values) const
      SHARED_LOCKS_REQUIRED(mutex_);

  // Validates the arguments to ExecuteAfterPrepareWithOrderedParams().
  zetasql_base::Status ValidateColumns(const ParameterValueList& columns) const
      SHARED_LOCKS_REQUIRED(mutex_);
  zetasql_base::Status ValidateParameters(const ParameterValueList& parameters) const
      SHARED_LOCKS_REQUIRED(mutex_);

  bool is_prepared() const SHARED_LOCKS_REQUIRED(mutex_) {
    return is_prepared_;
  }

  bool has_prepare_succeeded() const SHARED_LOCKS_REQUIRED(mutex_) {
    if (!is_prepared()) return false;
    if (is_query_) return compiled_relational_op_ != nullptr;
    return compiled_value_expr_ != nullptr;
  }

  std::unique_ptr<EvaluationContext> CreateEvaluationContext() const
      SHARED_LOCKS_REQUIRED(mutex_) {
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

    auto context = absl::make_unique<EvaluationContext>(evaluation_options);

    context->SetClockAndClearCurrentTimestamp(evaluator_options_.clock);
    if (evaluator_options_.default_time_zone.has_value()) {
      context->SetDefaultTimeZone(evaluator_options_.default_time_zone.value());
    }
    context->SetLanguageOptions(analyzer_options_.language_options());

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

  // The original SQL.  Not present if expr_ or query_ was passed in directly.
  const std::string sql_;
  // True for queries, false for expressions. (Set by the constructor).
  const bool is_query_;

  // The resolved expression. Only valid if is_query_ is false, in which case
  // either sql_ or expr_ is passed in to the constructor. If sql_ is passed,
  // this is populated by Prepare.
  const ResolvedExpr* expr_ = nullptr;
  // The resolved query. Only valid if is_query_ is true, in which case
  // either sql_ or query_ is passed in to the constructor. If sql_ is passed,
  // this is populated by Prepare.
  const ResolvedQueryStmt* query_ = nullptr;

  mutable absl::Mutex mutex_;
  EvaluatorOptions evaluator_options_ GUARDED_BY(mutex_);
  AnalyzerOptions analyzer_options_ GUARDED_BY(mutex_);
  // map or list of parameters
  Parameters algebrizer_parameters_ GUARDED_BY(mutex_);
  // maps to variables
  ParameterMap algebrizer_column_map_ GUARDED_BY(mutex_);
  bool is_prepared_ GUARDED_BY(mutex_) = false;
  std::unique_ptr<TypeFactory> owned_type_factory_ GUARDED_BY(mutex_)
      PT_GUARDED_BY(mutex_);
  // std::unique_ptr<EvaluationContext> evaluation_context_;
  std::unique_ptr<const AnalyzerOutput> analyzer_output_ GUARDED_BY(mutex_)
      PT_GUARDED_BY(mutex_);

  // For expressions, Prepare populates compiled_value_expr_. For queries, it
  // populates compiled_relational_op.
  std::unique_ptr<ValueExpr> compiled_value_expr_ GUARDED_BY(mutex_)
      PT_GUARDED_BY(mutex_);
  std::unique_ptr<RelationalOp> compiled_relational_op_ GUARDED_BY(mutex_)
      PT_GUARDED_BY(mutex_);

  // Output columns corresponding to 'compiled_relational_op_' Only valid if
  // 'is_query_' is true.
  std::vector<NameAndType> output_columns_ GUARDED_BY(mutex_);
  // The i-th element corresponds to 'output_columns_[i]' in the TupleIterator
  // returned by 'compiled_relational_op_'. Only valid if 'is_query_' is true.
  std::vector<VariableId> output_column_variables_ GUARDED_BY(mutex_);

  mutable absl::Mutex num_live_iterators_mutex_;
  // The number of live iterators corresponding to
  // 'compiled_relational_op_'. Only valid if 'is_query_' is true.  Mutable so
  // that it can be modified in ExecuteAfterPrepare(). It is only used for
  // sanity checking that an iterator does not outlive the Evaluator.
  mutable int num_live_iterators_ GUARDED_BY(num_live_iterators_mutex_) = 0;

  // The last EvaluationContext that we created, only for use by unit tests. May
  // be NULL.
  std::unique_ptr<std::function<void(EvaluationContext*)>>
      create_evaluation_context_cb_test_only_ GUARDED_BY(mutex_)
          PT_GUARDED_BY(mutex_);
};

namespace {
// Anonymous columns get empty names.
static std::string HideInternalName(const std::string& name) {
  return IsInternalAlias(name) ? "" : name;
}
}  // namespace

zetasql_base::Status Evaluator::PrepareLocked(const AnalyzerOptions& options,
                                      Catalog* catalog) {
  if (is_prepared()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
           << "Prepare called twice";
  }
  is_prepared_ = true;
  analyzer_options_ = options;
  analyzer_options_.set_prune_unused_columns(true);

  std::unique_ptr<SimpleCatalog> simple_catalog;
  if (catalog == nullptr) {
    simple_catalog = absl::make_unique<SimpleCatalog>(
        "default_catalog", evaluator_options_.type_factory);
    catalog = simple_catalog.get();
    // Add built-in functions to the catalog, using provided <options>.
    simple_catalog->AddZetaSQLFunctions(options.language());
  }

  AlgebrizerOptions algebrizer_options;
  algebrizer_options.consolidate_proto_field_accesses = true;
  algebrizer_options.allow_hash_join = true;
  algebrizer_options.allow_order_by_limit_operator = true;
  algebrizer_options.push_down_filters = true;

  SystemVariablesAlgebrizerMap algebrizer_system_variables;
  if (is_query_) {
    if (query_ == nullptr) {
      ZETASQL_RETURN_IF_ERROR(AnalyzeStatement(sql_, options, catalog,
                                       evaluator_options_.type_factory,
                                       &analyzer_output_));
      if (analyzer_output_->resolved_statement()->node_kind() !=
          RESOLVED_QUERY_STMT) {
        // This error is only reachable if the user enabled additional
        // statement kinds in their AnalyzerOptions.
        return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
               << "Statement is not a query: "
               << analyzer_output_->resolved_statement()->node_kind_string();
      }

      query_ =
          analyzer_output_->resolved_statement()->GetAs<ResolvedQueryStmt>();
    } else {
      // TODO: When we're confident that it's no longer possible to
      // crash the reference implementation, remove this validation step.
      ZETASQL_RETURN_IF_ERROR(Validator().ValidateResolvedStatement(query_));
    }

    // Algebrize.
    if (analyzer_options_.parameter_mode() == PARAMETER_POSITIONAL) {
      algebrizer_parameters_.set_named(false);
    }
    ResolvedColumnList output_column_list;
    std::vector<std::string> output_column_names;
    ZETASQL_RETURN_IF_ERROR(Algebrizer::AlgebrizeQueryStatementAsRelation(
        options.language(), algebrizer_options, evaluator_options_.type_factory,
        query_, &output_column_list, &compiled_relational_op_,
        &output_column_names, &output_column_variables_,
        &algebrizer_parameters_, &algebrizer_column_map_,
        &algebrizer_system_variables));
    ZETASQL_RET_CHECK_EQ(output_column_list.size(), output_column_names.size());
    for (int i = 0; i < output_column_list.size(); ++i) {
      output_columns_.emplace_back(HideInternalName(output_column_names[i]),
                                   output_column_list[i].type());
    }
  } else {
    if (expr_ == nullptr) {
      ZETASQL_RETURN_IF_ERROR(AnalyzeExpression(sql_, options, catalog,
                                        evaluator_options_.type_factory,
                                        &analyzer_output_));
      expr_ = analyzer_output_->resolved_expr();
    } else {
      // TODO: When we're confident that it's no longer possible to
      // crash the reference implementation, remove this validation step.
      ZETASQL_RETURN_IF_ERROR(Validator().ValidateStandaloneResolvedExpr(expr_));
    }

    // Algebrize.
    if (analyzer_options_.parameter_mode() == PARAMETER_POSITIONAL) {
      algebrizer_parameters_.set_named(false);
    }
    ZETASQL_RETURN_IF_ERROR(Algebrizer::AlgebrizeExpression(
        options.language(), algebrizer_options, evaluator_options_.type_factory,
        expr_, &compiled_value_expr_, &algebrizer_parameters_,
        &algebrizer_column_map_, &algebrizer_system_variables));
  }
  // TODO: Remove this check when execution of system variables
  // is supported.
  if (!algebrizer_system_variables.empty()) {
    return zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
           << "System variables are not supported.";
  }

  // Build the TupleSchema for the parameterss.
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
  const TupleSchema params_schema(vars);

  // Set the TupleSchema for the parameters.
  if (is_query_) {
    ZETASQL_RETURN_IF_ERROR(
        compiled_relational_op_->SetSchemasForEvaluation({&params_schema}));
  } else {
    ZETASQL_RETURN_IF_ERROR(
        compiled_value_expr_->SetSchemasForEvaluation({&params_schema}));
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::StatusOr<std::vector<std::string>> Evaluator::GetReferencedColumns() const {
  absl::ReaderMutexLock l(&mutex_);
  if (!is_prepared()) {
    return ::zetasql_base::FailedPreconditionErrorBuilder(ZETASQL_LOC)
           << "Expression/Query has not been prepared";
  }
  if (!has_prepare_succeeded()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
           << "Invalid prepared expression/query";
  }
  std::vector<std::string> referenced_columns;
  for (const auto& col_var : algebrizer_column_map_) {
    referenced_columns.push_back(col_var.first);
  }
  return referenced_columns;
}

zetasql_base::StatusOr<std::vector<std::string>> Evaluator::GetReferencedParameters() const {
  absl::ReaderMutexLock l(&mutex_);
  if (!is_prepared()) {
    return ::zetasql_base::FailedPreconditionErrorBuilder(ZETASQL_LOC)
           << "Expression/Query has not been prepared";
  }
  if (!has_prepare_succeeded()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
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

zetasql_base::StatusOr<int> Evaluator::GetPositionalParameterCount() const {
  absl::ReaderMutexLock l(&mutex_);
  if (!is_prepared()) {
    return ::zetasql_base::FailedPreconditionErrorBuilder(ZETASQL_LOC)
           << "Expression/Query has not been prepared";
  }
  if (!has_prepare_succeeded()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
           << "Invalid prepared expression/query";
  }
  std::vector<int> referenced_positional_parameters;
  if (!algebrizer_parameters_.is_named()) {
    return static_cast<int>(
        algebrizer_parameters_.positional_parameters().size());
  }
  return 0;
}

zetasql_base::Status Evaluator::TranslateParameterValueMapToList(
    const ParameterValueMap& parameters_map, const ParameterMap& variable_map,
    ParameterKind kind, ParameterValueList* variable_values) const {
  std::unordered_map<std::string, const Value*> normalized_parameters;
  for (const auto& value : parameters_map) {
    normalized_parameters[absl::AsciiStrToLower(value.first)] = &value.second;
  }
  for (const auto& v : variable_map) {
    const std::string& variable_name = v.first;
    const Value* value =
        zetasql_base::FindPtrOrNull(normalized_parameters, variable_name);
    if (value == nullptr) {
      return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
             << "Incomplete " << ParameterKindToString(kind) << " parameters "
             << v.first;
    }
    variable_values->push_back(*value);
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Evaluator::Execute(
    const ParameterValueMap& columns, const ParameterValues& parameters,
    Value* expression_output_value,
    std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) {
  {
    absl::MutexLock l(&mutex_);
    // Call Prepare() implicitly if not done by the user.
    if (!is_prepared()) {
      ZETASQL_RET_CHECK(analyzer_options_.query_parameters().empty() &&
                analyzer_options_.positional_query_parameters().empty() &&
                analyzer_options_.expression_columns().empty() &&
                analyzer_options_.in_scope_expression_column_type() == nullptr);
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
      for (const auto& p : columns) {
        // If we a column with an empty name, we'll treat it as an
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
  return ExecuteAfterPrepareLocked(columns, parameters, expression_output_value,
                                   query_output_iterator);
}

zetasql_base::Status Evaluator::ExecuteAfterPrepareLocked(
    const ParameterValueMap& columns, const ParameterValues& parameters,
    Value* expression_output_value,
    std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const {
  if (!has_prepare_succeeded()) {
    // Previous Prepare() failed with an analysis error or Prepare was never
    // called. Returns an error for consistency.
    return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
           << "Invalid prepared expression/query";
  }

  ParameterValueList columns_list;
  ZETASQL_RETURN_IF_ERROR(TranslateParameterValueMapToList(
      columns, algebrizer_column_map_, COLUMN_PARAMETER, &columns_list));

  ParameterValueList parameters_list;
  if (parameters.is_named()) {
    ZETASQL_RETURN_IF_ERROR(TranslateParameterValueMapToList(
        parameters.named_parameters(),
        algebrizer_parameters_.named_parameters(), QUERY_PARAMETER,
        &parameters_list));
  } else {
    parameters_list = parameters.positional_parameters();
  }

  return ExecuteAfterPrepareWithOrderedParamsLocked(
      columns_list, parameters_list, expression_output_value,
      query_output_iterator);
}

namespace {
// An EvaluatorTableIterator representation of a TupleIterator.
class TupleIteratorAdaptor : public EvaluatorTableIterator {
 public:
  using NameAndType = PreparedQuery::NameAndType;

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

  zetasql_base::Status Status() const override {
    absl::MutexLock l(&mutex_);
    return iter_->Status();
  }

  zetasql_base::Status Cancel() override {
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
  std::unique_ptr<EvaluationContext> context_ GUARDED_BY(mutex_)
      PT_GUARDED_BY(mutex_);
  bool called_next_ GUARDED_BY(mutex_) = false;
  std::unique_ptr<TupleIterator> iter_ GUARDED_BY(mutex_) PT_GUARDED_BY(mutex_);
  const TupleData* current_ GUARDED_BY(mutex_) PT_GUARDED_BY(mutex_) = nullptr;
  zetasql_base::Status status_ GUARDED_BY(mutex_);
};
}  // namespace

zetasql_base::Status Evaluator::ExecuteAfterPrepareWithOrderedParamsLocked(
    const ParameterValueList& columns, const ParameterValueList& parameters,
    Value* expression_output_value,
    std::unique_ptr<EvaluatorTableIterator>* query_output_iterator) const {
  if (!has_prepare_succeeded()) {
    // Previous Prepare() failed with an analysis error or Prepare was never
    // called. Returns an error for consistency.
    return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
           << "Invalid prepared expression/query";
  }

  ZETASQL_RETURN_IF_ERROR(ValidateColumns(columns));
  ZETASQL_RETURN_IF_ERROR(ValidateParameters(parameters));

  std::unique_ptr<EvaluationContext> context = CreateEvaluationContext();

  ParameterValueList params;
  params.reserve(columns.size() + parameters.size());
  params.insert(params.end(), columns.begin(), columns.end());
  params.insert(params.end(), parameters.begin(), parameters.end());
  const TupleData params_data = CreateTupleDataFromValues(params);

  if (is_query_) {
    ZETASQL_RET_CHECK(compiled_relational_op_ != nullptr);
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
    ::zetasql_base::Status status;
    if (!compiled_value_expr_->EvalSimple({&params_data}, context.get(),
                                          &result, &status)) {
      return status;
    }
    *expression_output_value = result.value();
  }

  return zetasql_base::OkStatus();
}

zetasql_base::StatusOr<std::string> Evaluator::ExplainAfterPrepare() const {
  absl::ReaderMutexLock l(&mutex_);
  ZETASQL_RET_CHECK(is_prepared()) << "Prepare must be called first";
  if (is_query_) {
    ZETASQL_RET_CHECK(compiled_relational_op_ != nullptr);
    return compiled_relational_op_->DebugString();
  } else {
    ZETASQL_RET_CHECK(compiled_value_expr_ != nullptr);
    return compiled_value_expr_->DebugString();
  }
}

const Type* Evaluator::expression_output_type() const {
  absl::ReaderMutexLock l(&mutex_);
  CHECK(!is_query_) << "Only expressions have output types";
  CHECK(is_prepared()) << "Prepare or Execute must be called first";
  CHECK(compiled_value_expr_ != nullptr) << "Invalid prepared expression";
  return compiled_value_expr_->output_type();
}

zetasql_base::Status Evaluator::ValidateColumns(
    const ParameterValueList& columns) const {
  if (columns.size() != algebrizer_column_map_.size()) {
    return zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
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
      return zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
             << "Expected column parameter '" << variable_name
             << "' to be of type " << expected_type->DebugString()
             << " but found " << value.type()->DebugString();
    }

    ++i;
  }

  return zetasql_base::OkStatus();
}

zetasql_base::Status Evaluator::ValidateParameters(
    const ParameterValueList& parameters) const {
  if (algebrizer_parameters_.is_named()) {
    if (parameters.size() != algebrizer_parameters_.named_parameters().size()) {
      return zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
             << "Incorrect number of named parameters. Expected "
             << algebrizer_parameters_.named_parameters().size()
             << " but found " << parameters.size();
    }

    int i = 0;
    for (const auto& elt : algebrizer_parameters_.named_parameters()) {
      const Value& value = parameters[i];

      const std::string& variable_name = elt.first;
      const Type* expected_type = zetasql_base::FindPtrOrNull(
          analyzer_options_.query_parameters(), variable_name);
      ZETASQL_RET_CHECK(expected_type != nullptr)
          << "Expected type not found for variable " << variable_name;
      if (!expected_type->Equals(value.type())) {
        return zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
               << "Expected query parameter '" << variable_name
               << "' to be of type " << expected_type->DebugString()
               << " but found " << value.type()->DebugString();
      }

      ++i;
    }
  } else {
    if (parameters.size() <
        algebrizer_parameters_.positional_parameters().size()) {
      return zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
             << "Incorrect number of positional parameters. Expected at "
             << "least "
             << algebrizer_parameters_.positional_parameters().size()
             << " but found " << parameters.size();
    }

    ZETASQL_RET_CHECK_GE(analyzer_options_.positional_query_parameters().size(),
                 algebrizer_parameters_.positional_parameters().size())
        << "Mismatch in number of analyzer parameters versus algebrizer "
        << "parameters";
    for (int i = 0; i < algebrizer_parameters_.positional_parameters().size();
         ++i) {
      const Type* expected_type =
          analyzer_options_.positional_query_parameters()[i];
      const Type* actual_type = parameters[i].type();
      if (!expected_type->Equals(actual_type)) {
        // Parameter positions are 1-based, so use the correct position in the
        // error message.
        return ::zetasql_base::InvalidArgumentErrorBuilder(ZETASQL_LOC)
               << "Expected positional parameter " << (i + 1)
               << " to be of type " << expected_type->DebugString()
               << " but found " << actual_type->DebugString();
      }
    }
  }

  return zetasql_base::OkStatus();
}

EvaluatorOptions EvaluatorOptionsFromTypeFactory(TypeFactory* type_factory) {
  EvaluatorOptions options;
  options.type_factory = type_factory;
  return options;
}

}  // namespace internal

PreparedExpression::PreparedExpression(const std::string& sql,
                                       TypeFactory* type_factory)
    : PreparedExpression(
          sql, internal::EvaluatorOptionsFromTypeFactory(type_factory)) {}

PreparedExpression::PreparedExpression(const std::string& sql,
                                       const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(sql, /*is_query=*/false, options)) {}

PreparedExpression::PreparedExpression(const ResolvedExpr* expression,
                                       const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(expression, options)) {}

PreparedExpression::~PreparedExpression() {}

zetasql_base::Status PreparedExpression::Prepare(const AnalyzerOptions& options,
                                         Catalog* catalog) {
  return evaluator_->Prepare(options, catalog);
}

zetasql_base::StatusOr<std::vector<std::string>> PreparedExpression::GetReferencedColumns()
    const {
  return evaluator_->GetReferencedColumns();
}

zetasql_base::StatusOr<std::vector<std::string>>
PreparedExpression::GetReferencedParameters() const {
  return evaluator_->GetReferencedParameters();
}

zetasql_base::StatusOr<int> PreparedExpression::GetPositionalParameterCount() const {
  return evaluator_->GetPositionalParameterCount();
}

zetasql_base::StatusOr<Value> PreparedExpression::Execute(
    const ParameterValueMap& columns, const ParameterValueMap& parameters) {
  Value output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->Execute(columns, ParameterValues(&parameters),
                                      &output,
                                      /*query_output_iterator=*/nullptr));
  return output;
}

zetasql_base::StatusOr<Value> PreparedExpression::ExecuteWithPositionalParams(
    const ParameterValueMap& columns,
    const ParameterValueList& positional_parameters) {
  Value output;
  ZETASQL_RETURN_IF_ERROR(
      evaluator_->Execute(columns, ParameterValues(&positional_parameters),
                          &output, /*query_output_iterator=*/nullptr));
  return output;
}

zetasql_base::StatusOr<Value> PreparedExpression::ExecuteAfterPrepare(
    const ParameterValueMap& columns,
    const ParameterValueMap& parameters) const {
  Value output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepare(
      columns, ParameterValues(&parameters), &output,
      /*query_output_iterator=*/nullptr));
  return output;
}

zetasql_base::StatusOr<Value>
PreparedExpression::ExecuteAfterPrepareWithPositionalParams(
    const ParameterValueMap& columns,
    const ParameterValueList& positional_parameters) const {
  Value output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepare(
      columns, ParameterValues(&positional_parameters), &output,
      /*query_output_iterator=*/nullptr));
  return output;
}

zetasql_base::StatusOr<Value> PreparedExpression::ExecuteAfterPrepareWithOrderedParams(
    const ParameterValueList& columns,
    const ParameterValueList& parameters) const {
  Value output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepareWithOrderedParams(
      columns, parameters, &output, /*query_output_iterator=*/nullptr));
  return output;
}

zetasql_base::StatusOr<std::string> PreparedExpression::ExplainAfterPrepare() const {
  return evaluator_->ExplainAfterPrepare();
}

const Type* PreparedExpression::output_type() const {
  return evaluator_->expression_output_type();
}

PreparedQuery::PreparedQuery(const std::string& sql, const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(sql, /*is_query=*/true, options)) {}

PreparedQuery::PreparedQuery(const ResolvedQueryStmt* stmt,
                             const EvaluatorOptions& options)
    : evaluator_(new internal::Evaluator(stmt, options)) {}

PreparedQuery::~PreparedQuery() {}

zetasql_base::Status PreparedQuery::Prepare(const AnalyzerOptions& options,
                                    Catalog* catalog) {
  return evaluator_->Prepare(options, catalog);
}

zetasql_base::StatusOr<std::vector<std::string>> PreparedQuery::GetReferencedParameters()
    const {
  return evaluator_->GetReferencedParameters();
}

zetasql_base::StatusOr<int> PreparedQuery::GetPositionalParameterCount() const {
  return evaluator_->GetPositionalParameterCount();
}

zetasql_base::StatusOr<std::unique_ptr<EvaluatorTableIterator>> PreparedQuery::Execute(
    const ParameterValueMap& parameters) {
  std::unique_ptr<EvaluatorTableIterator> output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->Execute(
      /*columns=*/{}, ParameterValues(&parameters),
      /*expression_output_value=*/nullptr, &output));
  return output;
}

zetasql_base::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
PreparedQuery::ExecuteWithPositionalParams(
    const ParameterValueList& positional_parameters) {
  std::unique_ptr<EvaluatorTableIterator> output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->Execute(
      /*columns=*/{}, ParameterValues(&positional_parameters),
      /*expression_output_value=*/nullptr, &output));
  return output;
}

zetasql_base::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
PreparedQuery::ExecuteAfterPrepareWithOrderedParams(
    const ParameterValueList& parameters) const {
  std::unique_ptr<EvaluatorTableIterator> output;
  ZETASQL_RETURN_IF_ERROR(evaluator_->ExecuteAfterPrepareWithOrderedParams(
      /*columns=*/{}, parameters, /*expression_output_value=*/nullptr,
      &output));
  return output;
}

zetasql_base::StatusOr<std::string> PreparedQuery::ExplainAfterPrepare() const {
  return evaluator_->ExplainAfterPrepare();
}

int PreparedQuery::num_columns() const {
  return evaluator_->query_output_columns().size();
}

std::string PreparedQuery::column_name(int i) const {
  DCHECK_LT(i, evaluator_->query_output_columns().size());
  return evaluator_->query_output_columns()[i].first;
}

const Type* PreparedQuery::column_type(int i) const {
  DCHECK_LT(i, evaluator_->query_output_columns().size());
  return evaluator_->query_output_columns()[i].second;
}

std::vector<PreparedQuery::NameAndType> PreparedQuery::GetColumns() const {
  return evaluator_->query_output_columns();
}

const ResolvedQueryStmt* PreparedQuery::resolved_query_stmt() const {
  return evaluator_->resolved_query_stmt();
}

void PreparedQuery::SetCreateEvaluationCallbackTestOnly(
    std::function<void(EvaluationContext*)> cb) {
  return evaluator_->SetCreateEvaluationCallbackTestOnly(cb);
}

}  // namespace zetasql
