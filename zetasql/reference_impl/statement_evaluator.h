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

#ifndef ZETASQL_REFERENCE_IMPL_STATEMENT_EVALUATOR_H_
#define ZETASQL_REFERENCE_IMPL_STATEMENT_EVALUATOR_H_

#include <memory>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/scripting/parsed_script.h"
#include "zetasql/scripting/script_executor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"

namespace zetasql {

// Invoked from ExecuteStatement()/EvaluateScalarExpression() for all calls to
// ExecuteStatement()/EvaluateExpression(), on both success and failure.
//
// In the event of an error, <status_or_result> will have a failed status and
// <resolved_stmt> may be null, depending on how far the analysis got, prior to
// the error.
//
// If status_or_result is ok, the resolved tree is non-null.
//
// The implementation may override the methods here to log or process the
// results.
class StatementEvaluatorCallback {
 public:
  explicit StatementEvaluatorCallback(int64_t bytes_per_iterator)
      : bytes_per_iterator_(bytes_per_iterator) {}

  virtual ~StatementEvaluatorCallback() = default;

  // Invoked on an attempted statement evaluation, whether successful or not.
  virtual void OnStatementResult(
      const ScriptSegment& segment, const ResolvedStatement* resolved_stmt,
      const absl::StatusOr<Value>& status_or_result) {}

  // Invoked on an attempted expression evaluation, whether successful or not.
  virtual void OnScalarExpressionResult(
      const ScriptSegment& segment, const ResolvedExpr* resolved_expr,
      const absl::StatusOr<Value>& status_or_result) {}

  // Invoked on an attempt to resolve a type name, whether successful or not.
  virtual void OnTypeResult(const ScriptSegment& segment,
                            absl::StatusOr<const Type*> status_or_type) {}

  // Invoked on an attempt to check if variable type is supported.
  // This method may return false to disallow certain variable types.
  virtual bool IsSupportedVariableType(const Type* type) { return true; }

  // Replaces all data in <table> with <rows>.  Each element in <rows>
  // contains one sub-element per column.
  virtual absl::Status SetTableContents(
      const Table* table, const std::vector<std::vector<Value>>& rows) {
    return zetasql_base::UnimplementedErrorBuilder() << "DML not supported.";
  }

  // Invoked on an attempt to load a procedure.
  virtual absl::StatusOr<std::unique_ptr<ProcedureDefinition>> LoadProcedure(
      const absl::Span<const std::string>& path) {
    return absl::NotFoundError("");
  }

  int64_t get_bytes_per_iterator() {
    return bytes_per_iterator_;
  }

 private:
  // Fake memory bytes that an iterator takes up, to be returned by
  // StatementEvaluatorImpl::GetIteratorMemoryUsage().
  // Default 0 if not set by test option.
  const int64_t bytes_per_iterator_;
};

class StatementEvaluatorImpl : public StatementEvaluator {
 public:
  // <type_factory>, and <catalog> are mandatory.
  // <callback> is optional.
  //
  // All pointer parameters must remain alive for the duration of the
  // StatementEvaluatorImpl object.
  //
  // The following caveats apply to fields set in <evaluator_options>:
  //   - Queries are run using the timezone in the AnalyzerOptions provided by
  //       the script executor.  The <default_time_zone> field of
  //       EvaluatorOptions is ignored.
  //   - For DML statements, all fields of <evaluator_options> are ignored,
  //       except for <clock>.
  StatementEvaluatorImpl(
      const AnalyzerOptions& initial_analyzer_options,
      const EvaluatorOptions& evaluator_options,
      absl::variant<ParameterValueList, ParameterValueMap> parameters,
      TypeFactory* type_factory, Catalog* catalog,
      StatementEvaluatorCallback* callback)
      : initial_analyzer_options_(initial_analyzer_options),
        options_(evaluator_options),
        parameters_(parameters),
        type_factory_(type_factory),
        catalog_(catalog),
        callback_(callback) {}

  absl::Status ExecuteStatement(const ScriptExecutor& executor,
                                const ScriptSegment& segment) override;

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  ExecuteQueryWithResult(const ScriptExecutor& executor,
                         const ScriptSegment& segment) override;

  absl::Status SerializeIterator(
      const EvaluatorTableIterator& iterator,
      google::protobuf::Any& out) override;

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  DeserializeToIterator(
    const google::protobuf::Any& msg,
    const ScriptExecutor& executor,
    const ParsedScript& parsed_script) override;

  absl::StatusOr<int64_t> GetIteratorMemoryUsage(
      const EvaluatorTableIterator& iterator) override;

  absl::StatusOr<int> EvaluateCaseExpression(
      const ScriptSegment& case_value,
      const std::vector<ScriptSegment>& when_values,
      const ScriptExecutor& executor) override;

  absl::StatusOr<Value> EvaluateScalarExpression(
      const ScriptExecutor& executor, const ScriptSegment& segment,
      const Type* target_type) override;

  absl::StatusOr<TypeWithParameters> ResolveTypeName(
      const ScriptExecutor& executor, const ScriptSegment& segment) override;

  bool IsSupportedVariableType(
      const TypeWithParameters& type_with_params) override;

  absl::Status ApplyTypeParameterConstraints(
      const TypeParameters& type_params, Value* value) override;

  absl::StatusOr<std::unique_ptr<ProcedureDefinition>> LoadProcedure(
      const ScriptExecutor& executor, const absl::Span<const std::string>& path,
      const int64_t num_arguments) override;

  TypeFactory* type_factory() { return type_factory_; }
  const AnalyzerOptions& initial_analyzer_options() const {
    return initial_analyzer_options_;
  }
  const EvaluatorOptions& options() const { return options_; }
  const ParameterValueMap* named_parameters() const {
    return absl::get_if<ParameterValueMap>(&parameters_);
  }
  const ParameterValueList* positional_parameters() const {
    return absl::get_if<ParameterValueList>(&parameters_);
  }
  const absl::variant<ParameterValueList, ParameterValueMap>& parameters()
      const {
    return parameters_;
  }

 private:
  // Represents the evaluation of a single statement or expression, using
  // derived classes StatementEvaluation and ExpressionEvaluation, respectively.
  //
  // Makes the resolved tree available to consumers after an evaluation, even in
  // case of failure, if the evaluation advanced far enough to generate the
  // tree.
  class Evaluation {
   public:
    virtual ~Evaluation() = default;

    TypeFactory* type_factory() const { return evaluator_->type_factory(); }

    // Evaluates a single expression or statement.
    // Stores the analyzer output if the evaluation advances far enough to
    // generate them.
    //
    // Evaluate() may only be called once per Evaluation object.
    absl::Status Evaluate(const ScriptExecutor& script_executor,
                          StatementEvaluatorImpl* evaluator,
                          const ScriptSegment& segment);

   protected:
    // Evaluates the statement or expression of text <sql>, storing the result
    // internally.
    //
    // <catalog> represents an externally-owned MultiCatalog including both
    // user-defined tables and script variables.
    virtual absl::Status EvaluateImpl(
        absl::string_view sql, const AnalyzerOptions& analyzer_options,
        Catalog* catalog, const EvaluatorOptions& evaluator_options,
        const SystemVariableValuesMap& system_variables,
        absl::variant<ParameterValueList, ParameterValueMap> parameters) = 0;

    absl::Status SetTableContents(const Table* table,
                                  const std::vector<std::vector<Value>>& rows) {
      return evaluator_->callback_->SetTableContents(table, rows);
    }

   private:
    absl::Status EvaluateInternal(const ScriptExecutor& script_executor,
                                  StatementEvaluatorImpl* evaluator,
                                  const ScriptSegment& segment);

    // Set during Evaluate().
    StatementEvaluatorImpl* evaluator_ = nullptr;
  };

  class StatementEvaluation : public Evaluation {
   public:
    const ResolvedStatement* resolved_statement() const;
    const Value& result() const { return result_; }
    // Returns the table iterator from the last executed query statement.
    // The returned iterator must not outlive this object.
    std::unique_ptr<EvaluatorTableIterator> get_table_iterator() {
      return std::move(table_iterator_);
    }
    std::unique_ptr<PreparedQuery> get_prepared_query() {
      return std::move(prepared_query_);
    }

   protected:
    absl::Status EvaluateImpl(
        absl::string_view sql, const AnalyzerOptions& analyzer_options,
        Catalog* catalog, const EvaluatorOptions& evaluator_options,
        const SystemVariableValuesMap& system_variables,
        absl::variant<ParameterValueList, ParameterValueMap> parameters)
        override;

   private:
    absl::Status Analyze(absl::string_view sql,
                         const AnalyzerOptions& analyzer_options,
                         Catalog* catalog);

    // Invoked after executing a DML statement; updates the modified table to
    // reflect the result. Returns the total number of rows inserted, modified,
    // or removed.
    absl::StatusOr<int> DoDmlSideEffects(
        EvaluatorTableModifyIterator* iterator);

    // Set from Execute().
    Value result_;

    // Set from Execute(), must be kept alive for table_iterator_ to remain
    // valid.
    std::unique_ptr<PreparedQuery> prepared_query_;

    // Set from Execute().
    std::unique_ptr<EvaluatorTableIterator> table_iterator_;

    std::unique_ptr<const AnalyzerOutput> analyzer_output_;
  };

  class ExpressionEvaluation : public Evaluation {
   public:
    explicit ExpressionEvaluation(const Type* target_type)
        : target_type_(target_type) {}

    const ResolvedExpr* resolved_expr() const;
    const Value& result() const { return result_; }

   protected:
    absl::Status EvaluateImpl(
        absl::string_view sql, const AnalyzerOptions& analyzer_options,
        Catalog* catalog, const EvaluatorOptions& evaluator_options,
        const SystemVariableValuesMap& system_variables,
        absl::variant<ParameterValueList, ParameterValueMap> parameters)
        override;

   private:
    const Type* target_type_;

    std::unique_ptr<const AnalyzerOutput> analyzer_output_;

    // Set from Execute().
    Value result_;
  };

  // AnalyzerOptions, excluding those, such as system variables, that are set by
  // the execution of the script.
  const AnalyzerOptions initial_analyzer_options_;

  EvaluatorOptions options_;
  const absl::variant<ParameterValueList, ParameterValueMap> parameters_;
  TypeFactory* type_factory_;
  Catalog* catalog_;
  StatementEvaluatorCallback* callback_;
};

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_STATEMENT_EVALUATOR_H_
