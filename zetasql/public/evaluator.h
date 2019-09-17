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

#ifndef ZETASQL_PUBLIC_EVALUATOR_H_
#define ZETASQL_PUBLIC_EVALUATOR_H_

// ZetaSQL in-memory expression or query evaluation using the reference
// implementation.
//
// Evaluating Expressions
// ----------------------
//
// When evaluating an expression, callers can provide
//   - A set of expression columns - column names usable in the expression.
//   - Optionally, one in-scope expression column - a column, possibly named,
//        that is implicitly in scope when evaluating the expression, so its
//        fields can be accessed directly without any qualifiers.
//   - A set of parameters - parameters usable in the expression as @param.
//
// Examples:
//
//   PreparedExpression expr("1 + 2");
//   Value result = expr.Execute().ValueOrDie();  // Value::Int64(3)
//
//   PreparedExpression expr("(@param1 + @param2) * col");
//   Value result = expr.Execute(
//     {{"col", Value::Int64(5)}},
//     {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)}}).ValueOrDie();
//   // result = Value::Int64(15)
//
// The above expression could also be set up as follows:
//
//   PreparedExpression expr("(@param1 + @param2) * col");
//   AnalyzerOptions options;
//   ZETASQL_CHECK_OK(options.AddExpressionColumn("col", types::Int64Type()));
//   ZETASQL_CHECK_OK(options.AddQueryParameter("param1", types::Int64Type()));
//   ZETASQL_CHECK_OK(options.AddQueryParameter("param2", types::Int64Type()));
//   ZETASQL_CHECK_OK(expr.Prepare(options));
//   CHECK(types::Int64Type()->Equals(expr.output_type()));
//   Value result = expr.Execute(
//     {{"col", Value::Int64(5)}},
//     {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)}}).ValueOrDie();
//
// An in-scope expression column can be used as follows:
//
//   TypeFactory type_factory;  // NOTE: Must outlive the PreparedExpression.
//   const ProtoType* proto_type;
//   ZETASQL_CHECK_OK(type_factory.MakeProtoType(MyProto::descriptor(), &proto_type));
//
//   AnalyzerOptions options;
//   ZETASQL_CHECK_OK(options.SetInScopeExpressionColumn("value", proto_type));
//
//   PreparedExpression expr("field1 + value.field2");
//   ZETASQL_CHECK_OK(expr.Prepare(options));
//
//   Value result = expr.Execute(
//     {{"value", values::Proto(proto_type, my_proto_value)}}).ValueOrDie();
//
// User-defined functions can be used in expressions as follows:
//
//   FunctionOptions function_options;
//   function_options.set_evaluator(
//       [](const absl::Span<const Value>& args) {
//         // Returns std::string length as int64_t.
//         DCHECK_EQ(args.size(), 1);
//         DCHECK(args[0].type()->Equals(zetasql::types::StringType()));
//         return Value::Int64(args[0].string_value().size());
//       });
//
//   AnalyzerOptions options;
//   SimpleCatalog catalog{"udf_catalog"};
//   catalog.AddZetaSQLFunctions(options.language_options());
//   catalog.AddOwnedFunction(new Function(
//       "MyStrLen", "udf", zetasql::Function::SCALAR,
//       {{zetasql::types::Int64Type(), {zetasql::types::StringType()},
//         5000}},  // some function id
//       function_options));
//
//   PreparedExpression expr("1 + mystrlen('foo')");
//   ZETASQL_CHECK_OK(expr.Prepare(options, &catalog));
//   Value result = expr.Execute().ValueOrDie();  // returns 4
//
// For more examples, see zetasql/common/evaluator_test.cc
//
// Parameters are passed as a map of strings to zetasql::Value. Multiple
// invocations of Execute() must use identical parameter names and types.
//
// The values returned by the Execute() method must not be accessed after
// destroying PreparedExpression that returned them.
//
// Thread safety: PreparedExpression is thread-safe. The recommended way to use
// PreparedExpression from multiple threads is to call Prepare() once and then
// call ExecuteAfterPrepare() in parallel from multiple threads for concurrent
// evaluations. (It is also possible to call Execute() in parallel multiple
// times, but in that case, each of the executions have to consider whether to
// call Prepare(), and there is some serialization there.)
//
// Evaluating Queries
// ------------------
// Queries can be evaluated using PreparedQuery.  This works similarly to
// PreparedExpression as described above, including the support for
// parameters and user-defined functions.
//
// User-defined tables can be used in queries (and even expressions) by
// implementing the EvaluatorTableIterator interface documented in
// evaluator_table_iter.h as follows:
//
//   std::unique_ptr<Table> table =
//   ... Create a Table that overrides
//       Table::CreateEvaluatorTableIterator() ...
//   SimpleCatalog catalog;
//   catalog.AddTable(table->Name(), table.get());
//   PreparedQuery query("select * from <table>");
//   ZETASQL_CHECK_OK(query.Prepare(AnalyzerOptions(), &catalog));
//   std::unique_ptr<EvaluatorTableIterator> result =
//     query.Execute().ValueOrDie();
//   ... Iterate over 'result' ...
//
// Once a query is successfully prepared, the output schema can be retrieved
// using num_columns(), column_name(), column_type(), etc.  After Execute(), the
// schema is also available from the EvaluatorTableIterator.

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"
#include "zetasql/base/clock.h"

namespace zetasql {

class EvaluationContext;
class ResolvedExpr;
class ResolvedQueryStmt;

using ParameterValueMap = std::map<std::string, Value>;
using ParameterValueList = std::vector<Value>;

namespace internal {
class Evaluator;
}  // namespace internal

struct EvaluatorOptions {
 public:
  // If 'type_factory' is provided, the return value's Type will
  // be allocated from 'type_factory'.  Otherwise, the returned Value is
  // allocated using an internal TypeFactory and is only valid for the lifetime
  // of the PreparedExpression.
  // Does not take ownership.
  TypeFactory* type_factory = nullptr;

  // Functions which rely on the current timestamp/date will be evaluated based
  // on the time returned by this clock. By default this is the normal system
  // clock but it can (optionally) be overridden.
  // Does not take ownership.
  zetasql_base::Clock* clock = zetasql_base::Clock::RealClock();

  // The default time zone to use. If not set, the default time zone is
  // "America/Los_Angeles".
  absl::optional<absl::TimeZone> default_time_zone;

  // If true, evaluation will scramble the order of relations whose order is not
  // defined by ZetaSQL. This requires some extra processing, so should only
  // be enabled in tests.
  bool scramble_undefined_orderings = false;

  // Limit on the maximum number of in-memory bytes used by an individual Value
  // that is constructed during evaluation. This bound applies to all Value
  // types, including variable-sized types like STRING, BYTES, ARRAY, and
  // STRUCT. Exceeding this limit results in an error. See the implementation of
  // Value::physical_byte_size for more details.
  int64_t max_value_byte_size = 1024 * 1024;

  // The limit on the maximum number of in-memory bytes that can be used for
  // storing accumulated rows (e.g., during an ORDER BY query). Exceeding this
  // limit results in an error.
  //
  // This is not a hard bound on total memory usage. Some additional memory will
  // be used proportional to query complexity, individual row sizes, etc. This
  // limit bounds memory that can be used by all operators that buffer multiple
  // rows of data (which could otherwise use unbounded memory, depending on
  // data size).
  //
  // It is also possible for the memory accounting to overestimate the amount of
  // memory being used. One way this can happen is if Values are copied between
  // rows during a query (e.g., while evaluating an array join). Large Values
  // use reference counting to share the same underlying memory, but the memory
  // accounting charges each of them individually. In some cases, it is
  // necessary to set this option to a very large value.
  int64_t max_intermediate_byte_size = 128 * 1024 * 1024;
};

class PreparedExpression {
 public:
  // Legacy constructor.
  // Prefer using the constructor which takes EvaluatorOptions (below).
  //
  // If 'type_factory' is provided, the return value's Type will
  // be allocated from 'type_factory'.  Otherwise, the returned Value is
  // allocated using an internal TypeFactory and is only valid for the lifetime
  // of the PreparedExpression.
  explicit PreparedExpression(const std::string& sql,
                              TypeFactory* type_factory = nullptr);

  // Constructor. Additional options can be provided by filling out the
  // EvaluatorOptions struct.
  PreparedExpression(const std::string& sql, const EvaluatorOptions& options);

  // Constructs a PreparedExpression using a ResolvedExpr directly. Does not
  // take ownership of <expression>. <expression> must outlive this.
  //
  // This is useful if you have an expression from some other source than
  // directly from raw SQL. For example, if you serialized a ResolvedExpr and
  // sent it to a server for execution, you could pass the deserialized node
  // directly to this, without first converting it to SQL.
  //
  // The AST must validate successfully with ValidateStandaloneResolvedExpr.
  // Otherwise, the program may crash in Prepare or Execute.
  PreparedExpression(const ResolvedExpr* expression,
                     const EvaluatorOptions& options);
  PreparedExpression(const PreparedExpression&) = delete;
  PreparedExpression& operator=(const PreparedExpression&) = delete;

  ~PreparedExpression();

  // This method can optionally be called before Execute() to set analyzer
  // options and to return parsing and analysis errors, if any. If Prepare() is
  // used, the names and types of query parameters and expression columns must
  // be set in 'options'. (We also force 'options.prune_unused_columns' since
  // that would ideally be the default.)
  //
  // If 'catalog' is set, it will be used to resolve tables and functions
  // occurring in the expression. Passing a custom 'catalog' allows defining
  // user-defined functions with custom evaluation specified via
  // FunctionOptions, as well as user-defined tables (see the file comment for
  // details). Calling any user-defined function that does not provide an
  // evaluator returns an error. 'catalog' must outlive Execute() and
  // output_type() calls.  'catalog' should contain ZetaSQL built-in functions
  // added by calling AddZetaSQLFunctions with 'options.language_options'.
  zetasql_base::Status Prepare(const AnalyzerOptions& options,
                       Catalog* catalog = nullptr);

  // Get the list of column names referenced in this expression. The columns
  // will be returned in lower case, as column expressions are case-insensitive
  // when evaluated.
  //
  // This can be used for efficiency, in the case where there are
  // many possible columns in a datastore but only a small fraction
  // of them are referenced in a query. The list of columns returned
  // from this method is the minimal set that must be provided to
  // Execute().
  //
  // Example:
  //   PreparedExpression expr("col > 1");
  //   options.AddExpressionColumn("col", types::Int64Type());
  //   options.AddExpressionColumn("extra_col", types::Int64Type());
  //   ZETASQL_CHECK_OK(expr.Prepare(options, &catalog));
  //   ...
  //   const std::vector<std::string> columns =
  //              expr.GetReferencedColumns().ConsumeValueOrDie();
  //   ParameterValueMap col_map;
  //   for (const std::string& col : columns) {
  //     col_map[col] = datastore.GetValueForColumn(col);
  //   }
  //   auto result = expr.Execute(col_map);
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<std::vector<std::string>> GetReferencedColumns() const;

  // Get the list of parameters referenced in this expression.
  //
  // This method is similar to GetReferencedColumns(), but for parameters
  // instead. This returns the minimal set of parameters that must be provided
  // to Execute(). Named and positional parameters are mutually exclusive, so
  // this will return an empty list if GetPositionalParameterCount() returns a
  // non-zero number.
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<std::vector<std::string>> GetReferencedParameters() const;

  // Gets the number of positional parameters in this expression.
  //
  // This returns the number of positional parameters that must be provided to
  // Execute(). Any extra positional parameters are ignored. Named and
  // positional parameters are mutually exclusive, so this will return 0 if
  // GetReferencedParameters() returns a non-empty list.
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<int> GetPositionalParameterCount() const;

  // Execute the expression.
  //
  // If Prepare has not been called, the first call to Execute will call
  // Prepare with implicitly constructed AnalyzerOptions using the
  // names and types from <columns> and <parameters>.
  //
  // If using an in-scope expression column, <columns> should have an entry
  // storing the Value for that column with its registered name (possibly "").
  // For the implicit Prepare case, an entry in <columns> with an empty
  // name will be treated as an anonymous in-scope expression column.
  //
  // NOTE: The returned Value is only valid for the lifetime of this
  // PreparedExpression unless an external TypeFactory was passed to the
  // constructor.
  zetasql_base::StatusOr<Value> Execute(
      const ParameterValueMap& columns = {},
      const ParameterValueMap& parameters = {});

  // Same as above, but uses positional instead of named parameters.
  zetasql_base::StatusOr<Value> ExecuteWithPositionalParams(
      const ParameterValueMap& columns,
      const ParameterValueList& positional_parameters);

  // This is the same as Execute, but is a const method, and requires that
  // Prepare has already been called. See the description of Execute for details
  // about the arguments and return value.
  //
  // Thread safe. Multiple evaluations can proceed in parallel.
  zetasql_base::StatusOr<Value> ExecuteAfterPrepare(
      const ParameterValueMap& columns = {},
      const ParameterValueMap& parameters = {}) const;

  // Same as above, but uses positional instead of named parameters.
  zetasql_base::StatusOr<Value> ExecuteAfterPrepareWithPositionalParams(
      const ParameterValueMap& columns,
      const ParameterValueList& positional_parameters) const;

  // More efficient form of Execute that requires column and parameter values to
  // be passed in a particular order. It is intended for users that want to
  // repeatedly evaluate an expression with different values of (hardcoded)
  // parameters. In that case, it is more efficient than the other forms of
  // Execute(), whose implementations involve map operations on column and/or
  // named query parameters. <columns> must be in the order returned by
  // GetReferencedColumns. If positional parameters are used, they are passed in
  // <parameters>. If named parameters are used, they are passed in <parameters>
  // in the order returned by GetReferencedParameters.
  //
  // REQUIRES: Prepare() has been called successfully.
  zetasql_base::StatusOr<Value> ExecuteAfterPrepareWithOrderedParams(
      const ParameterValueList& columns,
      const ParameterValueList& parameters) const;

  // Returns a human-readable representation of how this expression would
  // actually be executed. Do not try to interpret this std::string with code, as the
  // format can change at any time. Requires that Prepare has already been
  // called.
  zetasql_base::StatusOr<std::string> ExplainAfterPrepare() const;

  // REQUIRES: Prepare() or Execute() must be called first.
  const Type* output_type() const;

 private:
  std::unique_ptr<internal::Evaluator> evaluator_;
};

class PreparedQuery {
 public:
  // Constructor. Additional options can be provided by filling out the
  // EvaluatorOptions struct.
  PreparedQuery(const std::string& sql, const EvaluatorOptions& options);

  // Constructs a PreparedQuery using a ResolvedQueryStmt directly. Does not
  // take ownership of <stmt>. <stmt> must outlive this object.
  //
  // This is useful if you have a query from some other source than directly
  // from raw SQL. For example, if you serialized a ResolvedQueryStmt and sent
  // it to a server for execution, you could pass the deserialized node directly
  // to this, without first converting it to SQL.
  //
  // The AST must validate successfully with ValidateResolvedStatement.
  // Otherwise, the program may crash in Prepare or Execute.
  PreparedQuery(const ResolvedQueryStmt* stmt, const EvaluatorOptions& options);

  PreparedQuery(const PreparedQuery&) = delete;
  PreparedQuery& operator=(const PreparedQuery&) = delete;

  // Crashes if any iterator returned by Execute() has not yet been destroyed.
  ~PreparedQuery();

  // This method can optionally be called before Execute() to set analyzer
  // options and to return parsing and analysis errors, if any. If Prepare() is
  // used, the names and types of query parameters must be set in 'options'. (We
  // also force 'options.prune_unused_columns' since that would ideally be the
  // default.)
  //
  // If 'catalog' is set, it will be used to resolve tables and functions
  // occurring in the query. Passing a custom 'catalog' allows defining
  // user-defined functions with custom evaluation specified via
  // FunctionOptions, as well as user-defined tables (see the file comment for
  // details). Calling any user-defined function that does not provide an
  // evaluator returns an error. 'catalog' must outlive Execute() and
  // output_type() calls.  'catalog' should contain ZetaSQL built-in functions
  // added by calling AddZetaSQLFunctions with 'options.language_options'.
  zetasql_base::Status Prepare(const AnalyzerOptions& options,
                       Catalog* catalog = nullptr);

  // Get the list of parameters referenced in this query.
  //
  // This returns the minimal set of parameters that must be provided
  // to Execute().
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<std::vector<std::string>> GetReferencedParameters() const;

  // Gets the number of positional parameters in this query.
  //
  // This returns the exact number of positional parameters that must be
  // provided to Execute(). Named and positional parameters are mutually
  // exclusive, so this will return 0 if GetReferencedParameters() returns a
  // non-empty list.
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  zetasql_base::StatusOr<int> GetPositionalParameterCount() const;

  // Execute the query. This object must outlive the return value.
  //
  // If Prepare() has not been called, the first call to Execute will call
  // Prepare with implicitly constructed AnalyzerOptions using the
  // names and types from <parameters>.
  //
  // This method is thread safe. Multiple executions can proceed in parallel,
  // each using a different iterator.
  zetasql_base::StatusOr<std::unique_ptr<EvaluatorTableIterator>> Execute(
      const ParameterValueMap& parameters = {});

  // Same as above, but uses positional instead of named parameters.
  zetasql_base::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  ExecuteWithPositionalParams(const ParameterValueList& positional_parameters);

  // More efficient form of Execute that requires parameter values to be passed
  // in a particular order. If positional parameters are used, they are passed
  // in <parameters>. If named parameters are used, they are passed in
  // <parameters> in the order returned by GetReferencedParameters.
  //
  // REQUIRES: Prepare() has been called successfully.
  zetasql_base::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  ExecuteAfterPrepareWithOrderedParams(
      const ParameterValueList& parameters) const;

  // Returns a human-readable representation of how this query would actually
  // be executed. Do not try to interpret this std::string with code, as the
  // format can change at any time. Requires that Prepare has already been
  // called.
  zetasql_base::StatusOr<std::string> ExplainAfterPrepare() const;

  // Get the schema of the output table of this query. Anonymous column names
  // are empty. (There may be more than one column with the same name.)
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  //
  // NOTE: The returned Types are only valid for the lifetime of this
  // PreparedQuery unless an external TypeFactory was passed to the
  // constructor.
  int num_columns() const;
  std::string column_name(int i) const;
  const Type* column_type(int i) const;
  using NameAndType = std::pair<std::string, const Type*>;
  std::vector<NameAndType> GetColumns() const;

  // Returns whether the output is a value table.
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  bool is_value_table() const {
    return resolved_query_stmt()->is_value_table();
  }

  // Get the ResolvedQueryStmt for this query.
  //
  // REQUIRES: Prepare() or Execute() has been called successfully.
  const ResolvedQueryStmt* resolved_query_stmt() const;

 private:
  friend class PreparedQueryTest;

  // For unit tests, it is possible to set a callback that is invoked every time
  // an EvaluationContext is created.
  void SetCreateEvaluationCallbackTestOnly(
      std::function<void(EvaluationContext*)> cb);

  std::unique_ptr<internal::Evaluator> evaluator_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_EVALUATOR_H_
