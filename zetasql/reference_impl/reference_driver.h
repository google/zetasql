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

#ifndef ZETASQL_REFERENCE_IMPL_REFERENCE_DRIVER_H_
#define ZETASQL_REFERENCE_IMPL_REFERENCE_DRIVER_H_

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "google/protobuf/compiler/importer.h"
#include "zetasql/compliance/test_database_catalog.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/rewrite_flags.h"
#include "zetasql/scripting/script_executor.h"
#include "zetasql/scripting/type_aliases.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// Implements the test driver for the reference implementation. It is used in
// compliance tests to ensure the conformance of the reference implementation
// and can be used for comparing the statement results produced by individual
// engines with those produced by the reference implementation.
//
// The reference implementation can run queries as of different ZetaSQL
// language versions or operations, according to LanguageOptions.
class ReferenceDriver : public TestDriver {
 public:
  // Options for ExecuteStatement.
  struct ExecuteStatementOptions {
    PrimaryKeyMode primary_key_mode = PrimaryKeyMode::DEFAULT;
  };

  explicit ReferenceDriver(
      LanguageOptions options = DefaultLanguageOptions(),
      absl::btree_set<ResolvedASTRewrite> enabled_rewrites =
          MinimalRewritesForReference());

  ReferenceDriver(const ReferenceDriver&) = delete;
  ReferenceDriver& operator=(const ReferenceDriver&) = delete;
  ~ReferenceDriver() override;

  // Creates a ReferenceDriver from a TestDriver, matching its options and
  // enabled rewrites.
  static std::unique_ptr<ReferenceDriver> CreateFromTestDriver(
      TestDriver* test_driver);

  // Returns true if the ResolvedAST tree at `root` contains any types not
  // supported by `options`. If `example` is non-null, populate it with a
  // pointer to an example unsupported type.
  static bool UsesUnsupportedType(const LanguageOptions& options,
                                  const ResolvedNode* root,
                                  const Type** example = nullptr);

  LanguageOptions GetSupportedLanguageOptions() override {
    return language_options_;
  }

  // The ReferenceDriver has some extra work to do in addition to the normal
  // TestDriver workflow.
  //
  // The ReferenceDriver is used to produce new tables for other test drivers.
  // Tables are represented as Value objects and the ReferenceDriver executes
  // queries to produce Value objects. Although we can call
  // CreateDatabase(TestDatabase) to create tables on a test driver, we cannot
  // do the same to the ReferenceDriver. This is because
  // CreateDatabase(TestDatabase) will reset the type factory and invalidate
  // all existing Value objects.
  //
  // Instead, the ReferenceDriver needs to add tables incrementally to an
  // existing database.
  //
  // Because tables created by the ReferenceDriver may have proto or enum typed
  // Values, it is convenient to be able to load proto and enum types
  // incrementally, as well.
  //
  // The overall workflow for the ReferenceDriver in terms of method signatures
  // is:
  //   1. CreateDatabase(TestDatabase{}) x 1
  //   2. LoadProtoEnumTypes() x n
  //   3. AddTable() x m

  // Incrementally add a table to bypass resetting type factory.
  void AddTable(const std::string& table_name, const TestTable& table);

  // Incrementally loads proto and enum types.
  absl::Status LoadProtoEnumTypes(const std::set<std::string>& filenames,
                                  const std::set<std::string>& proto_names,
                                  const std::set<std::string>& enum_names);

  // Must be called prior to ExecuteQuery().
  absl::Status CreateDatabase(const TestDatabase& test_db) override;

  // Set the current LanguageOptions, which will control what features and
  // functions are available and how they behave.
  // This can be called between ExecuteQuery calls to change options.
  void SetLanguageOptions(const LanguageOptions& options);

  // Adds some SQL UDFs to the catalog owned by this test driver. The argument
  // is a collection of "CREATE TEMP FUNCTION" statements.
  absl::Status AddSqlUdfs(
      absl::Span<const std::string> create_function_stmts) override;
  // A reference-driver specific overload of AddSqlUdfs that also takes a
  // FunctionOptions. Even though most function options cannot be controlled
  // through a CREATE FUNCTION statement, this driver is used for the query
  // generator. We supply FunctionOptions to affect the RQG behavior.
  absl::Status AddSqlUdfs(absl::Span<const std::string> create_function_stmts,
                          FunctionOptions function_options);

  // Adds some viewss to the catalog owned by this test driver. The argument
  // is a collection of "CREATE TEMP VIEW" statements.
  absl::Status AddViews(
      absl::Span<const std::string> create_view_stmts) override;

  // Implements TestDriver::ExecuteStatement()
  absl::StatusOr<Value> ExecuteStatement(
      const std::string& sql, const std::map<std::string, Value>& parameters,
      TypeFactory* type_factory) override;

  // Implements TestDriver::ExecuteScript(), which documents that this method
  // is not supposed be called because IsReferenceImplementation() returns true.
  absl::StatusOr<ScriptResult> ExecuteScript(
      const std::string& sql, const std::map<std::string, Value>& parameters,
      TypeFactory* type_factory) override {
    return zetasql_base::InternalErrorBuilder()
           << "ExecuteScript() is not supported for the reference "
           << "implementation; call  "
           << "ReferenceDriver::ExecuteScriptForReferenceDriver() instead";
  }

  struct ExecuteStatementAuxOutput {
    // If this has a value, it indicates whether the reference evaluation
    // engine detected non-determinism.
    std::optional<bool> is_deterministic_output;
    // If this has a value, it indicates the statement included unsupported
    // types.  This will generally cause a failure of the query.
    std::optional<bool> uses_unsupported_type;
    // If a new table is created via a DDL statement (see restrictions above)
    // this will contain the name of the created table.
    std::optional<std::string> created_table_name;
    // If set, will contain the runtime_info extracted from AnalyzerOutput.
    std::optional<AnalyzerRuntimeInfo> analyzer_runtime_info;
  };

  // The same as TestDriver::ExecuteStatement(), but with more arguments. Uses
  // INVALID_ARGUMENT errors to represent parser/analyzer errors and
  // OUT_OF_RANGE to represent runtime errors.
  //
  // DDL is supported only if 'database' is not null, and only for a limited
  // set of statement types (currently CREATE TABLE AS (...)). Executing a
  // DDL statement modifies 'database' to reflect the change and returns a
  // value representing the contents of the new table.
  //
  // 'aux_output' contains additional information. These values may provided
  // even in the cause of failures in some cases.
  absl::StatusOr<Value> ExecuteStatementForReferenceDriver(
      absl::string_view sql, const std::map<std::string, Value>& parameters,
      const ExecuteStatementOptions& options, TypeFactory* type_factory,
      ExecuteStatementAuxOutput& aux_output, TestDatabase* database = nullptr);

  // The same as ExecuteStatementForReferenceDriver(), except executes a script
  // instead of a statement.
  absl::StatusOr<ScriptResult> ExecuteScriptForReferenceDriver(
      absl::string_view sql, const std::map<std::string, Value>& parameters,
      const ExecuteStatementOptions& options, TypeFactory* type_factory,
      bool* uses_unsupported_type);

  bool IsReferenceImplementation() const override { return true; }

  // Sets a new query evalution duration that is less than
  // --reference_driver_query_eval_timeout_sec or returns an error.
  absl::Status SetStatementEvaluationTimeout(absl::Duration timeout) override;

  // Returns a pointer to the owned catalog.
  SimpleCatalog* catalog() const { return catalog_.catalog(); }

  // Returns a pointer to the owned reference type factory.
  TypeFactory* type_factory() { return type_factory_.get(); }

  const absl::TimeZone GetDefaultTimeZone() const override;
  absl::Status SetDefaultTimeZone(const std::string& time_zone) override;

  LanguageOptions language_options() { return language_options_; }

  absl::StatusOr<std::vector<Value>> RepeatExecuteStatement(
      const std::string& sql, const std::map<std::string, Value>& parameters,
      TypeFactory* type_factory, uint64_t times) override;

  virtual absl::StatusOr<AnalyzerOptions> GetAnalyzerOptions(
      const std::map<std::string, Value>& parameters,
      std::optional<bool>& uses_unsupported_type) const;

  const absl::btree_set<ResolvedASTRewrite>& enabled_rewrites() const {
    return enabled_rewrites_;
  }

  // The LanguageOptions used by the zero-arg constructor.
  static LanguageOptions DefaultLanguageOptions();

 private:
  struct TableInfo {
    std::string table_name;
    std::set<LanguageFeature> required_features;
    bool is_value_table;
    Value array;
    SimpleTable* table;  // Owned by catalog_ in the ReferenceDriver
  };

  absl::Status ExecuteScriptForReferenceDriverInternal(
      absl::string_view sql, const std::map<std::string, Value>& parameters,
      const ExecuteStatementOptions& options, TypeFactory* type_factory,
      bool* uses_unsupported_type, ScriptResult* result);

  absl::StatusOr<Value> ExecuteStatementForReferenceDriverInternal(
      absl::string_view sql, const AnalyzerOptions& analyzer_options,
      const std::map<std::string, Value>& parameters,
      const VariableMap& script_variables,
      const SystemVariableValuesMap& system_variables,
      const ExecuteStatementOptions& options, TypeFactory* type_factory,
      TestDatabase* database,
      // If provide, uses this instead of calling analyzer.
      const AnalyzerOutput* analyzed_input,
      ExecuteStatementAuxOutput& aux_output);

  virtual absl::StatusOr<std::unique_ptr<const AnalyzerOutput>>
  AnalyzeStatement(absl::string_view sql, TypeFactory* type_factory,
                   const std::map<std::string, Value>& parameters,
                   Catalog* catalog, const AnalyzerOptions& analyzer_options);

  void AddTableInternal(const std::string& table_name, const TestTable& table);

  friend class ReferenceDriverStatementEvaluator;
  std::unique_ptr<TypeFactory> type_factory_;
  LanguageOptions language_options_;
  absl::btree_set<ResolvedASTRewrite> enabled_rewrites_;
  std::vector<TableInfo> tables_;

  // Procedures created inside the current script. Reset at the start of each
  // script so that procedures cannot leak across testcase boundaries.
  // In the key, all names are lowercase.
  absl::flat_hash_map<std::vector<std::string>,
                      std::unique_ptr<ProcedureDefinition>>
      procedures_;
  TestDatabaseCatalog catalog_;

  // Maintains lifetime of objects referenced by SQL UDFs added to catalog_.
  std::vector<std::unique_ptr<const AnalyzerOutput>> sql_udf_artifacts_;

  // Defaults to America/Los_Angeles.
  absl::TimeZone default_time_zone_;
  absl::Duration statement_evaluation_timeout_;

  // The name of dumping catalog for fuzz testing.
  std::string fuzzing_catalog_name_;
};

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_REFERENCE_DRIVER_H_
