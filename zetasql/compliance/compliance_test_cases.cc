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

#include "zetasql/compliance/compliance_test_cases.h"

#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/protobuf/message.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/compliance/sql_test_base.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/reference_driver.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/type_util.h"
#include "zetasql/testing/using_test_value.cc"  // NOLINT
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include <cstdint>
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/file_util.h"
#include "zetasql/base/map_util.h"

ABSL_FLAG(std::string, file_pattern, "*.test", "File pattern for test files.");

// Two flags to help engines control their sharding of compliance test cases.
// When codebased tests are disabled, we do not need to set up the test
// environment. When file based tests are disabled, we do not need to try
// reading any files.
ABSL_FLAG(bool, zetasql_run_codebased_tests, true,
          "Run the code based compliance tests.");
ABSL_FLAG(bool, zetasql_run_filebased_tests, true,
          "Run the file based compliance tests.");

using zetasql::test_values::kIgnoresOrder;

namespace zetasql {

const char kAnonymousColumnName[] = "";
const char kColA[] = "ColA";

// Column names for test tables.
const char kPrimaryKey[] = "primary_key";
const char kBoolColumnName[] = "bool_val";
const char kDoubleColumnName[] = "double_val";
const char kInt64ColumnName[] = "int64_val";
const char kStringColumnName[] = "str_val";

// The test environment will be initialized by SetUpTestCase of the first test
// class that uses it. We do not set up before that point because it will
// conflict with engine-specific test environments that must be set up before
// the driver may be accessed.
static CodebasedTestsEnvironment* GetCodeBasedTestsEnvironment() {
  static CodebasedTestsEnvironment* env = []() {
    CodebasedTestsEnvironment* code_based_tests_environment =
        new CodebasedTestsEnvironment;
    code_based_tests_environment->SetUp();
    // Register the global environment. Even though it is too late to get auto
    // SetUp, we still want to trigger auto TearDown.
    ::testing::AddGlobalTestEnvironment(code_based_tests_environment);
    return code_based_tests_environment;
  }();
  return env;
}

static Value MakeProtoValue(const google::protobuf::Message* msg) {
  const ProtoType* proto_type;
  ZETASQL_CHECK_OK(test_values::static_type_factory()->MakeProtoType(
      msg->GetDescriptor(), &proto_type));
  return Value::Proto(proto_type, SerializeToCord(*msg));
}

static std::string ParametersWithSeparator(int num_parameters,
                                           const std::string& separator) {
  std::vector<std::string> arg_str;
  arg_str.reserve(num_parameters);
  for (int i = 0; i < num_parameters; i++) {
    arg_str.push_back(absl::StrCat("@p", i));
  }
  return absl::StrJoin(arg_str, separator);
}

static Value Singleton(const ValueConstructor& v) {
  return StructArray({kColA}, {{v}});
}

static void ConvertResultsToSingletons(QueryParamsWithResult* param) {
  QueryParamsWithResult::ResultMap new_result_map = param->results();
  for (auto& pair : new_result_map) {
    Value& result = pair.second.result;
    result = Singleton(result);
  }
  param->set_results(new_result_map);
}

// Make a label "type_<kind_name>" based on <type>.
// e.g. type_INT64, type_DATE, type_PROTO, type_ENUM.
static std::string MakeLabelForType(const Type* type) {
  return absl::StrCat("type_",
                      Type::TypeKindToString(type->kind(), PRODUCT_INTERNAL));
}

// Get type labels from MakeLabelForType for the parameters and return value
// specified in <query_params_with_result>.
static std::vector<std::string> GetTypeLabels(
    const QueryParamsWithResult& query_params_with_result) {
  std::vector<std::string> labels;
  for (const Value& param : query_params_with_result.params()) {
    labels.push_back(MakeLabelForType(param.type()));
  }
  labels.push_back(MakeLabelForType(query_params_with_result.GetResultType()));
  return labels;
}

// Returns a SQL literal for the value.
static std::string MakeLiteral(const Value& value) {
  ZETASQL_LOG_IF(FATAL, value.is_null()) << "Null value " << value.DebugString();

  switch (value.type_kind()) {
    case TYPE_STRING:
      return ToStringLiteral(value.string_value());
    case TYPE_BYTES:
      return ToBytesLiteral(value.bytes_value());
    default:
      ZETASQL_LOG(FATAL) << "Not supported type " << value.DebugString();
  }
}

std::vector<FunctionTestCall> WrapFeatureAdditionalStringFunctions(
    const std::vector<FunctionTestCall>& tests) {
  std::vector<FunctionTestCall> wrapped_tests;
  for (auto call : tests) {
    call.params =
        call.params.WrapWithFeature(FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS);
    wrapped_tests.emplace_back(call);
  }
  return wrapped_tests;
}

std::vector<FunctionTestCall> WrapFeatureLastDay(
    const std::vector<FunctionTestCall>& tests) {
  std::vector<FunctionTestCall> wrapped_tests;
  for (auto call : tests) {
    QueryParamsWithResult::FeatureSet feature_set = {
      FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS, FEATURE_V_1_2_CIVIL_TIME};
    functions::DateTimestampPart date_part =
        static_cast<functions::DateTimestampPart>
        (call.params.param(1).enum_value());
    switch (date_part) {
      case functions::WEEK_MONDAY:
      case functions::WEEK_TUESDAY:
      case functions::WEEK_WEDNESDAY:
      case functions::WEEK_THURSDAY:
      case functions::WEEK_FRIDAY:
      case functions::WEEK_SATURDAY:
        feature_set.insert(FEATURE_V_1_2_WEEK_WITH_WEEKDAY);
        break;
      default:
        break;
    }

    call.params = call.params.WrapWithFeatureSet(feature_set);
    wrapped_tests.emplace_back(call);
  }
  return wrapped_tests;
}

static std::vector<QueryParamsWithResult> WrapFeatureJSON(
    const std::vector<QueryParamsWithResult>& tests) {
  std::vector<QueryParamsWithResult> wrapped_tests;
  wrapped_tests.reserve(tests.size());
  for (auto& test_case : tests) {
    wrapped_tests.emplace_back(test_case.WrapWithFeature(FEATURE_JSON_TYPE));
  }
  return wrapped_tests;
}

// Owned by
// CodebasedTestsEnvironment::{SetUp,TearDown}(). 'code_based_reference_driver'
// is NULL if and only if either 'code_based_test_driver' is NULL, or
// 'code_based_test_driver->IsReferenceImplementation()' is true.
static TestDriver* code_based_test_driver = nullptr;
static ReferenceDriver* code_based_reference_driver = nullptr;

bool CodebasedTestsEnvironment::skip_codebased_tests() {
  return !absl::GetFlag(FLAGS_zetasql_run_codebased_tests);
}

// The compliance code-based tests operate in two distinct modes:
// 1) testing the reference driver
// 2) testing an engine driver
//
// When testing the reference driver, the statements are executed
// against the reference driver and the result is compared against
// the test case result.
//
// When testing an engine driver, the statements are executed against
// both the engine driver and the reference driver, and the
// results from the two drivers are compared.

// static
void ComplianceCodebasedTests::SetUpTestSuite() {
  ZETASQL_CHECK(GetCodeBasedTestsEnvironment() != nullptr);
}

void CodebasedTestsEnvironment::SetUp() {
  // The advantage of skipping codebased tests is to skip this potentially
  // very expensive setup.
  if (skip_codebased_tests()) {
    ZETASQL_LOG(INFO) << "Skipping codebased tests.";
    return;
  }
  ZETASQL_LOG(INFO) << "Setting up codebased tests environment.";
  // Create a test driver, reference driver (if necessary), and
  // default test database to use for all the code-based tests.
  Value table_empty;
  Value table1;
  Value table2;
  Value table3;
  Value table_large;
  Value table_all_null;
  Value table_distincts;
  std::vector<std::string> table_columns = {
      "primary_key", "bool_val", "double_val", "int64_val", "str_val"};
  std::vector<const Type*> table_column_types = {
      Int64Type(), BoolType(), DoubleType(), Int64Type(), StringType()};
  std::vector<std::string> primary_key = {kPrimaryKey};
  std::vector<std::string> bool_column = {kBoolColumnName};
  std::vector<std::string> double_column = {kDoubleColumnName};
  std::vector<std::string> int64_column = {kInt64ColumnName};
  std::vector<std::string> string_column = {kStringColumnName};
  // Create an empty test / result table.
  std::vector<StructType::StructField> fields;
  fields.reserve(table_columns.size());
  for (int i = 0; i < table_columns.size(); ++i) {
    fields.emplace_back(
        StructType::StructField(table_columns[i], table_column_types[i]));
  }
  const StructType* table_struct = MakeStructType(fields);
  const ArrayType* array_type = MakeArrayType(table_struct);
  table_empty = Value::EmptyArray(array_type);

  // Create non-empty test / result tables.
  table1 = StructArray(table_columns, {
      {1ll, True(),  0.1, 1ll, "1"},
      {2ll, False(), 0.2, 2ll, "2"}, }, kIgnoresOrder);
  table2 = StructArray(table_columns, {
      {1ll, True(),  0.3, 3ll, "3"},
      {2ll, False(), 0.4, 4ll, "4"}, }, kIgnoresOrder);
  table3 = StructArray(table_columns, {
      {1ll, True(),  0.5, 5ll, "5"},
      {2ll, False(), 0.6, 6ll, "6"}, }, kIgnoresOrder);
  table_large = StructArray(table_columns, {
      {1ll, NullBool(), NullDouble(), NullInt64(), NullString()},
      {2ll, True(),  NullDouble(), NullInt64(), NullString()},
      {3ll, False(), 0.2, NullInt64(), NullString()},
      {4ll, True(),  0.3, 3ll, NullString()},
      {5ll, False(), 0.4, 4ll, "4"},
      {6ll, True(),  0.5, 5ll, "5"},
      {7ll, False(), 0.6, 6ll, "6"},
      {8ll, True(),  0.7, 7ll, "7"},
      {9ll, False(), 0.8, 8ll, "8"},
      {10ll, True(),  0.9, 9ll, "9"},
      {11ll, False(), 1.0, 10ll, "10"}, }, kIgnoresOrder);
  table_all_null = StructArray(
      table_columns,
      {{1ll, NullBool(), NullDouble(), NullInt64(), NullString()}, },
      kIgnoresOrder);
  table_distincts = StructArray(
      {"primary_key", "distinct_1", "distinct_2", "distinct_4", "distinct_8",
            "distinct_16", "distinct_2B", "distinct_4B"}, {
        {1ll, 1ll, 1ll, 1ll, 1ll, 1ll,  /**/ 1ll, 1ll},
        {2ll, 1ll, 2ll, 2ll, 2ll, 2ll,  /**/ 1ll, 1ll},
        {3ll, 1ll, 1ll, 3ll, 3ll, 3ll,  /**/ 1ll, 1ll},
        {4ll, 1ll, 2ll, 4ll, 4ll, 4ll,  /**/ 1ll, 1ll},
        {5ll, 1ll, 1ll, 1ll, 5ll, 5ll,  /**/ 1ll, 2ll},
        {6ll, 1ll, 2ll, 2ll, 6ll, 6ll,  /**/ 1ll, 2ll},
        {7ll, 1ll, 1ll, 3ll, 7ll, 7ll,  /**/ 1ll, 2ll},
        {8ll, 1ll, 2ll, 4ll, 8ll, 8ll,  /**/ 1ll, 2ll},
        {9ll, 1ll, 1ll, 1ll, 1ll, 9ll,  /**/ 2ll, 3ll},
        {10ll, 1ll, 2ll, 2ll, 2ll, 10ll, /**/ 2ll, 3ll},
        {11ll, 1ll, 1ll, 3ll, 3ll, 11ll, /**/ 2ll, 3ll},
        {12ll, 1ll, 2ll, 4ll, 4ll, 12ll, /**/ 2ll, 3ll},
        {13ll, 1ll, 1ll, 1ll, 5ll, 13ll, /**/ 2ll, 4ll},
        {14ll, 1ll, 2ll, 2ll, 6ll, 14ll, /**/ 2ll, 4ll},
        {15ll, 1ll, 1ll, 3ll, 7ll, 15ll, /**/ 2ll, 4ll},
        {16ll, 1ll, 2ll, 4ll, 8ll, 16ll, /**/ 2ll, 4ll}, }, kIgnoresOrder);

  code_based_test_driver = GetComplianceTestDriver();

  TestDatabase test_db;
  test_db.tables.emplace("TableEmpty", TestTable{table_empty});
  test_db.tables.emplace("Table1", TestTable{table1});
  test_db.tables.emplace("Table2", TestTable{table2});
  test_db.tables.emplace("Table3", TestTable{table3});
  test_db.tables.emplace("TableLarge", TestTable{table_large});
  test_db.tables.emplace("TableAllNull", TestTable{table_all_null});
  test_db.tables.emplace("TableDistincts", TestTable{table_distincts});
  for (const std::string& proto_file : testing::ZetaSqlTestProtoFilepaths()) {
    test_db.proto_files.insert(proto_file);
  }
  for (const std::string& proto_name : testing::ZetaSqlTestProtoNames()) {
    test_db.proto_names.insert(proto_name);
  }
  for (const std::string& enum_name : testing::ZetaSqlTestEnumNames()) {
    test_db.enum_names.insert(enum_name);
  }

  Value mytable = StructArray({"int64_val", "str_val"},
                              {{1ll, "foo"}, {2ll, "bar"}}, kIgnoresOrder);
  test_db.tables.emplace("MyTable", TestTable{mytable});

  test_db.tables.emplace(
      "T", TestTable{StructArray({"x", "y"}, {{1ll, "foo"}, {2ll, "bar"}})});

  ZETASQL_CHECK_OK(SQLTestBase::ValidateFirstColumnPrimaryKey(
      test_db, code_based_test_driver->GetSupportedLanguageOptions()));
  ZETASQL_CHECK_OK(code_based_test_driver->CreateDatabase(test_db));

  if (!code_based_test_driver->IsReferenceImplementation()) {
    code_based_reference_driver = new ReferenceDriver(
        code_based_test_driver->GetSupportedLanguageOptions());
    ZETASQL_EXPECT_OK(code_based_reference_driver->CreateDatabase(test_db));
  }
}

void CodebasedTestsEnvironment::TearDown() {
  delete code_based_test_driver;
  delete code_based_reference_driver;
}

ComplianceCodebasedTests::ComplianceCodebasedTests()
    : SuperclassAlias(code_based_test_driver, code_based_reference_driver) {
  // Sanity check the initialization of the test driver. The superclass
  // constructor will sanity check that 'code_based_reference_driver' is
  // consistent with 'code_based_test_driver'.
  ZETASQL_CHECK_EQ(GetCodeBasedTestsEnvironment()->skip_codebased_tests(),
           code_based_test_driver == nullptr);

  if (code_based_test_driver == nullptr) {
    // Sanity check that any subclass that overrides DriverCanRunTests() also
    // calls the superclass method
    // ComplianceCodebasedTests::DriverCanRunTests().
    ZETASQL_CHECK(!DriverCanRunTests());
  }
}

ComplianceCodebasedTests::~ComplianceCodebasedTests() {
}

void ComplianceCodebasedTests::RunStatementTests(
    const std::vector<QueryParamsWithResult>& statement_tests,
    const std::string& sql_string) {
  return RunStatementTestsCustom(
      statement_tests,
      [sql_string](const QueryParamsWithResult& p) { return sql_string; });
}

void ComplianceCodebasedTests::RunFunctionTestsInfix(
    const std::vector<QueryParamsWithResult>& function_tests,
    const std::string& operator_name) {
  return RunStatementTestsCustom(
      function_tests, [operator_name](const QueryParamsWithResult& p) {
        return ParametersWithSeparator(p.num_params(),
                                       absl::StrCat(" ", operator_name, " "));
      });
}

void ComplianceCodebasedTests::RunFunctionTestsInOperator(
    const std::vector<QueryParamsWithResult>& function_tests) {
  return RunStatementTestsCustom(
      function_tests, [](const QueryParamsWithResult& p) {
        std::vector<std::string> arg_str;
        for (int i = 1; i < p.num_params(); i++) {
          arg_str.push_back(absl::StrCat("@p", i));
        }
        return absl::StrCat("@p0 IN (", absl::StrJoin(arg_str, ","), ")");
      });
}

void ComplianceCodebasedTests::RunFunctionTestsInOperatorUnnestArray(
    const std::vector<QueryParamsWithResult>& function_tests) {
  return RunStatementTestsCustom(
      function_tests, [](const QueryParamsWithResult& p) {
        std::vector<std::string> arg_str;
        for (int i = 1; i < p.num_params(); i++) {
          arg_str.push_back(absl::StrCat("@p", i));
        }
        return absl::StrCat("@p0 IN UNNEST([", absl::StrJoin(arg_str, ","),
                            "])");
      });
}

void ComplianceCodebasedTests::RunFunctionTestsPrefix(
    const std::vector<QueryParamsWithResult>& function_tests,
    const std::string& function_name) {
  return RunStatementTestsCustom(
      function_tests, [function_name](const QueryParamsWithResult& p) {
        return absl::StrCat(function_name, "(",
                            ParametersWithSeparator(p.num_params(), ", "), ")");
      });
}

template <typename FCT>
void ComplianceCodebasedTests::RunFunctionTestsCustom(
    const std::vector<FunctionTestCall>& function_tests,
    FCT get_sql_string) {
  for (const auto& params : function_tests) {
    std::string pattern = get_sql_string(params);
    std::string sql = absl::StrCat("SELECT ", pattern, " AS ", kColA);
    QueryParamsWithResult new_params = params.params;
    ConvertResultsToSingletons(&new_params);
    SetNamePrefix(params.function_name);
    auto label = MakeScopedLabel(GetTypeLabels(params.params));
    RunStatementOnFeatures(sql, new_params);
  }
}

std::vector<FunctionTestCall> ComplianceCodebasedTests::AddSafeFunctionCalls(
    const std::vector<FunctionTestCall>& calls) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_SAFE_FUNCTION_CALL)) {
    return calls;
  }
  std::vector<FunctionTestCall> safe_calls = calls;
  for (const FunctionTestCall& call : calls) {
    bool has_errors = false;
    for (const auto& map_iter : call.params.results()) {
      if (!map_iter.second.status.ok()) {
        has_errors = true;
      }
    }
    // Don't add SAFE tests for functions that never had any errors.
    if (!has_errors) continue;

    QueryParamsWithResult::ResultMap safe_results_map;
    // QueryParamsWithResult requires us to have one result without the SAFE
    // mode feature added. This will give an error because SAFE won't be
    // enabled. We use a feature set from the first element so that the error
    // is due to the lack of the safe feature and not the lack of some other.
    auto map_begin = call.params.results().begin();
    ZETASQL_CHECK(
        !zetasql_base::ContainsKey(map_begin->first, FEATURE_V_1_2_SAFE_FUNCTION_CALL));
    safe_results_map.insert(
        std::make_pair(map_begin->first,
                       QueryParamsWithResult::Result(
                           Value::NullInt64(),  // Type doesn't matter here.
                           absl::StatusCode::kInvalidArgument)));

    // For each existing call, add another, with SAFE_FUNCTION_CALL enabled,
    // and error results replaced with NULLs.
    for (const auto& map_iter : call.params.results()) {
      QueryParamsWithResult::FeatureSet safe_feature_set = map_iter.first;
      safe_feature_set.insert(FEATURE_V_1_2_SAFE_FUNCTION_CALL);

      // If we had an OUT_OF_RANGE error, replace that with a NULL result.
      // For OK and for other errors (e.g. analysis errors), keep the original
      // expected result or error.
      safe_results_map.insert(
          std::make_pair(safe_feature_set,
                         !absl::IsOutOfRange(map_iter.second.status)
                             ? map_iter.second
                             : QueryParamsWithResult::Result(
                                   Value::Null(map_iter.second.result.type()),
                                   absl::OkStatus())));
    }

    std::vector<ValueConstructor> safe_params;
    for (const Value& param : call.params.params()) {
      safe_params.emplace_back(param);
    }
    QueryParamsWithResult safe_result(safe_params, safe_results_map);
    FunctionTestCall safe_call(absl::StrCat("safe.", call.function_name),
                               safe_result);
    safe_calls.push_back(safe_call);
  }
  return safe_calls;
}

// If a function name starts with "safe.", add a prefix "safe_error_mode__"
// onto it.  This makes it easier to add known error entries like
// "safe_error_mode__.*" without excluding tests for function that actually
// start with "SAFE_".
static std::string AddPrefixForSafeFunctionCalls(
    const std::string& function_name) {
  if (absl::StartsWith(function_name, "safe.")) {
    return absl::StrCat("safe_error_mode__", function_name);
  }
  return function_name;
}

void ComplianceCodebasedTests::RunFunctionCalls(
    const std::vector<FunctionTestCall>& function_calls) {
  for (const auto& call : AddSafeFunctionCalls(function_calls)) {
    std::string sql = absl::Substitute(
        "SELECT $0($1) AS $2", call.function_name,
        ParametersWithSeparator(call.params.num_params(), ", "), kColA);
    QueryParamsWithResult new_params = call.params;
    ConvertResultsToSingletons(&new_params);
    SetNamePrefix(AddPrefixForSafeFunctionCalls(call.function_name));
    auto label = MakeScopedLabel(GetTypeLabels(call.params));
    RunStatementOnFeatures(sql, new_params);
  }
}

void ComplianceCodebasedTests::RunFunctionCalls(
    const std::vector<QueryParamsWithResult>& test_cases,
    const std::string& function_name) {
  std::vector<FunctionTestCall> function_test_calls;
  function_test_calls.reserve(test_cases.size());
  for (const QueryParamsWithResult& test_case : test_cases) {
    function_test_calls.push_back({function_name, test_case});
  }
  RunFunctionCalls(function_test_calls);
}

void ComplianceCodebasedTests::RunNormalizeFunctionCalls(
    const std::vector<FunctionTestCall>& function_calls) {
  for (const auto& call : AddSafeFunctionCalls(function_calls)) {
    std::string sql = absl::Substitute(
        "SELECT $0(@p0$1) AS $2", call.function_name,
        call.params.num_params() <= 1
            ? ""
            : absl::StrCat(", ", call.params.param(1).enum_name()),
        kColA);
    QueryParamsWithResult new_params = call.params;
    ConvertResultsToSingletons(&new_params);
    SetNamePrefix(AddPrefixForSafeFunctionCalls(call.function_name));
    auto label = MakeScopedLabel(GetTypeLabels(call.params));
    RunStatementOnFeatures(sql, new_params);
  }
}

void ComplianceCodebasedTests::RunAggregationFunctionCalls(
    const std::vector<FunctionTestCall>& function_calls) {
  for (const auto& call : AddSafeFunctionCalls(function_calls)) {
    std::vector<std::string> arg_str = {"x"};
    for (int i = 1; i < call.params.num_params(); i++) {
      arg_str.push_back(absl::StrCat("@p", i));
    }

    std::string sql = absl::Substitute(
        "SELECT $0($1) AS $2 FROM UNNEST(@p0) AS x", call.function_name,
        absl::StrJoin(arg_str, ", "), kColA);
    QueryParamsWithResult new_params = call.params;
    ConvertResultsToSingletons(&new_params);
    SetNamePrefix(AddPrefixForSafeFunctionCalls(call.function_name));
    auto label = MakeScopedLabel(GetTypeLabels(call.params));
    RunStatementOnFeatures(sql, new_params);
  }
}

template <typename FCT>
void ComplianceCodebasedTests::RunStatementTestsCustom(
    const std::vector<QueryParamsWithResult>& statement_tests,
    FCT get_sql_string) {
  for (const auto& params : statement_tests) {
    std::string pattern = get_sql_string(params);
    std::string sql = absl::StrCat("SELECT ", pattern, " AS ", kColA);
    QueryParamsWithResult new_params = params;
    ConvertResultsToSingletons(&new_params);
    SetResultTypeName(params.GetResultType()->TypeName(PRODUCT_INTERNAL));
    auto label = MakeScopedLabel(GetTypeLabels(params));
    RunStatementOnFeatures(sql, new_params);
  }
}

absl::StatusOr<Value> ComplianceCodebasedTests::ExecuteStatementWithFeatures(
    const std::string& sql, const std::map<std::string, Value>& params,
    const QueryParamsWithResult::FeatureSet& features) {
  if (!DriverCanRunTests()) {
    return absl::Status(absl::StatusCode::kAborted, "driver cannot run tests");
  }

  if (IsTestingReferenceImpl()) {
    const LanguageOptions original_language_options =
        driver()->GetSupportedLanguageOptions();

    LanguageOptions language_options;
    language_options.SetEnabledLanguageFeatures(
        LanguageOptions::LanguageFeatureSet(features.begin(), features.end()));

    auto* reference_driver = static_cast<ReferenceDriver*>(driver());
    reference_driver->SetLanguageOptions(language_options);

    auto result = ExecuteStatement(sql, params);
    reference_driver->SetLanguageOptions(original_language_options);
    return result;
  }

  for (const LanguageFeature& feature : features) {
    if (!DriverSupportsFeature(feature)) {
      return absl::Status(absl::StatusCode::kAborted,
                          "driver does not support feature");
    }
  }

  TypeFactory type_factory;
  bool is_deterministic_output;
  bool uses_unsupported_type = false;
  absl::StatusOr<Value> reference_result =
      reference_driver()->ExecuteStatementForReferenceDriver(
          sql, params, GetExecuteStatementOptions(), &type_factory,
          &is_deterministic_output, &uses_unsupported_type);
  if (uses_unsupported_type) {
    return absl::Status(absl::StatusCode::kAborted,
                        "driver does not support type");
  }
  return reference_result;
}

void ComplianceCodebasedTests::RunStatementOnFeatures(
    const std::string& sql, const QueryParamsWithResult& statement_tests) {
  if (!DriverCanRunTests()) {
    return;
  }
  std::map<std::string, Value> param_map;

  for (int i = 0; i < statement_tests.num_params(); i++) {
    std::string param_name = absl::StrCat("p", i);
    param_map[param_name] = statement_tests.param(i);
  }

  if (IsTestingReferenceImpl()) {
    // For each pair of <Features, Result>, set 'Features' to reference driver,
    // execute the statement and check if the result matches with 'Result'.
    for (const auto& pair : statement_tests.results()) {
      const QueryParamsWithResult::FeatureSet& test_features = pair.first;
      const Value& result = pair.second.result;
      const absl::Status& status = pair.second.status;
      const FloatMargin& float_margin = pair.second.float_margin;

      const LanguageOptions original_language_options =
          driver()->GetSupportedLanguageOptions();

      LanguageOptions language_options;
      language_options.SetEnabledLanguageFeatures(
          LanguageOptions::LanguageFeatureSet(test_features.begin(),
                                              test_features.end()));

      auto* reference_driver = static_cast<ReferenceDriver*>(driver());
      reference_driver->SetLanguageOptions(language_options);

      EXPECT_THAT(ExecuteStatement(sql, param_map),
                  Returns(result, status, float_margin))
          << "FullName: " << full_name() << "; "
          << "Labels: " << absl::StrJoin(GetCodeBasedLabels(), ", ");

      reference_driver->SetLanguageOptions(original_language_options);
    }
  } else {
    // Get the loosest float margin over the language features the engine
    // supports.
    bool found_feature_set = false;
    FloatMargin float_margin = kExactFloatMargin;
    for (const auto& pair : statement_tests.results()) {
      const QueryParamsWithResult::FeatureSet& test_features = pair.first;
      const FloatMargin& float_margin_for_features = pair.second.float_margin;
      bool support_all = true;
      for (const LanguageFeature& feature : test_features) {
        support_all &= DriverSupportsFeature(feature);
      }
      if (support_all) {
        found_feature_set = true;
        float_margin = FloatMargin::MinimalLooseError(
            float_margin, float_margin_for_features);
      }
    }

    if (found_feature_set) {
      // Only run once. Use reference engine to check result. We don't compare
      // the reference engine output against QueryParamsWithResult::results()
      // because it isn't clear what feature set to use.
      TypeFactory type_factory;
      bool is_deterministic_output;
      bool uses_unsupported_type = false;
      absl::StatusOr<Value> reference_result =
          reference_driver()->ExecuteStatementForReferenceDriver(
              sql, param_map, GetExecuteStatementOptions(), &type_factory,
              &is_deterministic_output, &uses_unsupported_type);
      if (uses_unsupported_type) {
        return;  // Skip this test. It uses types not supported by the driver.
      }
      SCOPED_TRACE(sql);
      // TODO: Should we do something with 'is_deterministic_output'?
      EXPECT_THAT(ExecuteStatement(sql, param_map),
                  Returns(reference_result, float_margin))
          << "FullName: " << full_name() << "; "
          << "Labels: " << absl::StrJoin(GetCodeBasedLabels(), ", ");
      return;
    }
    return;  // Skip this test.
  }
}

std::vector<QueryParamsWithResult>
ComplianceCodebasedTests::GetFunctionTestsDateArithmetics(
    const std::vector<FunctionTestCall>& tests) {
  if (!DriverSupportsFeature(FEATURE_V_1_3_DATE_ARITHMETICS)) {
    return {};
  }
  std::vector<QueryParamsWithResult> out;
  for (const auto& test : tests) {
    // Only look at tests which use DAY as datepart
    if (test.params.param(2).is_null() ||
        test.params.param(2).enum_value() !=
            functions::DateTimestampPart::DAY) {
      continue;
    }
    QueryParamsWithResult::ResultMap result_map;
    for (const auto& p : test.params.results()) {
      QueryParamsWithResult::FeatureSet feature_set = p.first;
      feature_set.insert(FEATURE_V_1_3_DATE_ARITHMETICS);
      result_map.insert(std::make_pair(feature_set, p.second));
    }
    QueryParamsWithResult params = test.params;
    params.set_results(result_map);
    out.push_back(params);
  }
  return out;
}

// A sample test to test feature lists.
TEST_F(ComplianceCodebasedTests, TestOnFeatures) {
  if (!DriverCanRunTests()) {
    return;
  }

  const QueryParamsWithResult::FeatureSet& empty_feature_set =
      QueryParamsWithResult::kEmptyFeatureSet;
  const QueryParamsWithResult::FeatureSet nonempty_feature_set{
      LanguageFeature::FEATURE_DISALLOW_GROUP_BY_FLOAT};
  const std::vector<ValueConstructor> empty_param_list;

  typedef QueryParamsWithResult::Result Result;

  // RunStatementOnFeatures().
  // With empty feature set, the query returns a result. With feature
  // "DISALLOW_GROUP_BY_FLOAT", the query returns an INVALID_ARGUMENT
  // error.
  SetNamePrefix("QueryOnFeatures");
  RunStatementOnFeatures(
      "SELECT DISTINCT double_val as ColA FROM TableAllNull",
      {empty_param_list, /* parameter list */
       {{empty_feature_set, Result{Singleton(NullDouble())}},
        {nonempty_feature_set, Result{Singleton(NullDouble()),
                                      absl::StatusCode::kInvalidArgument}}}});

  // RunStatementTests().
  // Either way the query returns the same result.
  SetNamePrefix("QueryTestsOnFeatures");
  RunStatementTests({{{NullDouble()}, /* parameter list */
                      {{empty_feature_set, Result{NullDouble()}},
                       {nonempty_feature_set, Result{NullDouble()}}}}},
                    "@p0");

  // RunFunctionTestsInfix().
  // Either way the query returns the same result.
  SetNamePrefix("FunctionTestsInfixOnFeatures");
  RunFunctionTestsInfix(
      {{{False(), True()}, /* parameter list */
        {{empty_feature_set,    Result{False()}},
         {nonempty_feature_set, Result{False()}}}}},
      "AND");

  // RunFunctionTestsPrefix().
  // Either way the query returns the same result.
  SetNamePrefix("FunctionTestsPrefixOnFeatures");
  RunFunctionTestsPrefix(
      {{{Int64(1), Int64(2)}, /* parameter list */
        {{empty_feature_set,    Result{Int64(2)}},
         {nonempty_feature_set, Result{Int64(2)}}}}},
      "Greatest");

  // RunFunctionCalls().
  // Either way the query returns the same result.
  // No need to SetNamePrefix, RunFunctionCalls() will do it.
  RunFunctionCalls({{"abs",       /* function name */
                     {Int64(-1)}, /* parameter list */
                     {{empty_feature_set, Result{Int64(1)}},
                      {nonempty_feature_set, Result{Int64(1)}}}}});
}

TEST_F(ComplianceCodebasedTests, TestQueryParameters) {
  if (!DriverCanRunTests()) {
    return;
  }
  // Tests query parameter support.
  SetNamePrefix("Param");
  EXPECT_THAT(RunStatement("SELECT @ColA AS ColA", {Int64(5)}, {"ColA"}),
              Returns(Singleton(Int64(5))));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_Negative, 1) {
  SetNamePrefix("Negative");
  RunStatementTests(Shard(GetFunctionTestsUnaryMinus()), "-@p0");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_Add, 1) {
  SetNamePrefix("Add");
  RunStatementTests(Shard(GetFunctionTestsAdd()), "@p0 + @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_Subtract, 1) {
  SetNamePrefix("Subtract");
  RunStatementTests(Shard(GetFunctionTestsSubtract()), "@p0 - @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_Multiply, 1) {
  SetNamePrefix("Multiply");
  RunStatementTests(Shard(GetFunctionTestsMultiply()), "@p0 * @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_Divide, 1) {
  SetNamePrefix("Divide");
  RunStatementTests(Shard(GetFunctionTestsDivide()), "@p0 / @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_Mod, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsModulo()), "MOD");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_Div, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsDiv()), "DIV");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_CoercedAdd,
               1) {
  SetNamePrefix("CoercedAdd");
  RunStatementTests(Shard(GetFunctionTestsCoercedAdd()), "@p0 + @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestArithmeticFunctions_CoercedSubtract, 1) {
  SetNamePrefix("CoercedSubtract");
  RunStatementTests(Shard(GetFunctionTestsCoercedSubtract()), "@p0 - @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestArithmeticFunctions_CoercedMultiply, 1) {
  SetNamePrefix("CoercedMultiply");
  RunStatementTests(Shard(GetFunctionTestsCoercedMultiply()), "@p0 * @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_CoercedDivide,
               1) {
  SetNamePrefix("CoercedDivide");
  RunStatementTests(Shard(GetFunctionTestsCoercedDivide()), "@p0 / @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_CoercedMod,
               1) {
  RunFunctionCalls(Shard(GetFunctionTestsCoercedModulo()), "MOD");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_CoercedDiv,
               1) {
  RunFunctionCalls(Shard(GetFunctionTestsCoercedDiv()), "DIV");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_SafeAdd, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsSafeAdd()), "SAFE_ADD");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_SafeSubtract,
               1) {
  RunFunctionCalls(Shard(GetFunctionTestsSafeSubtract()), "SAFE_SUBTRACT");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_SafeMultiply,
               1) {
  RunFunctionCalls(Shard(GetFunctionTestsSafeMultiply()), "SAFE_MULTIPLY");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_SafeDivide,
               1) {
  RunFunctionCalls(Shard(GetFunctionTestsSafeDivide()), "SAFE_DIVIDE");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArithmeticFunctions_SafeNegate,
               1) {
  RunFunctionCalls(Shard(GetFunctionTestsSafeNegate()), "SAFE_NEGATE");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArrayConcatOperator, 1) {
  SetNamePrefix("ConcatOperator");
  RunStatementTests(Shard(GetFunctionTestsArrayConcatOperator()), "@p0 || @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringConcatOperator, 1) {
  SetNamePrefix("ConcatOperator");
  RunStatementTests(Shard(GetFunctionTestsStringConcatOperator()),
                    "@p0 || @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestGreatestFunctions, 3) {
  SetNamePrefix("Greatest");
  RunFunctionTestsPrefix(Shard(GetFunctionTestsGreatest(
                             DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                         "Greatest");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestLeastFunctions, 3) {
  SetNamePrefix("Least");
  RunFunctionTestsPrefix(Shard(GetFunctionTestsLeast(
                             DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                         "Least");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestLogicalFunctions_AND, 1) {
  SetNamePrefix("And");
  RunFunctionTestsInfix(Shard(GetFunctionTestsAnd()), "AND");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestLogicalFunctions_OR, 1) {
  SetNamePrefix("Or");
  RunFunctionTestsInfix(Shard(GetFunctionTestsOr()), "OR");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestLogicalFunctions_NOT, 1) {
  SetNamePrefix("Not");
  RunStatementTests(Shard(GetFunctionTestsNot()), "NOT @p0");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_EQ, 3) {
  SetNamePrefix("EQ");
  RunStatementTests(Shard(GetFunctionTestsEqual(
                        DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                    "@p0 = @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_NE, 3) {
  SetNamePrefix("NE");
  RunStatementTests(Shard(GetFunctionTestsNotEqual(
                        DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                    "@p0 != @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_GT, 2) {
  SetNamePrefix("GT");
  RunStatementTests(Shard(GetFunctionTestsGreater(
                        DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                    "@p0 > @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_GE, 2) {
  SetNamePrefix("GE");
  RunStatementTests(Shard(GetFunctionTestsGreaterOrEqual(
                        DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                    "@p0 >= @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_StructIn, 1) {
  SetNamePrefix("IN");
  RunFunctionTestsInOperator(Shard(GetFunctionTestsStructIn()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_InList, 1) {
  SetNamePrefix("InList");
  RunFunctionTestsInOperator(Shard(GetFunctionTestsIn()));
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestComparisonFunctions_InArrayWithNulls, 1) {
  // SELECT <expr> IN UNNEST([...<array with NULLs>...]])
  SetNamePrefix("InArray_WithNulls");
  RunFunctionTestsInOperatorUnnestArray(Shard(GetFunctionTestsInWithNulls()));
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestComparisonFunctions_InArrayWithoutNulls, 1) {
  // SELECT <expr> IN UNNEST([...<array without NULLs>...]])
  SetNamePrefix("InArray_WithoutNulls");
  RunFunctionTestsInOperatorUnnestArray(
      Shard(GetFunctionTestsInWithoutNulls()));

  // Empty matching set.
  RunFunctionTestsInOperatorUnnestArray(
      {{{String("x")}, false}, {{NullString()}, false}});
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_IsNull, 1) {
  SetNamePrefix("IsNull");
  RunStatementTests(Shard(GetFunctionTestsIsNull()), "@p0 IS NULL");
  RunStatementTests(Shard(WrapFeatureJSON(GetFunctionTestsJsonIsNull())),
                    "@p0 IS NULL");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_IsNotNull, 1) {
  SetNamePrefix("IsNotNull");
  RunStatementTests(InvertResults(Shard(GetFunctionTestsIsNull())),
                    "@p0 IS NOT NULL");
  RunStatementTests(
      WrapFeatureJSON(InvertResults(Shard(GetFunctionTestsJsonIsNull()))),
      "@p0 IS NOT NULL");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_LT, 2) {
  SetNamePrefix("LT");
  RunStatementTests(Shard(GetFunctionTestsLess(
                        DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                    "@p0 < @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestComparisonFunctions_LE, 2) {
  SetNamePrefix("LE");
  RunStatementTests(Shard(GetFunctionTestsLessOrEqual(
                        DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))),
                    "@p0 <= @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestCastFunction, 4) {
  // TODO: This needs to be sensitive to ProductMode, or
  // maybe just switched to PRODUCT_EXTERNAL.
  auto format_fct = [](const QueryParamsWithResult& p) {
    std::string optional_format_param = "";
    if (p.num_params() == 2 || p.num_params() == 3) {
      optional_format_param = absl::StrCat(
          " FORMAT ",
          p.param(1).is_null()
              ? "NULL"
              : absl::StrCat("\'", p.param(1).string_value(), "\'"));
      if (p.num_params() == 3) {
        absl::StrAppend(
            &optional_format_param,
            " AT TIME ZONE ",
            p.param(2).is_null()
                ? "CAST(NULL AS STRING)"
                : absl::StrCat("\'", p.param(2).string_value(), "\'"));
      }
    }
    return absl::StrCat("CAST(@p0 AS ",
                        p.GetResultType()->TypeName(PRODUCT_INTERNAL),
                        optional_format_param, ")");
  };
  SetNamePrefix("CastTo", true /* Need Result Type */);
  RunStatementTestsCustom(Shard(GetFunctionTestsCast()), format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestSafeCastFunction, 4) {
  auto format_fct = [](const QueryParamsWithResult& p) {
    std::string optional_format_param = "";
    if (p.num_params() == 2 || p.num_params() == 3) {
      optional_format_param = absl::StrCat(
          " FORMAT ",
          p.param(1).is_null()
              ? "NULL"
              : absl::StrCat("\'", p.param(1).string_value(), "\'"));
      if (p.num_params() == 3) {
        absl::StrAppend(
            &optional_format_param, " AT TIME ZONE ",
            p.param(2).is_null()
                ? "NULL"
                : absl::StrCat("\'", p.param(2).string_value(), "\'"));
      }
    }
    return absl::StrCat("SAFE_CAST(@p0 AS ",
                        p.GetResultType()->TypeName(PRODUCT_INTERNAL),
                        optional_format_param, ")");
  };
  SetNamePrefix("SafeCastTo", true /* Need Result Type */);
  RunStatementTestsCustom(Shard(GetFunctionTestsSafeCast()), format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestCastArraysWithNullsOfDifferentTypesFunction, 3) {
  auto format_fct = [](const QueryParamsWithResult& p) {
    return absl::StrCat("CAST(@p0 AS ",
                        p.GetResultType()->TypeName(PRODUCT_INTERNAL), ")");
  };
  SetNamePrefix("CastArraysBetweenDifferentType_WithNulls",
                true /* Need Result Type */);
  RunStatementTestsCustom(
      Shard(GetFunctionTestsCastBetweenDifferentArrayTypes(
          /*arrays_with_nulls=*/true)),
      format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestCastArraysWithoutNullsOfDifferentTypesFunction, 3) {
  auto format_fct = [](const QueryParamsWithResult& p) {
    return absl::StrCat("CAST(@p0 AS ",
                        p.GetResultType()->TypeName(PRODUCT_INTERNAL), ")");
  };
  SetNamePrefix("CastArraysBetweenDifferentType_WithoutNulls",
                true /* Need Result Type */);
  RunStatementTestsCustom(
      Shard(GetFunctionTestsCastBetweenDifferentArrayTypes(
          /*arrays_with_nulls=*/false)),
      format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitCastFunctions, 1) {
  SetNamePrefix("BitCast");
  RunFunctionCalls(Shard(GetFunctionTestsBitCast()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestGenerateArray, 1) {
  SetNamePrefix("GenerateArray");
  RunFunctionCalls(Shard(GetFunctionTestsGenerateArray()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestRangeBucket, 1) {
  SetNamePrefix("RangeBucket");
  RunFunctionCalls(Shard(GetFunctionTestsRangeBucket()));
}

namespace {

// Wraps test cases for functions that take a DateTimestampPart enum arguments.
// Converts the date/time part arguments to strings so that they are not
// added as enum-typed parameter bindings which can trip engines that don't
// support enum types. The date/time part parameter bindings can anyways not be
// used as arguments in the actual functions as they always have to be literals
// and so do not actually appear in the generated test case query.
std::vector<FunctionTestCall> ConvertDateTimePartBindingsToString(
    std::vector<FunctionTestCall> tests, int date_part_param_idx) {
  for (auto& test_case : tests) {
    if (test_case.params.num_params() <= date_part_param_idx) {
      continue;
    }
    const auto& date_part_param = test_case.params.param(date_part_param_idx);
    *test_case.params.mutable_param(date_part_param_idx) = Value::String(
        date_part_param.is_null()
            ? "NULL"
            : functions::DateTimestampPartToSQL(date_part_param.enum_value()));
  }
  return tests;
}

}  // namespace

SHARDED_TEST_F(ComplianceCodebasedTests, TestGenerateDateArray, 1) {
  SetNamePrefix("GenerateDateArray");
  auto generate_date_array_format_fct = [](const FunctionTestCall& f) {
    if (f.params.params().size() == 2) {
      return absl::Substitute("$0(@p0, @p1)", f.function_name);
    }
    if (f.params.params().size() == 3) {
      // Degenerate case.
      return absl::Substitute("$0(@p0, @p1, INTERVAL @p2)", f.function_name);
    }
    return absl::Substitute("$0(@p0, @p1, INTERVAL @p2 $1)", f.function_name,
                            f.params.param(3).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(
          GetFunctionTestsGenerateDateArray(), /*date_part_param_idx=*/3)),
      generate_date_array_format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestGenerateTimestampArray, 1) {
  SetNamePrefix("GenerateTimestampArray");
  auto generate_timestamp_array_format_fct = [](const FunctionTestCall& f) {
    if (f.params.params().size() == 2) {
      return absl::Substitute("$0(@p0, @p1)", f.function_name);
    }
    if (f.params.params().size() == 3) {
      // Degenerate case.
      return absl::Substitute("$0(@p0, @p1, INTERVAL @p2)", f.function_name);
    }
    return absl::Substitute("$0(@p0, @p1, INTERVAL @p2 $1)", f.function_name,
                            f.params.param(3).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(
          GetFunctionTestsGenerateTimestampArray(), /*date_part_param_idx=*/3)),
      generate_timestamp_array_format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonQuery, 1) {
  SetNamePrefix("StringJsonQuery");
  RunFunctionCalls(Shard(GetFunctionTestsStringJsonQuery()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonExtract, 1) {
  SetNamePrefix("StringJsonExtract");
  RunFunctionCalls(Shard(GetFunctionTestsStringJsonExtract()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonValue, 1) {
  SetNamePrefix("StringJsonValue");
  RunFunctionCalls(Shard(GetFunctionTestsStringJsonValue()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonExtractScalar, 1) {
  SetNamePrefix("StringJsonExtractScalar");
  RunFunctionCalls(Shard(GetFunctionTestsStringJsonExtractScalar()));
}

namespace {

// Wraps test cases with FEATURE_JSON_ARRAY_FUNCTIONS.
std::vector<FunctionTestCall> EnableStringJsonArrayFunctionsForTest(
    std::vector<FunctionTestCall> tests) {
  for (auto& test_case : tests) {
    test_case.params =
        test_case.params.WrapWithFeature(FEATURE_JSON_ARRAY_FUNCTIONS);
  }
  return tests;
}

}  // namespace

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonQueryArray, 1) {
  SetNamePrefix("StringJsonQueryArray");
  RunFunctionCalls(Shard(EnableStringJsonArrayFunctionsForTest(
      GetFunctionTestsStringJsonQueryArray())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonExtractArray, 1) {
  SetNamePrefix("StringJsonExtractArray");
  RunFunctionCalls(Shard(GetFunctionTestsStringJsonExtractArray()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonValueArray, 1) {
  SetNamePrefix("StringJsonValueArray");
  RunFunctionCalls(Shard(EnableStringJsonArrayFunctionsForTest(
      GetFunctionTestsStringJsonValueArray())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringJsonExtractStringArray, 1) {
  SetNamePrefix("StringJsonExtractStringArray");
  RunFunctionCalls(Shard(EnableStringJsonArrayFunctionsForTest(
      GetFunctionTestsStringJsonExtractStringArray())));
}

namespace {

// Wraps test cases with FEATURE_JSON_TYPE.
// If a test case already has a feature set, do not wrap it.
std::vector<FunctionTestCall> EnableJsonFeatureForTest(
    std::vector<FunctionTestCall> tests) {
  for (auto& test_case : tests) {
    if (test_case.params.HasEmptyFeatureSetAndNothingElse()) {
      test_case.params =
          test_case.params.WrapWithFeatureSet({FEATURE_JSON_TYPE});
    }
  }
  return tests;
}

// Wraps test cases with FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS.
// If a test case already has a feature set, do not wrap it.
std::vector<FunctionTestCall> EnableJsonValueExtractionFunctionsForTest(
    std::vector<FunctionTestCall> tests) {
  for (auto& test_case : tests) {
    if (test_case.params.HasEmptyFeatureSetAndNothingElse()) {
      test_case.params = test_case.params.WrapWithFeatureSet(
          {FEATURE_JSON_TYPE, FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS});
    }
  }
  return tests;
}

// Wraps test cases with FEATURE_JSON_TYPE and FEATURE_JSON_ARRAY_FUNCTIONS.
// If a test case already has a feature set, do not wrap it.
std::vector<FunctionTestCall> EnableNativeJsonArrayFunctionsForTest(
    std::vector<FunctionTestCall> tests) {
  for (auto& test_case : tests) {
    if (test_case.params.HasEmptyFeatureSetAndNothingElse()) {
      test_case.params = test_case.params.WrapWithFeatureSet(
          {FEATURE_JSON_TYPE, FEATURE_JSON_ARRAY_FUNCTIONS});
    }
  }
  return tests;
}

}  // namespace

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonQuery, 1) {
  SetNamePrefix("NativeJsonQuery");
  RunFunctionCalls(
      Shard(EnableJsonFeatureForTest(GetFunctionTestsNativeJsonQuery())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonExtract, 1) {
  SetNamePrefix("NativeJsonExtract");
  RunFunctionCalls(
      Shard(EnableJsonFeatureForTest(GetFunctionTestsNativeJsonExtract())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonValue, 1) {
  SetNamePrefix("NativeJsonValue");
  RunFunctionCalls(
      Shard(EnableJsonFeatureForTest(GetFunctionTestsNativeJsonValue())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonExtractScalar, 1) {
  SetNamePrefix("NativeJsonExtractScalar");
  RunFunctionCalls(Shard(
      EnableJsonFeatureForTest(GetFunctionTestsNativeJsonExtractScalar())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonQueryArray, 1) {
  SetNamePrefix("NativeJsonQueryArray");
  RunFunctionCalls(Shard(EnableNativeJsonArrayFunctionsForTest(
      GetFunctionTestsNativeJsonQueryArray())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonExtractArray, 1) {
  SetNamePrefix("NativeJsonExtractArray");
  RunFunctionCalls(Shard(EnableNativeJsonArrayFunctionsForTest(
      GetFunctionTestsNativeJsonExtractArray())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonValueArray, 1) {
  SetNamePrefix("NativeJsonValueArray");
  RunFunctionCalls(Shard(EnableNativeJsonArrayFunctionsForTest(
      GetFunctionTestsNativeJsonValueArray())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNativeJsonExtractStringArray, 1) {
  SetNamePrefix("NativeJsonExtractStringArray");
  RunFunctionCalls(Shard(EnableNativeJsonArrayFunctionsForTest(
      GetFunctionTestsNativeJsonExtractStringArray())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestToJsonString, 1) {
  SetNamePrefix("ToJsonString");
  RunFunctionCalls(Shard(GetFunctionTestsToJsonString(
      DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS))));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestToJson, 1) {
  SetNamePrefix("ToJson");
  auto to_json_fct = [](const FunctionTestCall& f) {
    if (f.params.params().size() == 1) {
      return absl::Substitute("$0(@p0)", f.function_name);
    }
    return absl::Substitute("$0(@p0, stringify_wide_numbers=>@p1)",
                            f.function_name);
  };
  RunFunctionTestsCustom(Shard(EnableJsonFeatureForTest(GetFunctionTestsToJson(
                             DriverSupportsFeature(FEATURE_TIMESTAMP_NANOS)))),
                         to_json_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestParseJson, 1) {
  SetNamePrefix("ParseJson");
  auto parse_json_fn_expr = [](const FunctionTestCall& f) {
    if (f.params.params().size() == 1) {
      return absl::Substitute("$0(@p0)", f.function_name);
    }
    return absl::Substitute("$0(@p0, wide_number_mode=>@p1)", f.function_name);
  };
  RunFunctionTestsCustom(
      Shard(EnableJsonFeatureForTest(GetFunctionTestsParseJson())),
      parse_json_fn_expr);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestConvertJson, 1) {
  SetNamePrefix("ConvertJson");
  auto convert_json_fn_expr = [](const FunctionTestCall& f) {
    if (f.params.params().size() == 1) {
      return absl::Substitute("$0(@p0)", f.function_name);
    }
    return absl::Substitute("$0(@p0, wide_number_mode=>@p1)", f.function_name);
  };
  RunFunctionTestsCustom(Shard(EnableJsonValueExtractionFunctionsForTest(
                             GetFunctionTestsConvertJson())),
                         convert_json_fn_expr);
  RunFunctionTestsCustom(Shard(EnableJsonValueExtractionFunctionsForTest(
                             GetFunctionTestsConvertJsonIncompatibleTypes())),
                         convert_json_fn_expr);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestHash, 1) {
  SetNamePrefix("Hash");
  RunFunctionCalls(Shard(GetFunctionTestsHash()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestFarmFingerprint, 1) {
  SetNamePrefix("FarmFingerprint");
  RunFunctionCalls(Shard(GetFunctionTestsFarmFingerprint()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestErrorFunction, 1) {
  SetNamePrefix("ErrorFunction");
  RunFunctionCalls(Shard(GetFunctionTestsError()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestRegexpFunctions, 1) {
  SetNamePrefix("Regex");
  RunFunctionCalls(Shard(GetFunctionTestsRegexp()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestRegexp2Functions, 1) {
  auto label = MakeScopedLabel("regexp2_test:func_regexp_extract");
  RunFunctionCalls(
      Shard(GetFunctionTestsRegexp2(/*=include_feature_set=*/true)));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestRegexpInstrFunctions, 1) {
  SetNamePrefix("RegexpInstr");
  RunFunctionCalls(Shard(GetFunctionTestsRegexpInstr()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestLike, 4) {
  SetNamePrefix("Like");
  auto query_params_with_results = GetFunctionTestsLike();
  // The LIKE pattern is not a constant, the regexp is constructed and
  // compiled at evaluation time.
  RunStatementTests(Shard(query_params_with_results), "@p0 LIKE @p1");

  // Generate queries where the LIKE pattern is a constant, the regexp is
  // constructed and compiled only once.
  SetNamePrefix("Like_with_constant_pattern");
  RunStatementTestsCustom(
      Shard(query_params_with_results), [](const QueryParamsWithResult& p) {
        std::string p0 = p.param(0).is_null() ? "@p0" : MakeLiteral(p.param(0));
        std::string p1 = p.param(1).is_null() ? "@p1" : MakeLiteral(p.param(1));
        return absl::Substitute("$0 LIKE $1", p0, p1);
      });
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNotLike, 2) {
  SetNamePrefix("NotLike");
  RunStatementTests(InvertResults(Shard(GetFunctionTestsLike())),
                    "@p0 NOT LIKE @p1");
}

TEST_F(ComplianceCodebasedTests, TestSimpleTable) {
  if (!DriverCanRunTests()) {
    return;
  }
  Value mytable = StructArray({"int64_val", "str_val"}, {
      {1ll, "foo"},
      {2ll, "bar"}},
      kIgnoresOrder);

  SetNamePrefix("Table");
  EXPECT_THAT(RunStatement("SELECT int64_val, str_val FROM MyTable"),
              Returns(mytable));

  // Scanned rows can be returned in any order.
  SetNamePrefix("PermutedTable");
  Value permuted_mytable = StructArray({"int64_val", "str_val"}, {
      {2ll, "bar"},
      {1ll, "foo"}},
      kIgnoresOrder);
  EXPECT_THAT(RunStatement("SELECT int64_val, str_val FROM MyTable"),
              Returns(permuted_mytable));
}

TEST_F(ComplianceCodebasedTests, TestAggregation) {
  if (!DriverCanRunTests()) {
    return;
  }
  auto label = MakeScopedLabel("code:TestAggregation");

  // ANY_VALUE
  // Read a table of all NULL values.
  Value any_result_nulls = StructArray(
      {"bool_any", "double_any", "int64_any", "str_val"}, {
      {NullBool(), NullDouble(), NullInt64(), NullString()}, });
  SetNamePrefix("TableAllNull");
  EXPECT_THAT(RunStatement("SELECT ANY_VALUE(bool_val) bool_any, "
                           "       ANY_VALUE(double_val) double_any, "
                           "       ANY_VALUE(int64_val) int64_any, "
                           "       ANY_VALUE(str_val) str_val "
                           "FROM TableAllNull"),
              Returns(any_result_nulls));
  // There are too many possible results to comprehensively check the result.
  SetNamePrefix("TableLarge");
  absl::StatusOr<Value> result(ExecuteStatement(
      "SELECT ANY_VALUE(bool_val) bool_any, "
      "       ANY_VALUE(double_val) double_any, "
      "       ANY_VALUE(int64_val) int64_any, ANY_VALUE(str_val) str_val "
      "FROM TableLarge",
      {}));
  EXPECT_THAT(result, ReturnsSuccess());
  // ANY_VALUE queries with tractable non-deterministic results.
  SetNamePrefix("TableLargeBool");
  EXPECT_THAT(
      ExecuteStatement("SELECT ANY_VALUE(bool_val) FROM TableLarge", {}),
      Returns(::testing::AnyOf(
          EqualsValue(StructArray({kAnonymousColumnName}, {{NullBool()}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Bool(true)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Bool(false)}})))));
  SetNamePrefix("TableLargeDouble");
  EXPECT_THAT(
      ExecuteStatement("SELECT ANY_VALUE(double_val) FROM TableLarge", {}),
      Returns(::testing::AnyOf(
          EqualsValue(StructArray({kAnonymousColumnName}, {{NullDouble()}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.2)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.3)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.4)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.5)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.6)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.7)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.8)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.9)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(1.0)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.2)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Double(0.2)}})))));
  SetNamePrefix("TableLargeInt64");
  EXPECT_THAT(
      ExecuteStatement("SELECT ANY_VALUE(int64_val) FROM TableLarge", {}),
      Returns(::testing::AnyOf(
          ReturnsNoValue("Known Error"),
          EqualsValue(StructArray({kAnonymousColumnName}, {{NullInt64()}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(3)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(4)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(5)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(6)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(7)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(8)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(9)}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{Int64(10)}})))));
  SetNamePrefix("TableLargeString");
  EXPECT_THAT(
      ExecuteStatement("SELECT ANY_VALUE(str_val) FROM TableLarge", {}),
      Returns(::testing::AnyOf(
          ReturnsNoValue("Known Error"),
          EqualsValue(StructArray({kAnonymousColumnName}, {{NullString()}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{String("4")}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{String("5")}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{String("6")}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{String("7")}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{String("8")}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{String("9")}})),
          EqualsValue(StructArray({kAnonymousColumnName}, {{String("10")}})))));
}  // namespace zetasql

SHARDED_TEST_F(ComplianceCodebasedTests, TestConditionals_If, 1) {
  SetNamePrefix("If");
  RunFunctionTestsPrefix(Shard(GetFunctionTestsIf()), "If");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestConditionals_IfNull, 1) {
  SetNamePrefix("IfNull");
  RunFunctionTestsPrefix(Shard(GetFunctionTestsIfNull()), "IfNull");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestConditionals_NullIf, 1) {
  SetNamePrefix("NullIf");
  RunFunctionTestsPrefix(Shard(GetFunctionTestsNullIf()), "NullIf");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestConditionals_Coalesce, 1) {
  SetNamePrefix("Coalesce");
  RunFunctionTestsPrefix(Shard(GetFunctionTestsCoalesce()), "Coalesce");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitwiseFunctions_bNOT, 1) {
  SetNamePrefix("bNot");
  RunStatementTests(Shard(GetFunctionTestsBitwiseNot()), "~@p0");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitwiseFunctions_bOR, 1) {
  SetNamePrefix("bOr");
  RunStatementTests(Shard(GetFunctionTestsBitwiseOr()), "@p0 | @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitwiseFunctions_bXOR, 1) {
  SetNamePrefix("bXor");
  RunStatementTests(Shard(GetFunctionTestsBitwiseXor()), "@p0 ^ @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitwiseFunctions_bAND, 1) {
  SetNamePrefix("bAnd");
  RunStatementTests(Shard(GetFunctionTestsBitwiseAnd()), "@p0 & @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitwiseFunctions_bLEFTSHIFT, 1) {
  SetNamePrefix("LeftShift");
  RunStatementTests(Shard(GetFunctionTestsBitwiseLeftShift()), "@p0 << @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitwiseFunctions_bRIGHTSHIFT, 1) {
  SetNamePrefix("RightShift");
  RunStatementTests(Shard(GetFunctionTestsBitwiseRightShift()), "@p0 >> @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBitCount, 1) {
  SetNamePrefix("BitCount");
  RunStatementTests(Shard(GetFunctionTestsBitCount()), "BIT_COUNT(@p0)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArrayAtOffset, 1) {
  SetNamePrefix("ArrayAtOffset");
  RunStatementTests(Shard(GetFunctionTestsAtOffset()), "@p0[OFFSET(@p1)]");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArrayAtOrdinal, 1) {
  SetNamePrefix("ArrayAtOrdinal");
  RunStatementTests(Shard(GetFunctionTestsAtOffset()), "@p0[ORDINAL(@p1 + 1)]");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestSafeArrayAtOffset, 1) {
  SetNamePrefix("SafeArrayAtOffset");
  RunStatementTests(Shard(GetFunctionTestsSafeAtOffset()),
                    "@p0[SAFE_OFFSET(@p1)]");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestSafeArrayAtOrdinal, 1) {
  SetNamePrefix("SafeArrayAtOrdinal");
  RunStatementTests(Shard(GetFunctionTestsSafeAtOffset()),
                    "@p0[SAFE_ORDINAL(@p1 + 1)]");
}

TEST_F(ComplianceCodebasedTests, TestProto) {
  if (!DriverCanRunTests()) {
    return;
  }
  zetasql_test__::KitchenSinkPB p;
  // Roundtrip proto parameter.
  // TODO: can we expect the same byte serialization in general?
  p.Clear();
  // Set all required fields.
  p.set_int64_key_1(3);
  p.set_int64_key_2(0);
  SetNamePrefix("Proto");
  RunStatementOnFeatures("SELECT @p0 AS ColA",
                         {/*arguments=*/{MakeProtoValue(&p)},
                          /*result=*/Singleton(MakeProtoValue(&p))});
  p.Clear();
  // Set all required fields.
  p.set_int64_key_1(3);
  p.set_int64_key_2(0);
  // Missing int32_t with default 77.
  SetNamePrefix("ProtoInt32");
  RunStatementOnFeatures("SELECT @p0.int32_val a, @p0.has_int32_val b",
                         {/*arguments=*/{MakeProtoValue(&p)},
                          /*result=*/StructArray({"a", "b"}, {{77, False()}})});
  p.Clear();
  // Set all required fields.
  p.set_int64_key_1(3);
  p.set_int64_key_2(0);
  // int32_t set to 123.
  p.set_int32_val(123);
  SetNamePrefix("ProtoInt32");
  RunStatementOnFeatures("SELECT @p0.int32_val a, @p0.has_int32_val b",
                         {/*arguments=*/{MakeProtoValue(&p)},
                          /*result=*/StructArray({"a", "b"}, {{123, True()}})});
  p.Clear();
  // Set all required fields.
  p.set_int64_key_1(3);
  p.set_int64_key_2(0);
  SetNamePrefix("ProtoInt64");
  RunStatementOnFeatures(
      "SELECT @p0.int64_key_1 a, @p0.has_int64_key_1 b",
      {/*arguments=*/{MakeProtoValue(&p)},
       /*result=*/StructArray({"a", "b"}, {{Int64(3), True()}})});
  p.Clear();
  // Set all required fields.
  p.set_int64_key_1(3);
  p.set_int64_key_2(0);
  // Nested repeated value. nested_int64 has default 88.
  zetasql_test__::KitchenSinkPB::Nested* n = p.mutable_nested_value();
  n->add_nested_repeated_int64(300);
  n->add_nested_repeated_int64(301);
  SetNamePrefix("ProtoNested");
  RunStatementOnFeatures(
      "SELECT nv.nested_int64 a, b FROM "
      "(SELECT @p0.nested_value nv) t, t.nv.nested_repeated_int64 b",
      {/*arguments=*/{MakeProtoValue(&p)},
       /*result=*/StructArray(
           {"a", "b"}, {{Int64(88), Int64(301)}, {Int64(88), Int64(300)}},
           kIgnoresOrder)});
  // TODO: port all tests from evaluation_test.cc
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestMathFunctions_Math, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(Shard(GetFunctionTestsMath()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestMathFunctions_Rounding, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(Shard(GetFunctionTestsRounding()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestMathFunctions_Trigonometric, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(Shard(GetFunctionTestsTrigonometric()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNetFunctions, 3) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(Shard(GetFunctionTestsNet()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDateTimeFunctionsDateDiffFormat,
               1) {
  // Note that date part arguments (DAY, WEEK, etc.) are required to
  // be date part names (not quoted literals) or NULL in the SQL syntax.
  // Parameters are not allowed.  So several functions in this file that
  // generate function calls with date part arguments must handle them
  // specially.
  auto date_diff_format_fct = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(3, f.params.num_params());
    return absl::Substitute("$0(@p0, @p1, $1)", f.function_name,
                            f.params.param(2).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(
          GetFunctionTestsDateAndTimestampDiff(), /*date_part_param_idx=*/2)),
      date_diff_format_fct);
}

// Even with 500 query shards, this test takes a long time relative to the
// others. The number of shards for this case is picked so that each shard has
// ~300 queries.
SHARDED_TEST_F(ComplianceCodebasedTests, TestDateTimeFunctionsExtractFormat,
               33) {
  auto extract_format_fct = [](const FunctionTestCall& f) {
    if (f.params.num_params() != 2 && f.params.num_params() != 3) {
      ZETASQL_LOG(FATAL) << "Unexpected number of parameters: "
                 << f.params.num_params();
    } else {
      const std::string& date_part_sql = f.params.param(1).string_value();
      if (f.params.num_params() == 2) {
        return absl::Substitute("$0($1 FROM @p0)", f.function_name,
                                date_part_sql);
      } else {
        ZETASQL_CHECK_EQ(f.params.num_params(), 3);
        return absl::Substitute("$0($1 FROM @p0 AT TIME ZONE @p2)",
                                f.function_name, date_part_sql);
      }
    }
  };

  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsExtractFrom(),
                                                /*date_part_param_idx=*/1)),
      extract_format_fct);
}

// Even with 500 query shards, this test takes a long time relative to the
// others. The number of shards for this case is picked so that each shard has
// ~300 queries.
SHARDED_TEST_F(ComplianceCodebasedTests, TestDateTimeFunctions_Standard, 12) {
  RunFunctionCalls(Shard(GetFunctionTestsDateTimeStandardFunctionCalls()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDateTimeFunctions_Conversion, 1) {
  // Always call Shard() inside a SHARDED_TEST_F().
  RunFunctionCalls(Shard(GetFunctionTestsTimestampConversion()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestTimestampFromDate, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsTimestampFromDate()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDateAddSubFunctions, 1) {
  auto date_diff_format_fct = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(3, f.params.num_params());
    return absl::Substitute("$0(@p0, INTERVAL @p1 $1)", f.function_name,
                            f.params.param(2).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsDateAddSub(),
                                                /*date_part_param_idx=*/2)),
      date_diff_format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDateMathOperators, 1) {
  const auto& add_tests =
      GetFunctionTestsDateArithmetics(GetFunctionTestsDateAdd());
  RunStatementTests(Shard(add_tests), "@p0 + @p1");
  RunStatementTests(Shard(add_tests), "@p1 + @p0");
  RunStatementTests(
      Shard(GetFunctionTestsDateArithmetics(GetFunctionTestsDateSub())),
      "@p0 - @p1");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestFromProto3Conversions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsFromProto()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestFromProto3TimeOfDay, 1) {
  RunStatementTests(Shard(GetFunctionTestsFromProto3TimeOfDay()),
                    "from_proto(@p0)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestToProto3Conversions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsToProto()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestToProto3TimeOfDay, 1) {
  RunStatementTests(Shard(GetFunctionTestsToProto3TimeOfDay()),
                    "to_proto(@p0)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDatetimeAddSubFunctions, 2) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  auto datetime_add_sub = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(3, f.params.num_params());
    return absl::Substitute("$0(@p0, INTERVAL @p1 $1)", f.function_name,
                            f.params.param(2).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(
          GetFunctionTestsDatetimeAddSub(), /*date_part_param_idx=*/2)),
      datetime_add_sub);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDatetimeDiffFunctions, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  auto datetime_diff = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(3, f.params.num_params());
    return absl::Substitute("$0(@p0, @p1, $1)", f.function_name,
                            f.params.param(2).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsDatetimeDiff(),
                                                /*date_part_param_idx=*/2)),
      datetime_diff);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestLastDayFunctions, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  auto last_day = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ("last_day", f.function_name);
    return absl::Substitute(
        "$0(@p0, $1)", f.function_name,
        functions::DateTimestampPartToSQL(f.params.param(1).enum_value()));
  };
  RunFunctionTestsCustom(
      Shard(WrapFeatureLastDay(GetFunctionTestsLastDay())), last_day);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDateTruncFunctions, 1) {
  auto date_trunc = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ("date_trunc", f.function_name);
    ZETASQL_CHECK_EQ(2, f.params.num_params());
    // We can't have a null DatePart.
    ZETASQL_CHECK(!f.params.param(1).is_null());
    return absl::Substitute("date_trunc(@p0, $0)",
                            f.params.param(1).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsDateTrunc(),
                                                /*date_part_param_idx=*/1)),
      date_trunc);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestDatetimeTruncFunctions, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  auto datetime_trunc = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(2, f.params.num_params());
    return absl::Substitute("$0(@p0, $1)", f.function_name,
                            f.params.param(1).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsDatetimeTrunc(),
                                                /*date_part_param_idx=*/1)),
      datetime_trunc);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestTimeAddSubFunctions, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  auto time_add_sub = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(3, f.params.num_params());
    return absl::Substitute("$0(@p0, INTERVAL @p1 $1)", f.function_name,
                            f.params.param(2).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsTimeAddSub(),
                                                /*date_part_param_idx=*/2)),
      time_add_sub);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestTimeDiffFunctions, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  auto time_diff = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(3, f.params.num_params());
    return absl::Substitute("$0(@p0, @p1, $1)", f.function_name,
                            f.params.param(2).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsTimeDiff(),
                                                /*date_part_param_idx=*/2)),
      time_diff);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestTimeTruncFunctions, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  auto time_trunc = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(2, f.params.num_params());
    return absl::Substitute("$0(@p0, $1)", f.function_name,
                            f.params.param(1).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(GetFunctionTestsTimeTrunc(),
                                                /*date_part_param_idx=*/1)),
      time_trunc);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestTimestampAddSubFunctions, 1) {
  auto timestamp_diff_format_fct = [](const FunctionTestCall& f) {
    ZETASQL_CHECK_EQ(3, f.params.num_params());
    return absl::Substitute("$0(@p0, INTERVAL @p1 $1)", f.function_name,
                            f.params.param(2).string_value());
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(
          GetFunctionTestsTimestampAddSub(), /*date_part_param_idx=*/2)),
      timestamp_diff_format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestTimestampTruncFunctions, 1) {
  auto timestamp_trunc_format_fct = [](const FunctionTestCall& f) {
    if (f.params.num_params() != 2 && f.params.num_params() != 3) {
      ZETASQL_LOG(FATAL) << "Unexpected number of parameters: "
                 << f.params.num_params();
    } else {
      absl::string_view date_part_sql = f.params.param(1).string_value();
      if (f.params.num_params() == 2) {
        return absl::Substitute("$0(@p0, $1)", f.function_name, date_part_sql);
      } else {
        ZETASQL_CHECK_EQ(f.params.num_params(), 3);
        return absl::Substitute("$0(@p0, $1, @p2)", f.function_name,
                                date_part_sql);
      }
    }
  };
  RunFunctionTestsCustom(
      Shard(ConvertDateTimePartBindingsToString(
          GetFunctionTestsTimestampTrunc(), /*date_part_param_idx=*/1)),
      timestamp_trunc_format_fct);
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestTimestampConversionFunctions, 1) {
  // Always call Shard() inside a SHARDED_TEST_F().
  RunFunctionCalls(Shard(GetFunctionTestsTimestampConversion()));
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestCivilTimeConstructionFunctions_Date, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  RunFunctionCalls(Shard(GetFunctionTestsDateConstruction()));
}

// Date is considered a "civil time" type, and the reference implementation now
// supports it with civil time code, but it predates FEATURE_V_1_2_CIVIL_TIME.
SHARDED_TEST_F(ComplianceCodebasedTests,
               TestCivilTimeConstructionFunctions_DateFromTimestamp, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsDateFromTimestamp()));
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestCivilTimeConstructionFunctions_Time, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  RunFunctionCalls(Shard(GetFunctionTestsTimeConstruction()));
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestCivilTimeConstructionFunctions_DateTime, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  RunFunctionCalls(Shard(GetFunctionTestsDatetimeConstruction()));
}

SHARDED_TEST_F(ComplianceCodebasedTests,
                 TestCivilTimeConversionFunctions_Date, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  RunFunctionCalls(Shard(GetFunctionTestsConvertDatetimeToTimestamp()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestCivilTimeConversionFunctions_Time,
               1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  RunFunctionCalls(Shard(GetFunctionTestsConvertTimestampToTime()));
}

SHARDED_TEST_F(ComplianceCodebasedTests,
               TestCivilTimeConversionFunctions_DateTime, 1) {
  if (!DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    SkipAllShards();
    return;
  }
  RunFunctionCalls(Shard(GetFunctionTestsConvertTimestampToDatetime()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestOctetLengthFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsOctetLength()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestAsciiFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsAscii()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestUnicodeFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsUnicode()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestChrFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsChr()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringFunctions, 2) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(Shard(GetFunctionTestsString()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringSubstringFunctions, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(
      Shard(WrapFeatureAdditionalStringFunctions(GetFunctionTestsSubstring())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringSoundexFunctions, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(
      Shard(WrapFeatureAdditionalStringFunctions(GetFunctionTestsSoundex())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringTranslateFunctions, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(Shard(
      WrapFeatureAdditionalStringFunctions(GetFunctionTestsTranslate())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringInstrFunctions1, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(
      Shard(WrapFeatureAdditionalStringFunctions(GetFunctionTestsInstr1())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringInstrFunctions2, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(
      Shard(WrapFeatureAdditionalStringFunctions(GetFunctionTestsInstr2())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringInstrFunctions3, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(
      Shard(WrapFeatureAdditionalStringFunctions(GetFunctionTestsInstr3())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringInitCapFunctions, 1) {
  // No need to set PREFIX, RunFunctionCalls() will do it.
  RunFunctionCalls(
      Shard(WrapFeatureAdditionalStringFunctions(GetFunctionTestsInitCap())));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestStringParseNumericFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsParseNumeric()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestArrayFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsArray()));
}

// Six way sharding puts each shard at ~100 queries as of Q1'17.
SHARDED_TEST_F(ComplianceCodebasedTests, TestFormatFunction, 6) {
  SetNamePrefix("Format");
  RunFunctionCalls(Shard(GetFunctionTestsFormat()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestNormalizeFunctions, 1) {
  RunNormalizeFunctionCalls(Shard(GetFunctionTestsNormalize()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBase32Functions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsBase32()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestBase64Functions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsBase64()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestHexFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsHex()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestReverseFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsReverse()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, TestCodePointsFunctions, 1) {
  RunFunctionCalls(Shard(GetFunctionTestsCodePoints()));
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalCtor, 1) {
  SetNamePrefix("IntervalConstructor");
  auto interval_ctor = [](const FunctionTestCall& f) {
    ZETASQL_CHECK(f.function_name.empty());
    return absl::Substitute("INTERVAL @p0 $0",
                            f.params.param(1).string_value());
  };
  RunFunctionTestsCustom(Shard(GetFunctionTestsIntervalConstructor()),
                         interval_ctor);
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalComparisons, 1) {
  SetNamePrefix("IntervalComparisons");
  auto cmp = [](const FunctionTestCall& f) {
    // Test cases only use "=" and "<", but we test all comparison operators
    // for given pair of values.
    if (f.function_name == "=") {
      return "@p0 = @p1 AND @p1 = @p0 AND @p0 <= @p1 AND @p0 >= @p1 AND "
             "NOT(@p0 != @p1) AND NOT(@p0 < @p1) AND NOT(@p0 > @p1)";
    } else if (f.function_name == "<") {
      return "@p0 < @p1 AND @p1 > @p0 AND @p0 <= @p1 AND @p0 != @p1 AND "
             "NOT(@p0 > @p1) AND NOT(@p0 >= @p1) AND NOT(@p0 = @p1)";
    } else {
      ZETASQL_LOG(FATAL) << f.function_name;
    }
  };
  RunFunctionTestsCustom(Shard(GetFunctionTestsIntervalComparisons()), cmp);
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalUnaryMinus, 1) {
  SetNamePrefix("IntervalUnaryMinus");
  RunStatementTests(Shard(GetFunctionTestsIntervalUnaryMinus()), "-@p0");
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalDateTimestampSubtractions, 1) {
  SetNamePrefix("IntervalDatetimeSubtractions");
  RunStatementTests(Shard(GetDateTimestampIntervalSubtractions()),
                    "CAST(@p0 - @p1 AS STRING)");
  RunStatementTests(Shard(GetDatetimeTimeIntervalSubtractions()),
                    "CAST(@p0 - @p1 AS STRING)");
  // Symmetric test cases
  RunStatementTests(Shard(GetDateTimestampIntervalSubtractions()),
                    "CAST(-(@p1 - @p0) AS STRING)");
  RunStatementTests(Shard(GetDatetimeTimeIntervalSubtractions()),
                    "CAST(-(@p1 - @p0) AS STRING)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, DateTimestampAddSubInterval, 1) {
  SetNamePrefix("DateTimestampAddSubInterval");
  // Timestamp
  RunStatementTests(Shard(GetTimestampAddSubInterval()), "@p0 + @p1");
  // Test subtraction through negation
  RunStatementTests(Shard(GetTimestampAddSubInterval()), "@p0 - (-@p1)");
  // // Datetime and Date
  RunStatementTests(Shard(GetDatetimeAddSubInterval()), "@p0 + @p1");
  // // Test subtraction through negation
  RunStatementTests(Shard(GetDatetimeAddSubInterval()), "@p0 - (-@p1)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalAddInterval, 1) {
  SetNamePrefix("IntervalAddInterval");
  RunStatementTests(Shard(GetFunctionTestsIntervalAdd()), "@p0 + @p1");
  RunStatementTests(Shard(GetFunctionTestsIntervalAdd()), "@p1 + @p0");
  RunStatementTests(Shard(GetFunctionTestsIntervalAdd()), "@p0 - (-@p1)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalSubInterval, 1) {
  SetNamePrefix("IntervalSubInterval");
  RunStatementTests(Shard(GetFunctionTestsIntervalSub()), "@p0 - @p1");
  RunStatementTests(Shard(GetFunctionTestsIntervalSub()), "@p0 + (-@p1)");
  RunStatementTests(Shard(GetFunctionTestsIntervalSub()), "-(@p1 - @p0)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalMultiplyInt64, 1) {
  SetNamePrefix("IntervalMultiplyInt64");
  RunStatementTests(Shard(GetFunctionTestsIntervalMultiply()), "@p0 * @p1");
  RunStatementTests(Shard(GetFunctionTestsIntervalMultiply()), "@p1 * @p0");
  RunStatementTests(Shard(GetFunctionTestsIntervalMultiply()),
                    "(-@p0) * (-@p1)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, IntervalDivideInt64, 1) {
  SetNamePrefix("IntervalDivideInt64");
  RunStatementTests(Shard(GetFunctionTestsIntervalDivide()), "@p0 / @p1");
  RunStatementTests(Shard(GetFunctionTestsIntervalDivide()), "(-@p0) / (-@p1)");
}

SHARDED_TEST_F(ComplianceCodebasedTests, ExtractFromInterval, 1) {
  SetNamePrefix("ExtractFromInterval");
  auto extract_fn = [](const QueryParamsWithResult& p) {
    return absl::Substitute("EXTRACT($0 FROM @p0)", p.param(1).string_value());
  };
  RunStatementTestsCustom(Shard(GetFunctionTestsExtractInterval()), extract_fn);
}

SHARDED_TEST_F(ComplianceCodebasedTests, JustifyInterval, 1) {
  SetNamePrefix("JustifyInterval");
  auto fn = [](const FunctionTestCall& f) {
    // Test cases expect result to be interval converted to string to compare
    // exact datetime parts.
    return absl::Substitute("CAST($0(@p0) AS STRING)", f.function_name);
  };
  RunFunctionTestsCustom(Shard(GetFunctionTestsJustifyInterval()), fn);
}

// Wrap the proto field test cases with civil time typed values. If the type of
// the parameter or the result of the test case is TIME or DATETIME, wrap the
// test case so that the original expected result requires
// FEATURE_V_1_2_CIVIL_TIME in the feature set, and an empty feature set should
// expect to result in an error.
ABSL_MUST_USE_RESULT static const std::vector<QueryParamsWithResult>
WrapProtoFieldTestCasesForCivilTime(
    const std::vector<QueryParamsWithResult>& original_results) {
  std::vector<QueryParamsWithResult> results(original_results);
  for (auto& each : results) {
    // There's only one parameter for each ProtoField test case.
    ZETASQL_CHECK_EQ(1, each.num_params());
    // Wrap the result when there is only one result in the test case and the
    // feature set in that result is empty.
    if (each.results().size() == 1 &&
        zetasql_base::ContainsKey(each.results(),
                         QueryParamsWithResult::kEmptyFeatureSet)) {
      if (each.param(0).type()->UsingFeatureV12CivilTimeType() ||
          each.result().type()->UsingFeatureV12CivilTimeType()) {
        QueryParamsWithResult::ResultMap wrapped_results = {
            {QueryParamsWithResult::kEmptyFeatureSet,
             QueryParamsWithResult::Result(Value::Null(each.result().type()),
                                           absl::StatusCode::kInvalidArgument)},
            {{FEATURE_V_1_2_CIVIL_TIME},
             QueryParamsWithResult::Result(each.result(), each.status())},
        };
        each.set_results(wrapped_results);
      }
    }
  }
  return results;
}

void ComplianceCodebasedTests::TestProtoFieldImpl(
    const Value& null_value, const Value& empty_value,
    const Value& filled_value, const std::string& proto_name,
    const std::string& field_name, const ValueConstructor& expected_default,
    const ValueConstructor& expected_filled_value,
    const absl::Status& expected_status, const TestProtoFieldOptions& options) {
  ZETASQL_LOG(INFO) << "TestProtoFieldImpl " << field_name;
  const std::string type_for_prefix =
      absl::StrCat(Type::TypeKindToString(expected_filled_value.type()->kind(),
                                          PRODUCT_INTERNAL),
                   ":", proto_name, "::", field_name, ":");

  // Get the proto field on a NULL proto, a mostly empty proto, and a proto
  // with the values filled in.
  SetNamePrefix(absl::StrCat("GetProtoField_", type_for_prefix));
  RunStatementTests(
      WrapProtoFieldTestCasesForCivilTime(
          {{{null_value}, Value::Null(expected_filled_value.type())},
           {{empty_value}, expected_default.get()},
           {{filled_value}, expected_filled_value, expected_status}}),
      absl::StrCat("@p0.", field_name));

  // has_<field> exists on all non-repeated fields.
  if (!expected_default.type()->IsArray()) {
    // Read has_<field> on a NULL proto, a mostly empty proto, and a proto
    // with the values filled in.
    SetNamePrefix(absl::StrCat("HasProtoField_", type_for_prefix));
    RunStatementTests(
        WrapProtoFieldTestCasesForCivilTime(
            {{{null_value}, NullBool()},
             {{empty_value}, Bool(options.expected_has_result_in_empty_value)},
             {{filled_value}, Bool(true)}}),
        absl::StrCat("@p0.has_", field_name));
  }

  if (options.test_build_proto_field) {
    // Build a proto with NEW, setting this field, and using either
    // a NULL value or the value from <expected_filled_value>, which should
    // result in the same proto as <filled_value>.  This is the inverse of
    // the GetProtoField operation above.
    SetNamePrefix(absl::StrCat("BuildProtoField_", type_for_prefix));
    RunStatementTests(
        WrapProtoFieldTestCasesForCivilTime({
            {{Value::Null(expected_filled_value.type())}, empty_value},
            {{expected_filled_value}, filled_value},
        }),
        absl::StrCat("NEW zetasql_test__.", proto_name,
                     "(1 AS int64_key_1, "
                     "2 AS int64_key_2, @p0 AS ",
                     field_name, ")"));
  }
}

std::vector<std::function<void()>>
ComplianceCodebasedTests::GetProtoFieldTests() {
  zetasql_test__::KitchenSinkPB empty_proto;
  empty_proto.set_int64_key_1(1);
  empty_proto.set_int64_key_2(2);

  const Value empty_value = MakeProtoValue(&empty_proto);
  const Value null_value = Value::Null(empty_value.type());

  zetasql_test__::CivilTimeTypesSinkPB empty_civil_time_proto;
  empty_civil_time_proto.set_int64_key_1(1);
  empty_civil_time_proto.set_int64_key_2(2);

  const Value empty_civil_time_value = MakeProtoValue(&empty_civil_time_proto);
  const Value null_civil_time_value =
      Value::Null(empty_civil_time_value.type());

  const absl::Status ok_status = absl::OkStatus();
  const absl::Status error_status(absl::StatusCode::kOutOfRange,
                                  "Corrupted protocol buffer");

  // Options to TestProtoFieldImpl.  Because COLLECT_TEST() converts TEST_*()
  // macros to lambda functions, and the lambda functions capture 'options' by
  // value, changes of options inside a TEST_*() are confined within the lamdba
  // function.
  TestProtoFieldOptions options;

  std::vector<std::function<void(void)>> all_functions;

// The following marco converts TEST_*() macros to lambdas and collects them
// into the all_functions list.
#define COLLECT_TEST(f) all_functions.push_back([=]() mutable f)

// The following macros test various fields on protos. The fields with regular
// types and civil time types are wrapped in different proto messages
// (KitchenSinkPB and CivilTimeTypesSinkPB) thus requires the test to work on
// these two proto messages, otherwise the test logic is similar and shared
// between them.

// Helper macro to test non-repeated fields on a proto.
#define TEST_FIELD_ON_PROTO(proto_name, empty_proto, empty_value, null_value, \
                            field_name, setter_value, expected_default,       \
                            expected_value, expected_status)                  \
  {                                                                           \
    zetasql_test__::proto_name filled_proto = empty_proto;                    \
    filled_proto.set_##field_name(setter_value);                              \
    const Value filled_value = MakeProtoValue(&filled_proto);                 \
    TestProtoFieldImpl(null_value, empty_value, filled_value, #proto_name,    \
                       #field_name, expected_default, expected_value,         \
                       expected_status, options);                             \
  }
#define TEST_FIELD(field_name, setter_value, expected_default, expected_value) \
  TEST_FIELD_ON_PROTO(KitchenSinkPB, empty_proto, empty_value, null_value,     \
                      field_name, setter_value, expected_default,              \
                      expected_value, ok_status)
#define TEST_CIVIL_TIME_FIELD(field_name, setter_value, expected_default, \
                              expected_value)                             \
  TEST_FIELD_ON_PROTO(CivilTimeTypesSinkPB, empty_civil_time_proto,       \
                      empty_civil_time_value, null_civil_time_value,      \
                      field_name, setter_value, expected_default,         \
                      expected_value, ok_status);
#define TEST_CIVIL_TIME_FIELD_INVALID_VALUE(field_name, setter_value,    \
                                            expected_default)            \
  {                                                                      \
    Value expected_invalid_value = Value::Null(expected_default.type()); \
    options.test_build_proto_field = false;                              \
    TEST_FIELD_ON_PROTO(CivilTimeTypesSinkPB, empty_civil_time_proto,    \
                        empty_civil_time_value, null_civil_time_value,   \
                        field_name, setter_value, expected_default,      \
                        expected_invalid_value, error_status);           \
  }

// Helper macros to test repeated fields on a proto
#define TEST_REPEATED_FIELD_ON_PROTO(proto_name, empty_proto, empty_value, \
                                     null_value, field_name, setter_value, \
                                     element_value, expected_status)       \
  {                                                                        \
    zetasql_test__::proto_name filled_proto = empty_proto;                 \
    filled_proto.add_##field_name(setter_value);                           \
    filled_proto.add_##field_name(setter_value);                           \
    const Value filled_value = MakeProtoValue(&filled_proto);              \
    const ArrayType* array_type =                                          \
        test_values::MakeArrayType(element_value.type());                  \
    TestProtoFieldImpl(                                                    \
        null_value, empty_value, filled_value, #proto_name, #field_name,   \
        Value::EmptyArray(array_type),                                     \
        Value::Array(array_type, {element_value, element_value}),          \
        expected_status, options);                                         \
  }
#define TEST_REPEATED_FIELD(field_name, setter_value, element_value)    \
  TEST_REPEATED_FIELD_ON_PROTO(KitchenSinkPB, empty_proto, empty_value, \
                               null_value, field_name, setter_value,    \
                               element_value, ok_status)
#define TEST_REPEATED_CIVIL_TIME_FIELD(field_name, setter_value,              \
                                       element_value)                         \
  TEST_REPEATED_FIELD_ON_PROTO(CivilTimeTypesSinkPB, empty_civil_time_proto,  \
                               empty_civil_time_value, null_civil_time_value, \
                               field_name, setter_value, element_value,       \
                               ok_status)
#define TEST_REPEATED_CIVIL_TIME_FIELD_INVALID_VALUE(field_name, setter_value, \
                                                     invalid_value)            \
  {                                                                            \
    options.test_build_proto_field = false;                                    \
    TEST_REPEATED_FIELD_ON_PROTO(CivilTimeTypesSinkPB, empty_civil_time_proto, \
                                 empty_civil_time_value,                       \
                                 null_civil_time_value, field_name,            \
                                 setter_value, invalid_value, error_status);   \
  }

// Helper macros to test non-repeated nested message fields on a proto
#define TEST_MESSAGE_FIELD_ON_PROTO(proto_name, empty_proto, empty_value,      \
                                    null_value, field_name, field_value,       \
                                    expected_status)                           \
  {                                                                            \
    const Value wrapped_field_value = MakeProtoValue(&field_value);            \
    zetasql_test__::proto_name filled_proto = empty_proto;                     \
    *filled_proto.mutable_##field_name() = field_value;                        \
    TestProtoFieldImpl(null_value, empty_value, MakeProtoValue(&filled_proto), \
                       #proto_name, #field_name,                               \
                       Value::Null(wrapped_field_value.type()),                \
                       wrapped_field_value, expected_status, options);         \
  }
#define TEST_MESSAGE_FIELD(field_name, field_value)                    \
  TEST_MESSAGE_FIELD_ON_PROTO(KitchenSinkPB, empty_proto, empty_value, \
                              null_value, field_name, field_value, ok_status)
#define TEST_MESSAGE_CIVIL_TIME_FIELD(field_name, field_value)               \
  TEST_MESSAGE_FIELD_ON_PROTO(CivilTimeTypesSinkPB, empty_civil_time_proto,  \
                              empty_civil_time_value, null_civil_time_value, \
                              field_name, field_value, ok_status)

// Helper macros to test repeated nested message fields on a proto
#define TEST_REPEATED_MESSAGE_FIELD(field_name, element_value)          \
  {                                                                     \
    const Value wrapped_element_value = MakeProtoValue(&element_value); \
    zetasql_test__::KitchenSinkPB filled_proto = empty_proto;           \
    *filled_proto.add_##field_name() = element_value;                   \
    *filled_proto.add_##field_name() = element_value;                   \
    const ArrayType* array_type =                                       \
        test_values::MakeArrayType(wrapped_element_value.type());       \
    TestProtoFieldImpl(                                                 \
        null_value, empty_value, MakeProtoValue(&filled_proto),         \
        "KitchenSinkPB", #field_name, Value::EmptyArray(array_type),    \
        Value::Array(array_type,                                        \
                     {wrapped_element_value, wrapped_element_value}),   \
        ok_status, options);                                            \
  }

  // Test basic scalar types.
  COLLECT_TEST(TEST_FIELD(int32_val, 12, Value::Int32(77), Value::Int32(12)));
  COLLECT_TEST(
      TEST_FIELD(uint32_val, 12, Value::Uint32(777), Value::Uint32(12)));
  COLLECT_TEST(TEST_FIELD(int64_val, 12, Value::Int64(0), Value::Int64(12)));
  COLLECT_TEST(TEST_FIELD(uint64_val, 12, Value::Uint64(0), Value::Uint64(12)));
  COLLECT_TEST(TEST_FIELD(string_val, "sss", Value::String("default_name"),
                          Value::String("sss")));
  COLLECT_TEST(
      TEST_FIELD(float_val, 12.5, Value::Float(0), Value::Float(12.5)));
  COLLECT_TEST(
      TEST_FIELD(double_val, 12.5, Value::Double(0), Value::Double(12.5)));
  COLLECT_TEST(
      TEST_FIELD(bytes_val, "bbb", Value::Bytes(""), Value::Bytes("bbb")));
  COLLECT_TEST(TEST_FIELD(bool_val, true, Value::Bool(0), Value::Bool(12)));
  COLLECT_TEST(
      TEST_FIELD(fixed32_val, 12, Value::Uint32(0), Value::Uint32(12)));
  COLLECT_TEST(
      TEST_FIELD(fixed64_val, 12, Value::Uint64(0), Value::Uint64(12)));
  COLLECT_TEST(TEST_FIELD(sfixed32_val, 12, Value::Int32(0), Value::Int32(12)));
  COLLECT_TEST(TEST_FIELD(sfixed64_val, 12, Value::Int64(0), Value::Int64(12)));
  COLLECT_TEST(TEST_FIELD(sint32_val, 12, Value::Int32(0), Value::Int32(12)));
  COLLECT_TEST(TEST_FIELD(sint64_val, 12, Value::Int64(0), Value::Int64(12)));

  // Negative integer values, which encode differently in protos.
  COLLECT_TEST(TEST_FIELD(int32_val, -12, Value::Int32(77), Value::Int32(-12)));
  COLLECT_TEST(TEST_FIELD(int64_val, -12, Value::Int64(0), Value::Int64(-12)));
  COLLECT_TEST(TEST_FIELD(sint32_val, -12, Value::Int32(0), Value::Int32(-12)));
  COLLECT_TEST(TEST_FIELD(sint64_val, -12, Value::Int64(0), Value::Int64(-12)));
  COLLECT_TEST(
      TEST_FIELD(sfixed32_val, -12, Value::Int32(0), Value::Int32(-12)));
  COLLECT_TEST(
      TEST_FIELD(sfixed64_val, -12, Value::Int64(0), Value::Int64(-12)));

  // Test a required field.  We already have 1 filled in to make it valid
  // so don't get a default.
  options.expected_has_result_in_empty_value = true;
  options.test_build_proto_field = false;
  COLLECT_TEST(TEST_FIELD(int64_key_1, 15, Value::Int64(1), Value::Int64(15)));
  options = TestProtoFieldOptions();

  // Test repeated basic scalars.
  COLLECT_TEST(TEST_REPEATED_FIELD(repeated_int32_val, 12, Value::Int32(12)));
  COLLECT_TEST(TEST_REPEATED_FIELD(repeated_uint32_val, 12, Value::Uint32(12)));
  COLLECT_TEST(TEST_REPEATED_FIELD(repeated_int64_val, 12, Value::Int64(12)));
  COLLECT_TEST(TEST_REPEATED_FIELD(repeated_uint64_val, 12, Value::Uint64(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_string_val, "abc", Value::String("abc")));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_bytes_val, "def", Value::Bytes("def")));
  COLLECT_TEST(TEST_REPEATED_FIELD(repeated_bool_val, true, Value::Bool(true)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_fixed32_val, 12, Value::Uint32(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_fixed64_val, 12, Value::Uint64(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sfixed32_val, -12, Value::Int32(-12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sfixed64_val, -12, Value::Int64(-12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sint32_val, -12, Value::Int32(-12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sint64_val, -12, Value::Int64(-12)));

  // Test packed repeated scalars.
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_int32_packed, 12, Value::Int32(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_uint32_packed, 12, Value::Uint32(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_int64_packed, 12, Value::Int64(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_uint64_packed, 12, Value::Uint64(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_bool_packed, true, Value::Bool(true)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_fixed32_packed, 12, Value::Uint32(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_fixed64_packed, 12, Value::Uint64(12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sfixed32_packed, -12, Value::Int32(-12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sfixed64_packed, -12, Value::Int64(-12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sint32_packed, -12, Value::Int32(-12)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_sint64_packed, -12, Value::Int64(-12)));
  const EnumType* enum_type = test_values::MakeEnumType(
      zetasql_test__::TestEnum_descriptor());
  COLLECT_TEST(TEST_REPEATED_FIELD(
      repeated_enum_packed, zetasql_test__::TESTENUMNEGATIVE,
      Value::Enum(enum_type, zetasql_test__::TESTENUMNEGATIVE)));

  // Dates with default and alternate encodings, and as int32_t and int64_t,
  // and with different forms of the annotation.
  COLLECT_TEST(TEST_FIELD(date, 16101, Value::Date(0), Value::Date(16101)));
  COLLECT_TEST(TEST_FIELD(date, -16101, Value::Date(0), Value::Date(-16101)));
  COLLECT_TEST(TEST_FIELD(date64, 16101, Value::Date(0), Value::Date(16101)));
  COLLECT_TEST(TEST_FIELD(date64, -16101, Value::Date(0), Value::Date(-16101)));
  // The DATE_DECIMAL encoding treats 0 as NULL, so we get defaults of NULL.
  // That won't round trip when we write it back because we'll omit
  // writing a field for the NULL.
  COLLECT_TEST(TEST_FIELD(date_decimal, 20140131, Value::NullDate(),
                          Value::Date(16101)));
  options.test_build_proto_field = false;
  COLLECT_TEST(
      TEST_FIELD(date_decimal, 0, Value::NullDate(), Value::NullDate()));
  options = TestProtoFieldOptions();

  COLLECT_TEST(TEST_FIELD(date64_decimal, 20140131, Value::NullDate(),
                          Value::Date(16101)));
  COLLECT_TEST(TEST_FIELD(date_decimal_legacy, 20140131, Value::NullDate(),
                          Value::Date(16101)));
  COLLECT_TEST(TEST_FIELD(date64_decimal_legacy, 20140131, Value::NullDate(),
                          Value::Date(16101)));

  COLLECT_TEST(TEST_REPEATED_FIELD(repeated_date, 16101, Value::Date(16101)));
  COLLECT_TEST(TEST_REPEATED_FIELD(repeated_date64, 16101, Value::Date(16101)));

  // TEST_TIMESTAMP_FIELDS invokes TEST_FIELD on all variations of
  // the TIMESTAMP-typed fields.  This includes fields with both the
  // (zetasql.format) annotation as well as the deprecated
  // (zetasql.type) annotation at seconds, millis, and micros
  // precision.
#define TEST_TIMESTAMP_FIELDS_1(value_prefix)   \
  TEST_FIELD(timestamp_seconds, value_prefix,   \
             Value::TimestampFromUnixMicros(0), \
             Value::TimestampFromUnixMicros(value_prefix##000000))
#define TEST_TIMESTAMP_FIELDS_2(value_prefix)     \
  TEST_FIELD(timestamp_millis, value_prefix##123, \
             Value::TimestampFromUnixMicros(0),   \
             Value::TimestampFromUnixMicros(value_prefix##123000))
#define TEST_TIMESTAMP_FIELDS_3(value_prefix)        \
  TEST_FIELD(timestamp_micros, value_prefix##123456, \
             Value::TimestampFromUnixMicros(0),      \
             Value::TimestampFromUnixMicros(value_prefix##123456))
#define TEST_TIMESTAMP_FIELDS_4(value_prefix)        \
  TEST_FIELD(timestamp_seconds_format, value_prefix, \
             Value::TimestampFromUnixMicros(0),      \
             Value::TimestampFromUnixMicros(value_prefix##000000))
#define TEST_TIMESTAMP_FIELDS_5(value_prefix)            \
  TEST_FIELD(timestamp_millis_format, value_prefix##123, \
             Value::TimestampFromUnixMicros(0),          \
             Value::TimestampFromUnixMicros(value_prefix##123000))
#define TEST_TIMESTAMP_FIELDS_6(value_prefix)               \
  TEST_FIELD(timestamp_micros_format, value_prefix##123456, \
             Value::TimestampFromUnixMicros(0),             \
             Value::TimestampFromUnixMicros(value_prefix##123456))

  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_1(1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_2(1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_3(1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_4(1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_5(1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_6(1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_1(-1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_2(-1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_3(-1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_4(-1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_5(-1409338039));
  COLLECT_TEST(TEST_TIMESTAMP_FIELDS_6(-1409338039));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_timestamp_micros, 1409338039111222,
                          Value::TimestampFromUnixMicros(1409338039111222)));
  COLLECT_TEST(
      TEST_REPEATED_FIELD(repeated_timestamp_micros_format, 1409338039111222,
                          Value::TimestampFromUnixMicros(1409338039111222)));

  // Timestamps after 1970-01-01 are normal uint64_t values
  COLLECT_TEST(TEST_FIELD(timestamp_uint64, 1412453870568456,
                          Value::TimestampFromUnixMicros(0),
                          Value::TimestampFromUnixMicros(1412453870568456)));
  // Timestamps before 1970-01-01 are very large uint64_t values which are
  // bit casted to negative int64_t values
  COLLECT_TEST(TEST_FIELD(timestamp_uint64,
                          absl::bit_cast<uint64_t>(-1394820405112092),
                          Value::TimestampFromUnixMicros(0),
                          Value::TimestampFromUnixMicros(-1394820405112092)));

  // Timestamp with default before 1970-01-01
  COLLECT_TEST(TEST_FIELD(timestamp_uint64_default, 1,
                          // bit_cast equivalent to default 18446467286025782675
                          Value::TimestampFromUnixMicros(-276787683768941),
                          Value::TimestampFromUnixMicros(1)));

#define TEST_TIME_MICROS_FIELDS(value)                                         \
  {                                                                            \
    TEST_CIVIL_TIME_FIELD(                                                     \
        time_micros, value.Packed64TimeMicros(),                               \
        Value::Time(TimeValue::FromHMSAndMicros(0, 0, 0, 0)),                  \
        Value::Time(value));                                                   \
    TEST_CIVIL_TIME_FIELD(                                                     \
        time_micros_default, value.Packed64TimeMicros(),                       \
        Value::Time(TimeValue::FromHMSAndMicros(12, 34, 56, 654321)),          \
        Value::Time(value));                                                   \
    TEST_REPEATED_CIVIL_TIME_FIELD(                                            \
        repeated_time_micros, value.Packed64TimeMicros(), Value::Time(value)); \
  }
  if (DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    COLLECT_TEST(
        TEST_TIME_MICROS_FIELDS(TimeValue::FromHMSAndMicros(0, 0, 0, 0)));
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS(
        TimeValue::FromHMSAndMicros(23, 59, 59, 999999)));
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS(
        TimeValue::FromHMSAndMicros(12, 34, 56, 654321)));
  }

#define TEST_TIME_MICROS_FIELDS_INVALID_VALUE(value)                          \
  {                                                                           \
    TEST_CIVIL_TIME_FIELD_INVALID_VALUE(                                      \
        time_micros, value,                                                   \
        Value::Time(TimeValue::FromHMSAndMicros(0, 0, 0, 0)));                \
    TEST_CIVIL_TIME_FIELD_INVALID_VALUE(                                      \
        time_micros_default, value,                                           \
        Value::Time(TimeValue::FromHMSAndMicros(12, 34, 56, 654321)));        \
    TEST_REPEATED_CIVIL_TIME_FIELD_INVALID_VALUE(repeated_time_micros, value, \
                                                 Value::NullTime());          \
  }
  if (DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS_INVALID_VALUE(-1));
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS_INVALID_VALUE(0x1800000000));  // 24h
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS_INVALID_VALUE(0xF0000000));    // 60m
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS_INVALID_VALUE(0x3C00000));     // 60s
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS_INVALID_VALUE(0xF4240));  // 1M micros
    // The lower 37 bits actually decode to 12:34:56.654321, but there is
    // something in the higher, unused bits, making it invalid.
    COLLECT_TEST(TEST_TIME_MICROS_FIELDS_INVALID_VALUE(0x100000c8b89fbf1));
  }

#define TEST_DATETIME_MICROS_FIELDS(value)                                    \
  {                                                                           \
    TEST_CIVIL_TIME_FIELD(datetime_micros, value.Packed64DatetimeMicros(),    \
                          Value::NullDatetime(), Value::Datetime(value));     \
    TEST_CIVIL_TIME_FIELD(datetime_micros_default,                            \
                          value.Packed64DatetimeMicros(),                     \
                          Value::Datetime(DatetimeValue::FromYMDHMSAndMicros( \
                              1970, 1, 1, 0, 0, 0, 0)),                       \
                          Value::Datetime(value));                            \
    TEST_REPEATED_CIVIL_TIME_FIELD(repeated_datetime_micros,                  \
                                   value.Packed64DatetimeMicros(),            \
                                   Value::Datetime(value));                   \
  }

  if (DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    COLLECT_TEST(TEST_DATETIME_MICROS_FIELDS(
        DatetimeValue::FromYMDHMSAndMicros(1, 1, 1, 0, 0, 0, 0)));
    COLLECT_TEST(TEST_DATETIME_MICROS_FIELDS(
        DatetimeValue::FromYMDHMSAndMicros(9999, 12, 31, 23, 59, 59, 999999)));
    COLLECT_TEST(TEST_DATETIME_MICROS_FIELDS(
        DatetimeValue::FromYMDHMSAndMicros(1970, 1, 1, 0, 0, 0, 0)));
    COLLECT_TEST(TEST_DATETIME_MICROS_FIELDS(
        DatetimeValue::FromYMDHMSAndMicros(2016, 2, 16, 13, 35, 57, 456789)));
  }

#define TEST_DATETIME_MICROS_FIELDS_INVALID_VALUE(value)                      \
  {                                                                           \
    TEST_CIVIL_TIME_FIELD_INVALID_VALUE(datetime_micros,                      \
                                        value.Packed64DatetimeMicros(),       \
                                        Value::NullDatetime());               \
    TEST_CIVIL_TIME_FIELD_INVALID_VALUE(                                      \
        datetime_micros_default, value.Packed64DatetimeMicros(),              \
        Value::Datetime(                                                      \
            DatetimeValue::FromYMDHMSAndMicros(1970, 1, 1, 0, 0, 0, 0)));     \
    TEST_REPEATED_CIVIL_TIME_FIELD_INVALID_VALUE(                             \
        repeated_datetime_micros, value.Packed64DatetimeMicros(),             \
        Value::NullDatetime());                                               \
  }
  if (DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    COLLECT_TEST(TEST_DATETIME_MICROS_FIELDS_INVALID_VALUE(
        DatetimeValue::FromYMDHMSAndMicros(0, 0, 0, -1, -1, -1, -1)));
    COLLECT_TEST(TEST_DATETIME_MICROS_FIELDS_INVALID_VALUE(
        DatetimeValue::FromYMDHMSAndMicros(99999, 99, 99, 99, 99, 99,
                                           9999999)));
  }

  // Test enums.
  COLLECT_TEST(TEST_FIELD(test_enum, zetasql_test__::TESTENUM2,
                          Value::Enum(enum_type, zetasql_test__::TESTENUM0),
                          Value::Enum(enum_type, zetasql_test__::TESTENUM2)));
  COLLECT_TEST(TEST_REPEATED_FIELD(
      repeated_test_enum, zetasql_test__::TESTENUMNEGATIVE,
      Value::Enum(enum_type, zetasql_test__::TESTENUMNEGATIVE)));

  // Test nested proto.
  zetasql_test__::KitchenSinkPB::Nested nested;
  nested.set_nested_int64(1234);
  nested.add_nested_repeated_int64(55);
  nested.add_nested_repeated_int64(-66);
  COLLECT_TEST(TEST_MESSAGE_FIELD(nested_value, nested));
  COLLECT_TEST(TEST_REPEATED_MESSAGE_FIELD(nested_repeated_value, nested));

  // Test nested civil time proto
  if (DriverSupportsFeature(FEATURE_V_1_2_CIVIL_TIME)) {
    zetasql_test__::CivilTimeTypesSinkPB::NestedCivilTimeFields
        civil_time_nested;
    civil_time_nested.set_time_micros(
        TimeValue::FromHMSAndMicros(12, 34, 56, 654321).Packed64TimeMicros());
    civil_time_nested.set_datetime_micros(
        DatetimeValue::FromYMDHMSAndMicros(2016, 3, 3, 16, 29, 48, 456789)
            .Packed64DatetimeMicros());
    COLLECT_TEST(TEST_MESSAGE_CIVIL_TIME_FIELD(nested_civil_time_fields,
                                               civil_time_nested));

    zetasql_test__::CivilTimeTypesSinkPB::NestedCivilTimeRepeatedFields
        civil_time_nested_repeated;
    civil_time_nested_repeated.add_repeated_datetime_micros(
        TimeValue::FromHMSAndMicros(0, 0, 0, 0).Packed64TimeMicros());
    civil_time_nested_repeated.add_repeated_datetime_micros(
        TimeValue::FromHMSAndMicros(23, 59, 59, 999999).Packed64TimeMicros());
    civil_time_nested_repeated.add_repeated_datetime_micros(
        DatetimeValue::FromYMDHMSAndMicros(1970, 1, 1, 0, 0, 0, 0)
            .Packed64DatetimeMicros());
    COLLECT_TEST(TEST_MESSAGE_CIVIL_TIME_FIELD(
        nested_civil_time_repeated_fields, civil_time_nested_repeated));
  }

  // Test group.
  zetasql_test__::KitchenSinkPB::OptionalGroup optional_group;
  optional_group.set_int64_val(-123);
  optional_group.add_optionalgroupnested()->set_int64_val(555);
  COLLECT_TEST(TEST_MESSAGE_FIELD(optional_group, optional_group));

  zetasql_test__::KitchenSinkPB::NestedRepeatedGroup nrg;
  nrg.set_id(10);
  nrg.set_idstr("abc");
  COLLECT_TEST(TEST_REPEATED_MESSAGE_FIELD(nested_repeated_group, nrg));

  // Default handling.
  COLLECT_TEST(
      TEST_FIELD(int_with_no_default, 100, Value::Int64(0), Value::Int64(100)));
  COLLECT_TEST(
      TEST_FIELD(int_with_default, 100, Value::Int64(17), Value::Int64(100)));
  COLLECT_TEST(TEST_FIELD(int_with_no_default_nullable, 100, Value::NullInt64(),
                          Value::Int64(100)));
  COLLECT_TEST(TEST_FIELD(int_with_default_nullable, 100, Value::NullInt64(),
                          Value::Int64(100)));

  // These are annotated with is_struct and is_wrapper, but we treat them
  // as regular protos and don't unwrap them to zetasql types.
  zetasql_test__::NullableInt nullable_int;
  nullable_int.set_value(111);
  zetasql_test__::KeyValueStruct key_value_struct;;
  key_value_struct.set_key("aaa");
  COLLECT_TEST(TEST_MESSAGE_FIELD(nullable_int, nullable_int));
  COLLECT_TEST(TEST_MESSAGE_FIELD(key_value, key_value_struct));
  COLLECT_TEST(TEST_REPEATED_MESSAGE_FIELD(nullable_int_array, nullable_int));
  COLLECT_TEST(TEST_REPEATED_MESSAGE_FIELD(key_value_array, key_value_struct));

  COLLECT_TEST(TEST_FIELD(has_confusing_name, "abc", Value::String(""),
                          Value::String("abc")));
  // This field is actually called MIXED_case, but generated proto methods
  // are lowercased, and lower-case SQL works too.
  COLLECT_TEST(
      TEST_FIELD(mixed_case, "abc", Value::String(""), Value::String("abc")));

  zetasql_test__::EmptyMessage empty_message;
  COLLECT_TEST(TEST_MESSAGE_FIELD(empty_message, empty_message));

  return all_functions;
}

bool ComplianceCodebasedTests::DriverCanRunTests() {
  return !GetCodeBasedTestsEnvironment()->skip_codebased_tests();
}

// Test reading proto fields, reading has_<field>, and building protos with
// particular fields set, for various field types in KitchenSinkPB.
// Even with 500 query shards of this tests take a long time reletive to others.
// The number of shards for this case is picked so that each shard has ~100
// functions.
SHARDED_TEST_F(ComplianceCodebasedTests, TestProtoFields, 9) {
  for (const auto& function : Shard(GetProtoFieldTests())) {
    function();
  }
}


TEST_F(ComplianceCodebasedTests, TestWideStruct) {
  if (!DriverCanRunTests()) {
    return;
  }

  SetNamePrefix("TestWideStruct");
  int kWideFields = 1000;
  std::vector<StructField> fields;
  std::vector<Value> values;
  for (int i = 0; i < kWideFields; i++) {
    fields.push_back(StructField(absl::StrCat("x", i), Int64Type()));
    values.push_back(Value::Int64(i));
  }
  const StructType* struct_type = MakeStructType(fields);
  Value struct_value = Value::Struct(struct_type, values);
  // Query for entire STRUCT.
  RunStatementTests({QueryParamsWithResult({ValueConstructor(struct_value)},
                                           ValueConstructor(struct_value))},
                    "@p0");
  // Fetch first, last and middle fields.
  for (int i : {0, kWideFields - 1, kWideFields / 2}) {
    RunStatementTests(
        {QueryParamsWithResult({ValueConstructor(struct_value), i},
                               ValueConstructor(Value::Int64(i)))},
        absl::StrCat("@p0.x", i));
  }
}

TEST_F(ComplianceCodebasedTests, TestDeepStruct) {
  if (!DriverCanRunTests()) {
    return;
  }

  SetNamePrefix("TestDeepStruct");
  int kDeepFields = 20;

  StructField struct_field("x", Int64Type());
  const StructType* struct_type = nullptr;
  Value struct_value = Value::Int64(1);
  for (int i = 0; i < kDeepFields; i++) {
    struct_type = MakeStructType({struct_field});
    struct_value = Value::Struct(struct_type, {struct_value});
    struct_field = StructField("x", struct_type);
  }

  // Query for entire STRUCT.
  RunStatementTests({QueryParamsWithResult({ValueConstructor(struct_value)},
                                           ValueConstructor(struct_value))},
                    "@p0");

  // Query for every field in the chain.
  for (int i = 0; i < kDeepFields; i++) {
    Value field_value = struct_value;
    std::string field_refs;
    for (int j = 0; j < i; j++) {
      absl::StrAppend(&field_refs, ".x");
      field_value = field_value.field(0);
    }
    RunStatementTests(
        {QueryParamsWithResult({ValueConstructor(struct_value), i},
                               ValueConstructor(field_value))},
        absl::StrCat("@p0", field_refs));
  }
}

TEST_F(ComplianceCodebasedTests, TestDeepArray) {
  if (!DriverCanRunTests()) {
    return;
  }

  SetNamePrefix("TestDeepArray");
  int kDepth = 10;

  const ArrayType* array_type = MakeArrayType(Int64Type());
  Value array_value = Value::Array(array_type, {Value::Int64(1),
                                                Value::Int64(2)});
  for (int i = 0; i < kDepth; i++) {
    StructField struct_field("x", array_type);
    const StructType* struct_type = MakeStructType({struct_field});
    Value struct_value = Value::Struct(struct_type, {array_value});
    array_type = MakeArrayType(struct_type);
    array_value = Value::Array(array_type, {struct_value});
    // Query for entire ARRAY.
    RunStatementTests({QueryParamsWithResult({ValueConstructor(array_value)},
                                             ValueConstructor(array_value))},
                      "@p0");
  }

  SetNamePrefix("TestDeepArray_UNNEST");
  std::string q = "@p0";
  for (int i = 0; i < kDepth; i++) {
    q = absl::Substitute("(SELECT t.x FROM UNNEST($0) t)", q);
  }
  RunStatementTests(
      {QueryParamsWithResult(
          {ValueConstructor(array_value)},
          ValueConstructor(Value::Array(MakeArrayType(Int64Type()),
                                        {Value::Int64(1), Value::Int64(2)})))},
      q);
}

TEST_F(ComplianceCodebasedTests, TestRecursiveProto) {
  if (!DriverCanRunTests()) {
    return;
  }

  const int kDepth = 20;

  zetasql_test__::RecursiveMessage message;
  const ProtoType* proto_type =
      test_values::MakeProtoType(message.GetDescriptor());

  // NULL recursive message.
  SetNamePrefix("TestRecursiveProto_NULL");
  RunStatementTests(
      {QueryParamsWithResult({}, ValueConstructor(Value::Null(proto_type)))},
      "CAST(NULL AS zetasql_test__.RecursiveMessage)");

  // Add one more recursion level at every step.
  SetNamePrefix("TestRecursiveProto_Message");
  zetasql_test__::RecursiveMessage* innermost_message = &message;
  Value proto_value;
  for (int i = 0; i < kDepth; i++) {
    proto_value = Value::Proto(proto_type, SerializeToCord(message));
    RunStatementTests({QueryParamsWithResult({ValueConstructor(proto_value), i},
                                             ValueConstructor(proto_value))},
                      "@p0");

    innermost_message->set_int64_field(i);
    innermost_message = innermost_message->mutable_recursive_msg();
  }
  proto_value = Value::Proto(proto_type, SerializeToCord(message));

  // Select message subfields.
  SetNamePrefix("TestRecursiveProto_SubMessage");
  std::string field_refs;
  innermost_message = &message;
  for (int i = 0; i < kDepth; i++) {
    Value field_value =
        Value::Proto(proto_type, SerializeToCord(*innermost_message));
    RunStatementTests({QueryParamsWithResult({ValueConstructor(proto_value), i},
                                             ValueConstructor(field_value))},
                      absl::StrCat("@p0", field_refs));
    absl::StrAppend(&field_refs, ".recursive_msg");
    innermost_message = innermost_message->mutable_recursive_msg();
  }

  // Select message subfield at level deeper than what exists in the message
  // results in NULL.
  RunStatementTests(
      {QueryParamsWithResult({ValueConstructor(proto_value)},
                             ValueConstructor(Value::Null(proto_type)))},
      absl::StrCat("@p0", field_refs, ".recursive_msg"));

  // Select scalar field of message subfield.
  SetNamePrefix("TestRecursiveProto_ScalarField");
  field_refs.clear();
  innermost_message = &message;
  for (int i = 0; i < kDepth; i++) {
    RunStatementTests({QueryParamsWithResult({ValueConstructor(proto_value), i},
                                             Value::Int64(i))},
                      absl::StrCat("@p0", field_refs, ".int64_field"));
    absl::StrAppend(&field_refs, ".recursive_msg");
    innermost_message = innermost_message->mutable_recursive_msg();
  }
}

// Test that TABLESAMPLE queries are repeatable if given a REPEAT input.
TEST_F(ComplianceCodebasedTests, TablesampleRepeatableTests) {
  if (!DriverCanRunTests() || !DriverSupportsFeature(FEATURE_TABLESAMPLE)) {
    return;
  }

  auto test_query_is_repeatable = [&](const std::string& query) {
    auto reference_result = ExecuteStatementWithFeatures(
        query, {}, {LanguageFeature::FEATURE_TABLESAMPLE});
    auto repeated_result = ExecuteStatementWithFeatures(
        query, {}, {LanguageFeature::FEATURE_TABLESAMPLE});

    // An engine may fail to execute the query, in which case the test is
    // ignored.
    EXPECT_EQ(reference_result.status(), repeated_result.status());
    if (reference_result.ok()) {
      Value a = reference_result.value();
      Value b = repeated_result.value();
      EXPECT_EQ(a, b);
    }
  };

  SetNamePrefix("Tablesample");

  test_query_is_repeatable(R"(
    SELECT primary_key
    FROM TableLarge
    TABLESAMPLE BERNOULLI(25 PERCENT) REPEATABLE(0)
    ORDER BY primary_key
  )");
  test_query_is_repeatable(R"(
    SELECT primary_key
    FROM TableLarge
    TABLESAMPLE BERNOULLI(0 PERCENT) REPEATABLE(10)
    ORDER BY primary_key
  )");
  test_query_is_repeatable(R"(
    SELECT primary_key
    FROM TableLarge
    TABLESAMPLE BERNOULLI(80 PERCENT) REPEATABLE(11)
    ORDER BY primary_key
    LIMIT 1
  )");

  test_query_is_repeatable(R"(
    SELECT primary_key
    FROM TableLarge
    TABLESAMPLE RESERVOIR(2000 ROWS) REPEATABLE(3250)
    ORDER BY primary_key
  )");
  test_query_is_repeatable(R"(
    SELECT primary_key
    FROM TableLarge
    TABLESAMPLE RESERVOIR(5 ROWS) REPEATABLE(-532)
    ORDER BY primary_key
  )");
  test_query_is_repeatable(R"(
    SELECT primary_key
    FROM TableLarge
    TABLESAMPLE RESERVOIR(1 ROWS) REPEATABLE(32)
    ORDER BY primary_key
  )");
  test_query_is_repeatable(R"(
    SELECT primary_key
    FROM TableLarge
    TABLESAMPLE RESERVOIR(0 ROWS) REPEATABLE(1003932)
    ORDER BY primary_key
    LIMIT 1
  )");
}

TEST_F(ComplianceCodebasedTests, IntegerRoundtrip) {
  if (!DriverCanRunTests()) {
    return;
  }

  SetNamePrefix("IntegerRoundtrip");
  std::vector<QueryParamsWithResult> tests;
  // Note - the below transformation only works correctly with non-negative
  // integers.
  for (int64_t number : std::vector<int64_t>({
           0,
           1,
           22,
           300,
           4848,
           55555,
           601111,
           7771234,
           59483726,
           123456789,
           9876543210,
           122333444455555,
           888887777666554,
           999999999999999999,
           1000000000000000000,
           5647382910564738291,
           std::numeric_limits<int64_t>::max() - 1,
           std::numeric_limits<int64_t>::max(),
       })) {
    // After transformation the result should be exactly the same as original
    // number.
    tests.push_back(QueryParamsWithResult({number}, number));
  }

  RunStatementTests(
      tests,
      // 1. Convert number to string
      // 2. Split string into digits array
      // 3. Sort digits array in reverse order (by offset)
      // 4. Convert string digits to integer and multiply by 10^offset
      // 5. Sum them up
      // The resulting number should be the same as the original number.
      R"(
          (select sum(cast(d as INT64) * cast(pow(10, o) as INT64)) from
            unnest(array(
              select d from
                unnest(split(cast(@p0 as string), "")) d WITH OFFSET o
                order by o desc)) d with offset o)
      )");
}

class ComplianceFilebasedTests : public SQLTestBase {
 public:
  const std::string GetTestSuiteName() override {
    return "ComplianceFilebasedTests";
  }

  // Use File::Match(...) to return all test files matching
  // kTestFilenamePattern. The result will be fed into
  // INSTANTIATE_TEST_CASE_P(...) to instantiate all file-base tests.
  static std::vector<std::string> AllTestFiles() {
    if (!absl::GetFlag(FLAGS_zetasql_run_filebased_tests)) {
      return {};
    }
    std::vector<std::string> test_filenames;
    std::string file_path = zetasql_base::JoinPath(
        getenv("TEST_SRCDIR"), "com_google_zetasql/zetasql/compliance/testdata",
        absl::GetFlag(FLAGS_file_pattern));
    ZETASQL_CHECK_OK(zetasql::internal::Match(file_path, &test_filenames));
    ZETASQL_CHECK(!test_filenames.empty()) << "No test files found at " << file_path;
    return test_filenames;
  }

 protected:
  ComplianceFilebasedTests() {}
  ~ComplianceFilebasedTests() override {}
};
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(
    ComplianceFilebasedTests);  // TODO (broken link)

// All file-based tests.
TEST_P(ComplianceFilebasedTests, FilebasedTest) {
  RunSQLTests(GetParam());
}

namespace {
std::string NameForFilebasedTest(
    const ::testing::TestParamInfo<std::string>& info) {
  return absl::StrReplaceAll(zetasql_base::Basename(info.param), {{".", "_"}});
}
}  // namespace

INSTANTIATE_TEST_SUITE_P(
    FilebasedTest, ComplianceFilebasedTests,
    ::testing::ValuesIn(ComplianceFilebasedTests::AllTestFiles()),
    NameForFilebasedTest);
}  // namespace zetasql
