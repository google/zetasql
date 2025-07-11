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

#include "zetasql/analyzer/run_analyzer_test.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/base/logging.h"
#include "zetasql/base/enum_utils.h"
#include "zetasql/analyzer/analyzer_output_mutator.h"
#include "zetasql/analyzer/analyzer_test_options.h"
#include "zetasql/common/internal_analyzer_output_properties.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/common/unicode_utils.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/literal_remover.h"
#include "zetasql/public/multi_catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/prepared_expression_constant_evaluator.h"
#include "zetasql/public/proto/logging.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/simple_catalog_util.h"
#include "zetasql/public/sql_constant.h"
#include "zetasql/public/sql_formatter.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/public/testing/test_case_options_util.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/query_expression.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast.pb.h"
#include "zetasql/resolved_ast/resolved_ast_comparator.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/sql_builder.h"
#include "zetasql/resolved_ast/target_syntax.h"
#include "zetasql/resolved_ast/validator.h"
#include "zetasql/testdata/sample_annotation.h"
#include "zetasql/testdata/sample_catalog.h"
#include "zetasql/testdata/sample_system_variables.h"
#include "zetasql/testdata/special_catalog.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/flags/commandlineflag.h"
#include "absl/flags/flag.h"
#include "absl/flags/reflection.h"
#include "absl/functional/bind_front.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "file_based_test_driver/file_based_test_driver.h"
#include "file_based_test_driver/run_test_case_result.h"
#include "file_based_test_driver/test_case_options.h"
#include "google/protobuf/text_format.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(std::string, test_file, "", "location of test data file.");

namespace zetasql {

// TODO: Remove this or make it a SQLBuilder option.
// SQLBuilder that will output a qualified path for tables in nested catalogs.
// This depends on the table's FullName being that path, which is not generally
// required to be true by the zetasql::Table contract, but is true for the
// test tables we provide in this test (and is also true for clients that need
// perform this kind of override). This override is the only way that a
// resolved statement that involves tables that can only be found in a nested
// catalog can be outputted into a correct SQL string (meaning one that can be
// re-resolved correctly into the same or equivalent resolved statement).
class SQLBuilderWithNestedCatalogSupport : public SQLBuilder {
 public:
  explicit SQLBuilderWithNestedCatalogSupport(
      const SQLBuilderOptions& options = SQLBuilderOptions())
      : SQLBuilder(options) {}

  // Executes CheckFieldsAccessed() on the resolved node and then calls
  // SQLBuilder::GetSql(). By calling CheckFieldsAccessed() it emulates
  // ZETASQL_DEBUG_MODE behavior of GetSql() to reproduce ZETASQL_DEBUG_MODE only errors.
  absl::StatusOr<std::string> GetSql() {
    // If sql_ is present, then GetSql() was already called and popped up the
    // query fragment.
    if (!sql_.empty()) {
      return sql_;
    }

    // Emulate ZETASQL_DEBUG_MODE behavior in GetSql() by checking fields accessed.
    if (!query_fragments_.empty()) {
      ZETASQL_RETURN_IF_ERROR(query_fragments_.back()->node->CheckFieldsAccessed());
    }

    return SQLBuilder::GetSql();
  }

  std::string TableToIdentifierLiteral(const Table* table) override {
    std::string full_name = table->FullName();
    std::vector<std::string> split = absl::StrSplit(full_name, '.');
    return IdentifierPathToString(split, true);
  }
};

// Make the unittest fail with message <str>, and also return <str> so it
// can be used in the file-based output to make it easy to find.
static std::string AddFailure(absl::string_view str) {
  ADD_FAILURE() << str;
  return std::string(str);
}

static AllowedHintsAndOptions GetAllowedHintsAndOptions(
    TypeFactory* type_factory) {
  const std::string kQualifier = "qual";
  AllowedHintsAndOptions allowed(kQualifier);

  const zetasql::Type* enum_type;
  ZETASQL_CHECK_OK(type_factory->MakeEnumType(zetasql_test__::TestEnum_descriptor(),
                                      &enum_type));

  const zetasql::Type* extra_proto_type;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      zetasql_test__::TestExtraPB::descriptor(), &extra_proto_type));

  const zetasql::Type* enum_array_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(enum_type, &enum_array_type));

  const zetasql::Type* string_array_type;
  ZETASQL_CHECK_OK(
      type_factory->MakeArrayType(types::StringType(), &string_array_type));

  const zetasql::Type* struct_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      {{"a", types::Int32Type()}, {"b", enum_type}}, &struct_type));

  allowed.AddOption("int64_option", types::Int64Type());
  allowed.AddOption("int32_option", types::Int32Type());
  allowed.AddOption("string_option", types::StringType(),
                    /*allow_alter_array=*/true);
  allowed.AddOption("string_array_option", string_array_type);
  allowed.AddOption("date_option", types::DateType());
  allowed.AddOption("enum_option", enum_type);
  allowed.AddOption("proto_option", extra_proto_type);
  allowed.AddOption("struct_option", struct_type);
  allowed.AddOption("enum_array_option", enum_array_type);
  allowed.AddOption("untyped_option", nullptr);
  allowed.AddOption("string_array_allow_alter_option", string_array_type,
                    /*allow_alter_array=*/true);
  allowed.AddOption("enum_array_allow_alter_option", enum_array_type,
                    /*allow_alter_array=*/true);

  allowed.AddHint(kQualifier, "int64_hint", types::Int64Type());
  allowed.AddHint(kQualifier, "int32_hint", types::Int32Type());
  allowed.AddHint(kQualifier, "string_hint", types::StringType());
  allowed.AddHint(kQualifier, "string_array_hint", string_array_type);
  allowed.AddHint(kQualifier, "date_hint", types::DateType());
  allowed.AddHint(kQualifier, "enum_hint", enum_type);
  allowed.AddHint(kQualifier, "proto_hint", extra_proto_type);
  allowed.AddHint(kQualifier, "struct_hint", struct_type);
  allowed.AddHint(kQualifier, "enum_array_hint", enum_array_type);
  allowed.AddHint(kQualifier, "untyped_hint", nullptr);

  allowed.AddHint("", "unqual_hint", types::Uint32Type());
  allowed.AddHint(kQualifier, "qual_hint", types::Uint32Type());
  allowed.AddHint(kQualifier, "must_qual_hint", types::Uint32Type(),
                  false /* allow_unqualified */);

  allowed.AddHint("other_qual", "int32_hint", types::Int32Type(),
                  false /* allow_unqualified */);

  return allowed;
}

namespace {
class StripParseLocationsVisitor : public ResolvedASTVisitor {
 public:
  absl::Status DefaultVisit(const ResolvedNode* node) override {
    const_cast<ResolvedNode*>(node)->ClearParseLocationRange();
    return node->ChildrenAccept(this);
  }
};
}  // namespace

std::unique_ptr<ResolvedNode> StripParseLocations(const ResolvedNode* node) {
  ResolvedASTDeepCopyVisitor deep_copy_visitor;
  ZETASQL_CHECK_OK(node->Accept(&deep_copy_visitor));
  absl::StatusOr<std::unique_ptr<ResolvedNode>> copy =
      deep_copy_visitor.ConsumeRootNode<ResolvedNode>();
  ZETASQL_CHECK_OK(copy.status());
  StripParseLocationsVisitor strip_visitor;
  ZETASQL_CHECK_OK(copy.value()->Accept(&strip_visitor));
  return std::move(copy).value();
}

static std::string FormatTableResolutionTimeInfoMap(
    const absl::Status& status,
    const TableResolutionTimeInfoMap& table_resolution_time_info_map) {
  if (!status.ok()) {
    return absl::StrCat("ERROR: ", FormatError(status));
  }
  std::string result;
  for (const auto& entry : table_resolution_time_info_map) {
    absl::StrAppend(&result, IdentifierPathToString(entry.first), " => {\n");
    for (const TableResolutionTimeExpr& expr : entry.second.exprs) {
      const ResolvedExpr* resolved_expr =
          expr.analyzer_output_with_expr->resolved_expr();
      absl::StrAppend(&result,
                      absl::StripAsciiWhitespace(resolved_expr->DebugString()),
                      ";\n");
    }
    if (entry.second.has_default_resolution_time) {
      absl::StrAppend(&result, "DEFAULT;\n");
    }
    absl::StrAppend(&result, "}\n");
  }

  return result;
}

namespace {

// Copies the provided AnalyzerOutput to a new AnalyzerOutput.
absl::StatusOr<std::unique_ptr<AnalyzerOutput>> CopyAnalyzerOutput(
    const AnalyzerOutput& output) {
  ResolvedASTDeepCopyVisitor visitor;
  std::unique_ptr<AnalyzerOutput> ret;
  if (output.resolved_statement() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(output.resolved_statement()->Accept(&visitor));
    ret = std::make_unique<AnalyzerOutput>(
        output.id_string_pool(), output.arena(),
        *visitor.ConsumeRootNode<ResolvedStatement>(),
        output.analyzer_output_properties(),
        /*parser_output=*/nullptr, output.deprecation_warnings(),
        output.undeclared_parameters(),
        output.undeclared_positional_parameters(), output.max_column_id(),
        output.has_graph_references());
  } else if (output.resolved_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(output.resolved_expr()->Accept(&visitor));
    ret = std::make_unique<AnalyzerOutput>(
        output.id_string_pool(), output.arena(),
        *visitor.ConsumeRootNode<ResolvedExpr>(),
        output.analyzer_output_properties(),
        /*parser_output=*/nullptr, output.deprecation_warnings(),
        output.undeclared_parameters(),
        output.undeclared_positional_parameters(), output.max_column_id(),
        output.has_graph_references());
  }

  ZETASQL_RET_CHECK(ret) << "No resolved AST in AnalyzerOutput";

  AnalyzerOutputMutator(ret).mutable_runtime_info() = output.runtime_info();
  return ret;
}

class TemplatedFunctionCallVisitor : public ResolvedASTVisitor {
 public:
  // Returns a list of all templated function calls (UDFs, UDAs, and TVFs)
  // including function calls that are nested inside of other function calls.
  static absl::StatusOr<std::vector<const ResolvedNode*>>
  FindTemplatedFunctionCalls(const ResolvedNode* node) {
    std::vector<const ResolvedNode*> templated_function_calls;
    TemplatedFunctionCallVisitor visitor(templated_function_calls);
    ZETASQL_RETURN_IF_ERROR(node->Accept(&visitor));
    return templated_function_calls;
  }

  template <typename T>
  absl::Status VisitFunctionCall(const T* function_call) {
    ZETASQL_RET_CHECK(function_call->template Is<ResolvedFunctionCall>() ||
              function_call->template Is<ResolvedAggregateFunctionCall>());
    if (function_call->function_call_info() != nullptr &&
        function_call->function_call_info()
            ->template Is<TemplatedSQLFunctionCall>()) {
      templated_function_calls_.push_back(function_call);
      const auto* templated_function_call =
          function_call->function_call_info()
              ->template GetAs<TemplatedSQLFunctionCall>();
      for (const auto& agg_expr :
           templated_function_call->aggregate_expression_list()) {
        ZETASQL_RETURN_IF_ERROR(agg_expr->Accept(this));
      }
      ZETASQL_RETURN_IF_ERROR(templated_function_call->expr()->Accept(this));
    }
    if (function_call->function()->template Is<SQLFunctionInterface>()) {
      const auto* sql_function =
          function_call->function()->template GetAs<SQLFunctionInterface>();
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
          aggregate_expression_list = sql_function->aggregate_expression_list();
      if (aggregate_expression_list != nullptr) {
        for (const auto& agg_expr : *aggregate_expression_list) {
          ZETASQL_RETURN_IF_ERROR(agg_expr->Accept(this));
        }
      }
      ZETASQL_RETURN_IF_ERROR(sql_function->FunctionExpression()->Accept(this));
    }
    return DefaultVisit(function_call);
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    return VisitFunctionCall(node);
  }

  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    return VisitFunctionCall(node);
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    if (node->signature() != nullptr &&
        node->signature()->Is<TemplatedSQLTVFSignature>()) {
      templated_function_calls_.push_back(node);
      ZETASQL_RETURN_IF_ERROR(node->signature()
                          ->GetAs<TemplatedSQLTVFSignature>()
                          ->resolved_templated_query()
                          ->Accept(this));
    }
    return DefaultVisit(node);
  }

 private:
  explicit TemplatedFunctionCallVisitor(
      std::vector<const ResolvedNode*>& templated_function_calls)
      : templated_function_calls_(templated_function_calls) {}

  std::vector<const ResolvedNode*>& templated_function_calls_;
};

}  // namespace

class AnalyzerTestRunner {
 public:  // Pointer-to-member-function usage requires public member functions
  explicit AnalyzerTestRunner(TestDumperCallback test_dumper_callback)
      : test_dumper_callback_(std::move(test_dumper_callback)) {
    // The supported option definitions are in analyzer_test_options.h.
    RegisterAnalyzerTestOptions(&test_case_options_);

    // Force a blank line at the start of every test case.
    absl::SetFlag(&FLAGS_file_based_test_driver_insert_leading_blank_lines, 1);
  }

  const std::vector<AnalyzerRuntimeInfo>& runtime_info_list() const {
    return runtime_info_list_;
  }

  // CatalogHolder is a wrapper of either a sample catalog or a special catalog,
  // depending on the value of catalog_name.
  class CatalogHolder {
   public:
    explicit CatalogHolder(Catalog* catalog) : catalog_(catalog) {}
    Catalog* catalog() { return catalog_; }

   private:
    Catalog* catalog_;
  };

  CatalogHolder CreateCatalog(const AnalyzerOptions& analyzer_options) {
    const std::string catalog_name = test_case_options_.GetString(kUseDatabase);
    std::vector<std::string> suppressed_functions =
        absl::StrSplit(test_case_options_.GetString(kSuppressFunctions), ',',
                       absl::SkipEmpty());
    auto result = catalog_factory_.GetCatalog(
        catalog_name, analyzer_options.language(), suppressed_functions);
    ZETASQL_CHECK_OK(result);
    return CatalogHolder(*result);
  }

  static constexpr absl::string_view kSampleCatalogName = "SampleCatalog";
  static constexpr absl::string_view kSpecialCatalogName = "SpecialCatalog";

  // Acts as a cache for catalog instances.
  // Constructing SampleCatalog instances is extremely slow, this class.
  // virtually eliminates this cost.
  class CatalogFactory {
   public:
    class PreparedDatabase {
     public:
      Catalog* catalog() const { return catalog_.get(); }
      SimpleCatalog* mutable_catalog() { return mutable_catalog_.get(); }
      void TakeOwnership(std::unique_ptr<TypeFactory> obj) {
        owned_type_factories_.emplace_back(std::move(obj));
      }

      void TakeOwnership(std::unique_ptr<const AnalyzerOutput> obj) {
        owned_analyzer_outputs_.emplace_back(std::move(obj));
      }

     private:
      friend class CatalogFactory;
      std::unique_ptr<MultiCatalog> catalog_;
      std::unique_ptr<SimpleCatalog> mutable_catalog_;
      std::vector<std::unique_ptr<TypeFactory>> owned_type_factories_;
      std::vector<std::unique_ptr<const AnalyzerOutput>>
          owned_analyzer_outputs_;
    };

    // Returns a catalog matching the given catalog_name (which must
    // be either 'SampleCatalog' or 'SpecialCatalog'.
    //
    // The returned Catalog remains owned by CatalogFactory.
    absl::StatusOr<Catalog*> GetCatalog(
        absl::string_view catalog_name, const LanguageOptions& language,
        const std::vector<std::string>& suppressed_functions) const {
      if (catalog_name == kSampleCatalogName) {
        return GetSampleCatalog(language, suppressed_functions);
      } else if (catalog_name == kSpecialCatalogName) {
        ZETASQL_RET_CHECK(suppressed_functions.empty());
        if (special_catalog_ == nullptr) {
          special_catalog_ = GetSpecialCatalog();
        }
        return special_catalog_.get();
      } else {
        auto it = prepared_databases_map_.find(catalog_name);

        if (it == prepared_databases_map_.end()) {
          return absl::NotFoundError(catalog_name);
        }
        const auto& [name, prepared_database] = *it;
        return prepared_database.catalog();
      }
    }

    absl::StatusOr<PreparedDatabase*> CreateDatabase(
        absl::string_view catalog_name, absl::string_view base_catalog_name,
        const LanguageOptions& language,
        std::vector<std::string> suppressed_functions) {
      if (catalog_name == kSampleCatalogName ||
          catalog_name == kSpecialCatalogName) {
        return absl::AlreadyExistsError(
            absl::StrFormat("'%s' is reserved and cannot be specified in"
                            " prepare_database. Did you mean use_database?",
                            catalog_name));
      }
      auto it = prepared_databases_map_.find(catalog_name);
      if (it != prepared_databases_map_.end()) {
        return absl::AlreadyExistsError(
            absl::StrFormat("'%s' already exists", catalog_name));
      }
      ZETASQL_ASSIGN_OR_RETURN(
          Catalog * base_catalog,
          GetCatalog(base_catalog_name, language, suppressed_functions));
      PreparedDatabase& entry = prepared_databases_map_[catalog_name];

      entry.mutable_catalog_ = std::make_unique<SimpleCatalog>(catalog_name);

      ZETASQL_RETURN_IF_ERROR(MultiCatalog::Create(
          catalog_name, {entry.mutable_catalog_.get(), base_catalog},
          &entry.catalog_));
      return &entry;
    }

    absl::StatusOr<PreparedDatabase*> GetMutableDatabase(
        absl::string_view catalog_name) {
      auto it = prepared_databases_map_.find(catalog_name);
      if (it == prepared_databases_map_.end()) {
        return absl::NotFoundError(
            absl::StrFormat("'%s' not found", catalog_name));
      }

      return &(it->second);
    }

   private:
    absl::StatusOr<Catalog*> GetSampleCatalog(
        const LanguageOptions& language,
        const std::vector<std::string>& suppressed_functions) const {
      CacheKey key = {language, suppressed_functions};
      auto it = sample_catalog_cache_.find(key);
      if (it != sample_catalog_cache_.end()) {
        return it->second->catalog();
      }
      auto sample_catalog = std::make_unique<SampleCatalog>(language);
      for (absl::string_view fn_name : suppressed_functions) {
        sample_catalog->catalog()->RemoveFunctions(
            [fn_name](const Function* fn) {
              return zetasql_base::CaseEqual(fn->Name(), fn_name);
            });
      }
      Catalog* output = sample_catalog->catalog();
      sample_catalog_cache_.emplace(std::move(key), std::move(sample_catalog));
      return output;
    }

    struct CacheKey {
      LanguageOptions options;
      std::vector<std::string> suppressed_functions;
      bool operator==(const CacheKey& rhs) const {
        if (options != rhs.options ||
            suppressed_functions.size() != rhs.suppressed_functions.size()) {
          return false;
        }
        for (int i = 0; i < suppressed_functions.size(); ++i) {
          if (suppressed_functions[i] != rhs.suppressed_functions[i]) {
            return false;
          }
        }
        return true;
      }
      template <typename H>
      friend H AbslHashValue(H h, const CacheKey& key) {
        return H::combine(std::move(h), key.options, key.suppressed_functions);
      }
    };

    mutable std::unique_ptr<SimpleCatalog> special_catalog_;
    mutable absl::node_hash_map<CacheKey, std::unique_ptr<SampleCatalog>>
        sample_catalog_cache_;
    absl::node_hash_map<std::string, PreparedDatabase> prepared_databases_map_;
  };

  void InitializeLiteralReplacementOptions() {
    literal_replacement_options_.ignored_option_names = absl::StrSplit(
        test_case_options_.GetString(kOptionNamesToIgnoreInLiteralReplacement),
        ',');
    literal_replacement_options_.scrub_limit_offset =
        test_case_options_.GetBool(kScrubLimitOffsetInLiteralReplacement);
  }

  void RunTest(absl::string_view test_case_input,
               file_based_test_driver::RunTestCaseResult* test_result) {
    std::string test_case = std::string(test_case_input);
    const absl::Status options_status =
        test_case_options_.ParseTestCaseOptions(&test_case);
    if (!options_status.ok()) {
      test_result->AddTestOutput(
          absl::StrCat("ERROR: Invalid test case options: ",
                       internal::StatusToString(options_status)));
      return;
    }

    InitializeLiteralReplacementOptions();

    const std::string& mode = test_case_options_.GetString(kModeOption);

    auto type_factory_memory = std::make_unique<TypeFactory>();
    TypeFactory& type_factory = *type_factory_memory;
    AnalyzerOptions options;

    options.set_fields_accessed_mode(
        AnalyzerOptions::FieldsAccessedMode::CLEAR_FIELDS);

    if (test_case_options_.GetBool(kEnableSampleAnnotation)) {
      engine_specific_annotation_specs_.push_back(
          std::make_unique<SampleAnnotation>());

      std::vector<AnnotationSpec*> annotation_specs;
      annotation_specs.push_back(
          engine_specific_annotation_specs_.back().get());

      options.set_annotation_specs(annotation_specs);
    }

    if (test_case_options_.GetBool(kIdStringAllowUnicodeCharacters)) {
      absl::SetFlag(&FLAGS_zetasql_idstring_allow_unicode_characters, true);
    }

    // Turn off AST rewrites. We'll run them later so we can show both ASTs.
    options.set_enabled_rewrites({});

    // If requested, add a PreparedExpressionConstantEvaluator to the analyzer
    // options.
    std::unique_ptr<PreparedExpressionConstantEvaluator> constant_evaluator =
        nullptr;
    if (test_case_options_.GetBool(kUseConstantEvaluator)) {
      constant_evaluator =
          std::make_unique<PreparedExpressionConstantEvaluator>(
              /*options=*/EvaluatorOptions{});
      options.set_constant_evaluator(constant_evaluator.get());
    }

    // Parse the language features first because other checks below may depend
    // on the features that are enabled for the test case.
    ZETASQL_ASSERT_OK_AND_ASSIGN(LanguageOptions::LanguageFeatureSet features,
                         GetRequiredLanguageFeatures(test_case_options_));
    options.mutable_language()->SetEnabledLanguageFeatures(features);

    // Enable all reservable keywords so that tests which make use of clauses
    // that require such keywords (e.g. QUALIFY) are able to parse.
    //
    // Keyword-as-identifier scenarios are already well-covered in parser tests,
    // and don't need repeat coverage in analyzer tests.
    options.mutable_language()->EnableAllReservableKeywords();

    if (test_case_options_.GetString(kSupportedStatementKinds).empty()) {
      // In general, analyzer tests support all statement kinds.
      options.mutable_language()->SetSupportsAllStatementKinds();
    } else {
      // If the supported statement kinds is specified, then use them.
      const std::vector<std::string> supported_statement_kind_names =
          absl::StrSplit(test_case_options_.GetString(kSupportedStatementKinds),
                         ',');
      std::set<ResolvedNodeKind> supported_statement_kinds;
      for (const std::string& kind_name : supported_statement_kind_names) {
        const std::string full_kind_name =
            absl::StrCat("RESOLVED_", kind_name, "_STMT");
        ResolvedNodeKind supported_kind;
        ASSERT_TRUE(ResolvedNodeKind_Parse(full_kind_name, &supported_kind))
            << "Unknown statement " << full_kind_name;
        supported_statement_kinds.insert(supported_kind);
      }
      options.mutable_language()->SetSupportedStatementKinds(
          supported_statement_kinds);
    }
    options.set_prune_unused_columns(
        test_case_options_.GetBool(kPruneUnusedColumns));
    // SQLBuilder test benchmarks in sql_builder.test reflect INTERNAL
    // product mode.
    options.mutable_language()->set_product_mode(PRODUCT_INTERNAL);

    // Set rewriter options
    const std::string& rewrite_options_str =
        test_case_options_.GetString(kRewriteOptions);
    if (!rewrite_options_str.empty()) {
      ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
          rewrite_options_str, options.mutable_rewrite_options()))
          << "Invalid rewrite_options: " << rewrite_options_str;
    }

    if (test_case_options_.GetBool(kUseHintsAllowlist)) {
      options.set_allowed_hints_and_options(
          GetAllowedHintsAndOptions(&type_factory));
    }

    if (test_case_options_.GetBool(kDisallowDuplicateOptions)) {
      AllowedHintsAndOptions copy = options.allowed_hints_and_options();
      copy.disallow_duplicate_option_names = true;
      options.set_allowed_hints_and_options(copy);
    }

    if (!test_case_options_.GetString(kParameterMode).empty()) {
      const std::string parameter_mode_string =
          test_case_options_.GetString(kParameterMode);
      if (parameter_mode_string == "named") {
        options.set_parameter_mode(PARAMETER_NAMED);
      } else if (parameter_mode_string == "positional") {
        options.set_parameter_mode(PARAMETER_POSITIONAL);
      } else if (parameter_mode_string == "none") {
        options.set_parameter_mode(PARAMETER_NONE);
      } else {
        FAIL() << "Unsupported parameter mode '" << parameter_mode_string
               << "'";
      }
    }

    if (options.parameter_mode() == PARAMETER_NAMED) {
      // Adding some fixed query parameters for testing purposes only.
      for (const auto& name_and_type : GetQueryParameters(&type_factory)) {
        if (name_and_type.second->IsSupportedType(options.language())) {
          ZETASQL_EXPECT_OK(options.AddQueryParameter(name_and_type.first,
                                              name_and_type.second));
        }
      }
    }

    if (!test_case_options_.GetString(kPositionalParameters).empty()) {
      // Add positional parameters based on test options.
      auto catalog =
          std::make_unique<zetasql::SimpleCatalog>("empty_catalog");
      const std::vector<std::string> positional_parameter_names =
          absl::StrSplit(test_case_options_.GetString(kPositionalParameters),
                         ',', absl::SkipEmpty());
      for (const std::string& parameter_name : positional_parameter_names) {
        const Type* parameter_type = nullptr;
        ZETASQL_ASSERT_OK(AnalyzeType(parameter_name, options, catalog.get(),
                              &type_factory, &parameter_type));
        ZETASQL_EXPECT_OK(options.AddPositionalQueryParameter(parameter_type));
      }
    }

    if (test_case_options_.GetBool(kEnhancedErrorRedaction)) {
      options.set_enhanced_error_redaction(true);
      options.set_error_message_stability(
          zetasql::ERROR_MESSAGE_STABILITY_TEST_REDACTED);
    }

    const std::string error_message_mode_string =
        test_case_options_.GetString(kErrorMessageMode);
    if (error_message_mode_string.empty() ||
        error_message_mode_string == "multi_line_with_caret") {
      options.set_error_message_mode(ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
    } else if (error_message_mode_string == "one_line") {
      options.set_error_message_mode(ERROR_MESSAGE_ONE_LINE);
    } else if (error_message_mode_string == "with_payload") {
      options.set_error_message_mode(ERROR_MESSAGE_WITH_PAYLOAD);
    } else {
      FAIL() << "Unsupported error message mode '" << error_message_mode_string
             << "'";
    }

    absl::FlagSaver saver;
    if (const std::string flags = test_case_options_.GetString(kSetFlag);
        !flags.empty()) {
      if (test_case_options_.GetBool(kRunInJava)) {
        FAIL()
            << "Tests with set_flag should also be marked with no_java, "
               "because set_flag is not implemented in the Java analyzer test.";
      }
      for (const absl::string_view flag : absl::StrSplit(flags, ',')) {
        const std::pair<absl::string_view, absl::string_view> split =
            absl::StrSplit(flag, '=');
        const absl::string_view key = split.first;
        const absl::string_view value = split.second;
        absl::CommandLineFlag* const flag_ref = absl::FindCommandLineFlag(key);
        if (flag_ref == nullptr) {
          FAIL() << "set_flag: no such flag: " << key;
        }
        if (std::string err; !flag_ref->ParseFrom(value, &err)) {
          FAIL() << "set_flag: error parsing " << key << "=" << value << ": "
                 << err;
        }
      }
    }

    const zetasql::Type* proto_type;
    ZETASQL_ASSERT_OK(type_factory.MakeProtoType(
        zetasql_test__::KitchenSinkPB::descriptor(), &proto_type));

    // Add some expression columns that can be used in AnalyzeStatement cases.
    ZETASQL_EXPECT_OK(
        options.AddExpressionColumn("column_int32", type_factory.get_int32()));
    ZETASQL_EXPECT_OK(options.AddExpressionColumn("column_KitchenSink", proto_type));

    // Add some pseudo-columns that can be used in DDL statements.
    const std::string ddl_pseudo_column_mode =
        test_case_options_.GetString(kDdlPseudoColumnMode);
    if (ddl_pseudo_column_mode.empty() || ddl_pseudo_column_mode == "list") {
      options.SetDdlPseudoColumns(
          {{"Pseudo_Column_int32", type_factory.get_int32()},
           {"pseudo_column_timestamp", type_factory.get_timestamp()},
           {"pseudo_column_KitchenSink", proto_type}});
    } else if (ddl_pseudo_column_mode == "callback") {
      options.SetDdlPseudoColumnsCallback(
          [&type_factory](const std::vector<std::string>& table_name,
                          const std::vector<const ResolvedOption*>& options,
                          std::vector<std::pair<std::string, const Type*>>*
                              pseudo_columns) {
            // Pseudo-column for a particular table name.
            if (absl::StrJoin(table_name, ".") ==
                "table_with_extra_pseudo_column") {
              pseudo_columns->push_back(std::make_pair(
                  "extra_pseudo_column_table_int64", type_factory.get_int64()));
            }
            // Extra pseudocolumn if a particular option name is present.
            if (absl::c_any_of(options, [](const ResolvedOption* option) {
                  return option->name() == "pseudo_column_option";
                })) {
              pseudo_columns->push_back(
                  std::make_pair("extra_pseudo_column_option_string",
                                 type_factory.get_string()));
            }
            return absl::OkStatus();
          });
    } else if (ddl_pseudo_column_mode == "none") {
      options.SetDdlPseudoColumnsCallback(nullptr);
    } else {
      FAIL() << "Unsupported DDL pseudo-column mode '" << ddl_pseudo_column_mode
             << "'";
    }

    if (!test_case_options_.GetString(kProductMode).empty()) {
      if (test_case_options_.GetString(kProductMode) == "external") {
        options.mutable_language()->set_product_mode(PRODUCT_EXTERNAL);
      } else if (test_case_options_.GetString(kProductMode) == "internal") {
        options.mutable_language()->set_product_mode(PRODUCT_INTERNAL);
      } else {
        ASSERT_EQ("", test_case_options_.GetString(kProductMode));
      }
    }
    if (!test_case_options_.GetString(kStatementContext).empty()) {
      if (test_case_options_.GetString(kStatementContext) == "default") {
        options.set_statement_context(CONTEXT_DEFAULT);
      } else if (test_case_options_.GetString(kStatementContext) == "module") {
        options.set_statement_context(CONTEXT_MODULE);
      } else {
        ASSERT_EQ("", test_case_options_.GetString(kStatementContext));
      }
    }

    if (test_case_options_.GetBool(kAllowAggregateStandaloneExpression)) {
      options.set_allow_aggregate_standalone_expression(true);
    }

    // Also add an in-scope expression column if either option is set.
    if (test_case_options_.IsExplicitlySet(kInScopeExpressionColumnName) ||
        test_case_options_.IsExplicitlySet(kInScopeExpressionColumnType)) {
      const Type* type = proto_type;  // Use KitchenSinkPB by default.
      if (test_case_options_.IsExplicitlySet(kInScopeExpressionColumnType)) {
        auto catalog_holder = CreateCatalog(options);
        const absl::Status type_status = AnalyzeType(
            test_case_options_.GetString(kInScopeExpressionColumnType), options,
            catalog_holder.catalog(), &type_factory, &type);
        if (!type_status.ok()) {
          test_result->AddTestOutput(AddFailure(absl::StrCat(
              "FAILED: Invalid type name in in_scope_expression_column_type: ",
              internal::StatusToString(type_status))));
          return;
        }
      }
      ZETASQL_EXPECT_OK(options.SetInScopeExpressionColumn(
          test_case_options_.GetString(kInScopeExpressionColumnName), type));
    }

    if (!test_case_options_.GetString(kCoercedQueryOutputTypes).empty()) {
      const Type* type;
      auto catalog_holder = CreateCatalog(options);
      const absl::Status type_status =
          AnalyzeType(test_case_options_.GetString(kCoercedQueryOutputTypes),
                      options, catalog_holder.catalog(), &type_factory, &type);
      if (!type_status.ok()) {
        test_result->AddTestOutput(AddFailure(absl::StrCat(
            "FAILED: Invalid type name in expected_query_output_types: ",
            internal::StatusToString(type_status))));
        return;
      }
      if (!type->IsStruct()) {
        test_result->AddTestOutput(
            AddFailure(absl::StrCat("FAILED: expected_query_output_types only "
                                    "accepts struct types, not: ",
                                    type->DebugString())));
        return;
      }
      std::vector<const Type*> types;
      for (int i = 0; i < type->AsStruct()->num_fields(); ++i) {
        types.push_back(type->AsStruct()->field(i).type);
      }
      options.set_target_column_types(types);
    }

    if (test_case_options_.GetBool(kUseSharedIdSequence)) {
      options.set_column_id_sequence_number(&custom_id_sequence_);
    }

    if (!test_case_options_.GetString(kDefaultTimezone).empty()) {
      absl::TimeZone timezone;
      ZETASQL_ASSERT_OK(functions::MakeTimeZone(
          test_case_options_.GetString(kDefaultTimezone), &timezone))
          << absl::StrCat("kDefaultTimezone: '", kDefaultTimezone, "'");
      options.set_default_time_zone(timezone);
    }

    ZETASQL_ASSERT_OK(options.set_default_anon_kappa_value(
        test_case_options_.GetInt64(kDefaultAnonKappaValue)));

    const std::string& parse_location_record_type_value =
        test_case_options_.GetString(kParseLocationRecordType);
    if (!parse_location_record_type_value.empty()) {
      ParseLocationRecordType type;
      ASSERT_TRUE(ParseLocationRecordType_Parse(
          absl::AsciiStrToUpper(parse_location_record_type_value), &type));
      options.set_parse_location_record_type(type);
    }

    if (test_case_options_.GetBool(kCreateNewColumnForEachProjectedOutput)) {
      options.set_create_new_column_for_each_projected_output(true);
    }

    if (test_case_options_.GetBool(kAllowUndeclaredParameters)) {
      options.set_allow_undeclared_parameters(true);
    }

    if (!test_case_options_.GetBool(kPreserveColumnAliases)) {
      options.set_preserve_column_aliases(false);
    }

    std::string entity_types_config =
        test_case_options_.GetString(kSupportedGenericEntityTypes);
    if (!entity_types_config.empty()) {
      std::vector<std::string> entity_types =
          absl::StrSplit(entity_types_config, ',');
      options.mutable_language()->SetSupportedGenericEntityTypes(entity_types);
    }
    std::string sub_entity_types_config =
        test_case_options_.GetString(kSupportedGenericSubEntityTypes);
    if (!sub_entity_types_config.empty()) {
      std::vector<std::string> sub_entity_types =
          absl::StrSplit(sub_entity_types_config, ',');
      options.mutable_language()->SetSupportedGenericSubEntityTypes(
          sub_entity_types);
    }

    options.set_preserve_unnecessary_cast(
        test_case_options_.GetBool(kPreserveUnnecessaryCast));

    {
      AllowedHintsAndOptions updated_hints_and_options =
          options.allowed_hints_and_options();
      std::string additional_allowed_anonymization_options_string =
          test_case_options_.GetString(kAdditionalAllowedAnonymizationOptions);
      for (const auto& option : absl::StrSplit(
               additional_allowed_anonymization_options_string, ',')) {
        if (option.empty()) continue;
        updated_hints_and_options.AddAnonymizationOption(option,
                                                         /*type=*/nullptr);
      }
      options.set_allowed_hints_and_options(updated_hints_and_options);
    }
    options.set_replace_table_not_found_error_with_tvf_error_if_applicable(
        test_case_options_.GetBool(
            kReplaceTableNotFoundErrorWithTvfErrorIfApplicable));

    SetupSampleSystemVariables(&type_factory, &options);
    auto catalog_holder = CreateCatalog(options);

    if (test_case_options_.GetBool(kParseMultiple)) {
      TestMulti(test_case, options, mode, catalog_holder.catalog(),
                &type_factory, test_result);
    } else {
      TestOne(test_case, options, mode, catalog_holder.catalog(),
              std::move(type_factory_memory), test_result);
    }
  }

 private:
  void TestOne(const std::string& test_case, const AnalyzerOptions& options,
               absl::string_view mode, Catalog* catalog,
               std::unique_ptr<TypeFactory> type_factory_memory,
               file_based_test_driver::RunTestCaseResult* test_result) {
    TypeFactory* type_factory = type_factory_memory.get();
    std::unique_ptr<const AnalyzerOutput> output;
    absl::Status status;
    if (mode == "statement") {
      status =
          AnalyzeStatement(test_case, options, catalog, type_factory, &output);
      ASSERT_FALSE(status.ok() && output == nullptr);

      if (status.ok()) {
        runtime_info_list_.push_back(output->runtime_info());
      }

      // For AnalyzeStatementFromASTStatement()
      if (!test_case_options_.GetBool(kUseSharedIdSequence)) {
        std::unique_ptr<ParserOutput> parser_output;
        ParserOptions parser_options = options.GetParserOptions();

        absl::Status parse_status =
            ParseStatement(test_case, parser_options, &parser_output);
        if (parse_status.ok()) {
          std::unique_ptr<const AnalyzerOutput> analyze_from_ast_output;
          const absl::Status analyze_from_ast_status =
              AnalyzeStatementFromParserOutputOwnedOnSuccess(
                  &parser_output, options, test_case, catalog, type_factory,
                  &analyze_from_ast_output);
          EXPECT_EQ(analyze_from_ast_status, status);
          if (analyze_from_ast_status.ok()) {
            ZETASQL_EXPECT_OK(analyze_from_ast_output->resolved_statement()
                          ->CheckNoFieldsAccessed());
            EXPECT_EQ(
                analyze_from_ast_output->resolved_statement()->DebugString(),
                output->resolved_statement()->DebugString());
            EXPECT_EQ(nullptr, parser_output);
          } else {
            EXPECT_NE(nullptr, parser_output);
          }
        }
      }

      // Also test ExtractTableNamesFromStatement on both successful and
      // failing queries.
      EXPECT_EQ(status.ok(), output != nullptr);
      if (test_case_options_.GetBool(kTestExtractTableNames)) {
        CheckExtractTableNames(test_case, options, output.get());
      }

      // Also run the query in strict mode and ensure we get a valid result.
      CheckStrictMode(test_case, options, status, output.get(), catalog,
                      type_factory, test_result);

      // For successfully analyzed queries, check that replacing literals by
      // parameters produces an equivalent query. Do this only if we aren't
      // explicitly testing parse locations (in parse_locations.test), and
      // if query is marked as "EnableLiteralReplacement" - for queries
      // which by design should not give same results for parameters as for
      // literals. For example SELECT "foo" GROUP BY "foo" is valid, but
      // SELECT @p1 GROUP BY "foo" is rejected even if @p1's value is "foo".
      if (status.ok() &&
          (test_case_options_.GetString(kParseLocationRecordType).empty() ||
           zetasql_base::CaseEqual(
               test_case_options_.GetString(kParseLocationRecordType),
               ParseLocationRecordType_Name(PARSE_LOCATION_RECORD_NONE))) &&
          test_case_options_.GetBool(kEnableLiteralReplacement)) {
        ZETASQL_EXPECT_OK(CheckLiteralReplacement(test_case, options, output.get()));
      }

      if (status.ok()) {
        CheckSupportedStatementKind(
            test_case, output->resolved_statement()->node_kind(), options);

        // Deep copy the AST and verify that it copies correctly.
        CheckDeepCopyAST(output.get());

        // Serialize and deserialize the AST and make sure it's the same.
        CheckSerializeDeserializeAST(options, catalog, type_factory, status,
                                     *output);

        // Check that Validator covers the full resolved AST.
        CheckValidatorCoverage(options, *output);
      }
    } else if (mode == "expression") {
      status =
          AnalyzeExpression(test_case, options, catalog, type_factory, &output);
      if (status.ok()) {
        ZETASQL_EXPECT_OK(output->resolved_expr()->CheckNoFieldsAccessed());
      }
    } else if (mode == "type") {
      // Use special-case handler since we don't get a resolved AST.
      HandleOneType(test_case, options, catalog, type_factory, test_result);
      return;
    } else {
      test_result->AddTestOutput(absl::StrCat("ERROR: Invalid mode: ", mode));
      return;
    }

    const ResolvedNodeKind guessed_node_kind = GetNextStatementKind(
        ParseResumeLocation::FromStringView(test_case), options.language());

    // Ensure that we can get the properties as well, and that the node
    // kind is consistent with GetStatementKind().
    StatementProperties extracted_statement_properties;
    ZETASQL_ASSERT_OK(GetStatementProperties(test_case, options.language(),
                                     &extracted_statement_properties))
        << test_case;
    EXPECT_EQ(guessed_node_kind, extracted_statement_properties.node_kind)
        << test_case
        << "\nguessed_node_kind: " << ResolvedNodeKind_Name(guessed_node_kind)
        << "\nextracted_statement_properties node_kind: "
        << ResolvedNodeKind_Name(extracted_statement_properties.node_kind);

    const AnalyzerOutput* output_ptr = output.get();
    if (status.ok()) {
      absl::Status prepare_database_status =
          HandlePrepareDatabase(options,
                                /* might take ownership */ type_factory_memory,
                                /* might take ownership */ output);
      if (!prepare_database_status.ok()) {
        test_result->AddTestOutput(absl::StrCat(
            "prepare_database ERROR: ", prepare_database_status.message()));
      }
    }

    HandleOneResult(test_case, options, type_factory, catalog, mode, status,
                    output_ptr, extracted_statement_properties, test_result);
  }

  void TestMulti(absl::string_view test_case, const AnalyzerOptions& options,
                 absl::string_view mode, Catalog* catalog,
                 TypeFactory* type_factory,
                 file_based_test_driver::RunTestCaseResult* test_result) {
    ASSERT_EQ("statement", mode)
        << kParseMultiple << " only works on statements";

    ParseResumeLocation location =
        ParseResumeLocation::FromStringView(test_case);
    ParseResumeLocation location_for_extract_table_names =
        ParseResumeLocation::FromStringView(test_case);
    while (true) {
      bool at_end_of_input;
      std::unique_ptr<const AnalyzerOutput> output;

      const ResolvedNodeKind guessed_node_kind = GetNextStatementKind(location);

      // Ensure that we can get the properties as well, and that the node
      // kind is consistent with GetStatementKind().
      StatementProperties extracted_statement_properties;
      ZETASQL_ASSERT_OK(GetNextStatementProperties(location, options.language(),
                                           &extracted_statement_properties));
      EXPECT_EQ(guessed_node_kind, extracted_statement_properties.node_kind)
          << "\nguessed_node_kind: " << ResolvedNodeKind_Name(guessed_node_kind)
          << "\nstatement_properties node_kind: "
          << ResolvedNodeKind_Name(extracted_statement_properties.node_kind);

      const absl::Status status = AnalyzeNextStatement(
          &location, options, catalog, type_factory, &output, &at_end_of_input);
      if (status.ok()) {
        ZETASQL_EXPECT_OK(output->resolved_statement()->CheckNoFieldsAccessed());
      }
      HandleOneResult(test_case, options, type_factory, catalog, mode, status,
                      output.get(), extracted_statement_properties,
                      test_result);

      if (test_case_options_.GetBool(kTestExtractTableNames)) {
        CheckExtractNextTableNames(&location_for_extract_table_names, options,
                                   location, output.get());
      }

      // Stop after EOF or error.
      // TODO If the API distinguished parse errors from analysis
      // errors, we could continue after analysis errors.
      if (at_end_of_input || !status.ok()) {
        break;
      }
    }
  }

  void HandleOneType(const std::string& test_case,
                     const AnalyzerOptions& options, Catalog* catalog,
                     TypeFactory* type_factory,
                     file_based_test_driver::RunTestCaseResult* test_result) {
    const Type* type;
    TypeModifiers type_modifiers;
    const absl::Status status = AnalyzeType(
        test_case, options, catalog, type_factory, &type, &type_modifiers);
    if (status.ok()) {
      test_result->AddTestOutput(
          type->TypeNameWithModifiers(type_modifiers, PRODUCT_INTERNAL)
              .value());

      // Test that the type's TypeName can be reparsed as the same type.
      const Type* reparsed_type;
      TypeModifiers reparsed_type_modifiers;
      const absl::Status reparse_status = AnalyzeType(
          type->TypeNameWithModifiers(type_modifiers, PRODUCT_INTERNAL).value(),
          options, catalog, type_factory, &reparsed_type,
          &reparsed_type_modifiers);
      if (!reparse_status.ok()) {
        test_result->AddTestOutput(
            AddFailure(absl::StrCat("FAILED reparsing type->DebugString: ",
                                    FormatError(reparse_status))));
      } else if (!type->Equals(reparsed_type)) {
        test_result->AddTestOutput(AddFailure(absl::StrCat(
            "FAILED: got different type on reparsing DebugString: ",
            reparsed_type->DebugString())));
      } else if (!type_modifiers.Equals(reparsed_type_modifiers)) {
        test_result->AddTestOutput(AddFailure(absl::StrCat(
            "FAILED: got different type modifiers on reparsing DebugString: ",
            reparsed_type_modifiers.DebugString())));
      }
    } else {
      test_result->AddTestOutput(absl::StrCat("ERROR: ", FormatError(status)));
    }
  }

  void AddDescendantFunctionCalls(
      const ResolvedNode* node,
      std::vector<const ResolvedNode*>& future_nodes) {
    // We do not add directly to 'future_nodes' because GetDescendantsSatisfying
    // clears its argument vector.
    std::vector<const ResolvedNode*> more_nodes;
    node->GetDescendantsSatisfying(&ResolvedNode::Is<ResolvedFunctionCallBase>,
                                   &more_nodes);
    future_nodes.insert(future_nodes.end(), more_nodes.begin(),
                        more_nodes.end());
  }

  // Checks that `node` is a ResolvedFunctionCall or
  // ResolvedAggregateFunctionCall and adds a debug string to `debug_strings`
  // and `test_result_string` if not already present in the former.
  // `debug_strings` is used to prevent duplicate function template expansions
  //     from appearing with the same test case.
  // `test_result_string` is the golden file output that we are appending to.
  absl::Status AddResultStringForTemplatedSqlFunctionCall(
      const ResolvedNode* node, absl::flat_hash_set<std::string>& debug_strings,
      std::string& test_result_string) {
    ZETASQL_RET_CHECK(node->Is<ResolvedFunctionCall>() ||
              node->Is<ResolvedAggregateFunctionCall>())
        << node->DebugString();
    const TemplatedSQLFunctionCall* sql_function_call = nullptr;
    std::string signature_string = "";
    if (node->node_kind() == RESOLVED_FUNCTION_CALL) {
      const auto* function_call = static_cast<const ResolvedFunctionCall*>(node);
      ZETASQL_RET_CHECK(
          function_call->function_call_info()->Is<TemplatedSQLFunctionCall>())
          << function_call->function_call_info()->DebugString();
      sql_function_call = function_call->function_call_info()
                              ->GetAs<TemplatedSQLFunctionCall>();
      signature_string = function_call->signature().DebugString(
          function_call->function()->FullName(),
          /*verbose=*/true);
    } else if (node->node_kind() == RESOLVED_AGGREGATE_FUNCTION_CALL) {
      const auto* function_call = node->GetAs<ResolvedAggregateFunctionCall>();
      ZETASQL_RET_CHECK(
          function_call->function_call_info()->Is<TemplatedSQLFunctionCall>())
          << function_call->function_call_info()->DebugString();
      sql_function_call = function_call->function_call_info()
                              ->GetAs<TemplatedSQLFunctionCall>();
      signature_string = function_call->signature().DebugString(
          function_call->function()->FullName(),
          /*verbose=*/true);
    }
    std::string templated_expr_debug_str =
        sql_function_call->expr()->DebugString();
    std::string debug_string = absl::StrCat(
        "\nWith Templated SQL function call:\n  ", signature_string,
        "\ncontaining resolved templated expression:\n",
        templated_expr_debug_str);
    for (const auto& agg_expr :
         sql_function_call->aggregate_expression_list()) {
      absl::StrAppend(
          &debug_string, "\n  ",
          absl::StripSuffix(
              absl::StrReplaceAll(agg_expr->DebugString(), {{"\n", "\n    "}}),
              "    "));
    }
    if (debug_strings.insert(debug_string).second) {
      absl::StrAppend(&test_result_string, debug_string);
    }
    return absl::OkStatus();
  }

  // Similar to `AddResultStringForTemplatedSqlFunctionCall`, but for
  // ResolvedTVFScan nodes.
  absl::Status AddResultStringForTemplatedSqlTvf(
      const ResolvedTVFScan* node,
      absl::flat_hash_set<std::string>& debug_strings,
      std::string& test_result_string) {
    ZETASQL_RET_CHECK(node->signature()->Is<TemplatedSQLTVFSignature>())
        << node->signature()->DebugString();
    const auto* tvf_signature =
        node->signature()->GetAs<TemplatedSQLTVFSignature>();
    std::string templated_query_debug_str =
        tvf_signature->resolved_templated_query()->DebugString();
    const std::string debug_string = absl::StrCat(
        "\nWith Templated SQL TVF signature:\n  ", node->tvf()->FullName(),
        node->signature()->DebugString(/*verbose=*/true),
        "\ncontaining resolved templated query:\n", templated_query_debug_str);
    if (debug_strings.insert(debug_string).second) {
      absl::StrAppend(&test_result_string, debug_string);
    }
    return absl::OkStatus();
  }

  absl::Status AddResultStringsForTemplatedSqlObjects(
      const ResolvedNode* node, std::string& test_result_string) {
    if (node == nullptr) return absl::OkStatus();

    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<const ResolvedNode*> templated_function_calls,
        TemplatedFunctionCallVisitor::FindTemplatedFunctionCalls(node));

    absl::flat_hash_set<std::string> debug_strings;
    for (const ResolvedNode* node : templated_function_calls) {
      if (node->Is<ResolvedTVFScan>()) {
        ZETASQL_RETURN_IF_ERROR(AddResultStringForTemplatedSqlTvf(
            node->GetAs<ResolvedTVFScan>(), debug_strings, test_result_string));
      } else {
        ZETASQL_RETURN_IF_ERROR(AddResultStringForTemplatedSqlFunctionCall(
            node, debug_strings, test_result_string));
      }
    }
    return absl::OkStatus();
  }

  void ExtractTableResolutionTimeInfoMapAsString(absl::string_view test_case,
                                                 const AnalyzerOptions& options,
                                                 TypeFactory* type_factory,
                                                 Catalog* catalog,
                                                 std::string* output) {
    std::unique_ptr<ParserOutput> parser_output;
    TableResolutionTimeInfoMap table_resolution_time_info_map;
    const absl::Status status = ExtractTableResolutionTimeFromStatement(
        test_case, options, type_factory, catalog,
        &table_resolution_time_info_map, &parser_output);
    *output = FormatTableResolutionTimeInfoMap(status,
                                               table_resolution_time_info_map);
  }

  void ExtractTableResolutionTimeInfoMapFromASTAsString(
      absl::string_view test_case, const AnalyzerOptions& options,
      TypeFactory* type_factory, Catalog* catalog, std::string* output,
      std::string* output_with_deferred_analysis) {
    std::unique_ptr<ParserOutput> parser_output;
    absl::Status status =
        ParseStatement(test_case, options.GetParserOptions(), &parser_output);
    status = MaybeUpdateErrorFromPayload(options.error_message_options(),
                                         test_case, status);
    TableResolutionTimeInfoMap table_resolution_time_info_map;
    if (status.ok()) {
      absl::Status status = ExtractTableResolutionTimeFromASTStatement(
          *parser_output->statement(), options, test_case, type_factory,
          catalog, &table_resolution_time_info_map);
      *output = FormatTableResolutionTimeInfoMap(
          status, table_resolution_time_info_map);

      auto deferred_extract_helper =
          [&parser_output, &options, test_case, type_factory, catalog,
           &table_resolution_time_info_map]() -> absl::Status {
        ZETASQL_RETURN_IF_ERROR(ExtractTableResolutionTimeFromASTStatement(
            *parser_output->statement(), options, test_case,
            /* type_factory = */ nullptr, /* catalog = */ nullptr,
            &table_resolution_time_info_map));
        for (auto& entry : table_resolution_time_info_map) {
          for (TableResolutionTimeExpr& expr : entry.second.exprs) {
            EXPECT_EQ(nullptr, expr.analyzer_output_with_expr);
            ZETASQL_RETURN_IF_ERROR(AnalyzeExpressionFromParserAST(
                *expr.ast_expr, options, test_case, type_factory, catalog,
                &expr.analyzer_output_with_expr));
          }
        }
        return absl::OkStatus();
      };
      status = deferred_extract_helper();
      *output_with_deferred_analysis = FormatTableResolutionTimeInfoMap(
          status, table_resolution_time_info_map);
    } else {
      *output = *output_with_deferred_analysis =
          FormatTableResolutionTimeInfoMap(status,
                                           table_resolution_time_info_map);
    }
  }

  void CheckExtractedStatementProperties(
      const ResolvedStatement* resolved_statement,
      const StatementProperties& extracted_statement_properties,
      file_based_test_driver::RunTestCaseResult* test_result) {
    // Check that the statement we resolved matches the statement kind we
    // extracted in GetStatementProperties().  Note that we do not check
    // the extracted StatementCategory, since it derives directly from
    // the statement's ResolvedNodeKind (we assume that if the ResolvedNodeKind
    // matches, that the StatementCategory also matches.
    if (extracted_statement_properties.node_kind == RESOLVED_LITERAL) {
      // RESOLVED_LITERAL indicates failure to identify a statement.
      test_result->AddTestOutput(AddFailure(
          "FAILED extracting statement kind. GetNextStatementProperties "
          "failed to find any possible kind."));
    } else if (resolved_statement->node_kind() ==
                   RESOLVED_GENERALIZED_QUERY_STMT &&
               extracted_statement_properties.node_kind ==
                   RESOLVED_QUERY_STMT) {
      // This case is allowed because ResolvedGeneralizedQueryStmt is an
      // expected form for some complex ASTQueryStatement patterns.
      // e.g. If we have a query containing pipe FORK, the statement is
      // detected as a QueryStmt based on the prefix, but resolved to a
      // GeneralizedQueryStmt.
    } else if (resolved_statement->node_kind() ==
               RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT) {
      // TODO: AST_ALTER_TABLE_STATEMENT sometimes will return
      // RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT for backward compatibility.
      // Disable the resulting test failure for now.
    } else if (resolved_statement->node_kind() !=
               extracted_statement_properties.node_kind) {
      test_result->AddTestOutput(AddFailure(absl::StrCat(
          "FAILED extracting statement kind. Extracted kind ",
          ResolvedNodeKindToString(extracted_statement_properties.node_kind),
          ", actual kind ", resolved_statement->node_kind_string())));
    }

    // Check whether the statement is CREATE TEMP matches the extracted
    // info.
    if (resolved_statement->Is<ResolvedCreateStatement>()) {
      const ResolvedCreateStatement* create_statement =
          resolved_statement->GetAs<ResolvedCreateStatement>();
      if ((create_statement->create_scope() ==
           ResolvedCreateStatementEnums::CREATE_TEMP) !=
          extracted_statement_properties.is_create_temporary_object) {
        test_result->AddTestOutput(AddFailure(absl::StrCat(
            "FAILED extracting whether statement is CREATE TEMP. "
            "Extracted is_create_temporary_object: ",
            extracted_statement_properties.is_create_temporary_object,
            ", statement: ", create_statement->DebugString())));
      }
    } else if (extracted_statement_properties.is_create_temporary_object) {
      test_result->AddTestOutput(AddFailure(absl::StrCat(
          "FAILED extracting whether statement is CREATE TEMP. "
          "Extracted is_create_temporary_object: ",
          extracted_statement_properties.is_create_temporary_object,
          ", statement: ", resolved_statement->DebugString())));
    }

    // Check that the number of top level statement hints match the number of
    // extracted hints.
    if (resolved_statement->hint_list().size() !=
        extracted_statement_properties.statement_level_hints.size()) {
      test_result->AddTestOutput(AddFailure(absl::StrCat(
          "FAILED extracting statement level hints.  Number of extracted "
          "hints (",
          extracted_statement_properties.statement_level_hints.size(),
          "), actual number of hints(", resolved_statement->hint_list().size(),
          ")")));
    }
  }

  void HandleOneResult(
      absl::string_view test_case, const AnalyzerOptions& options,
      TypeFactory* type_factory, Catalog* catalog, absl::string_view mode,
      const absl::Status& status,
      const AnalyzerOutput* output,
      const StatementProperties& extracted_statement_properties,
      file_based_test_driver::RunTestCaseResult* test_result) {
    std::string test_result_string;
    if (status.ok()) {
      const ResolvedStatement* resolved_statement =
          output->resolved_statement();
      const ResolvedExpr* resolved_expr = output->resolved_expr();

      const ResolvedNode* node;
      if (resolved_statement != nullptr) {
        ASSERT_TRUE(resolved_expr == nullptr);
        node = resolved_statement;
      } else {
        ASSERT_TRUE(resolved_expr != nullptr);
        node = resolved_expr;
      }
      node->ClearFieldsAccessed();
      test_result_string = node->DebugString();
      ZETASQL_ASSERT_OK(node->CheckNoFieldsAccessed());
      if (!test_case_options_.GetBool(kShowResolvedAST)) {
        // Hide debug string from test result if not requested in test case
        // options.
        //
        // Note: DebugString() is still computed, even if not requested, so we
        // can verify that DebugString() does not crash and does not
        // accidentally mark fields as accessed.
        test_result_string.clear();
      }

      // Append strings for any resolved templated objects in 'output'.
      ZETASQL_ASSERT_OK(
          AddResultStringsForTemplatedSqlObjects(node, test_result_string));

      // Check that the statement we resolved matches the statement properties
      // we extracted with GetStatementProperties.
      if (resolved_statement != nullptr) {
        CheckExtractedStatementProperties(
            resolved_statement, extracted_statement_properties, test_result);
      }

      if (!output->deprecation_warnings().empty()) {
        for (const absl::Status& warning : output->deprecation_warnings()) {
          // FormatError() also prints the attached DeprecationWarning.
          absl::StrAppend(&test_result_string, "\n\nDEPRECATION WARNING:\n",
                          FormatError(warning));
        }
      }
    } else {
      // This hides generic::INVALID_ARGUMENT, which is the normal error
      // code, and formats error location as " [at line:column]".
      // Other error codes will be shown unchanged.
      test_result_string = absl::StrCat("ERROR: ", FormatError(status));

      EXPECT_NE(status.code(), absl::StatusCode::kUnknown)
          << "UNKNOWN errors are not expected";

      if (!test_case_options_.GetBool(kAllowInternalError)) {
        EXPECT_NE(status.code(), absl::StatusCode::kInternal)
            << "Query cannot return internal error without ["
            << kAllowInternalError << "] option: " << status;
      }

      if (status.code() != absl::StatusCode::kInternal &&
          test_case_options_.GetBool(kExpectErrorLocation) &&
          options.error_message_mode() != ERROR_MESSAGE_WITH_PAYLOAD) {
        if (test_case_options_.GetBool(kEnhancedErrorRedaction)) {
          EXPECT_FALSE(absl::StrContains(status.message(), " [at "))
              << "Error location was not redacted: " << status;
        } else {
          EXPECT_TRUE(absl::StrContains(status.message(), " [at "))
              << "Error message has no ErrorLocation: " << status;
        }
      }
    }

    if (test_case_options_.GetBool(kShowExtractedTableNames)) {
      std::set<std::vector<std::string>> table_names;
      const absl::Status extract_status =
          ExtractTableNamesFromStatement(test_case, options, &table_names);

      std::string table_names_string = "Extracted table names:\n";
      if (!extract_status.ok()) {
        absl::StrAppend(&table_names_string,
                        "ERROR: ", FormatError(extract_status));
      } else {
        for (const std::vector<std::string>& path : table_names) {
          absl::StrAppend(&table_names_string, IdentifierPathToString(path),
                          "\n");
        }
      }

      test_result_string =
          absl::StrCat(table_names_string, "\n", test_result_string);
      absl::StripAsciiWhitespace(&test_result_string);
    }

    if (test_case_options_.GetBool(kShowTableResolutionTime)) {
      std::string table_resolution_time_str;
      ExtractTableResolutionTimeInfoMapAsString(test_case, options,
                                                type_factory, catalog,
                                                &table_resolution_time_str);
      std::string table_resolution_time_str_from_ast;
      std::string table_resolution_time_str_from_ast_with_deferred_analysis;
      ExtractTableResolutionTimeInfoMapFromASTAsString(
          test_case, options, type_factory, catalog,
          &table_resolution_time_str_from_ast,
          &table_resolution_time_str_from_ast_with_deferred_analysis);
      EXPECT_EQ(table_resolution_time_str, table_resolution_time_str_from_ast);
      EXPECT_EQ(table_resolution_time_str,
                table_resolution_time_str_from_ast_with_deferred_analysis);
      test_result_string =
          absl::StrCat("Table resolution time:\n", table_resolution_time_str,
                       "\n", test_result_string);
      absl::StripTrailingAsciiWhitespace(&test_result_string);
    }

    if (test_case_options_.GetBool(kRunSqlBuilder) &&
        // We do not run the SQLBuilder if the original query failed analysis.
        output != nullptr) {
      std::string result_string;
      TestSqlBuilder(test_case, options, catalog, mode == "statement", output,
                     &result_string);
      absl::StrAppend(&test_result_string, "\n", result_string);
      absl::StripAsciiWhitespace(&test_result_string);
    }

    if (mode == "statement" &&
        !test_case_options_.GetBool(kDoNotShowReplacedLiterals) &&
        !test_case_options_.GetString(kParseLocationRecordType).empty() &&
        !zetasql_base::CaseEqual(
            test_case_options_.GetString(kParseLocationRecordType),
            ParseLocationRecordType_Name(PARSE_LOCATION_RECORD_NONE)) &&
        // We do not print the literal-free string if the original query failed
        // analysis.
        output != nullptr) {
      std::string result_string;
      TestLiteralReplacementInGoldens(test_case, options, output,
                                      &result_string);
      absl::StrAppend(&test_result_string, "\n", result_string);
      absl::StripAsciiWhitespace(&test_result_string);
    }

    if (test_case_options_.GetBool(kAllowUndeclaredParameters) &&
        // We do not print the untyped parameters if the original query failed
        // analysis.
        output != nullptr) {
      std::string result_string;
      TestUndeclaredParameters(output, &result_string);
      absl::StrAppend(&test_result_string, "\n", result_string);
      absl::StripAsciiWhitespace(&test_result_string);
    }

    if (test_case_options_.GetBool(kShowReferencedPropertyGraphs) &&
        output != nullptr) {
      std::string result_string;
      TestReferencedPropertyGraph(output, &result_string);
      absl::StrAppend(&test_result_string, "\n", result_string);
      absl::StripAsciiWhitespace(&test_result_string);
    }

    ZETASQL_ASSERT_OK_AND_ASSIGN(AnalyzerTestRewriteGroups rewrite_groups,
                         GetEnabledRewrites(test_case_options_));
    if (!rewrite_groups.empty() &&
        // We do not print the rewritten AST if the original query failed
        // analysis.
        status.ok() && output != nullptr) {
      struct RewriteGroupOutcome {
        absl::Status status;
        std::string ast_debug;
        std::string sqlbuilder_output;
        std::vector<std::string> rewrite_group_keys;
        std::string key() {
          return std::string(status.ok() ? ast_debug : status.message());
        }
      };

      // Compute the pre-rewrite result string, which consists of the AST and
      // any templated SQL objects. Rewrites can also be applied to templated
      // SQL objects which may be nested inside of other SQL objects. In this
      // case the debug string of the main statement's AST will be the same, but
      // the templated SQL objects will have different ASTs.
      std::string pre_rewrite_result_string;
      if (output->resolved_statement() != nullptr) {
        pre_rewrite_result_string = output->resolved_statement()->DebugString();
        ZETASQL_ASSERT_OK(AddResultStringsForTemplatedSqlObjects(
            output->resolved_statement(), pre_rewrite_result_string));
      }

      std::vector<RewriteGroupOutcome> rewrite_group_results;
      absl::flat_hash_map<std::string, int64_t> rewrite_group_result_map;
      for (auto& [key, rewrites] : rewrite_groups) {
        RewriteGroupOutcome outcome;
        // Copy the analyzer output so it can be rewritten. Normally we wouldn't
        // care about keeping the original output and would just move it in.
        ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<AnalyzerOutput> rewrite_output,
                             CopyAnalyzerOutput(*output));
        AnalyzerOptions rewrite_options(options);
        rewrite_options.set_enabled_rewrites(rewrites);
        outcome.status = RewriteResolvedAst(rewrite_options, test_case, catalog,
                                            type_factory, *rewrite_output);

        if (outcome.status.ok()) {
          ZETASQL_EXPECT_OK(rewrite_output->resolved_node()->CheckNoFieldsAccessed());
        }

        if (outcome.status.ok() &&
            rewrite_output->resolved_statement() != nullptr) {
          outcome.ast_debug =
              rewrite_output->resolved_statement()->DebugString();
          // Append strings for any resolved templated objects in 'output'.
          ZETASQL_ASSERT_OK(AddResultStringsForTemplatedSqlObjects(
              rewrite_output->resolved_statement(), outcome.ast_debug));
          if (outcome.ast_debug == pre_rewrite_result_string) {
            continue;
          }
          if (test_case_options_.GetBool(kRunSqlBuilder)) {
            TestSqlBuilder(test_case, options, catalog, /*is_statement=*/true,
                           rewrite_output.get(), &outcome.sqlbuilder_output);
          }
        } else if (outcome.status.ok()) {
          ASSERT_NE(rewrite_output->resolved_expr(), nullptr);
          outcome.ast_debug = rewrite_output->resolved_expr()->DebugString();
          if (outcome.ast_debug == output->resolved_expr()->DebugString()) {
            continue;
          }
          if (test_case_options_.GetBool(kRunSqlBuilder)) {
            TestSqlBuilder(test_case, options, catalog, /*is_statement=*/false,
                           rewrite_output.get(), &outcome.sqlbuilder_output);
          }
        }
        std::string outcome_key = outcome.key();
        size_t outcome_index = 0;
        if (rewrite_group_result_map.contains(outcome_key)) {
          outcome_index = rewrite_group_result_map[outcome_key];
        } else {
          rewrite_group_results.push_back(outcome);
          outcome_index = rewrite_group_results.size() - 1;
          rewrite_group_result_map[outcome_key] = outcome_index;
        }
        rewrite_group_results[outcome_index].rewrite_group_keys.push_back(key);
      }
      if (!rewrite_group_results.empty()) {
        if (rewrite_group_results.size() == 1 &&
            !rewrite_group_results[0].status.ok()) {
          test_result_string =
              absl::StrCat("[PRE-REWRITE AST]\n", test_result_string);
        }
        for (auto& outcome : rewrite_group_results) {
          std::string groups = absl::StrJoin(outcome.rewrite_group_keys, "|");
          std::string groups_header = absl::StrCat(
              "\n\n[[ REWRITER ARTIFACTS FOR RULE GROUPS '", groups, "' ]]\n");
          if (rewrite_group_results.size() == 1) {
            // TODO: Remove this exception and update relevant goldens.
            groups_header = "\n\n";
          }
          if (!outcome.status.ok()) {
            absl::StrAppend(&test_result_string, groups_header,
                            "Rewrite ERROR: ", FormatError(outcome.status));
          } else {
            absl::StrAppend(&test_result_string, groups_header);
            if (test_case_options_.GetBool(kShowResolvedAST) &&
                !outcome.ast_debug.empty()) {
              absl::StrAppend(&test_result_string, "[REWRITTEN AST]\n",
                              outcome.ast_debug);
            }
            absl::StrAppend(&test_result_string, outcome.sqlbuilder_output);
            absl::StripAsciiWhitespace(&test_result_string);
          }
        }
      }
    }

    // Skip adding a second output if it would be empty and we've already got
    // an output string.
    if (!test_result_string.empty() || test_result->test_outputs().empty()) {
      test_result->AddTestOutput(test_result_string);
    }

    if (test_dumper_callback_ != nullptr) {
      if (status.ok()) {
        test_dumper_callback_(test_case, test_case_options_, options, output,
                              test_result);
      } else {
        test_dumper_callback_(test_case, test_case_options_, options, status,
                              test_result);
      }
    }
  }

  // This fully handles 'prepare_database' (including ignoring it).
  absl::Status HandlePrepareDatabase(
      const AnalyzerOptions& options,
      std::unique_ptr<TypeFactory>& type_factory,
      std::unique_ptr<const AnalyzerOutput>& output) {
    std::string prepare_database_name =
        test_case_options_.GetString(kPrepareDatabase);
    if (prepare_database_name.empty()) {
      return absl::OkStatus();
    }
    std::string use_database_name = test_case_options_.GetString(kUseDatabase);
    std::string suppressed_functions =
        test_case_options_.GetString(kSuppressFunctions);
    ZETASQL_RET_CHECK(suppressed_functions.empty())
        << "prepare_database with suppress_functions not yet supported";
    bool new_database = prev_prepare_database_name_.empty() ||
                        prev_prepare_database_name_ != prepare_database_name;
    prev_prepare_database_name_ = prepare_database_name;
    CatalogFactory::PreparedDatabase* database_entry = nullptr;
    if (new_database) {
      ZETASQL_ASSIGN_OR_RETURN(database_entry,
                       catalog_factory_.CreateDatabase(
                           prepare_database_name, use_database_name,
                           options.language(), /* suppress_functions*/ {}));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(database_entry, catalog_factory_.GetMutableDatabase(
                                           prepare_database_name));
    }
    ABSL_CHECK_NE(database_entry, nullptr);

    ZETASQL_RET_CHECK(output->resolved_statement() != nullptr);
    switch (output->resolved_statement()->node_kind()) {
      case RESOLVED_CREATE_FUNCTION_STMT: {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<Function> function,
                         MakeFunctionFromCreateFunction(
                             *output->resolved_statement()
                                  ->GetAs<ResolvedCreateFunctionStmt>(),
                             /*function_options=*/nullptr));
        if (!database_entry->mutable_catalog()->AddOwnedFunctionIfNotPresent(
                &function)) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "prepare_database duplicate function definition %s",
              function->DebugString()));
        }
        break;
      }
      case RESOLVED_CREATE_TABLE_STMT: {
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<SimpleTable> table,
            MakeTableFromCreateTable(*output->resolved_statement()
                                          ->GetAs<ResolvedCreateTableStmt>()));
        const std::string table_name = table->Name();
        if (!database_entry->mutable_catalog()->AddOwnedTableIfNotPresent(
                table_name, std::move(table))) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "prepare_database duplicate table definition %s", table_name));
        }
        break;
      }
      case RESOLVED_CREATE_TABLE_FUNCTION_STMT: {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TableValuedFunction> tvf,
                         MakeTVFFromCreateTableFunction(
                             *output->resolved_statement()
                                  ->GetAs<ResolvedCreateTableFunctionStmt>()));
        if (!database_entry->mutable_catalog()
                 ->AddOwnedTableValuedFunctionIfNotPresent(&tvf)) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "prepare_database duplicate TVF definition %s", tvf->Name()));
        }
        break;
      }
      case RESOLVED_CREATE_CONSTANT_STMT: {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<SQLConstant> owned_constant,
                         MakeConstantFromCreateConstant(
                             *output->resolved_statement()
                                  ->GetAs<ResolvedCreateConstantStmt>()));
        if (options.constant_evaluator() != nullptr) {
          ZETASQL_RETURN_IF_ERROR(owned_constant->SetEvaluationResult(
              options.constant_evaluator()->Evaluate(
                  *owned_constant->constant_expression())));
        }

        const SQLConstant* constant = owned_constant.get();
        if (!database_entry->mutable_catalog()->AddOwnedConstantIfNotPresent(
                std::move(owned_constant))) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "prepare_database duplicate constant definition %s",
              constant->Name()));
        }
        break;
      }
      default:
        return absl::InvalidArgumentError(
            absl::StrFormat("prepare_database does not support %s",
                            output->resolved_statement()->node_kind_string()));
    }
    database_entry->TakeOwnership(std::move(type_factory));
    database_entry->TakeOwnership(std::move(output));

    return absl::OkStatus();
  }

  static std::vector<std::string> ToLower(
      const std::vector<std::string>& path) {
    std::vector<std::string> result;
    for (const std::string& identifier : path) {
      result.push_back(absl::AsciiStrToLower(identifier));
    }
    return result;
  }

  void CheckExtractTableNames(absl::string_view test_case,
                              const AnalyzerOptions& options,
                              const AnalyzerOutput* analyzer_output) {
    {
      // For ExtractTableNamesFromStatement()
      std::set<std::vector<std::string>> extracted_table_names;
      const absl::Status find_tables_status = ExtractTableNamesFromStatement(
          test_case, options, &extracted_table_names);
      CheckExtractTableResult(find_tables_status, extracted_table_names,
                              analyzer_output);
    }
    {
      // For ExtractTableNamesFromASTStatement()
      std::set<std::vector<std::string>> extracted_table_names;

      std::unique_ptr<ParserOutput> parser_output;
      if (ParseStatement(test_case, options.GetParserOptions(), &parser_output)
              .ok()) {
        const absl::Status find_tables_status =
            ExtractTableNamesFromASTStatement(*parser_output->statement(),
                                              options, test_case,
                                              &extracted_table_names);
        CheckExtractTableResult(find_tables_status, extracted_table_names,
                                analyzer_output);
      }
    }
  }

  void CheckExtractNextTableNames(
      ParseResumeLocation* location, const AnalyzerOptions& options,
      const ParseResumeLocation& location_for_analysis,
      const AnalyzerOutput* analyzer_output) {
    std::set<std::vector<std::string>> extracted_table_names;
    bool at_end_of_input;
    const absl::Status find_tables_status = ExtractTableNamesFromNextStatement(
        location, options, &extracted_table_names, &at_end_of_input);
    CheckExtractTableResult(find_tables_status, extracted_table_names,
                            analyzer_output);
    EXPECT_EQ(location->byte_position(), location_for_analysis.byte_position());
  }

  void CheckExtractTableResult(
      const absl::Status& find_tables_status,
      const std::set<std::vector<std::string>>& extracted_table_names,
      const AnalyzerOutput* analyzer_output) {
    if (!find_tables_status.ok()) {
      // If ExtractTableNames failed, analysis should have failed too.
      EXPECT_TRUE(analyzer_output == nullptr)
          << "ExtractTableNames failed but query analysis succeeded: "
          << find_tables_status;
      return;
    }

    if (analyzer_output == nullptr) {
      // The full statement is not valid.  We can't check that ExtractTables
      // output makes sense.
      return;
    }

    std::set<std::vector<std::string>> actual_tables_scanned;
    std::vector<const ResolvedNode*> nodes;
    analyzer_output->resolved_statement()->GetDescendantsWithKinds(
        {RESOLVED_TABLE_SCAN, RESOLVED_FOREIGN_KEY}, &nodes);
    for (const ResolvedNode* node : nodes) {
      const Table* table;
      if (node->Is<ResolvedTableScan>()) {
        const ResolvedTableScan* table_scan = node->GetAs<ResolvedTableScan>();
        table = table_scan->table();
      } else {
        const ResolvedForeignKey* foreign_key =
            node->GetAs<ResolvedForeignKey>();
        table = foreign_key->referenced_table();
      }

      // This splitting is not generally correct because table names could
      // have dots, but it's good enough for the test schema we have.
      const std::vector<std::string> table_path =
          ToLower(absl::StrSplit(table->FullName(), '.'));
      actual_tables_scanned.insert(table_path);
    }

    // Templated SQL TVFs and UDFs can have tables referenced in their
    // SQL expressions, but only the SQL expression text appears in the
    // ResolvedAST, not the resolved SQL expression (we have no way to
    // resolve the expression if we do not know what the argument types
    // are).  So extracted table names may be found, while there is no
    // resolved expression and therefore we will not find any actual tables
    // scanned.  So we expect that we did not find any actual table scans,
    // and do not try to match them against the exracted table names.
    if (analyzer_output->resolved_statement()->node_kind() ==
            RESOLVED_CREATE_TABLE_FUNCTION_STMT &&
        analyzer_output->resolved_statement()
            ->GetAs<ResolvedCreateTableFunctionStmt>()
            ->signature()
            .IsTemplated()) {
      EXPECT_TRUE(actual_tables_scanned.empty());
      return;
    } else if (analyzer_output->resolved_statement()->node_kind() ==
                   RESOLVED_CREATE_FUNCTION_STMT &&
               analyzer_output->resolved_statement()
                   ->GetAs<ResolvedCreateFunctionStmt>()
                   ->signature()
                   .IsTemplated()) {
      EXPECT_TRUE(actual_tables_scanned.empty());
      return;
    }

    std::set<std::vector<std::string>> extracted_table_names_lower;
    for (const std::vector<std::string>& path : extracted_table_names) {
      extracted_table_names_lower.insert(ToLower(path));
    }

    // For create table statement with like, the like table is extracted but
    // not scanned.
    if (test_case_options_.GetBool(kCreateTableLikeNotScanned) ||
        test_case_options_.GetBool(kPrivilegeRestrictionTableNotScanned)) {
      EXPECT_EQ(extracted_table_names_lower.size() - 1,
                actual_tables_scanned.size());
      return;
    }

    // We expect that the number of extracted names and the number of actual
    // table scan names are the same.
    EXPECT_EQ(extracted_table_names_lower.size(), actual_tables_scanned.size())
        << "Extracted table names: "
        << absl::StrJoin(extracted_table_names_lower, ", ",
                         [](std::string* out, const auto& a) {
                           absl::StrAppend(out, "[", absl::StrJoin(a, ", "),
                                           "]");
                         })
        << ", actual tables scanned: "
        << absl::StrJoin(actual_tables_scanned, ", ",
                         [](std::string* out, const auto& a) {
                           absl::StrAppend(out, "[", absl::StrJoin(a, ", "),
                                           "]");
                         });

    // All of the single-part table names that we've extracted should match
    // actual tables scanned.
    for (const std::vector<std::string>& extracted_name :
         extracted_table_names_lower) {
      if (extracted_name.size() == 1) {
        // If the statement has tables in a nested namespace (subcatalog),
        // then the equivalence check between an actual table scanned and
        // its corresponding extracted table name will fail.  This is because
        // extracted table names include the full name path in the query,
        // while the actual tables scanned names only includes the scan
        // Table's name() - which does not include the namespace in the
        // ZetaSQL test data (and in some cases is completely different
        // from the name registered in the Catalog).  So we ignore this
        // equivalence check if the extracted name has multiple parts.
        EXPECT_TRUE(zetasql_base::ContainsKey(actual_tables_scanned, extracted_name));
      }
    }
  }

  void CheckStrictMode(absl::string_view test_case,
                       const AnalyzerOptions& orig_options,
                       const absl::Status& orig_status,
                       const AnalyzerOutput* orig_output, Catalog* catalog,
                       TypeFactory* type_factory,
                       file_based_test_driver::RunTestCaseResult* test_result) {
    AnalyzerOptions strict_options = orig_options;
    strict_options.mutable_language()->set_name_resolution_mode(
        NAME_RESOLUTION_STRICT);
    // Don't mess up any shared sequence generator for the original query.
    strict_options.set_column_id_sequence_number(nullptr);

    std::unique_ptr<const AnalyzerOutput> strict_output;
    const absl::Status strict_status = AnalyzeStatement(
        test_case, strict_options, catalog, type_factory, &strict_output);
    if (orig_status.ok() && strict_status.ok()) {
      // If column_id_sequence_number was set, we'll get different column_ids
      // in the new resolved tree, so skip the diff.
      if (orig_options.column_id_sequence_number() == nullptr &&
          orig_output->resolved_statement()->DebugString() !=
              strict_output->resolved_statement()->DebugString()) {
        test_result->AddTestOutput(AddFailure(
            absl::StrCat("FAILURE: Strict mode resolved AST differs:\n",
                         strict_output->resolved_statement()->DebugString())));
      }
    } else if (orig_status != strict_status) {
      if (test_case_options_.GetBool(kShowStrictMode)) {
        // We don't show these errors all the time because the majority
        // of our test queries fail in stict mode.
        test_result->AddTestOutput(
            absl::StrCat("STRICT MODE ERROR: ", FormatError(strict_status)));
      }
      if (strict_status.ok()) {
        AddFailure("Query passed in strict mode but failed in default mode");
      }
    }
  }

  void CheckDeepCopyAST(const AnalyzerOutput* orig_output) {
    // Get the debug string of the regular AST.
    const std::string original_debug_string =
        orig_output->resolved_statement()->DebugString();

    // Create deep copy visitor.
    ResolvedASTDeepCopyVisitor visitor;
    // Accept the visitor on the resolved query.
    ZETASQL_EXPECT_OK(orig_output->resolved_statement()->Accept(&visitor));

    // Consume the root to initiate deep copy.
    auto deep_copy = visitor.ConsumeRootNode<ResolvedNode>();

    // ConsumeRootNode returns StatusOr -- verify that the status was OK.
    ZETASQL_ASSERT_OK(deep_copy);

    // Get the value from the StatusOr.
    auto deep_copy_ast = std::move(deep_copy).value();

    // Verify that the debug string matches.
    EXPECT_EQ(original_debug_string, deep_copy_ast->DebugString());
  }

  void CheckSerializeDeserializeAST(const AnalyzerOptions& options_in,
                                    Catalog* catalog, TypeFactory* type_factory,
                                    const absl::Status& orig_status,
                                    const AnalyzerOutput& orig_output) {
    if (!orig_status.ok() || !test_case_options_.GetBool(kRunDeserializer)) {
      return;
    }
    AnalyzerOptions options = options_in;
    options.CreateDefaultArenasIfNotSet();

    // Parse locations are not preserved across serialization, so strip them
    // when generating expected output.
    std::unique_ptr<ResolvedNode> stripped =
        StripParseLocations(orig_output.resolved_statement());

    std::string original_debug_string = stripped->DebugString();

    AnyResolvedStatementProto proto;
    FileDescriptorSetMap map;
    ZETASQL_ASSERT_OK(orig_output.resolved_statement()->SaveTo(&map, &proto));

    std::vector<const google::protobuf::DescriptorPool*> pools;
    for (const auto& elem : map) pools.push_back(elem.first);
    ResolvedNode::RestoreParams restore_params(pools, catalog, type_factory,
                                               options.id_string_pool().get());

    auto restored = ResolvedStatement::RestoreFrom(proto, restore_params);
    ZETASQL_ASSERT_OK(restored.status()) << "error restoring: " << proto.DebugString();
    EXPECT_EQ(original_debug_string, restored.value()->DebugString());
  }

  void CheckValidatorCoverage(const AnalyzerOptions& options,
                              const AnalyzerOutput& output) {
    const ResolvedStatement* stmt = output.resolved_statement();

    // For queries, we track accessed fields carefully, and check here
    // that the validator accessed all fields.
    // For other statement kinds, we don't bother because there are too
    // many scalar modifier fields to tag them all.
    if (stmt->node_kind() == RESOLVED_QUERY_STMT) {
      stmt->ClearFieldsAccessed();

      ValidatorOptions validator_options{
          .allowed_hints_and_options = options.allowed_hints_and_options()};
      Validator validator(options.language(), std::move(validator_options));
      ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(stmt));

      ZETASQL_EXPECT_OK(stmt->CheckFieldsAccessed())
          << "ERROR: Validator did not traverse all fields in the resolved AST "
             "for this statement. Updates to validator.cc are probably "
             "required so it will traverse or ignore new nodes/fields.\n"
          << stmt->DebugString();
    }
  }

  // We do custom comparison on Statements because we want to do custom
  // comparison on the OutputColumnLists.
  //
  // NOTE: The result from this is currently ignored by default because we can't
  // guarantee that the exact same resolved AST comes back after re-analyzing
  // the SQLBuilder output. This is controlled by the
  // show_sqlbuilder_resolved_ast_diff option.
  //
  // CompareStatementShape is a weaker form of this that just checks that the
  // result shape (column names, types, etc) matches, and that is tested by
  // default.
  bool CompareStatement(const ResolvedStatement* output_stmt,
                        const ResolvedStatement* sqlbuilder_stmt) {
    if (output_stmt->node_kind() != sqlbuilder_stmt->node_kind()) {
      return false;
    }

    switch (sqlbuilder_stmt->node_kind()) {
      case RESOLVED_EXPLAIN_STMT:
        return CompareStatement(
            output_stmt->GetAs<ResolvedExplainStmt>()->statement(),
            sqlbuilder_stmt->GetAs<ResolvedExplainStmt>()->statement());
      case RESOLVED_DEFINE_TABLE_STMT:
        return CompareOptionList(
            output_stmt->GetAs<ResolvedDefineTableStmt>()->option_list(),
            sqlbuilder_stmt->GetAs<ResolvedDefineTableStmt>()->option_list());
      case RESOLVED_QUERY_STMT: {
        const ResolvedQueryStmt* output_query_stmt =
            output_stmt->GetAs<ResolvedQueryStmt>();
        const ResolvedQueryStmt* sqlbuilder_query_stmt =
            sqlbuilder_stmt->GetAs<ResolvedQueryStmt>();
        return CompareNode(output_query_stmt->query(),
                           sqlbuilder_query_stmt->query()) &&
               CompareOutputColumnList(
                   output_query_stmt->output_column_list(),
                   sqlbuilder_query_stmt->output_column_list());
      }
      case RESOLVED_DELETE_STMT: {
        const ResolvedDeleteStmt* output_delete_stmt =
            output_stmt->GetAs<ResolvedDeleteStmt>();
        const ResolvedDeleteStmt* sqlbuilder_delete_stmt =
            sqlbuilder_stmt->GetAs<ResolvedDeleteStmt>();
        return CompareNode(output_delete_stmt->returning(),
                           sqlbuilder_delete_stmt->returning());
      }
      case RESOLVED_UPDATE_STMT: {
        const ResolvedUpdateStmt* output_update_stmt =
            output_stmt->GetAs<ResolvedUpdateStmt>();
        const ResolvedUpdateStmt* sqlbuilder_update_stmt =
            sqlbuilder_stmt->GetAs<ResolvedUpdateStmt>();
        return CompareNode(output_update_stmt->returning(),
                           sqlbuilder_update_stmt->returning());
      }
      case RESOLVED_INSERT_STMT: {
        const ResolvedInsertStmt* output_insert_stmt =
            output_stmt->GetAs<ResolvedInsertStmt>();
        const ResolvedInsertStmt* sqlbuilder_insert_stmt =
            sqlbuilder_stmt->GetAs<ResolvedInsertStmt>();
        return CompareNode(output_insert_stmt->returning(),
                           sqlbuilder_insert_stmt->returning());
      }
      case RESOLVED_EXECUTE_IMMEDIATE_STMT: {
        const ResolvedExecuteImmediateStmt* output_exec =
            output_stmt->GetAs<ResolvedExecuteImmediateStmt>();
        const ResolvedExecuteImmediateStmt* sqlbuilder_exec =
            sqlbuilder_stmt->GetAs<ResolvedExecuteImmediateStmt>();
        return CompareNode(output_exec, sqlbuilder_exec);
      }
      case RESOLVED_CREATE_INDEX_STMT: {
        const ResolvedCreateIndexStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateIndexStmt>();
        const ResolvedCreateIndexStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateIndexStmt>();
        return CompareIndexItemList(
                   output_create_stmt->index_item_list(),
                   sqlbuilder_create_stmt->index_item_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CREATE_DATABASE_STMT: {
        const ResolvedCreateDatabaseStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        const ResolvedCreateDatabaseStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        return CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CREATE_SCHEMA_STMT: {
        const ResolvedCreateSchemaStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateSchemaStmt>();
        const ResolvedCreateSchemaStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateSchemaStmt>();
        return ComparePath(output_create_stmt->name_path(),
                           sqlbuilder_create_stmt->name_path()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CREATE_TABLE_STMT: {
        const ResolvedCreateTableStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableStmt>();
        const ResolvedCreateTableStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateTableStmt>();
        return CompareColumnDefinitionList(
                   output_create_stmt->column_definition_list(),
                   sqlbuilder_create_stmt->column_definition_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CREATE_SNAPSHOT_TABLE_STMT: {
        const ResolvedCreateSnapshotTableStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateSnapshotTableStmt>();
        const ResolvedCreateSnapshotTableStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateSnapshotTableStmt>();
        return CompareNode(output_create_stmt->clone_from(),
                           sqlbuilder_create_stmt->clone_from()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CREATE_TABLE_AS_SELECT_STMT: {
        const ResolvedCreateTableAsSelectStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        const ResolvedCreateTableAsSelectStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        return CompareNode(output_create_stmt->query(),
                           sqlbuilder_create_stmt->query()) &&
               CompareOutputColumnList(
                   output_create_stmt->output_column_list(),
                   sqlbuilder_create_stmt->output_column_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CLONE_DATA_STMT: {
        const ResolvedCloneDataStmt* resolved =
            output_stmt->GetAs<ResolvedCloneDataStmt>();
        const ResolvedCloneDataStmt* sqlbuilder =
            sqlbuilder_stmt->GetAs<ResolvedCloneDataStmt>();
        return CompareNode(resolved->target_table(),
                           sqlbuilder->target_table()) &&
               CompareNode(resolved->clone_from(), sqlbuilder->clone_from());
      }
      case RESOLVED_EXPORT_DATA_STMT: {
        const ResolvedExportDataStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportDataStmt>();
        const ResolvedExportDataStmt* sqlbuilder_export_stmt =
            sqlbuilder_stmt->GetAs<ResolvedExportDataStmt>();
        return CompareNode(output_export_stmt->query(),
                           sqlbuilder_export_stmt->query()) &&
               CompareOutputColumnList(
                   output_export_stmt->output_column_list(),
                   sqlbuilder_export_stmt->output_column_list()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 sqlbuilder_export_stmt->option_list());
      }
      case RESOLVED_EXPORT_MODEL_STMT: {
        const ResolvedExportModelStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportModelStmt>();
        const ResolvedExportModelStmt* sqlbuilder_export_stmt =
            sqlbuilder_stmt->GetAs<ResolvedExportModelStmt>();
        return ComparePath(output_export_stmt->model_name_path(),
                           sqlbuilder_export_stmt->model_name_path()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 sqlbuilder_export_stmt->option_list());
      }
      case RESOLVED_CREATE_CONSTANT_STMT: {
        const ResolvedCreateConstantStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateConstantStmt>();
        const ResolvedCreateConstantStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateConstantStmt>();
        return ComparePath(output_create_stmt->name_path(),
                           sqlbuilder_create_stmt->name_path()) &&
               CompareNode(output_create_stmt->expr(),
                           sqlbuilder_create_stmt->expr());
      }
      case RESOLVED_CREATE_ENTITY_STMT: {
        const auto* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateEntityStmt>();
        const auto* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateEntityStmt>();
        return ComparePath(output_create_stmt->name_path(),
                           sqlbuilder_create_stmt->name_path()) &&
               output_create_stmt->entity_type() ==
                   sqlbuilder_create_stmt->entity_type() &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_AUX_LOAD_DATA_STMT: {
        const ResolvedAuxLoadDataStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedAuxLoadDataStmt>();
        const ResolvedAuxLoadDataStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedAuxLoadDataStmt>();
        return CompareColumnDefinitionList(
                   output_create_stmt->column_definition_list(),
                   sqlbuilder_create_stmt->column_definition_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list()) &&
               CompareOptionList(
                   output_create_stmt->from_files_option_list(),
                   sqlbuilder_create_stmt->from_files_option_list());
      }
      default:
        ABSL_LOG(ERROR) << "Statement type " << sqlbuilder_stmt->node_kind_string()
                   << " not supported";
        return false;
    }
  }

  bool CompareNode(const ResolvedNode* output_query,
                   const ResolvedNode* sqlbuilder_query) {
    absl::StatusOr<bool> compare_result =
        ResolvedASTComparator::CompareResolvedAST(output_query,
                                                  sqlbuilder_query);
    if (!compare_result.status().ok()) {
      return false;
    }
    return compare_result.value();
  }

  // This compares the final output shape (column names, types,
  // value-table-ness, orderedness, etc), but not the actual nodes in the tree.
  bool CompareStatementShape(const ResolvedStatement* output_stmt,
                             const ResolvedStatement* sqlbuilder_stmt) {
    if (output_stmt->node_kind() != sqlbuilder_stmt->node_kind()) {
      return false;
    }

    switch (sqlbuilder_stmt->node_kind()) {
      case RESOLVED_EXPLAIN_STMT:
        return CompareStatementShape(
            output_stmt->GetAs<ResolvedExplainStmt>()->statement(),
            sqlbuilder_stmt->GetAs<ResolvedExplainStmt>()->statement());
      case RESOLVED_DEFINE_TABLE_STMT:
        return CompareOptionList(
            output_stmt->GetAs<ResolvedDefineTableStmt>()->option_list(),
            sqlbuilder_stmt->GetAs<ResolvedDefineTableStmt>()->option_list());
      case RESOLVED_QUERY_STMT: {
        const ResolvedQueryStmt* output_query_stmt =
            output_stmt->GetAs<ResolvedQueryStmt>();
        const ResolvedQueryStmt* sqlbuilder_query_stmt =
            sqlbuilder_stmt->GetAs<ResolvedQueryStmt>();
        return CompareOutputColumnList(
            output_query_stmt->output_column_list(),
            sqlbuilder_query_stmt->output_column_list());
        // TODO This should also be checking is_ordered, but that
        // is always broken right now because of http://b/36682469.
        //   output_query_stmt->query()->is_ordered() ==
        //   sqlbuilder_query_stmt->query()->is_ordered();
      }
      case RESOLVED_CREATE_INDEX_STMT: {
        const ResolvedCreateIndexStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateIndexStmt>();
        const ResolvedCreateIndexStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateIndexStmt>();
        return CompareIndexItemList(
                   output_create_stmt->index_item_list(),
                   sqlbuilder_create_stmt->index_item_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CREATE_DATABASE_STMT: {
        const ResolvedCreateDatabaseStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        const ResolvedCreateDatabaseStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        return CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
      }
      case RESOLVED_CREATE_TABLE_STMT: {
        const ResolvedCreateTableStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableStmt>();
        const ResolvedCreateTableStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateTableStmt>();
        return CompareColumnDefinitionList(
                   output_create_stmt->column_definition_list(),
                   sqlbuilder_create_stmt->column_definition_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list());
        // TODO Also verify primary key, etc.
      }
      case RESOLVED_CREATE_TABLE_AS_SELECT_STMT: {
        const ResolvedCreateTableAsSelectStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        const ResolvedCreateTableAsSelectStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        return CompareOutputColumnList(
                   output_create_stmt->output_column_list(),
                   sqlbuilder_create_stmt->output_column_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 sqlbuilder_create_stmt->option_list()) &&
               output_create_stmt->is_value_table() ==
                   sqlbuilder_create_stmt->is_value_table();
      }
      case RESOLVED_EXPORT_DATA_STMT: {
        const ResolvedExportDataStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportDataStmt>();
        const ResolvedExportDataStmt* sqlbuilder_export_stmt =
            sqlbuilder_stmt->GetAs<ResolvedExportDataStmt>();
        return CompareOutputColumnList(
                   output_export_stmt->output_column_list(),
                   sqlbuilder_export_stmt->output_column_list()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 sqlbuilder_export_stmt->option_list()) &&
               output_export_stmt->is_value_table() ==
                   sqlbuilder_export_stmt->is_value_table();
      }
      case RESOLVED_EXPORT_MODEL_STMT: {
        const ResolvedExportModelStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportModelStmt>();
        const ResolvedExportModelStmt* sqlbuilder_export_stmt =
            sqlbuilder_stmt->GetAs<ResolvedExportModelStmt>();
        return ComparePath(output_export_stmt->model_name_path(),
                           sqlbuilder_export_stmt->model_name_path()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 sqlbuilder_export_stmt->option_list());
      }
      case RESOLVED_EXPORT_METADATA_STMT: {
        const ResolvedExportMetadataStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportMetadataStmt>();
        const ResolvedExportMetadataStmt* sqlbuilder_export_stmt =
            sqlbuilder_stmt->GetAs<ResolvedExportMetadataStmt>();
        return ComparePath(output_export_stmt->name_path(),
                           sqlbuilder_export_stmt->name_path()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 sqlbuilder_export_stmt->option_list());
      }
      case RESOLVED_CREATE_CONSTANT_STMT: {
        return CompareNode(output_stmt, sqlbuilder_stmt);
      }
      case RESOLVED_START_BATCH_STMT: {
        const ResolvedStartBatchStmt* output_batch_stmt =
            output_stmt->GetAs<ResolvedStartBatchStmt>();
        const ResolvedStartBatchStmt* sqlbuilder_batch_stmt =
            sqlbuilder_stmt->GetAs<ResolvedStartBatchStmt>();
        return output_batch_stmt->batch_type() ==
               sqlbuilder_batch_stmt->batch_type();
      }
      case RESOLVED_ASSIGNMENT_STMT:
        return CompareExpressionShape(
                   output_stmt->GetAs<ResolvedAssignmentStmt>()->target(),
                   sqlbuilder_stmt->GetAs<ResolvedAssignmentStmt>()
                       ->target()) &&
               CompareExpressionShape(
                   output_stmt->GetAs<ResolvedAssignmentStmt>()->expr(),
                   sqlbuilder_stmt->GetAs<ResolvedAssignmentStmt>()->expr());
      case RESOLVED_CREATE_PROPERTY_GRAPH_STMT: {
        const ResolvedCreatePropertyGraphStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreatePropertyGraphStmt>();
        const ResolvedCreatePropertyGraphStmt* sqlbuilder_create_stmt =
            sqlbuilder_stmt->GetAs<ResolvedCreatePropertyGraphStmt>();

        if (!ComparePath(output_create_stmt->name_path(),
                         sqlbuilder_create_stmt->name_path())) {
          return false;
        }
        if (output_create_stmt->create_mode() !=
            sqlbuilder_create_stmt->create_mode()) {
          return false;
        }
        if (output_create_stmt->create_scope() !=
            sqlbuilder_create_stmt->create_scope()) {
          return false;
        }
        if (!CompareOptionList(output_create_stmt->option_list(),
                               sqlbuilder_create_stmt->option_list())) {
          return false;
        }
        if (!absl::c_equal(
                output_create_stmt->label_list(),
                sqlbuilder_create_stmt->label_list(),
                absl::bind_front(&AnalyzerTestRunner::GraphElementLabelEqual,
                                 this))) {
          return false;
        }
        if (!absl::c_equal(
                output_create_stmt->property_declaration_list(),
                sqlbuilder_create_stmt->property_declaration_list(),
                absl::bind_front(
                    &AnalyzerTestRunner::GraphPropertyDeclarationEqual,
                    this))) {
          return false;
        }
        if (!absl::c_equal(
                output_create_stmt->node_table_list(),
                sqlbuilder_create_stmt->node_table_list(),
                absl::bind_front(&AnalyzerTestRunner::GraphElementTableEqual,
                                 this))) {
          return false;
        }
        if (!absl::c_equal(
                output_create_stmt->edge_table_list(),
                sqlbuilder_create_stmt->edge_table_list(),
                absl::bind_front(&AnalyzerTestRunner::GraphElementTableEqual,
                                 this))) {
          return false;
        }
        ZETASQL_EXPECT_OK(output_create_stmt->CheckFieldsAccessed());
        ZETASQL_EXPECT_OK(sqlbuilder_create_stmt->CheckFieldsAccessed());
        return true;
      }

      // There is nothing to test for these statement kinds, or we just
      // haven't implemented any comparison yet.  Some of these could do
      // full CompareNode comparison.
      case RESOLVED_ABORT_BATCH_STMT:
      case RESOLVED_ALTER_CONNECTION_STMT:
      case RESOLVED_ALTER_ALL_ROW_ACCESS_POLICIES_STMT:
      case RESOLVED_ALTER_DATABASE_STMT:
      case RESOLVED_ALTER_MATERIALIZED_VIEW_STMT:
      case RESOLVED_ALTER_APPROX_VIEW_STMT:
      case RESOLVED_ALTER_PRIVILEGE_RESTRICTION_STMT:
      case RESOLVED_ALTER_MODEL_STMT:
      case RESOLVED_ALTER_ROW_ACCESS_POLICY_STMT:
      case RESOLVED_ALTER_SCHEMA_STMT:
      case RESOLVED_ALTER_EXTERNAL_SCHEMA_STMT:
      case RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT:
      case RESOLVED_ALTER_TABLE_STMT:
      case RESOLVED_ALTER_VIEW_STMT:
      case RESOLVED_ALTER_ENTITY_STMT:
      case RESOLVED_ALTER_INDEX_STMT:
      case RESOLVED_ANALYZE_STMT:
      case RESOLVED_ASSERT_STMT:
      case RESOLVED_BEGIN_STMT:
      case RESOLVED_CALL_STMT:
      case RESOLVED_CLONE_DATA_STMT:
      case RESOLVED_COMMIT_STMT:
      case RESOLVED_CREATE_CONNECTION_STMT:
      case RESOLVED_CREATE_EXTERNAL_TABLE_STMT:
      case RESOLVED_CREATE_FUNCTION_STMT:
      case RESOLVED_CREATE_MATERIALIZED_VIEW_STMT:
      case RESOLVED_CREATE_APPROX_VIEW_STMT:
      case RESOLVED_CREATE_MODEL_STMT:
      case RESOLVED_CREATE_PROCEDURE_STMT:
      case RESOLVED_CREATE_PRIVILEGE_RESTRICTION_STMT:
      case RESOLVED_CREATE_ROW_ACCESS_POLICY_STMT:
      case RESOLVED_CREATE_SCHEMA_STMT:
      case RESOLVED_CREATE_EXTERNAL_SCHEMA_STMT:
      case RESOLVED_CREATE_SNAPSHOT_TABLE_STMT:
      case RESOLVED_CREATE_TABLE_FUNCTION_STMT:
      case RESOLVED_CREATE_VIEW_STMT:
      case RESOLVED_CREATE_ENTITY_STMT:
      case RESOLVED_DELETE_STMT:
      case RESOLVED_DESCRIBE_STMT:
      case RESOLVED_DROP_FUNCTION_STMT:
      case RESOLVED_DROP_INDEX_STMT:
      case RESOLVED_DROP_TABLE_FUNCTION_STMT:
      case RESOLVED_DROP_PRIVILEGE_RESTRICTION_STMT:
      case RESOLVED_DROP_ROW_ACCESS_POLICY_STMT:
      case RESOLVED_DROP_STMT:
      case RESOLVED_DROP_MATERIALIZED_VIEW_STMT:
      case RESOLVED_DROP_SNAPSHOT_TABLE_STMT:
      case RESOLVED_EXECUTE_IMMEDIATE_STMT:
      case RESOLVED_GRANT_STMT:
      case RESOLVED_IMPORT_STMT:
      case RESOLVED_INSERT_STMT:
      case RESOLVED_MERGE_STMT:
      case RESOLVED_MODULE_STMT:
      case RESOLVED_RENAME_STMT:
      case RESOLVED_REVOKE_STMT:
      case RESOLVED_ROLLBACK_STMT:
      case RESOLVED_RUN_BATCH_STMT:
      case RESOLVED_SET_TRANSACTION_STMT:
      case RESOLVED_SHOW_STMT:
      case RESOLVED_TRUNCATE_STMT:
      case RESOLVED_UNDROP_STMT:
      case RESOLVED_UPDATE_STMT:
      case RESOLVED_AUX_LOAD_DATA_STMT:
        return true;

      default:
        ABSL_LOG(ERROR) << "Statement type " << sqlbuilder_stmt->node_kind_string()
                   << " not supported";
        return false;
    }
  }

  bool CompareExpressionShape(const ResolvedExpr* output_expr,
                              const ResolvedExpr* sqlbuilder_expr,
                              const bool mark_fields_accessed = false) {
    if (mark_fields_accessed) {
      output_expr->MarkFieldsAccessed();
      sqlbuilder_expr->MarkFieldsAccessed();
    }
    return output_expr->type()->DebugString() ==
           sqlbuilder_expr->type()->DebugString();
  }

  // returns true if `output_expr` produces the same type of output as
  // `sqlbuilder_expr`. Other fields in these expressions are ignored and marked
  // as accessed.
  bool ExpressionShapeEqual(const ResolvedExpr* output_expr,
                            const ResolvedExpr* sqlbuilder_expr) {
    return CompareExpressionShape(output_expr, sqlbuilder_expr,
                                  /*mark_fields_accessed=*/true);
  }

  bool ExpressionPtrShapeEqual(
      const std::unique_ptr<const ResolvedExpr>& output_expr,
      const std::unique_ptr<const ResolvedExpr>& sqlbuilder_expr) {
    return ExpressionShapeEqual(output_expr.get(), sqlbuilder_expr.get());
  }

  // We do custom comparison of output_column_list of the statement node where
  // we only compare the type and alias (excluding anonymous columns) for the
  // ResolvedOutputColumns.
  // NOTE: We currently allow giving aliases to columns that were anonymous in
  // the original ResolvedAST, but may want to change that at some point.
  bool CompareOutputColumnList(
      absl::Span<const std::unique_ptr<const ResolvedOutputColumn>>
          output_col_list,
      absl::Span<const std::unique_ptr<const ResolvedOutputColumn>>
          sqlbuilder_col_list) {
    if (output_col_list.size() != sqlbuilder_col_list.size()) {
      return false;
    }

    for (int i = 0; i < output_col_list.size(); ++i) {
      const ResolvedOutputColumn* output_col = output_col_list[i].get();
      const ResolvedOutputColumn* sqlbuilder_col = sqlbuilder_col_list[i].get();
      // The SQLBuilder does not generate queries with anonymous columns, so
      // we can't check that IsInternalAlias always matches.
      if (!IsInternalAlias(output_col->name()) &&
          IsInternalAlias(sqlbuilder_col->name())) {
        return false;
      }
      if (!IsInternalAlias(output_col->name()) &&
          output_col->name() != sqlbuilder_col->name()) {
        return false;
      }
      // This uses Equivalent rather than Equals because in tests where
      // we have alternate versions of the same proto (e.g. using
      // alt_descriptor_pool), the SQLBuilder doesn't know how to generate
      // an explicit CAST to get a particular instance of that proto type.
      if (!output_col->column().type()->Equivalent(
              sqlbuilder_col->column().type())) {
        return false;
      }

      if (!AnnotationMap::Equals(
              output_col->column().type_annotation_map(),
              sqlbuilder_col->column().type_annotation_map())) {
        return false;
      }
    }

    return true;
  }

  bool CompareIndexItemList(
      absl::Span<const std::unique_ptr<const ResolvedIndexItem>>
          output_item_list,
      absl::Span<const std::unique_ptr<const ResolvedIndexItem>>
          sqlbuilder_item_list) {
    if (output_item_list.size() != sqlbuilder_item_list.size()) {
      return false;
    }

    for (int i = 0; i < output_item_list.size(); ++i) {
      const ResolvedIndexItem* output_item = output_item_list[i].get();
      const ResolvedIndexItem* sqlbuilder_item = sqlbuilder_item_list[i].get();
      // This uses Equivalent rather than Equals because in tests where
      // we have alternate versions of the same proto (e.g. using
      // alt_descriptor_pool), the SQLBuilder doesn't know how to generate
      // an explicit CAST to get a particular instance of that proto type.
      if (!output_item->column_ref()->column().type()->Equivalent(
              sqlbuilder_item->column_ref()->column().type())) {
        return false;
      }
    }

    return true;
  }

  bool CompareColumnDefinitionList(
      absl::Span<const std::unique_ptr<const ResolvedColumnDefinition>>
          output_col_list,
      absl::Span<const std::unique_ptr<const ResolvedColumnDefinition>>
          sqlbuilder_col_list) {
    if (output_col_list.size() != sqlbuilder_col_list.size()) {
      return false;
    }

    for (int i = 0; i < output_col_list.size(); ++i) {
      const ResolvedColumnDefinition* output_col = output_col_list[i].get();
      const ResolvedColumnDefinition* sqlbuilder_col =
          sqlbuilder_col_list[i].get();
      if (IsInternalAlias(output_col->name()) ||
          IsInternalAlias(sqlbuilder_col->name())) {
        return false;
      }
      if (output_col->name() != sqlbuilder_col->name()) {
        return false;
      }
      if (!output_col->type()->Equals(sqlbuilder_col->type())) {
        return false;
      }
      if (!CompareColumnAnnotations(output_col->annotations(),
                                    sqlbuilder_col->annotations())) {
        return false;
      }
    }

    return true;
  }

  bool CompareColumnAnnotations(
      const ResolvedColumnAnnotations* output_annotations,
      const ResolvedColumnAnnotations* sqlbuilder_annotations) {
    if ((output_annotations == nullptr) !=
        (sqlbuilder_annotations == nullptr)) {
      return false;
    }
    if (output_annotations == nullptr) {
      return true;
    }
    if (output_annotations->not_null() != sqlbuilder_annotations->not_null()) {
      return false;
    }
    if (!CompareOptionList(output_annotations->option_list(),
                           sqlbuilder_annotations->option_list())) {
      return false;
    }
    if (!output_annotations->type_parameters().Equals(
            sqlbuilder_annotations->type_parameters())) {
      return false;
    }
    if (output_annotations->child_list().size() !=
        sqlbuilder_annotations->child_list().size()) {
      return false;
    }
    for (int i = 0; i < output_annotations->child_list().size(); ++i) {
      if (!CompareColumnAnnotations(output_annotations->child_list(i),
                                    sqlbuilder_annotations->child_list(i))) {
        return false;
      }
    }
    return true;
  }

  bool CompareOptionList(absl::Span<const std::unique_ptr<const ResolvedOption>>
                             output_option_list,
                         absl::Span<const std::unique_ptr<const ResolvedOption>>
                             sqlbuilder_option_list) {
    if (output_option_list.size() != sqlbuilder_option_list.size()) {
      return false;
    }

    for (int i = 0; i < sqlbuilder_option_list.size(); ++i) {
      const ResolvedOption* output_option = output_option_list[i].get();
      const ResolvedOption* sqlbuilder_option = sqlbuilder_option_list[i].get();
      // Note that we only check the Type of the option, not the entire
      // expression. This is because STRUCT-type options will have an explicit
      // Type in the SQLBuilder output SQL, even if the original SQL did not
      // specify an explicit Type for them (so we cannot use CompareNode()) on
      // these expressions.  This logic is consistent with
      // CompareOutputColumnList(), which only checks the Type and alias (and
      // not the expressions themselves).
      if (output_option->qualifier() != sqlbuilder_option->qualifier() ||
          output_option->name() != sqlbuilder_option->name() ||
          !ExpressionShapeEqual(output_option->value(),
                                sqlbuilder_option->value())) {
        return false;
      }
    }

    return true;
  }

  bool ComparePath(absl::Span<const std::string> output_path,
                   absl::Span<const std::string> sqlbuilder_path) {
    if (output_path.size() != sqlbuilder_path.size()) {
      return false;
    }

    for (int i = 0; i < sqlbuilder_path.size(); ++i) {
      if (!zetasql_base::CaseEqual(output_path[i], sqlbuilder_path[i])) {
        return false;
      }
    }

    return true;
  }

  bool GraphElementLabelEqual(
      const std::unique_ptr<const ResolvedGraphElementLabel>& output,
      const std::unique_ptr<const ResolvedGraphElementLabel>& sqlbuilder) {
    return CompareNode(output.get(), sqlbuilder.get());
  }

  bool GraphPropertyDeclarationEqual(
      const std::unique_ptr<const ResolvedGraphPropertyDeclaration>&
          output_query,
      const std::unique_ptr<const ResolvedGraphPropertyDeclaration>&
          sqlbuilder_query) {
    return CompareNode(output_query.get(), sqlbuilder_query.get());
  }

  bool GraphPropertyDefinitionEqual(
      const std::unique_ptr<const ResolvedGraphPropertyDefinition>& output,
      const std::unique_ptr<const ResolvedGraphPropertyDefinition>&
          sqlbuilder) {
    return output->property_declaration_name() ==
               sqlbuilder->property_declaration_name() &&
           output->sql() == sqlbuilder->sql() &&
           CompareExpressionShape(output->expr(), sqlbuilder->expr(),
                                  /*mark_fields_accessed=*/true);
  }

  bool GraphNodeTableReferenceEqual(
      const ResolvedGraphNodeTableReference* output,
      const ResolvedGraphNodeTableReference* sqlbuilder) {
    if ((output == nullptr) != (sqlbuilder == nullptr)) {
      return false;
    }
    if (output == nullptr) {
      return true;
    }
    if (output->node_table_identifier() !=
        sqlbuilder->node_table_identifier()) {
      return false;
    }
    if (!absl::c_equal(
            output->edge_table_column_list(),
            sqlbuilder->edge_table_column_list(),
            absl::bind_front(&AnalyzerTestRunner::ExpressionPtrShapeEqual,
                             this))) {
      return false;
    }
    if (!absl::c_equal(
            output->node_table_column_list(),
            sqlbuilder->node_table_column_list(),
            absl::bind_front(&AnalyzerTestRunner::ExpressionPtrShapeEqual,
                             this))) {
      return false;
    }
    return true;
  }

  bool GraphElementTableEqual(
      const std::unique_ptr<const ResolvedGraphElementTable>& output,
      const std::unique_ptr<const ResolvedGraphElementTable>& sqlbuilder) {
    if (output->alias() != sqlbuilder->alias()) {
      return false;
    }
    if (!CompareNode(output->input_scan(), sqlbuilder->input_scan())) {
      return false;
    }
    if (!absl::c_equal(
            output->key_list(), sqlbuilder->key_list(),
            absl::bind_front(&AnalyzerTestRunner::ExpressionPtrShapeEqual,
                             this))) {
      return false;
    }
    if (output->label_name_list() != sqlbuilder->label_name_list()) {
      return false;
    }
    if (!absl::c_equal(
            output->property_definition_list(),
            sqlbuilder->property_definition_list(),
            absl::bind_front(&AnalyzerTestRunner::GraphPropertyDefinitionEqual,
                             this))) {
      return false;
    }
    if (!GraphNodeTableReferenceEqual(output->source_node_reference(),
                                      sqlbuilder->source_node_reference())) {
      return false;
    }
    if (!GraphNodeTableReferenceEqual(output->dest_node_reference(),
                                      sqlbuilder->dest_node_reference())) {
      return false;
    }
    if (!CompareNode(output->dynamic_label(), sqlbuilder->dynamic_label())) {
      return false;
    }
    if (!CompareNode(output->dynamic_properties(),
                     sqlbuilder->dynamic_properties())) {
      return false;
    }
    return true;
  }

  // This method is executed to populate the output of
  // undeclared_parameters.test.
  void TestUndeclaredParameters(const AnalyzerOutput* analyzer_output,
                                std::string* result_string) {
    std::string parameters_str;
    if (!analyzer_output->undeclared_parameters().empty()) {
      EXPECT_THAT(analyzer_output->undeclared_positional_parameters(),
                  ::testing::IsEmpty());
      parameters_str = absl::StrJoin(
          analyzer_output->undeclared_parameters(), "\n",
          [](std::string* out,
             const std::pair<std::string, const Type*>& name_and_type) {
            absl::StrAppend(out, name_and_type.first, ": ",
                            name_and_type.second->DebugString());
          });
    } else if (!analyzer_output->undeclared_positional_parameters().empty()) {
      EXPECT_THAT(analyzer_output->undeclared_parameters(),
                  ::testing::IsEmpty());
      parameters_str =
          absl::StrJoin(analyzer_output->undeclared_positional_parameters(),
                        "\n", [](std::string* out, const Type* type) {
                          absl::StrAppend(out, type->DebugString());
                        });
    }
    absl::StrAppend(result_string, "[UNDECLARED_PARAMETERS]\n", parameters_str,
                    "\n\n");
  }

  // This method is called to augment test output when
  // show_referenced_property_graph option is specified.
  void TestReferencedPropertyGraph(const AnalyzerOutput* analyzer_output,
                                   std::string* result_string) {
    absl::StrAppend(result_string, "\n[has_graph_references=",
                    analyzer_output->has_graph_references(), "]\n\n");
  }

  // This method is executed to populate the output of parse_locations.test.
  void TestLiteralReplacementInGoldens(absl::string_view sql,
                                       const AnalyzerOptions& analyzer_options,
                                       const AnalyzerOutput* analyzer_output,
                                       std::string* result_string) {
    std::string new_sql;
    LiteralReplacementMap literal_map;
    GeneratedParameterMap generated_parameters;
    absl::Status status = ReplaceLiteralsByParameters(
        sql, literal_replacement_options_, analyzer_options,
        analyzer_output->resolved_statement(), &literal_map,
        &generated_parameters, &new_sql);
    if (status.ok()) {
      absl::StrAppend(result_string, "[REPLACED_LITERALS]\n", new_sql, "\n\n");
    } else {
      absl::StrAppend(result_string,
                      "Failed to replace literals: ", status.message());
    }
  }

  void CheckSupportedStatementKind(absl::string_view sql, ResolvedNodeKind kind,
                                   AnalyzerOptions options) {
    // This is used so that we only test each statement kind once.
    static std::set<ResolvedNodeKind> statement_kinds_to_be_skipped = {
        // Skip EXPLAIN statements, which fail the below checks because they
        // also require the explained statement kind to be supported.
        RESOLVED_EXPLAIN_STMT};

    if (zetasql_base::ContainsKey(statement_kinds_to_be_skipped, kind)) return;

    // Make the given kind the only supported statement kind.
    options.mutable_language()->SetSupportedStatementKinds({kind});

    // ResolvedGeneralizedQueryStmt comes out for queries, which can't be
    // started unless ResolvedQueryStmt is also enabled.
    if (kind == RESOLVED_GENERALIZED_QUERY_STMT) {
      options.mutable_language()->AddSupportedStatementKind(
          RESOLVED_QUERY_STMT);
    }

    options.set_column_id_sequence_number(nullptr);

    TypeFactory factory;
    std::unique_ptr<const AnalyzerOutput> output;

    auto catalog_holder = CreateCatalog(options);

    // Analyzer should work when the statement kind is supported.
    ZETASQL_EXPECT_OK(AnalyzeStatement(sql, options, catalog_holder.catalog(), &factory,
                               &output));

    // Analyzer should fail when the statement kind is not supported.
    options.mutable_language()->SetSupportedStatementKinds(
        {RESOLVED_EXPLAIN_STMT});
    EXPECT_FALSE(AnalyzeStatement(sql, options, catalog_holder.catalog(),
                                  &factory, &output)
                     .ok());

    // We only need to test each statement kind once, add it to skipped set
    // to save testing time.
    statement_kinds_to_be_skipped.insert(kind);
  }

  // This method is called for every valid query statement used in the analyzer
  // golden files. It does not produce any golden output, just check that
  // literals can be successfully replaced by parameters. If this test method
  // fails, be sure to add the query that caused the failure to
  // parse_locations.test.
  absl::Status CheckLiteralReplacement(
      absl::string_view sql, const AnalyzerOptions& original_options,
      const AnalyzerOutput* original_analyzer_output) {
    // Do not attempt literal replacement, since query parameters are disallowed
    // in all of below cases.
    if (original_analyzer_output->resolved_statement()->node_kind() ==
            RESOLVED_CREATE_FUNCTION_STMT ||
        original_analyzer_output->resolved_statement()->node_kind() ==
            RESOLVED_CREATE_TABLE_FUNCTION_STMT ||
        original_analyzer_output->resolved_statement()->node_kind() ==
            RESOLVED_CREATE_PROCEDURE_STMT ||
        original_analyzer_output->resolved_statement()->node_kind() ==
            RESOLVED_CREATE_VIEW_STMT ||
        original_analyzer_output->resolved_statement()->node_kind() ==
            RESOLVED_CREATE_MATERIALIZED_VIEW_STMT ||
        original_analyzer_output->resolved_statement()->node_kind() ==
            RESOLVED_CREATE_APPROX_VIEW_STMT ||
        original_options.statement_context() ==
            StatementContext::CONTEXT_MODULE) {
      return absl::OkStatus();
    }

    // Only attempt literal replacement when the query uses named parameters,
    // not positional ? parameters.
    if (original_options.parameter_mode() != PARAMETER_NAMED) {
      return absl::OkStatus();
    }

    AnalyzerOptions options = original_options;
    options.set_record_parse_locations(true);

    // Don't mess up any shared sequence generator for the original query.
    options.set_column_id_sequence_number(nullptr);

    TypeFactory type_factory;
    std::unique_ptr<const AnalyzerOutput> analyzer_output;
    auto catalog_holder = CreateCatalog(options);
    ZETASQL_CHECK_OK(AnalyzeStatement(sql, options, catalog_holder.catalog(),
                              &type_factory, &analyzer_output));

    std::string dbg_info = absl::StrCat(
        "\n[QUERY WITH LITERALS]:\n", sql, "\n Initial resolved AST:\n",
        analyzer_output->resolved_statement()->DebugString());

    std::string new_sql;
    LiteralReplacementMap literal_map;
    GeneratedParameterMap generated_parameters;
    absl::Status status = ReplaceLiteralsByParameters(
        sql, literal_replacement_options_, options,
        analyzer_output->resolved_statement(), &literal_map,
        &generated_parameters, &new_sql);
    ZETASQL_EXPECT_OK(status) << dbg_info;
    ZETASQL_RETURN_IF_ERROR(status);

    absl::StrAppend(&dbg_info, "\n[REPLACED LITERALS]:\n", new_sql);

    TypeFactory new_type_factory;

    // Make sure that re-analyzing the statement produces the same output types.
    std::unique_ptr<const AnalyzerOutput> new_analyzer_output;
    AnalyzerOptions new_options = options;
    for (const auto& pair : generated_parameters) {
      const std::string& parameter_name = pair.first;
      const Type* parameter_type = pair.second.type();
      ZETASQL_CHECK_OK(new_options.AddQueryParameter(parameter_name, parameter_type))
          << dbg_info;
    }
    auto new_catalog_holder = CreateCatalog(new_options);
    status =
        AnalyzeStatement(new_sql, new_options, new_catalog_holder.catalog(),
                         &new_type_factory, &new_analyzer_output);
    // analyzer_function_test.cc has a test that introduces a test function
    // NULL_OF_TYPE that only accepts literals, no parameters. Conversion to
    // parameters will fail for that function. We could introduce an extra
    // test option to turn off literal replacement testing but that's too
    // much bloat for a one-off.
    if (absl::StrContains(status.message(),
                          "Argument to NULL_OF_TYPE must be a literal")) {
      // Expected failure for NULL_OF_TYPE test function, ignore.
      return absl::OkStatus();
    }
    // Another case includes functions that require expressions with values that
    // can be known at compile time. Parameters do not satisfy this property.
    // Approximate distance functions are one such example. This class of
    // functions takes an optional third argument `options` of JSON or PROTO
    // Type, and requires that `options` is a constant expression which can
    // be evaluated at compile-time. Since the value of parameters is known
    // execution time, they should be excluded. Thus, an error is correctly
    // returned if a parameter is passed to one of these functions, and we
    // should also disable literal to parameter replacement in analyzer tests
    // that contain such function calls.
    if (absl::StrContains(
            status.message(),
            "No matching signature for function APPROX_COSINE_DISTANCE")) {
      return absl::OkStatus();
    }
    if (absl::StrContains(
            status.message(),
            "No matching signature for function APPROX_EUCLIDEAN_DISTANCE")) {
      return absl::OkStatus();
    }
    if (absl::StrContains(
            status.message(),
            "No matching signature for function APPROX_DOT_PRODUCT")) {
      return absl::OkStatus();
    }
    ZETASQL_EXPECT_OK(status) << dbg_info;
    ZETASQL_RETURN_IF_ERROR(status);
    absl::StrAppend(&dbg_info, "\n New resolved AST:\n",
                    new_analyzer_output->resolved_statement()->DebugString());

    // Check the return types of old and new output.
    if (analyzer_output->resolved_statement()->node_kind() ==
        RESOLVED_QUERY_STMT) {
      const auto& columns = analyzer_output->resolved_statement()
                                ->GetAs<ResolvedQueryStmt>()
                                ->output_column_list();
      const auto& new_columns = new_analyzer_output->resolved_statement()
                                    ->GetAs<ResolvedQueryStmt>()
                                    ->output_column_list();
      EXPECT_EQ(columns.size(), new_columns.size()) << dbg_info;
      for (int i = 0; i < std::min(columns.size(), new_columns.size()); i++) {
        EXPECT_EQ(columns[i]->column().type()->DebugString(),
                  new_columns[i]->column().type()->DebugString())
            << "\ncolumns[" << i
            << "]: " << columns[i]->column().type()->DebugString()
            << "\nnew_columns[" << i
            << "]: " << new_columns[i]->column().type()->DebugString() << "\n"
            << dbg_info;
      }
    }
    // Replacing literals by parameters again should not change the output nor
    // detect any new literals.
    std::string new_new_sql;
    LiteralReplacementMap new_literal_map;
    GeneratedParameterMap new_generated_parameters;
    status = ReplaceLiteralsByParameters(
        new_sql, literal_replacement_options_, new_options,
        new_analyzer_output->resolved_statement(), &new_literal_map,
        &new_generated_parameters, &new_new_sql);
    ZETASQL_EXPECT_OK(status) << dbg_info;
    ZETASQL_RETURN_IF_ERROR(status);
    EXPECT_EQ(new_sql, new_new_sql) << dbg_info;
    EXPECT_EQ(new_literal_map.size(), 0) << dbg_info;
    EXPECT_EQ(new_generated_parameters.size(), 0) << dbg_info;
    return absl::OkStatus();
  }

  void TestSqlBuilder(absl::string_view test_case,
                      const AnalyzerOptions& orig_options, Catalog* catalog,
                      bool is_statement, const AnalyzerOutput* analyzer_output,
                      std::string* result_string) {
    ABSL_CHECK(analyzer_output != nullptr);
    result_string->clear();

    absl::string_view target_syntax_mode =
        test_case_options_.GetString(kSqlBuilderTargetSyntaxMode);
    if (target_syntax_mode == kSqlBuilderTargetSyntaxModePipe) {
      TestSqlBuilderForTargetSyntaxMode(
          test_case, SQLBuilder::TargetSyntaxMode::kPipe,
          kSqlBuilderTargetSyntaxModePipe, false, orig_options, catalog,
          is_statement, analyzer_output, result_string);
    } else if (target_syntax_mode == kSqlBuilderTargetSyntaxModeStandard) {
      TestSqlBuilderForTargetSyntaxMode(
          test_case, SQLBuilder::TargetSyntaxMode::kStandard,
          kSqlBuilderTargetSyntaxModeStandard, false, orig_options, catalog,
          is_statement, analyzer_output, result_string);
    } else if (target_syntax_mode == kSqlBuilderTargetSyntaxModeBoth) {
      TestSqlBuilderForTargetSyntaxMode(
          test_case, SQLBuilder::TargetSyntaxMode::kStandard,
          kSqlBuilderTargetSyntaxModeStandard, true, orig_options, catalog,
          is_statement, analyzer_output, result_string);
      TestSqlBuilderForTargetSyntaxMode(
          test_case, SQLBuilder::TargetSyntaxMode::kPipe,
          kSqlBuilderTargetSyntaxModePipe, true, orig_options, catalog,
          is_statement, analyzer_output, result_string);
    } else {
      FAIL() << "Unrecognized value for " << kSqlBuilderTargetSyntaxMode
             << ": '" << target_syntax_mode << "'";
    }
  }

  void FillTargetSyntaxMapFromOptions(const ResolvedNode* root_node,
                                      TargetSyntaxMap* target_syntax_map) {
    absl::string_view mode =
        test_case_options_.GetString(kSqlBuilderTargetSyntaxMapMode);
    if (mode.empty()) {
      return;
    } else if (mode == "kChainedFunctionCall") {
      // Set this TargetSyntaxMap mode on all ResolvedFunctionCalls.
      std::vector<const ResolvedNode*> function_call_nodes;
      root_node->GetDescendantsSatisfying(
          &ResolvedNode::Is<ResolvedFunctionCallBase>, &function_call_nodes);
      for (const ResolvedNode* node : function_call_nodes) {
        (*target_syntax_map)[node] = SQLBuildTargetSyntax::kChainedFunctionCall;
      }
    } else {
      FAIL() << "Unrecognized value for " << kSqlBuilderTargetSyntaxMapMode
             << ": '" << mode << "'";
    }
  }

  void TestSqlBuilderForTargetSyntaxMode(
      absl::string_view test_case,
      SQLBuilder::TargetSyntaxMode target_syntax_mode,
      absl::string_view target_syntax_mode_name, bool show_target_syntax_mode,
      const AnalyzerOptions& orig_options, Catalog* catalog, bool is_statement,
      const AnalyzerOutput* analyzer_output, std::string* result_string) {
    const ResolvedNode* ast;
    if (is_statement) {
      ast = analyzer_output->resolved_statement();
    } else {
      ast = analyzer_output->resolved_expr();
    }
    ABSL_CHECK(ast != nullptr)
        << "ResolvedAST passed to SQLBuilder should be either a "
           "ResolvedStatement or a ResolvedExpr";

    AnalyzerOptions options = orig_options;
    // Don't mess up any shared sequence generator for the original query.
    options.set_column_id_sequence_number(nullptr);
    // The SQLBuilder sometimes adds an outer project to fix column names,
    // which can result in WITH clauses in subqueries, so we always enable
    // that feature.
    options.mutable_language()->EnableLanguageFeature(FEATURE_WITH_ON_SUBQUERY);

    SQLBuilder::SQLBuilderOptions builder_options(options.language());
    builder_options.undeclared_parameters =
        analyzer_output->undeclared_parameters();
    builder_options.undeclared_positional_parameters =
        analyzer_output->undeclared_positional_parameters();
    builder_options.catalog = catalog;
    builder_options.target_syntax_map =
        InternalAnalyzerOutputProperties::GetTargetSyntaxMap(
            analyzer_output->analyzer_output_properties());
    FillTargetSyntaxMapFromOptions(ast, &builder_options.target_syntax_map);
    builder_options.target_syntax_mode = target_syntax_mode;
    const std::string positional_parameter_mode =
        test_case_options_.GetString(kSqlBuilderPositionalParameterMode);
    if (positional_parameter_mode == "question_mark") {
      builder_options.positional_parameter_mode =
          SQLBuilder::SQLBuilderOptions::kQuestionMark;
    } else if (positional_parameter_mode == "named") {
      options.clear_positional_query_parameters();
      options.set_parameter_mode(ParameterMode::PARAMETER_NAMED);
      options.set_allow_undeclared_parameters(true);
      builder_options.positional_parameter_mode =
          SQLBuilder::SQLBuilderOptions::kNamed;
    } else {
      FAIL() << "Unrecognized value for " << kSqlBuilderPositionalParameterMode
             << ": '" << positional_parameter_mode << "'";
    }

    SQLBuilderWithNestedCatalogSupport builder(builder_options);
    absl::Status visitor_status = builder.Process(*ast);

    std::string builder_sql;
    if (visitor_status.ok()) {
      auto sql_or_error = builder.GetSql();
      if (sql_or_error.ok()) {
        builder_sql = sql_or_error.value();
      } else {
        visitor_status = sql_or_error.status();
      }
    }

    if (!visitor_status.ok()) {
      if (test_case_options_.GetBool(kAllowInternalError)) {
        EXPECT_EQ(visitor_status.code(), absl::StatusCode::kInternal)
            << "Query cannot return internal error without ["
            << kAllowInternalError << "] option: " << visitor_status
            << "; input_ast: " << ast->DebugString();
      } else {
        ZETASQL_EXPECT_OK(visitor_status) << "; input_ast: " << ast->DebugString();
      }

      *result_string =
          absl::StrCat("ERROR from SQLBuilder: ", FormatError(visitor_status));
      FAIL() << *result_string;
    }

    std::string formatted_sql;
    if (is_statement) {
      const absl::Status status = FormatSql(builder_sql, &formatted_sql);
      if (!status.ok()) {
        ZETASQL_VLOG(1) << "FormatSql error: " << FormatError(status);
      }
    } else {
      formatted_sql = builder_sql;
    }

    // Enable Pipes syntax in reanalyzer if the target syntax mode is kPipe.
    if (builder_options.target_syntax_mode ==
        QueryExpression::TargetSyntaxMode::kPipe) {
      options.mutable_language()->EnableLanguageFeature(FEATURE_PIPES);
    }
    TypeFactory type_factory;
    auto catalog_holder = CreateCatalog(options);

    std::unique_ptr<const AnalyzerOutput> sqlbuilder_output;
    absl::Status re_analyze_status =
        is_statement
            ? AnalyzeStatement(builder_sql, options, catalog_holder.catalog(),
                               &type_factory, &sqlbuilder_output)
            : AnalyzeExpression(builder_sql, options, catalog_holder.catalog(),
                                &type_factory, &sqlbuilder_output);
    bool sqlbuilder_tree_matches_original_tree = false;
    // Re-analyzing the query should never fail.
    if (!re_analyze_status.ok()) {
      *result_string = absl::StrCat(
          "ERROR while analyzing SQLBuilder output: ", re_analyze_status,
          "\n[SQLBUILDER SQL]\n", formatted_sql);
      FAIL() << *result_string;
    }

    sqlbuilder_tree_matches_original_tree =
        is_statement ? CompareStatement(analyzer_output->resolved_statement(),
                                        sqlbuilder_output->resolved_statement())
                     : CompareNode(analyzer_output->resolved_expr(),
                                   sqlbuilder_output->resolved_expr());

    if (test_case_options_.GetBool(kShowSqlBuilderOutput)) {
      if (show_target_syntax_mode) {
        absl::StrAppend(result_string, "[SQLBUILDER_TARGET_SYNTAX_MODE ",
                        target_syntax_mode_name, "]\n");
      }
      absl::StrAppend(result_string, "[SQLBUILDER_OUTPUT]\n", formatted_sql,
                      "\n\n");
    }

    // Skip printing analysis of the SQLBuilder output if positional parameters
    // were output with names.
    if (options.parameter_mode() == PARAMETER_POSITIONAL &&
        builder_options.positional_parameter_mode ==
            SQLBuilder::SQLBuilderOptions::kNamed) {
      return;
    }

    if (!sqlbuilder_tree_matches_original_tree &&
        test_case_options_.GetBool(kShowSqlBuilderResolvedASTDiff)) {
      absl::StrAppend(
          result_string, "[SQLBUILDER_ANALYSIS]\n",
          "* Resolved AST tree of SQLBuilder output does not match the "
          "original resolved tree\n",
          "\n[SQLBUILDER_RESOLVED_AST]\n",
          sqlbuilder_output->resolved_statement()->DebugString(),
          "\n[ORIGINAL_RESOLVED_AST]\n",
          analyzer_output->resolved_statement()->DebugString(), "\n");
    }

    const bool shape_matches =
        is_statement
            ? CompareStatementShape(analyzer_output->resolved_statement(),
                                    sqlbuilder_output->resolved_statement())
            : CompareExpressionShape(analyzer_output->resolved_expr(),
                                     sqlbuilder_output->resolved_expr());
    if (!shape_matches) {
      if (!test_case_options_.GetBool(kShowSqlBuilderOutput)) {
        absl::StrAppend(result_string, "[SQLBUILDER_OUTPUT]\n", formatted_sql,
                        "\n\n");
      }
      absl::StrAppend(result_string, "[SQLBUILDER_RESOLVED_AST_SHAPE]\n",
                      "* Resolved tree of SQLBuilder output does not have the "
                      "same shape as the original resolved tree\n",
                      "\n[SQLBUILDER_RESOLVED_AST]\n",
                      sqlbuilder_output->resolved_statement()->DebugString(),
                      "\n");
    }
  }

  file_based_test_driver::TestCaseOptions test_case_options_;
  zetasql_base::SequenceNumber custom_id_sequence_;
  TestDumperCallback test_dumper_callback_ = nullptr;
  std::vector<std::unique_ptr<AnnotationSpec>>
      engine_specific_annotation_specs_;
  std::vector<AnalyzerRuntimeInfo> runtime_info_list_;
  LiteralReplacementOptions literal_replacement_options_;
  CatalogFactory catalog_factory_;
  std::string prev_prepare_database_name_;
};

void ValidateRuntimeInfo(const AnalyzerRuntimeInfo& info) {
  // We can't really check the absolute values, since timers are weird. But
  // we should able to check that some relative properties hold.
  for (ResolvedASTRewrite rewriter :
       zetasql_base::EnumerateEnumValues<ResolvedASTRewrite>()) {
    EXPECT_LE(info.rewriters_details(rewriter).elapsed_duration(),
              info.rewriters_timed_value().elapsed_duration());
    if (info.rewriters_details(rewriter).elapsed_duration() >
        absl::Duration{}) {
      EXPECT_GE(info.rewriters_details(rewriter).count, 0);
    }
  }
  EXPECT_GT(info.sum_elapsed_duration(), absl::ZeroDuration());
  EXPECT_GT(info.parser_runtime_info().parser_timed_value().elapsed_duration(),
            absl::ZeroDuration());
  EXPECT_GT(info.overall_timed_value().elapsed_duration(),
            absl::ZeroDuration());

  AnalyzerLogEntry log_entry = info.log_entry();

  EXPECT_GT(log_entry.num_lexical_tokens(), 0);

  std::vector<AnalyzerLogEntry::LoggedOperationCategory> expected_categories = {
      AnalyzerLogEntry::RESOLVER, AnalyzerLogEntry::PARSER};
  for (auto& stage : expected_categories) {
    int count = 0;
    for (const AnalyzerLogEntry::ExecutionStatsByOpEntry& op :
         log_entry.execution_stats_by_op()) {
      if (op.key() == stage) {
        const auto& stats = op.value();
        EXPECT_GT(1e9 * stats.wall_time().seconds() + stats.wall_time().nanos(),
                  0)
            << stage << " has " << stats.DebugString();
        ++count;
      }
    }
    EXPECT_EQ(count, 1) << stage;
  }
}

bool RunAllTests(TestDumperCallback callback) {
  AnalyzerTestRunner runner(std::move(callback));
  std::string filename = absl::GetFlag(FLAGS_test_file);
  bool result = file_based_test_driver::RunTestCasesFromFiles(
      filename, absl::bind_front(&AnalyzerTestRunner::RunTest, &runner));

  AnalyzerRuntimeInfo aggregate_info;
  for (const AnalyzerRuntimeInfo& info : runner.runtime_info_list()) {
    ValidateRuntimeInfo(info);
    aggregate_info.AccumulateAll(info);
  }

  ABSL_LOG(INFO) << "Aggregate Runtime Info:\n"
            << aggregate_info.DebugString(runner.runtime_info_list().size());

  return result;
}

class RunAnalyzerTest : public ::testing::Test {};

TEST_F(RunAnalyzerTest, AnalyzeQueries) {
  // Run all test files that matches the pattern flag.
  EXPECT_TRUE(RunAllTests(nullptr /* test_dumper_callback */));
}

}  // namespace zetasql
