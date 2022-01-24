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

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/analyzer_test_options.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/literal_remover.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_formatter.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
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
#include "zetasql/resolved_ast/validator.h"
#include "zetasql/testdata/sample_catalog.h"
#include "zetasql/testdata/sample_system_variables.h"
#include "zetasql/testdata/special_catalog.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/algorithm/container.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/functional/bind_front.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "file_based_test_driver/file_based_test_driver.h"
#include "file_based_test_driver/run_test_case_result.h"
#include "file_based_test_driver/test_case_options.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(std::string, test_file, "", "location of test data file.");

namespace zetasql {

// Make the unittest fail with message <str>, and also return <str> so it
// can be used in the file-based output to make it easy to find.
static std::string AddFailure(const std::string& str) {
  ADD_FAILURE() << str;
  return str;
}

static AllowedHintsAndOptions GetAllowedHintsAndOptions(
    TypeFactory* type_factory) {
  const std::string kQualifier = "qual";
  AllowedHintsAndOptions allowed(kQualifier);

  const zetasql::Type* enum_type;
  ZETASQL_CHECK_OK(type_factory->MakeEnumType(
      zetasql_test__::TestEnum_descriptor(), &enum_type));

  const zetasql::Type* extra_proto_type;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      zetasql_test__::TestExtraPB::descriptor(), &extra_proto_type));

  const zetasql::Type* enum_array_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(enum_type, &enum_array_type));

  const zetasql::Type* string_array_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(types::StringType(),
                                       &string_array_type));

  const zetasql::Type* struct_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      {{"a", types::Int32Type()}, {"b", enum_type}}, &struct_type));

  allowed.AddOption("int64_option", types::Int64Type());
  allowed.AddOption("int32_option", types::Int32Type());
  allowed.AddOption("string_option", types::StringType());
  allowed.AddOption("string_array_option", string_array_type);
  allowed.AddOption("date_option", types::DateType());
  allowed.AddOption("enum_option", enum_type);
  allowed.AddOption("proto_option", extra_proto_type);
  allowed.AddOption("struct_option", struct_type);
  allowed.AddOption("enum_array_option", enum_array_type);
  allowed.AddOption("untyped_option", nullptr);

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
    return absl::StrCat("ERROR: ", internal::StatusToString(status));
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
  if (output.resolved_statement() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(output.resolved_statement()->Accept(&visitor));
    return absl::make_unique<AnalyzerOutput>(
        output.id_string_pool(), output.arena(),
        *visitor.ConsumeRootNode<ResolvedStatement>(),
        output.analyzer_output_properties(),
        /*parser_output=*/nullptr, output.deprecation_warnings(),
        output.undeclared_parameters(),
        output.undeclared_positional_parameters(), output.max_column_id());
  } else if (output.resolved_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(output.resolved_expr()->Accept(&visitor));
    return absl::make_unique<AnalyzerOutput>(
        output.id_string_pool(), output.arena(),
        *visitor.ConsumeRootNode<ResolvedExpr>(),
        output.analyzer_output_properties(),
        /*parser_output=*/nullptr, output.deprecation_warnings(),
        output.undeclared_parameters(),
        output.undeclared_positional_parameters(), output.max_column_id());
  }
  ZETASQL_RET_CHECK_FAIL() << "No resolved AST in AnalyzerOutput";
}

}  // namespace

namespace {
// Adds information from 'table_scan_groups' (if not empty) to '*output'.
void OutputAnonymizationTableScanGroups(
    const absl::flat_hash_map<const ResolvedTableScan*,
                              const ResolvedAnonymizedAggregateScan*>&
        table_scan_groups,
    std::string* output) {
  if (table_scan_groups.empty()) return;
  absl::StrAppend(output, "\n[TableScan Groups]\n");

  // To output the map ordered by the key's debug string, we use btree_map
  // here.
  absl::btree_map<const std::string, std::multiset<std::string>>
      table_scan_string_map;

  for (auto resolved_table_scan_map_entry : table_scan_groups) {
    const std::string table_scan =
        resolved_table_scan_map_entry.first->DebugString();
    const std::string anonymized_aggregate_scan =
        resolved_table_scan_map_entry.second->DebugString();

    if (table_scan_string_map.find(anonymized_aggregate_scan) !=
        table_scan_string_map.end()) {
      std::multiset<std::string> table_scans;
      table_scan_string_map.emplace(anonymized_aggregate_scan, table_scans);
    }
    table_scan_string_map[anonymized_aggregate_scan].insert(table_scan);
  }
  bool first_table_group = true;
  for (auto table_scan_string_map_entry : table_scan_string_map) {
    const std::multiset<std::string>& table_scans =
        table_scan_string_map_entry.second;
    if (first_table_group) {
      first_table_group = false;
    } else {
      absl::StrAppend(output, ",\n");
    }
    absl::StrAppend(output, "{\n  ", absl::StrJoin(table_scans, "  "), "}");
  }
  absl::StrAppend(output, "\n");
}
}  // namespace

class AnalyzerTestRunner {
 public:   // Pointer-to-member-function usage requires public member functions
  explicit AnalyzerTestRunner(TestDumperCallback test_dumper_callback)
      : test_dumper_callback_(std::move(test_dumper_callback)) {
    // The supported option definitions are in analyzer_test_options.h.
    RegisterAnalyzerTestOptions(&test_case_options_);

    // Force a blank line at the start of every test case.
    absl::SetFlag(&FLAGS_file_based_test_driver_insert_leading_blank_lines, 1);
  }

  // CatalogHolder is a wrapper of either a sample catalog or a special catalog,
  // depending on the value of catalog_name.
  class CatalogHolder {
   public:
    CatalogHolder(const std::string& catalog_name,
                  const AnalyzerOptions& analyzer_options,
                  TypeFactory* type_factory) {
      Init(catalog_name, analyzer_options, type_factory);
    }
    Catalog* catalog() { return catalog_; }

   private:
    void Init(const std::string& catalog_name,
              const AnalyzerOptions& analyzer_options,
              TypeFactory* type_factory) {
      if (catalog_name == "SampleCatalog") {
        sample_catalog_ = absl::make_unique<SampleCatalog>(
            analyzer_options.language(), type_factory);
        catalog_ = sample_catalog_->catalog();
      } else if (catalog_name == "SpecialCatalog") {
        special_catalog_ = GetSpecialCatalog();
        catalog_ = special_catalog_.get();
      } else {
        FAIL() << "Unsupported use_catalog: " << catalog_name;
      }
    }

    Catalog* catalog_;
    std::unique_ptr<SampleCatalog> sample_catalog_;
    std::unique_ptr<SimpleCatalog> special_catalog_;

    CatalogHolder(const CatalogHolder&) = delete;
    CatalogHolder& operator=(const CatalogHolder&) = delete;
  };

  std::unique_ptr<CatalogHolder> CreateCatalog(
      const AnalyzerOptions& analyzer_options,
      TypeFactory* type_factory) {
    const std::string catalog_name = test_case_options_.GetString(kUseCatalog);
    auto holder = absl::make_unique<CatalogHolder>(
        catalog_name, analyzer_options, type_factory);
    return holder;
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
    const std::string& mode = test_case_options_.GetString(kModeOption);

    TypeFactory type_factory;
    AnalyzerOptions options;
    // Turn off AST rewrites. We'll run them later so we can show both ASTs.
    options.set_enabled_rewrites({});

    // Parse the language features first because other checks below may depend
    // on the features that are enabled for the test case.
    ZETASQL_ASSERT_OK_AND_ASSIGN(LanguageOptions::LanguageFeatureSet features,
                         GetRequiredLanguageFeatures(test_case_options_));
    options.mutable_language()->SetEnabledLanguageFeatures(features);

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
    // Prune unused columns by default.  We intend to make this the default
    // once all engines are updated.
    options.set_prune_unused_columns(true);
    // SQLBuilder test benchmarks in sql_builder.test reflect INTERNAL
    // product mode.
    options.mutable_language()->set_product_mode(PRODUCT_INTERNAL);

    if (test_case_options_.GetBool(kUseHintsAllowlist)) {
      options.set_allowed_hints_and_options(
          GetAllowedHintsAndOptions(&type_factory));
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
      const std::vector<std::string> positional_parameter_names =
          absl::StrSplit(test_case_options_.GetString(kPositionalParameters),
                         ',', absl::SkipEmpty());
      for (const std::string& parameter_name : positional_parameter_names) {
        const Type* parameter_type = nullptr;
        ZETASQL_ASSERT_OK(AnalyzeType(parameter_name, options, nullptr, &type_factory,
                              &parameter_type));
        ZETASQL_EXPECT_OK(options.AddPositionalQueryParameter(parameter_type));
      }
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

    const zetasql::Type* proto_type;
    ZETASQL_ASSERT_OK(type_factory.MakeProtoType(
        zetasql_test__::KitchenSinkPB::descriptor(), &proto_type));

    // Add some expression columns that can be used in AnalyzeStatement cases.
    ZETASQL_EXPECT_OK(options.AddExpressionColumn("column_int32",
                                          type_factory.get_int32()));
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

    // Also add an in-scope expression column if either option is set.
    if (test_case_options_.IsExplicitlySet(kInScopeExpressionColumnName) ||
        test_case_options_.IsExplicitlySet(kInScopeExpressionColumnType)) {
      const Type* type = proto_type;  // Use KitchenSinkPB by default.
      if (test_case_options_.IsExplicitlySet(kInScopeExpressionColumnType)) {
        auto catalog_holder = CreateCatalog(options, &type_factory);
        const absl::Status type_status = AnalyzeType(
            test_case_options_.GetString(kInScopeExpressionColumnType), options,
            catalog_holder->catalog(), &type_factory, &type);
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
      auto catalog_holder = CreateCatalog(options, &type_factory);
      const absl::Status type_status =
          AnalyzeType(test_case_options_.GetString(kCoercedQueryOutputTypes),
                      options, catalog_holder->catalog(), &type_factory, &type);
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

    SetupSampleSystemVariables(&type_factory, &options);
    auto catalog_holder = CreateCatalog(options, &type_factory);

    if (test_case_options_.GetBool(kParseMultiple)) {
      TestMulti(test_case, options, mode, catalog_holder->catalog(),
                &type_factory, test_result);
    } else {
      TestOne(test_case, options, mode, catalog_holder->catalog(),
              &type_factory, test_result);
    }
  }

 private:
  void TestOne(const std::string& test_case, const AnalyzerOptions& options,
               const std::string& mode, Catalog* catalog,
               TypeFactory* type_factory,
               file_based_test_driver::RunTestCaseResult* test_result) {
    std::unique_ptr<const AnalyzerOutput> output;
    absl::Status status;
    if (mode == "statement") {
      status = AnalyzeStatement(test_case, options, catalog, type_factory,
                                &output);

      // For AnalyzeStatementFromASTStatement()
      if (!test_case_options_.GetBool(kUseSharedIdSequence)) {
        std::unique_ptr<ParserOutput> parser_output;
        ParserOptions parser_options = options.GetParserOptions();
        if (ParseStatement(test_case, parser_options, &parser_output).ok()) {
          std::unique_ptr<const AnalyzerOutput> analyze_from_ast_output;
          const absl::Status analyze_from_ast_status =
              AnalyzeStatementFromParserOutputOwnedOnSuccess(
                  &parser_output, options, test_case, catalog, type_factory,
                  &analyze_from_ast_output);
          EXPECT_EQ(analyze_from_ast_status, status);
          if (analyze_from_ast_status.ok()) {
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
            test_case,
            output->resolved_statement()->node_kind(),
            options);

        // Deep copy the AST and verify that it copies correctly.
        CheckDeepCopyAST(output.get());

        // Serialize and deserialize the AST and make sure it's the same.
        CheckSerializeDeserializeAST(options, catalog, type_factory, status,
                                     *output);

        // Check that Validator covers the full resolved AST.
        CheckValidatorCoverage(options, *output);
      }
    } else if (mode == "expression") {
      status = AnalyzeExpression(test_case, options, catalog, type_factory,
                                 &output);
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

    HandleOneResult(test_case, options, type_factory, catalog, mode, status,
                    output, extracted_statement_properties, test_result);
  }

  void TestMulti(const std::string& test_case, const AnalyzerOptions& options,
                 const std::string& mode, Catalog* catalog,
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

      const ResolvedNodeKind guessed_node_kind =
          GetNextStatementKind(location);

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

      HandleOneResult(test_case, options, type_factory, catalog, mode, status,
                      output, extracted_statement_properties, test_result);

      if (test_case_options_.GetBool(kTestExtractTableNames)) {
        CheckExtractNextTableNames(&location_for_extract_table_names,
                                   options, location, output.get());
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
    TypeParameters type_params;
    const absl::Status status = AnalyzeType(test_case, options, catalog,
                                            type_factory, &type, &type_params);

    if (status.ok()) {
      test_result->AddTestOutput(
          type->TypeNameWithParameters(type_params, PRODUCT_INTERNAL).value());

      // Test that the type's TypeName can be reparsed as the same type.
      const Type* reparsed_type;
      TypeParameters reparsed_type_params;
      const absl::Status reparse_status = AnalyzeType(
          type->TypeNameWithParameters(type_params, PRODUCT_INTERNAL).value(),
          options, catalog, type_factory, &reparsed_type,
          &reparsed_type_params);
      if (!reparse_status.ok()) {
        test_result->AddTestOutput(
            AddFailure(absl::StrCat("FAILED reparsing type->DebugString: ",
                                    FormatError(reparse_status))));
      } else if (!type->Equals(reparsed_type)) {
        test_result->AddTestOutput(AddFailure(absl::StrCat(
            "FAILED: got different type on reparsing DebugString: ",
            reparsed_type->DebugString())));
      } else if (!type_params.Equals(reparsed_type_params)) {
        test_result->AddTestOutput(AddFailure(absl::StrCat(
            "FAILED: got different type parameters on reparsing DebugString: ",
            reparsed_type_params.DebugString())));
      }
    } else {
      test_result->AddTestOutput(absl::StrCat("ERROR: ", FormatError(status)));
    }
  }

  // Visits a ResolvedNode 'node' and traverses it to search for resolved
  // templated scalar function calls. If any are found, adds additional debug
  // strings to 'debug_strings' and appends them to 'test_result_string' if not
  // already present in the former.
  void VisitResolvedTemplatedSQLUDFObjects(const ResolvedNode* node,
                                           std::set<std::string>* debug_strings,
                                           std::string* test_result_string) {
    std::vector<const ResolvedNode*> future_nodes;
    if (node->node_kind() == RESOLVED_FUNCTION_CALL) {
      node->GetChildNodes(&future_nodes);
      const auto* function_call = static_cast<const ResolvedFunctionCall*>(node);
      if (function_call->function()->Is<TemplatedSQLFunction>()) {
        const auto* sql_function_call =
            static_cast<const TemplatedSQLFunctionCall*>(
                function_call->function_call_info().get());
        std::string templated_expr_debug_str =
            sql_function_call->expr()->DebugString();
        std::string debug_string =
            absl::StrCat("\nWith Templated SQL function call:\n  ",
                         function_call->signature().DebugString(
                             function_call->function()->FullName(),
                             /*verbose=*/true),
                         "\ncontaining resolved templated expression:\n",
                         templated_expr_debug_str);
        if (zetasql_base::InsertIfNotPresent(debug_strings, debug_string)) {
          absl::StrAppend(test_result_string, debug_string);
        }
        future_nodes.push_back(sql_function_call->expr());
      }
    } else {
      node->GetDescendantsWithKinds({RESOLVED_FUNCTION_CALL}, &future_nodes);
    }
    for (const ResolvedNode* future_node : future_nodes) {
      VisitResolvedTemplatedSQLUDFObjects(future_node, debug_strings,
                                          test_result_string);
    }
  }

  // Visits a ResolvedNode 'node' and traverses it to search for resolved
  // templated table-valued function calls. If any are found, adds additional
  // debug strings to 'debug_strings' and appends them to 'test_result_string'
  // if not already present in the former.
  void VisitResolvedTemplatedSQLTVFObjects(const ResolvedNode* node,
                                           std::set<std::string>* debug_strings,
                                           std::string* test_result_string) {
    std::vector<const ResolvedNode*> future_nodes;
    if (node->node_kind() == RESOLVED_TVFSCAN) {
      node->GetChildNodes(&future_nodes);
      const auto* tvf = static_cast<const ResolvedTVFScan*>(node);
      if (tvf->tvf()->Is<TemplatedSQLTVF>()) {
        const auto* call =
            static_cast<const TemplatedSQLTVFSignature*>(tvf->signature().get());
        std::string templated_query_debug_str =
            call->resolved_templated_query()->DebugString();
        const std::string debug_string = absl::StrCat(
            "\nWith Templated SQL TVF signature:\n  ", tvf->tvf()->FullName(),
            tvf->signature()->DebugString(/*verbose=*/true),
            "\ncontaining resolved templated query:\n",
            templated_query_debug_str);
        if (zetasql_base::InsertIfNotPresent(debug_strings, debug_string)) {
          absl::StrAppend(test_result_string, debug_string);
        }
        future_nodes.push_back(call->resolved_templated_query()->query());
      }
    } else {
      node->GetDescendantsWithKinds({RESOLVED_TVFSCAN}, &future_nodes);
    }
    for (const ResolvedNode* future_node : future_nodes) {
      VisitResolvedTemplatedSQLTVFObjects(future_node, debug_strings,
                                          test_result_string);
    }
  }

  void ExtractTableResolutionTimeInfoMapAsString(const std::string& test_case,
                                                 const AnalyzerOptions& options,
                                                 TypeFactory* type_factory,
                                                 Catalog* catalog,
                                                 std::string* output) {
    std::unique_ptr<ParserOutput> parser_output;
    TableResolutionTimeInfoMap table_resolution_time_info_map;
    const absl::Status status =
        ExtractTableResolutionTimeFromStatement(
            test_case, options, type_factory, catalog,
            &table_resolution_time_info_map, &parser_output);
    *output = FormatTableResolutionTimeInfoMap(
        status, table_resolution_time_info_map);
  }

  void ExtractTableResolutionTimeInfoMapFromASTAsString(
      const std::string& test_case, const AnalyzerOptions& options,
      TypeFactory* type_factory, Catalog* catalog, std::string* output,
      std::string* output_with_deferred_analysis) {
    std::unique_ptr<ParserOutput> parser_output;
    absl::Status status =
        ParseStatement(test_case, options.GetParserOptions(), &parser_output);
    status = MaybeUpdateErrorFromPayload(
        options.error_message_mode(), test_case, status);
    TableResolutionTimeInfoMap table_resolution_time_info_map;
    if (status.ok()) {
      absl::Status status = ExtractTableResolutionTimeFromASTStatement(
          *parser_output->statement(), options, test_case, type_factory,
              catalog, &table_resolution_time_info_map);
      *output = FormatTableResolutionTimeInfoMap(
          status, table_resolution_time_info_map);

      auto deferred_extract_helper =
          [&parser_output, &options, &test_case, type_factory, catalog,
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
          FormatTableResolutionTimeInfoMap(
              status, table_resolution_time_info_map);
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
    } else if (resolved_statement->node_kind() !=
                   extracted_statement_properties.node_kind
               && resolved_statement->node_kind() !=
                   RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT) {
      // TODO: AST_ALTER_TABLE_STATEMENT sometimes will return
      // RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT for backward compatibility.
      // Disable the resulting test failure for now.
      test_result->AddTestOutput(AddFailure(
          absl::StrCat("FAILED extracting statement kind. Extracted kind ",
                       ResolvedNodeKindToString(
                           extracted_statement_properties.node_kind),
                       ", actual kind ",
                       resolved_statement->node_kind_string())));
    }

    // Check whether the statement is CREATE TEMP matches the extracted
    // info.
    if (resolved_statement->Is<ResolvedCreateStatement>()) {
      const ResolvedCreateStatement* create_statement =
          resolved_statement->GetAs<ResolvedCreateStatement>();
      if ((create_statement->create_scope() ==
           ResolvedCreateStatementEnums::CREATE_TEMP) !=
          extracted_statement_properties.is_create_temporary_object) {
        test_result->AddTestOutput(AddFailure(
            absl::StrCat(
                "FAILED extracting whether statement is CREATE TEMP. "
                "Extracted is_create_temporary_object: ",
                extracted_statement_properties.is_create_temporary_object,
                ", statement: ", create_statement->DebugString())));
      }
    } else if (extracted_statement_properties.is_create_temporary_object) {
      test_result->AddTestOutput(AddFailure(
          absl::StrCat(
              "FAILED extracting whether statement is CREATE TEMP. "
              "Extracted is_create_temporary_object: ",
              extracted_statement_properties.is_create_temporary_object,
              ", statement: ", resolved_statement->DebugString())));
    }

    // Check that the number of top level statement hints match the number of
    // extracted hints.
    if (resolved_statement->hint_list().size() !=
        extracted_statement_properties.statement_level_hints.size()) {
      test_result->AddTestOutput(AddFailure(
          absl::StrCat(
              "FAILED extracting statement level hints.  Number of extracted "
              "hints (",
              extracted_statement_properties.statement_level_hints.size(),
              "), actual number of hints(",
              resolved_statement->hint_list().size(), ")")));
    }
  }

  void HandleOneResult(
      const std::string& test_case, const AnalyzerOptions& options,
      TypeFactory* type_factory, Catalog* catalog, const std::string& mode,
      const absl::Status& status,
      const std::unique_ptr<const AnalyzerOutput>& output,
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
      std::set<std::string> debug_strings;
      VisitResolvedTemplatedSQLUDFObjects(node, &debug_strings,
                                          &test_result_string);
      VisitResolvedTemplatedSQLTVFObjects(node, &debug_strings,
                                          &test_result_string);

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
        EXPECT_FALSE(!absl::StrContains(status.message(), " [at "))
            << "Error message has no ErrorLocation: " << status;
      }
    }

    if (test_case_options_.GetBool(kShowExtractedTableNames)) {
      std::set<std::vector<std::string>> table_names;
      const absl::Status extract_status =
          ExtractTableNamesFromStatement(test_case, options, &table_names);

      std::string table_names_string = "Extracted table names:\n";
      if (!extract_status.ok()) {
        absl::StrAppend(&table_names_string,
                        "ERROR: ", internal::StatusToString(extract_status));
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
      ExtractTableResolutionTimeInfoMapAsString(
          test_case, options, type_factory, catalog,
          &table_resolution_time_str);
      std::string table_resolution_time_str_from_ast;
      std::string table_resolution_time_str_from_ast_with_deferred_analysis;
      ExtractTableResolutionTimeInfoMapFromASTAsString(
          test_case, options, type_factory, catalog,
          &table_resolution_time_str_from_ast,
          &table_resolution_time_str_from_ast_with_deferred_analysis);
      EXPECT_EQ(table_resolution_time_str,
                table_resolution_time_str_from_ast);
      EXPECT_EQ(table_resolution_time_str,
                table_resolution_time_str_from_ast_with_deferred_analysis);
      test_result_string =
          absl::StrCat("Table resolution time:\n", table_resolution_time_str,
                       "\n", test_result_string);
      absl::StripTrailingAsciiWhitespace(&test_result_string);
    }

    if (test_case_options_.GetBool(kRunUnparser) &&
        // We do not run the unparser if the original query failed analysis.
        output != nullptr) {
      std::string result_string;
      TestUnparsing(test_case, options, mode == "statement",
                    output.get(), &result_string);
      absl::StrAppend(&test_result_string, "\n", result_string);
      absl::StripAsciiWhitespace(&test_result_string);
    }

    if (!test_case_options_.GetString(kParseLocationRecordType).empty() &&
        !zetasql_base::CaseEqual(
            test_case_options_.GetString(kParseLocationRecordType),
            ParseLocationRecordType_Name(PARSE_LOCATION_RECORD_NONE)) &&
        // We do not print the literal-free string if the original query failed
        // analysis.
        output != nullptr) {
      std::string result_string;
      TestLiteralReplacementInGoldens(
          test_case, options, output.get(), &result_string);
      absl::StrAppend(&test_result_string, "\n", result_string);
      absl::StripAsciiWhitespace(&test_result_string);
    }

    if (test_case_options_.GetBool(kAllowUndeclaredParameters) &&
        // We do not print the untyped parameters if the original query failed
        // analysis.
        output != nullptr) {
      std::string result_string;
      TestUndeclaredParameters(output.get(), &result_string);
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
        std::string anon_table_scan_groups;
        std::string unparsed;
        std::vector<std::string> rewrite_group_keys;
        std::string key() {
          return std::string(status.ok() ? ast_debug : status.message());
        }
      };
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
        if (outcome.status.ok() &&
            rewrite_output->resolved_statement() != nullptr) {
          outcome.ast_debug =
              rewrite_output->resolved_statement()->DebugString();
          if (outcome.ast_debug ==
              output->resolved_statement()->DebugString()) {
            continue;
          }
          if (test_case_options_.GetBool(kRunUnparser)) {
            TestUnparsing(test_case, options, /*is_statement=*/true,
                          rewrite_output.get(), &outcome.unparsed);
          }
        } else if (outcome.status.ok()) {
          ASSERT_NE(rewrite_output->resolved_expr(), nullptr);
          outcome.ast_debug = rewrite_output->resolved_expr()->DebugString();
          if (outcome.ast_debug == output->resolved_expr()->DebugString()) {
            continue;
          }
          if (test_case_options_.GetBool(kRunUnparser)) {
            TestUnparsing(test_case, options, /*is_statement=*/false,
                          rewrite_output.get(), &outcome.unparsed);
          }
        }
        OutputAnonymizationTableScanGroups(
            rewrite_output->analyzer_output_properties()
                .resolved_table_scan_to_anonymized_aggregate_scan_map,
            &outcome.anon_table_scan_groups);
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
            absl::StrAppend(&test_result_string, outcome.anon_table_scan_groups,
                            outcome.unparsed);
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
        test_dumper_callback_(test_case, test_case_options_, options,
                              output.get(), test_result);
      } else {
        test_dumper_callback_(test_case, test_case_options_, options, status,
                              test_result);
      }
    }
  }

  static std::vector<std::string> ToLower(
      const std::vector<std::string>& path) {
    std::vector<std::string> result;
    for (const std::string& identifier : path) {
      result.push_back(absl::AsciiStrToLower(identifier));
    }
    return result;
  }

  void CheckExtractTableNames(const std::string& test_case,
                              const AnalyzerOptions& options,
                              const AnalyzerOutput* analyzer_output) {
    {
      // For ExtractTableNamesFromStatement()
      std::set<std::vector<std::string>> extracted_table_names;
      const absl::Status find_tables_status =
          ExtractTableNamesFromStatement(test_case, options,
                                         &extracted_table_names);
      CheckExtractTableResult(
          find_tables_status, extracted_table_names, analyzer_output);
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
        CheckExtractTableResult(
            find_tables_status, extracted_table_names, analyzer_output);
      }
    }
  }

  void CheckExtractNextTableNames(
      ParseResumeLocation* location,
      const AnalyzerOptions& options,
      const ParseResumeLocation& location_for_analysis,
      const AnalyzerOutput* analyzer_output) {
    std::set<std::vector<std::string>> extracted_table_names;
    bool at_end_of_input;
    const absl::Status find_tables_status =
        ExtractTableNamesFromNextStatement(
            location, options, &extracted_table_names, &at_end_of_input);
    CheckExtractTableResult(
        find_tables_status, extracted_table_names, analyzer_output);
    EXPECT_EQ(location->byte_position(), location_for_analysis.byte_position());
  }

  void CheckExtractTableResult(
      const absl::Status& find_tables_status,
      const std::set<std::vector<std::string>>& extracted_table_names,
      const AnalyzerOutput* analyzer_output) {
    if (!find_tables_status.ok()) {
      // If ExtractTableNames failed, analysis should have failed too.
      EXPECT_TRUE(analyzer_output == nullptr)
          << "ExtractTableNames failed but query analysis succeeded";
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
        analyzer_output->resolved_statement()->
          GetAs<ResolvedCreateTableFunctionStmt>()->
            signature().IsTemplated()) {
      EXPECT_TRUE(actual_tables_scanned.empty());
      return;
    } else if (analyzer_output->resolved_statement()->node_kind() ==
                 RESOLVED_CREATE_FUNCTION_STMT &&
               analyzer_output->resolved_statement()->
                 GetAs<ResolvedCreateFunctionStmt>()->
                   signature().IsTemplated()) {
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
    for (const std::vector<std::string>& extracted_name
             : extracted_table_names_lower) {
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

  void CheckStrictMode(const std::string& test_case,
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
    const absl::Status strict_status =
        AnalyzeStatement(test_case, strict_options, catalog,
                         type_factory, &strict_output);
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

      Validator validator(options.language());
      ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(stmt));

      ZETASQL_EXPECT_OK(stmt->CheckFieldsAccessed())
          << "ERROR: Validator did not traverse all fields in the resolved AST "
          << "for this statement. Updates to validator.cc are probably "
          << "required so it will traverse or ignore new nodes/fields.\n"
          << stmt->DebugString();
    }
  }

  // We do custom comparison on Statements because we want to do custom
  // comparison on the OutputColumnLists.
  //
  // NOTE: The result from this is always ignored currently because we can't
  // guarantee that the exact same resolved AST comes back after unparsing.
  // This is controlled by the show_unparsed_resolved_ast_diff option.
  //
  // CompareStatementShape is a weaker form of this that just checks that the
  // result shape (column names, types, etc) matches, and that is tested by
  // default.
  bool CompareStatement(const ResolvedStatement* output_stmt,
                        const ResolvedStatement* unparsed_stmt) {
    if (output_stmt->node_kind() != unparsed_stmt->node_kind()) {
      return false;
    }

    switch (unparsed_stmt->node_kind()) {
      case RESOLVED_EXPLAIN_STMT:
        return CompareStatement(
            output_stmt->GetAs<ResolvedExplainStmt>()->statement(),
            unparsed_stmt->GetAs<ResolvedExplainStmt>()->statement());
      case RESOLVED_DEFINE_TABLE_STMT:
        return CompareOptionList(
            output_stmt->GetAs<ResolvedDefineTableStmt>()->option_list(),
            unparsed_stmt->GetAs<ResolvedDefineTableStmt>()->option_list());
      case RESOLVED_QUERY_STMT: {
        const ResolvedQueryStmt* output_query_stmt =
            output_stmt->GetAs<ResolvedQueryStmt>();
        const ResolvedQueryStmt* unparsed_query_stmt =
            unparsed_stmt->GetAs<ResolvedQueryStmt>();
        return CompareNode(output_query_stmt->query(),
                           unparsed_query_stmt->query()) &&
               CompareOutputColumnList(
                   output_query_stmt->output_column_list(),
                   unparsed_query_stmt->output_column_list());
      }
      case RESOLVED_DELETE_STMT: {
        const ResolvedDeleteStmt* output_delete_stmt =
            output_stmt->GetAs<ResolvedDeleteStmt>();
        const ResolvedDeleteStmt* unparsed_delete_stmt =
            unparsed_stmt->GetAs<ResolvedDeleteStmt>();
        return CompareNode(output_delete_stmt->returning(),
                           unparsed_delete_stmt->returning());
      }
      case RESOLVED_UPDATE_STMT: {
        const ResolvedUpdateStmt* output_update_stmt =
            output_stmt->GetAs<ResolvedUpdateStmt>();
        const ResolvedUpdateStmt* unparsed_update_stmt =
            unparsed_stmt->GetAs<ResolvedUpdateStmt>();
        return CompareNode(output_update_stmt->returning(),
                           unparsed_update_stmt->returning());
      }
      case RESOLVED_INSERT_STMT: {
        const ResolvedInsertStmt* output_insert_stmt =
            output_stmt->GetAs<ResolvedInsertStmt>();
        const ResolvedInsertStmt* unparsed_insert_stmt =
            unparsed_stmt->GetAs<ResolvedInsertStmt>();
        return CompareNode(output_insert_stmt->returning(),
                           unparsed_insert_stmt->returning());
      }
      case RESOLVED_EXECUTE_IMMEDIATE_STMT: {
        const ResolvedExecuteImmediateStmt* output_exec =
            output_stmt->GetAs<ResolvedExecuteImmediateStmt>();
        const ResolvedExecuteImmediateStmt* unparsed_exec =
            unparsed_stmt->GetAs<ResolvedExecuteImmediateStmt>();
        return CompareNode(output_exec, unparsed_exec);
      }
      case RESOLVED_CREATE_INDEX_STMT: {
        const ResolvedCreateIndexStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateIndexStmt>();
        const ResolvedCreateIndexStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateIndexStmt>();
        return CompareIndexItemList(output_create_stmt->index_item_list(),
                                    unparsed_create_stmt->index_item_list()) &&
            CompareOptionList(output_create_stmt->option_list(),
                              unparsed_create_stmt->option_list());
      }
      case RESOLVED_CREATE_DATABASE_STMT: {
        const ResolvedCreateDatabaseStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        const ResolvedCreateDatabaseStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        return CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
      }
      case RESOLVED_CREATE_SCHEMA_STMT: {
        const ResolvedCreateSchemaStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateSchemaStmt>();
        const ResolvedCreateSchemaStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateSchemaStmt>();
        return ComparePath(output_create_stmt->name_path(),
                           unparsed_create_stmt->name_path()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
      }
      case RESOLVED_CREATE_TABLE_STMT: {
        const ResolvedCreateTableStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableStmt>();
        const ResolvedCreateTableStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateTableStmt>();
        return CompareColumnDefinitionList(
                   output_create_stmt->column_definition_list(),
                   unparsed_create_stmt->column_definition_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
      }
      case RESOLVED_CREATE_SNAPSHOT_TABLE_STMT: {
        const ResolvedCreateSnapshotTableStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateSnapshotTableStmt>();
        const ResolvedCreateSnapshotTableStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateSnapshotTableStmt>();
        return CompareNode(output_create_stmt->clone_from(),
                           unparsed_create_stmt->clone_from()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
      }
      case RESOLVED_CREATE_TABLE_AS_SELECT_STMT: {
        const ResolvedCreateTableAsSelectStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        const ResolvedCreateTableAsSelectStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        return CompareNode(output_create_stmt->query(),
                           unparsed_create_stmt->query()) &&
               CompareOutputColumnList(
                   output_create_stmt->output_column_list(),
                   unparsed_create_stmt->output_column_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
      }
      case RESOLVED_CLONE_DATA_STMT: {
        const ResolvedCloneDataStmt* resolved =
            output_stmt->GetAs<ResolvedCloneDataStmt>();
        const ResolvedCloneDataStmt* unparsed =
            unparsed_stmt->GetAs<ResolvedCloneDataStmt>();
        return CompareNode(resolved->target_table(), unparsed->target_table())
            && CompareNode(resolved->clone_from(), unparsed->clone_from());
      }
      case RESOLVED_EXPORT_DATA_STMT: {
        const ResolvedExportDataStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportDataStmt>();
        const ResolvedExportDataStmt* unparsed_export_stmt =
            unparsed_stmt->GetAs<ResolvedExportDataStmt>();
        return CompareNode(output_export_stmt->query(),
                           unparsed_export_stmt->query()) &&
               CompareOutputColumnList(
                   output_export_stmt->output_column_list(),
                   unparsed_export_stmt->output_column_list()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 unparsed_export_stmt->option_list());
      }
      case RESOLVED_EXPORT_MODEL_STMT: {
        const ResolvedExportModelStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportModelStmt>();
        const ResolvedExportModelStmt* unparsed_export_stmt =
            unparsed_stmt->GetAs<ResolvedExportModelStmt>();
        return ComparePath(output_export_stmt->model_name_path(),
                           unparsed_export_stmt->model_name_path()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 unparsed_export_stmt->option_list());
      }
      case RESOLVED_CREATE_CONSTANT_STMT: {
        const ResolvedCreateConstantStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateConstantStmt>();
        const ResolvedCreateConstantStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateConstantStmt>();
        return ComparePath(output_create_stmt->name_path(),
                           unparsed_create_stmt->name_path()) &&
               CompareNode(output_create_stmt->expr(),
                           unparsed_create_stmt->expr());
      }
      case RESOLVED_CREATE_ENTITY_STMT: {
        const auto* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateEntityStmt>();
        const auto* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateEntityStmt>();
        return ComparePath(output_create_stmt->name_path(),
                           unparsed_create_stmt->name_path()) &&
               output_create_stmt->entity_type() ==
                   unparsed_create_stmt->entity_type() &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
      }
      case RESOLVED_AUX_LOAD_DATA_STMT: {
        const ResolvedAuxLoadDataStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedAuxLoadDataStmt>();
        const ResolvedAuxLoadDataStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedAuxLoadDataStmt>();
        return CompareColumnDefinitionList(
                   output_create_stmt->column_definition_list(),
                   unparsed_create_stmt->column_definition_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list()) &&
               CompareOptionList(
                   output_create_stmt->from_files_option_list(),
                   unparsed_create_stmt->from_files_option_list());
      }
      default:
        ZETASQL_LOG(ERROR) << "Statement type " << unparsed_stmt->node_kind_string()
                   << " not supported";
        return false;
    }
  }

  bool CompareNode(const ResolvedNode* output_query,
                   const ResolvedNode* unparsed_query) {
    absl::StatusOr<bool> compare_result =
        ResolvedASTComparator::CompareResolvedAST(output_query, unparsed_query);
    if (!compare_result.status().ok()) {
      return false;
    }
    return compare_result.value();
  }

  // This compares the final output shape (column names, types,
  // value-table-ness, orderedness, etc), but not the actual nodes in the tree.
  bool CompareStatementShape(const ResolvedStatement* output_stmt,
                             const ResolvedStatement* unparsed_stmt) {
    if (output_stmt->node_kind() != unparsed_stmt->node_kind()) {
      return false;
    }

    switch (unparsed_stmt->node_kind()) {
      case RESOLVED_EXPLAIN_STMT:
        return CompareStatementShape(
            output_stmt->GetAs<ResolvedExplainStmt>()->statement(),
            unparsed_stmt->GetAs<ResolvedExplainStmt>()->statement());
      case RESOLVED_DEFINE_TABLE_STMT:
        return CompareOptionList(
            output_stmt->GetAs<ResolvedDefineTableStmt>()->option_list(),
            unparsed_stmt->GetAs<ResolvedDefineTableStmt>()->option_list());
      case RESOLVED_QUERY_STMT: {
        const ResolvedQueryStmt* output_query_stmt =
            output_stmt->GetAs<ResolvedQueryStmt>();
        const ResolvedQueryStmt* unparsed_query_stmt =
            unparsed_stmt->GetAs<ResolvedQueryStmt>();
        return CompareOutputColumnList(
                   output_query_stmt->output_column_list(),
                   unparsed_query_stmt->output_column_list());
        // TODO This should also be checking is_ordered, but that
        // is always broken right now because of http://b/36682469.
        //   output_query_stmt->query()->is_ordered() ==
        //   unparsed_query_stmt->query()->is_ordered();
      }
      case RESOLVED_CREATE_INDEX_STMT: {
        const ResolvedCreateIndexStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateIndexStmt>();
        const ResolvedCreateIndexStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateIndexStmt>();
        return CompareIndexItemList(output_create_stmt->index_item_list(),
                                    unparsed_create_stmt->index_item_list()) &&
            CompareOptionList(output_create_stmt->option_list(),
                              unparsed_create_stmt->option_list());
      }
      case RESOLVED_CREATE_DATABASE_STMT: {
        const ResolvedCreateDatabaseStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        const ResolvedCreateDatabaseStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateDatabaseStmt>();
        return CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
      }
      case RESOLVED_CREATE_TABLE_STMT: {
        const ResolvedCreateTableStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableStmt>();
        const ResolvedCreateTableStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateTableStmt>();
        return CompareColumnDefinitionList(
                   output_create_stmt->column_definition_list(),
                   unparsed_create_stmt->column_definition_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list());
        // TODO Also verify primary key, etc.
      }
      case RESOLVED_CREATE_TABLE_AS_SELECT_STMT: {
        const ResolvedCreateTableAsSelectStmt* output_create_stmt =
            output_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        const ResolvedCreateTableAsSelectStmt* unparsed_create_stmt =
            unparsed_stmt->GetAs<ResolvedCreateTableAsSelectStmt>();
        return CompareOutputColumnList(
                   output_create_stmt->output_column_list(),
                   unparsed_create_stmt->output_column_list()) &&
               CompareOptionList(output_create_stmt->option_list(),
                                 unparsed_create_stmt->option_list()) &&
               output_create_stmt->is_value_table() ==
                   unparsed_create_stmt->is_value_table();
      }
      case RESOLVED_EXPORT_DATA_STMT: {
        const ResolvedExportDataStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportDataStmt>();
        const ResolvedExportDataStmt* unparsed_export_stmt =
            unparsed_stmt->GetAs<ResolvedExportDataStmt>();
        return CompareOutputColumnList(
                   output_export_stmt->output_column_list(),
                   unparsed_export_stmt->output_column_list()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 unparsed_export_stmt->option_list()) &&
               output_export_stmt->is_value_table() ==
                   unparsed_export_stmt->is_value_table();
      }
      case RESOLVED_EXPORT_MODEL_STMT: {
        const ResolvedExportModelStmt* output_export_stmt =
            output_stmt->GetAs<ResolvedExportModelStmt>();
        const ResolvedExportModelStmt* unparsed_export_stmt =
            unparsed_stmt->GetAs<ResolvedExportModelStmt>();
        return ComparePath(output_export_stmt->model_name_path(),
                           unparsed_export_stmt->model_name_path()) &&
               CompareOptionList(output_export_stmt->option_list(),
                                 unparsed_export_stmt->option_list());
      }
      case RESOLVED_CREATE_CONSTANT_STMT: {
        return CompareNode(output_stmt, unparsed_stmt);
      }
      case RESOLVED_START_BATCH_STMT: {
        const ResolvedStartBatchStmt* output_batch_stmt =
            output_stmt->GetAs<ResolvedStartBatchStmt>();
        const ResolvedStartBatchStmt* unparsed_batch_stmt =
            unparsed_stmt->GetAs<ResolvedStartBatchStmt>();
        return output_batch_stmt->batch_type() ==
               unparsed_batch_stmt->batch_type();
      }
      case RESOLVED_ASSIGNMENT_STMT:
        return CompareExpressionShape(
                   output_stmt->GetAs<ResolvedAssignmentStmt>()->target(),
                   unparsed_stmt->GetAs<ResolvedAssignmentStmt>()->target()) &&
               CompareExpressionShape(
                   output_stmt->GetAs<ResolvedAssignmentStmt>()->expr(),
                   unparsed_stmt->GetAs<ResolvedAssignmentStmt>()->expr());

      // There is nothing to test for these statement kinds, or we just
      // haven't implemented any comparison yet.  Some of these could do
      // full CompareNode comparison.
      case RESOLVED_ABORT_BATCH_STMT:
      case RESOLVED_ALTER_ALL_ROW_ACCESS_POLICIES_STMT:
      case RESOLVED_ALTER_DATABASE_STMT:
      case RESOLVED_ALTER_MATERIALIZED_VIEW_STMT:
      case RESOLVED_ALTER_PRIVILEGE_RESTRICTION_STMT:
      case RESOLVED_ALTER_ROW_ACCESS_POLICY_STMT:
      case RESOLVED_ALTER_SCHEMA_STMT:
      case RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT:
      case RESOLVED_ALTER_TABLE_STMT:
      case RESOLVED_ALTER_VIEW_STMT:
      case RESOLVED_ALTER_ENTITY_STMT:
      case RESOLVED_ANALYZE_STMT:
      case RESOLVED_ASSERT_STMT:
      case RESOLVED_BEGIN_STMT:
      case RESOLVED_CALL_STMT:
      case RESOLVED_CLONE_DATA_STMT:
      case RESOLVED_COMMIT_STMT:
      case RESOLVED_CREATE_EXTERNAL_TABLE_STMT:
      case RESOLVED_CREATE_FUNCTION_STMT:
      case RESOLVED_CREATE_MATERIALIZED_VIEW_STMT:
      case RESOLVED_CREATE_MODEL_STMT:
      case RESOLVED_CREATE_PROCEDURE_STMT:
      case RESOLVED_CREATE_PRIVILEGE_RESTRICTION_STMT:
      case RESOLVED_CREATE_ROW_ACCESS_POLICY_STMT:
      case RESOLVED_CREATE_SCHEMA_STMT:
      case RESOLVED_CREATE_SNAPSHOT_TABLE_STMT:
      case RESOLVED_CREATE_TABLE_FUNCTION_STMT:
      case RESOLVED_CREATE_VIEW_STMT:
      case RESOLVED_CREATE_ENTITY_STMT:
      case RESOLVED_DELETE_STMT:
      case RESOLVED_DESCRIBE_STMT:
      case RESOLVED_DROP_FUNCTION_STMT:
      case RESOLVED_DROP_TABLE_FUNCTION_STMT:
      case RESOLVED_DROP_PRIVILEGE_RESTRICTION_STMT:
      case RESOLVED_DROP_ROW_ACCESS_POLICY_STMT:
      case RESOLVED_DROP_SEARCH_INDEX_STMT:
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
      case RESOLVED_UPDATE_STMT:
      case RESOLVED_AUX_LOAD_DATA_STMT:
        return true;

      default:
        ZETASQL_LOG(ERROR) << "Statement type " << unparsed_stmt->node_kind_string()
                   << " not supported";
        return false;
    }
  }

  bool CompareExpressionShape(const ResolvedExpr* output_expr,
                              const ResolvedExpr* unparsed_expr) {
    return output_expr->type()->DebugString() ==
           unparsed_expr->type()->DebugString();
  }

  // We do custom comparison of output_column_list of the statement node where
  // we only compare the type and alias (excluding anonymous columns) for the
  // ResolvedOutputColumns.
  // NOTE: We currently allow giving aliases to columns that were anonymous in
  // the original ResolvedAST, but may want to change that at some point.
  bool CompareOutputColumnList(
      const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
          output_col_list,
      const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
          unparsed_col_list) {
    if (output_col_list.size() != unparsed_col_list.size()) {
      return false;
    }

    for (int i = 0; i < output_col_list.size(); ++i) {
      const ResolvedOutputColumn* output_col = output_col_list[i].get();
      const ResolvedOutputColumn* unparsed_col = unparsed_col_list[i].get();
      // The SQLBuilder does not generate queries with anonymous columns, so
      // we can't check that IsInternalAlias always matches.
      if (!IsInternalAlias(output_col->name()) &&
          IsInternalAlias(unparsed_col->name())) {
        return false;
      }
      if (!IsInternalAlias(output_col->name()) &&
          output_col->name() != unparsed_col->name()) {
        return false;
      }
      // This uses Equivalent rather than Equals because in tests where
      // we have alternate versions of the same proto (e.g. using
      // alt_descriptor_pool), the SQLBuilder doesn't know how to generate
      // an explicit CAST to get a particular instance of that proto type.
      if (!output_col->column().type()->Equivalent(
              unparsed_col->column().type())) {
        return false;
      }

      if (!AnnotationMap::Equals(
              output_col->column().type_annotation_map(),
              unparsed_col->column().type_annotation_map())) {
        return false;
      }
    }

    return true;
  }

  bool CompareIndexItemList(
      const std::vector<std::unique_ptr<const ResolvedIndexItem>>&
          output_item_list,
      const std::vector<std::unique_ptr<const ResolvedIndexItem>>&
          unparsed_item_list) {
    if (output_item_list.size() != unparsed_item_list.size()) {
      return false;
    }

    for (int i = 0; i < output_item_list.size(); ++i) {
      const ResolvedIndexItem* item = output_item_list[i].get();
      const ResolvedIndexItem* unparsed_item = unparsed_item_list[i].get();
      // This uses Equivalent rather than Equals because in tests where
      // we have alternate versions of the same proto (e.g. using
      // alt_descriptor_pool), the SQLBuilder doesn't know how to generate
      // an explicit CAST to get a particular instance of that proto type.
      if (!item->column_ref()->column().type()->Equivalent(
              unparsed_item->column_ref()->column().type())) {
        return false;
      }
    }

    return true;
  }

  bool CompareColumnDefinitionList(
      const std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
          output_col_list,
      const std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
          unparsed_col_list) {
    if (output_col_list.size() != unparsed_col_list.size()) {
      return false;
    }

    for (int i = 0; i < output_col_list.size(); ++i) {
      const ResolvedColumnDefinition* output_col = output_col_list[i].get();
      const ResolvedColumnDefinition* unparsed_col = unparsed_col_list[i].get();
      if (IsInternalAlias(output_col->name()) ||
          IsInternalAlias(unparsed_col->name())) {
        return false;
      }
      if (output_col->name() != unparsed_col->name()) {
        return false;
      }
      if (!output_col->type()->Equals(unparsed_col->type())) {
        return false;
      }
      if (!CompareColumnAnnotations(output_col->annotations(),
                                    unparsed_col->annotations())) {
        return false;
      }
    }

    return true;
  }

  bool CompareColumnAnnotations(
      const ResolvedColumnAnnotations* output_annotations,
      const ResolvedColumnAnnotations* unparsed_annotations) {
    if ((output_annotations == nullptr) != (unparsed_annotations == nullptr)) {
      return false;
    }
    if (output_annotations == nullptr) {
      return true;
    }
    if (output_annotations->not_null() != unparsed_annotations->not_null()) {
      return false;
    }
    if (!CompareOptionList(output_annotations->option_list(),
                           unparsed_annotations->option_list())) {
      return false;
    }
    if (!output_annotations->type_parameters().Equals(
            unparsed_annotations->type_parameters())) {
      return false;
    }
    if (output_annotations->child_list().size() !=
        unparsed_annotations->child_list().size()) {
      return false;
    }
    for (int i = 0; i < output_annotations->child_list().size(); ++i) {
      if (!CompareColumnAnnotations(output_annotations->child_list(i),
                                    unparsed_annotations->child_list(i))) {
        return false;
      }
    }
    return true;
  }

  bool CompareOptionList(
      const std::vector<std::unique_ptr<const ResolvedOption>>&
          output_option_list,
      const std::vector<std::unique_ptr<const ResolvedOption>>&
          unparsed_option_list) {
    if (output_option_list.size() != unparsed_option_list.size()) {
      return false;
    }

    for (int i = 0; i < unparsed_option_list.size(); ++i) {
      const ResolvedOption* output_option = output_option_list[i].get();
      const ResolvedOption* unparsed_option = unparsed_option_list[i].get();
      // Note that we only check the Type of the option, not the entire
      // expression.  This is because STRUCT-type options will have an
      // explicit Type in the unparsed SQL, even if the original SQL did
      // not specify an explicit Type for them (so we cannot use CompareNode())
      // on these expressions.  This logic is consistent with
      // CompareOutputColumnList(), which only checks the Type and alias
      // (and not the expressions themselves).
      if (output_option->qualifier() != unparsed_option->qualifier() ||
          output_option->name() != unparsed_option->name() ||
          !output_option->value()->type()->Equals(
              unparsed_option->value()->type())) {
        return false;
      }
    }

    return true;
  }

  bool ComparePath(const std::vector<std::string>& output_path,
                   const std::vector<std::string>& unparsed_path) {
    if (output_path.size() != unparsed_path.size()) {
      return false;
    }

    for (int i = 0; i < unparsed_path.size(); ++i) {
      if (!zetasql_base::CaseEqual(output_path[i], unparsed_path[i])) {
        return false;
      }
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

  // This method is executed to populate the output of parse_locations.test.
  void TestLiteralReplacementInGoldens(const std::string& sql,
                                       const AnalyzerOptions& analyzer_options,
                                       const AnalyzerOutput* analyzer_output,
                                       std::string* result_string) {
    std::string new_sql;
    LiteralReplacementMap literal_map;
    GeneratedParameterMap generated_parameters;
    absl::Status status = ReplaceLiteralsByParameters(
        sql, analyzer_options, analyzer_output,
        &literal_map, &generated_parameters, &new_sql);
    if (status.ok()) {
      absl::StrAppend(result_string, "[REPLACED_LITERALS]\n", new_sql, "\n\n");
    } else {
      absl::StrAppend(result_string,
                      "Failed to replace literals: ", status.message());
    }
  }

  void CheckSupportedStatementKind(const std::string& sql,
                                   ResolvedNodeKind kind,
                                   AnalyzerOptions options) {
    // This is used so that we only test each statement kind once.
    static std::set<ResolvedNodeKind> statement_kinds_to_be_skipped = {
      // Skip EXPLAIN statements, which fail the below checks because they also
      // require the explained statement kind to be supported.
      RESOLVED_EXPLAIN_STMT
    };

    if (zetasql_base::ContainsKey(statement_kinds_to_be_skipped, kind)) return;

    // Make the given kind the only supported statement kind.
    options.mutable_language()->SetSupportedStatementKinds({kind});

    options.set_column_id_sequence_number(nullptr);

    TypeFactory factory;
    std::unique_ptr<const AnalyzerOutput> output;

    auto catalog_holder = CreateCatalog(options, &factory);

    // Analyzer should work when the statement kind is supported.
    ZETASQL_EXPECT_OK(AnalyzeStatement(sql, options, catalog_holder->catalog(),
                               &factory, &output));

    // Analyzer should fail when the statement kind is not supported.
    options.mutable_language()->SetSupportedStatementKinds(
        {RESOLVED_EXPLAIN_STMT});
    EXPECT_FALSE(AnalyzeStatement(sql, options, catalog_holder->catalog(),
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
      const std::string& sql, const AnalyzerOptions& original_options,
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
    auto catalog_holder = CreateCatalog(options, &type_factory);
    ZETASQL_CHECK_OK(AnalyzeStatement(sql, options, catalog_holder->catalog(),
                              &type_factory, &analyzer_output));

    std::string dbg_info = absl::StrCat(
        "\n[QUERY WITH LITERALS]:\n", sql, "\n Initial resolved AST:\n",
        analyzer_output->resolved_statement()->DebugString());

    std::string new_sql;
    LiteralReplacementMap literal_map;
    GeneratedParameterMap generated_parameters;
    absl::Status status = ReplaceLiteralsByParameters(
        sql, options, analyzer_output.get(),
        &literal_map, &generated_parameters, &new_sql);
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
    auto new_catalog_holder = CreateCatalog(new_options, &type_factory);
    status =
        AnalyzeStatement(new_sql, new_options, new_catalog_holder->catalog(),
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
    ZETASQL_EXPECT_OK(status) << dbg_info;
    ZETASQL_RETURN_IF_ERROR(status);
    absl::StrAppend(&dbg_info, "\n New resolved AST:\n",
                    new_analyzer_output->resolved_statement()->DebugString());

    // Check the return types of old and new output.
    if (analyzer_output->resolved_statement()->node_kind() ==
        RESOLVED_QUERY_STMT) {
      const auto& columns =
          analyzer_output->resolved_statement()->GetAs<ResolvedQueryStmt>()
          ->output_column_list();
      const auto& new_columns =
          new_analyzer_output->resolved_statement()->GetAs<ResolvedQueryStmt>()
          ->output_column_list();
      EXPECT_EQ(columns.size(), new_columns.size()) << dbg_info;
      for (int i = 0; i < std::min(columns.size(), new_columns.size()); i++) {
        EXPECT_EQ(columns[i]->column().type()->DebugString(),
                  new_columns[i]->column().type()->DebugString())
            << "\ncolumns[" << i << "]: "
            << columns[i]->column().type()->DebugString()
            << "\nnew_columns[" << i << "]: "
            << new_columns[i]->column().type()->DebugString()
            << "\n" << dbg_info;
      }
    }
    // Replacing literals by parameters again should not change the output nor
    // detect any new literals.
    std::string new_new_sql;
    LiteralReplacementMap new_literal_map;
    GeneratedParameterMap new_generated_parameters;
    status = ReplaceLiteralsByParameters(
        new_sql, new_options, new_analyzer_output.get(),
        &new_literal_map, &new_generated_parameters, &new_new_sql);
    ZETASQL_EXPECT_OK(status) << dbg_info;
    ZETASQL_RETURN_IF_ERROR(status);
    EXPECT_EQ(new_sql, new_new_sql) << dbg_info;
    EXPECT_EQ(new_literal_map.size(), 0) << dbg_info;
    EXPECT_EQ(new_generated_parameters.size(), 0) << dbg_info;
    return absl::OkStatus();
  }

  void TestUnparsing(const std::string& test_case,
                     const AnalyzerOptions& orig_options, bool is_statement,
                     const AnalyzerOutput* analyzer_output,
                     std::string* result_string) {
    ZETASQL_CHECK(analyzer_output != nullptr);
    result_string->clear();

    AnalyzerOptions options = orig_options;
    // Don't mess up any shared sequence generator for the original query.
    options.set_column_id_sequence_number(nullptr);
    // The SQLBuilder sometimes adds an outer project to fix column names,
    // which can result in WITH clauses in subqueries, so we always enable
    // that feature.
    options.mutable_language()->EnableLanguageFeature(
        FEATURE_V_1_1_WITH_ON_SUBQUERY);

    SQLBuilder::SQLBuilderOptions builder_options(options.language());
    builder_options.undeclared_parameters =
        analyzer_output->undeclared_parameters();
    builder_options.undeclared_positional_parameters =
        analyzer_output->undeclared_positional_parameters();
    const std::string positional_parameter_mode =
        test_case_options_.GetString(kUnparserPositionalParameterMode);
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
      FAIL() << "Unrecognized value for " << kUnparserPositionalParameterMode
             << ": '" << positional_parameter_mode << "'";
    }
    const ResolvedNode* ast;
    if (is_statement) {
      ast = analyzer_output->resolved_statement();
    } else {
      ast = analyzer_output->resolved_expr();
    }
    ZETASQL_CHECK(ast != nullptr)
        << "ResolvedAST passed to SQLBuilder should be either a "
        "ResolvedStatement or a ResolvedExpr";

    SQLBuilder builder(builder_options);
    absl::Status visitor_status = builder.Process(*ast);

    if (!visitor_status.ok()) {
      if (test_case_options_.GetBool(kAllowInternalError)) {
        EXPECT_EQ(visitor_status.code(), absl::StatusCode::kInternal)
            << "Query cannot return internal error without "
               "["
            << kAllowInternalError << "] option: " << visitor_status;
      } else {
        ZETASQL_EXPECT_OK(visitor_status);
      }

      *result_string =
          absl::StrCat("ERROR from SQLBuilder: ", FormatError(visitor_status));
      return;
    }

    TypeFactory type_factory;
    auto catalog_holder = CreateCatalog(options, &type_factory);

    std::unique_ptr<const AnalyzerOutput> unparsed_output;
    absl::Status re_analyze_status =
        is_statement
            ? AnalyzeStatement(builder.sql(), options,
                               catalog_holder->catalog(), &type_factory,
                               &unparsed_output)
            : AnalyzeExpression(builder.sql(), options,
                                catalog_holder->catalog(), &type_factory,
                                &unparsed_output);
    bool unparsed_tree_matches_original_tree = false;
    // Re-analyzing the query should never fail.
    ZETASQL_ASSERT_OK(re_analyze_status)
        << "Original SQL:\n" << test_case
        << "\nSQLBuilder SQL:\n" << builder.sql();

    unparsed_tree_matches_original_tree = is_statement
        ? CompareStatement(analyzer_output->resolved_statement(),
                           unparsed_output->resolved_statement())
        : CompareNode(analyzer_output->resolved_expr(),
                      unparsed_output->resolved_expr());

    std::string query;
    if (is_statement) {
      const absl::Status status = FormatSql(builder.sql(), &query);
      if (!status.ok()) {
        ZETASQL_VLOG(1) << "FormatSql error: " << FormatError(status);
      }
    } else {
      query = builder.sql();
    }

    if (test_case_options_.GetBool(kShowUnparsed)) {
      absl::StrAppend(result_string, "[UNPARSED_SQL]\n", query, "\n\n");
    }

    // Skip printing analysis of the unparser output if positional parameters
    // were unparsed with names.
    if (options.parameter_mode() == PARAMETER_POSITIONAL &&
        builder_options.positional_parameter_mode ==
            SQLBuilder::SQLBuilderOptions::kNamed) {
      return;
    }

    if (!unparsed_tree_matches_original_tree &&
        test_case_options_.GetBool(kShowUnparsedResolvedASTDiff)) {
      absl::StrAppend(
          result_string, "[UNPARSED_SQL_ANALYSIS]\n",
          "* Unparsed tree does not match the original resolved tree\n",
          "\n[UNPARSED_SQL_RESOLVED_AST]\n",
          unparsed_output->resolved_statement()->DebugString(),
          "\n[ORIGINAL_RESOLVED_AST]\n",
          analyzer_output->resolved_statement()->DebugString(), "\n");
    }

    const bool shape_matches = is_statement
        ? CompareStatementShape(analyzer_output->resolved_statement(),
                                unparsed_output->resolved_statement())
        : CompareExpressionShape(analyzer_output->resolved_expr(),
                                 unparsed_output->resolved_expr());
    if (!shape_matches) {
      if (!test_case_options_.GetBool(kShowUnparsed)) {
        absl::StrAppend(result_string, "[UNPARSED_SQL]\n", query, "\n\n");
      }
      absl::StrAppend(result_string, "[UNPARSED_SQL_SHAPE]\n",
                      "* Unparsed tree does not produce same result shape as "
                      "the original resolved tree\n",
                      "\n[UNPARSED_SQL_RESOLVED_AST]\n",
                      unparsed_output->resolved_statement()->DebugString(),
                      "\n");
    }
  }

  file_based_test_driver::TestCaseOptions test_case_options_;
  zetasql_base::SequenceNumber custom_id_sequence_;
  TestDumperCallback test_dumper_callback_ = nullptr;
};

bool RunAllTests(TestDumperCallback callback) {
  AnalyzerTestRunner runner(std::move(callback));
  std::string filename = absl::GetFlag(FLAGS_test_file);
  return file_based_test_driver::RunTestCasesFromFiles(
      filename, absl::bind_front(&AnalyzerTestRunner::RunTest, &runner));
}

class RunAnalyzerTest : public ::testing::Test {
};

TEST_F(RunAnalyzerTest, AnalyzeQueries) {
  // Run all test files that matches the pattern flag.
  EXPECT_TRUE(RunAllTests(nullptr /* test_dumper_callback */));
}

}  // namespace zetasql
