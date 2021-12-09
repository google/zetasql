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

#include "zetasql/analyzer/analyzer_test_options.h"

#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/types/span.h"
#include "file_based_test_driver/test_case_options.h"
#include "zetasql/base/status.h"

namespace zetasql {

const char* const kModeOption = "mode";
const char* const kUseSharedIdSequence = "use_shared_column_id_sequence_number";
const char* const kExpectErrorLocation = "expect_error_location";
const char* const kAllowInternalError = "allow_internal_error_TODO_fix_this";
const char* const kAllowUndeclaredParameters = "allow_undeclared_parameters";
const char* const kParameterMode = "parameter_mode";
const char* const kPositionalParameters = "positional_parameter_types";
const char* const kTestExtractTableNames = "test_extract_table_names";
const char* const kShowExtractedTableNames = "show_extracted_table_names";
const char* const kShowTableResolutionTime = "show_table_resolution_time";
const char* const kShowResolvedAST = "show_resolved_ast";
const char* const kShowStrictMode = "show_strict_mode";
const char* const kParseLocationRecordType = "parse_location_record_type";
const char* const kCreateNewColumnForEachProjectedOutput =
    "create_new_column_for_each_projected_output";
const char* const kParseMultiple = "parse_multiple";
const char* const kDefaultTimezone = "default_timezone";
const char* const kRunUnparser = "run_unparser";
const char* const kUnparserPositionalParameterMode =
    "unparser_positional_parameter_mode";
const char* const kShowUnparsed = "show_unparsed";
const char* const kShowUnparsedResolvedASTDiff =
    "show_unparsed_resolved_ast_diff";
const char* const kLanguageFeatures = "language_features";
const char* const kInScopeExpressionColumnName =
    "in_scope_expression_column_name";
const char* const kInScopeExpressionColumnType =
    "in_scope_expression_column_type";
const char* const kCoercedQueryOutputTypes = "coerced_query_output_types";
const char* const kStatementContext = "statement_context";
const char* const kProductMode = "product_mode";
const char* const kUseHintsAllowlist = "use_hints_allowlist";
const char* const kRunInJava = "java";
const char* const kSupportedStatementKinds = "supported_statement_kinds";
const char* const kUseCatalog = "use_catalog";
const char* const kRunDeserializer = "run_deserializer";
const char* const kEnableLiteralReplacement = "enable_literal_replacement";
const char* const kErrorMessageMode = "error_message_mode";
const char* const kDdlPseudoColumnMode = "ddl_pseudo_column_mode";
const char* const kPreserveColumnAliases = "preserve_column_aliases";
const char* const kSupportedGenericEntityTypes =
    "supported_generic_entity_types";
const char* const kEnabledASTRewrites = "enabled_ast_rewrites";
const char* const kCreateTableLikeNotScanned = "create_table_like_not_scanned";
const char* const kPrivilegeRestrictionTableNotScanned =
    "privilege_restriction_table_not_scanned";

void RegisterAnalyzerTestOptions(
    file_based_test_driver::TestCaseOptions* test_case_options) {
  test_case_options->RegisterString(kModeOption, "statement");
  test_case_options->RegisterBool(kUseSharedIdSequence, false);
  test_case_options->RegisterBool(kExpectErrorLocation, true);
  test_case_options->RegisterBool(kAllowInternalError, false);
  test_case_options->RegisterBool(kAllowUndeclaredParameters, false);
  test_case_options->RegisterString(kParameterMode, "named");
  test_case_options->RegisterString(kPositionalParameters, "");
  test_case_options->RegisterBool(kTestExtractTableNames, true);
  test_case_options->RegisterBool(kShowExtractedTableNames, false);
  test_case_options->RegisterBool(kShowTableResolutionTime, false);
  test_case_options->RegisterBool(kShowResolvedAST, true);
  test_case_options->RegisterBool(kShowStrictMode, false);
  test_case_options->RegisterString(kParseLocationRecordType, "");
  test_case_options->RegisterBool(kCreateNewColumnForEachProjectedOutput,
                                  false);
  test_case_options->RegisterBool(kParseMultiple, false);
  test_case_options->RegisterString(kDefaultTimezone, "");
  test_case_options->RegisterBool(kRunUnparser, true);
  test_case_options->RegisterString(kUnparserPositionalParameterMode,
                                    "question_mark");
  test_case_options->RegisterBool(kShowUnparsed, false);
  test_case_options->RegisterBool(kShowUnparsedResolvedASTDiff, false);
  test_case_options->RegisterString(kLanguageFeatures, "");
  test_case_options->RegisterString(kInScopeExpressionColumnName, "");
  test_case_options->RegisterString(kInScopeExpressionColumnType,
                                    "`zetasql_test__.KitchenSinkPB`");
  test_case_options->RegisterString(kCoercedQueryOutputTypes, "");
  test_case_options->RegisterString(kStatementContext, "");
  test_case_options->RegisterString(kProductMode, "");
  test_case_options->RegisterBool(kUseHintsAllowlist, false);
  test_case_options->RegisterBool(kRunInJava, true);
  test_case_options->RegisterString(kSupportedStatementKinds, "");
  test_case_options->RegisterString(kUseCatalog, "SampleCatalog");
  test_case_options->RegisterBool(kRunDeserializer, true);
  test_case_options->RegisterBool(kEnableLiteralReplacement, true);
  test_case_options->RegisterString(kErrorMessageMode, "");
  test_case_options->RegisterString(kDdlPseudoColumnMode, "");
  test_case_options->RegisterBool(kPreserveColumnAliases, true);
  test_case_options->RegisterString(kSupportedGenericEntityTypes, "");
  test_case_options->RegisterString(kEnabledASTRewrites, "");
  test_case_options->RegisterBool(kCreateTableLikeNotScanned, false);
  test_case_options->RegisterBool(kPrivilegeRestrictionTableNotScanned, false);
}

std::vector<std::pair<std::string, const zetasql::Type*>> GetQueryParameters(
    TypeFactory* type_factory) {
  const zetasql::Type* array_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(type_factory->get_int32(), &array_type));

  const zetasql::Type* array_int64_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(type_factory->get_int64(),
                                       &array_int64_type));

  const zetasql::Type* array_string_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(type_factory->get_string(),
                                       &array_string_type));

  const zetasql::Type* struct_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      {{"a", type_factory->get_int32()}, {"b", type_factory->get_string()}},
      &struct_type));

  const zetasql::Type* empty_struct_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType({}, &empty_struct_type));

  const zetasql::Type* proto_type;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      zetasql_test__::KitchenSinkPB::descriptor(), &proto_type));

  const zetasql::Type* enum_type;
  ZETASQL_CHECK_OK(type_factory->MakeEnumType(
      zetasql_test__::TestEnum_descriptor(), &enum_type));

  const zetasql::Type* array_enum_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(enum_type, &array_enum_type));

  const zetasql::Type* struct_int64_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType({{"", type_factory->get_int64()}},
                                        &struct_int64_type));

  const zetasql::Type* array_struct_int64_type;
  ZETASQL_CHECK_OK(
      type_factory->MakeArrayType(struct_int64_type, &array_struct_int64_type));

  return std::vector<std::pair<std::string, const zetasql::Type*>>{
      {"test_param_bool", type_factory->get_bool()},
      {"test_param_int32", type_factory->get_int32()},
      {"test_param_int64", type_factory->get_int64()},
      {"test_param_uint32", type_factory->get_uint32()},
      {"test_param_uint64", type_factory->get_uint64()},
      {"test_param_double", type_factory->get_double()},
      {"test_param_numeric", type_factory->get_numeric()},
      {"test_param_bignumeric", type_factory->get_bignumeric()},
      {"test_param_bytes", type_factory->get_bytes()},
      {"test_param_string", type_factory->get_string()},
      {"test_param_MixEdCaSe", type_factory->get_string()},
      {"test_param_proto", proto_type},
      {"test_param_struct", struct_type},
      {"test_param_empty_struct", empty_struct_type},
      {"test_param_enum", enum_type},
      {"test_param_array", array_type},
      {"test_param_array_int64", array_int64_type},
      {"test_param_array_string", array_string_type},
      {"test_param_array_enum", array_enum_type},
      {"test_param_array_struct_int64", array_struct_int64_type},
      // Parameter names that are reserved keywords are supported.
      {"select", type_factory->get_bool()},
      {"proto", proto_type},
      // used in parse_locations.test
      {"_p3_StrinG", type_factory->get_string()},
      {"_P4_string", type_factory->get_string()},
  };
}

absl::StatusOr<LanguageOptions::LanguageFeatureSet> GetRequiredLanguageFeatures(
    const file_based_test_driver::TestCaseOptions& test_case_options) {
  LanguageOptions::LanguageFeatureSet enabled_set;
  if (!test_case_options.GetString(kLanguageFeatures).empty()) {
    const std::vector<std::string> feature_list =
        absl::StrSplit(test_case_options.GetString(kLanguageFeatures), ',');
    for (const std::string& feature_name : feature_list) {
      const std::string full_feature_name =
          absl::StrCat("FEATURE_", feature_name);
      LanguageFeature feature;
      ZETASQL_RET_CHECK(LanguageFeature_Parse(full_feature_name, &feature))
          << full_feature_name;
      enabled_set.insert(feature);
    }
  }
  return enabled_set;
}

static AnalyzerOptions::ASTRewriteSet GetAllRewrites() {
  AnalyzerOptions::ASTRewriteSet enabled_set;
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<ResolvedASTRewrite>();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor = descriptor->value(i);
    if (value_descriptor->number() == 0) {
      // This is the "INVALID" entry. Skip this case.
      continue;
    }
    enabled_set.insert(
        static_cast<ResolvedASTRewrite>(value_descriptor->number()));
  }
  return enabled_set;
}

static AnalyzerOptions::ASTRewriteSet GetDefaultRewrites() {
  AnalyzerOptions::ASTRewriteSet enabled_set;
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<ResolvedASTRewrite>();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor = descriptor->value(i);
    if (value_descriptor->options()
            .GetExtension(rewrite_options)
            .default_enabled()) {
      enabled_set.insert(
          static_cast<ResolvedASTRewrite>(value_descriptor->number()));
    }
  }
  return enabled_set;
}

absl::StatusOr<AnalyzerTestRewriteGroups> GetEnabledRewrites(
    const file_based_test_driver::TestCaseOptions& test_case_options) {
  AnalyzerTestRewriteGroups rewrite_groups;
  absl::flat_hash_set<std::string> seen_rewrite_group_keys;
  const std::string raw_rewrites =
      test_case_options.GetString(kEnabledASTRewrites);
  if (raw_rewrites.empty()) {
    return rewrite_groups;
  }
  for (absl::string_view raw_group_view : absl::StrSplit(raw_rewrites, '|')) {
    std::vector<std::string> key_parts;
    absl::btree_set<std::string> canonicalized_edits;
    AnalyzerOptions::ASTRewriteSet rewrite_group;
    std::string raw_rewrites = absl::AsciiStrToUpper(raw_group_view);
    absl::string_view rewrites_string =
        absl::StripAsciiWhitespace(raw_rewrites);
    bool is_all_mode = false;
    bool is_none_mode = false;
    if (absl::ConsumePrefix(&rewrites_string, "NONE")) {
      key_parts.push_back("NONE");
      is_none_mode = true;
    } else if (absl::ConsumePrefix(&rewrites_string, "DEFAULTS")) {
      key_parts.push_back("DEFAULTS");
      rewrite_group = GetDefaultRewrites();
    } else if (absl::ConsumePrefix(&rewrites_string, "ALL")) {
      key_parts.push_back("ALL");
      is_all_mode = true;
      rewrite_group = GetAllRewrites();
    } else {
      // We may add other magic first entries such as 'ALL', 'NONE', or some
      // other word to mean, the rewites enabled in the previous test.
      ZETASQL_RET_CHECK_FAIL() << "Rewite list should always start with one of NONE, "
                       << "ALL, or DEFAULTS: " << raw_rewrites;
    }
    std::set<ResolvedASTRewrite> seen_overrides;
    for (absl::string_view entry :
         absl::StrSplit(rewrites_string, ',', absl::SkipWhitespace())) {
      entry = absl::StripAsciiWhitespace(entry);
      bool enable = true;
      if (absl::ConsumePrefix(&entry, "-")) {
        enable = false;
      } else if (!absl::ConsumePrefix(&entry, "+")) {
        ZETASQL_RET_CHECK_FAIL()
            << "Rewrite entries should be prefixed with '+' or '-'";
      }
      ZETASQL_RET_CHECK(!absl::ConsumePrefix(&entry, "REWRITE_"))
          << "For consistency, do not include the REWRITE_ prefix in "
          << "analyzer test files.";
      ResolvedASTRewrite rewrite;
      ZETASQL_RET_CHECK(
          ResolvedASTRewrite_Parse(absl::StrCat("REWRITE_", entry), &rewrite))
          << entry;
      ZETASQL_RET_CHECK(seen_overrides.insert(rewrite).second)
          << "Duplicate override for rewriter: "
          << ResolvedASTRewrite_Name(rewrite);
      if (enable) {
        ZETASQL_RET_CHECK(!is_all_mode)
            << "Attempting to add rewrite, but already started from ALL.";
        rewrite_group.insert(rewrite);
        canonicalized_edits.insert(absl::StrCat("+", entry));
      } else {
        ZETASQL_RET_CHECK(!is_none_mode)
            << "Attempting to remove rewrite, but already started from NONE.";
        rewrite_group.erase(rewrite);
        canonicalized_edits.insert(absl::StrCat("-", entry));
      }
    }
    if (!canonicalized_edits.empty()) {
      key_parts.push_back(absl::StrJoin(canonicalized_edits, ","));
    }
    std::string key = absl::StrJoin(key_parts, ",");
    if (!rewrite_group.empty()) {
      ZETASQL_RET_CHECK(seen_rewrite_group_keys.insert(key).second)
          << "Multiple rewrite groups canonicalize to: " << key;
      rewrite_groups.push_back({key, rewrite_group});
    }
  }
  return rewrite_groups;
}

}  // namespace zetasql
