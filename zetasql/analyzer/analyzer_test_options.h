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

#ifndef ZETASQL_ANALYZER_ANALYZER_TEST_OPTIONS_H_
#define ZETASQL_ANALYZER_ANALYZER_TEST_OPTIONS_H_

// This file defines the test_case_options that are supported by
// run_analyzer_test, the set of expected query parameters.
// This is written in a separate file so tests under compliance can
// consume the same tests using appropriate options.

#include <string>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/type.h"
#include "file_based_test_driver/test_case_options.h"

namespace zetasql {

class AnalyzerTestCase;

// Valid options in the case cases:
//   mode - "statement" (default) or "expression" or "type"
//   use_custom_id_sequence - use a shared column_id allocator sequence.
//                            See AnalyzerOptions::column_id_sequence_number.
//   expect_error_location - if true (default), errors must have a location.
//   allow_internal_error_TODO_fix_this - must be explicitly set to true to
//                                        allow a test case that returns
//                                        internal error to pass.
//   allow_undeclared_parameters - if true (default is false) allows parameters
//                                 whose type has not been specified explicitly
//   parameter_mode - either "named" or "positional"
//   positional_parameter_types - comma-separated list of type names to use for
//                                positional parameters
//   test_extract_table_names - if true (default), test that ExtractTableNames
//                              returns names matching the actual scans in
//                              the resolved query, when possible.
//   show_extracted_table_names - show the table names from ExtractTableNames
//                                before the query result.
//   show_temporal_table_references - show temporal table references from
//                                    ExtractTemporalTableReferences
//                                    before the query result.
//   show_resolved_ast - if true (default), show the resolved AST.
//   show_strict_mode - also show strict mode resolver output if it differs.
//                      By default, we run strict mode but don't show output.
//   parse_location_record_type - What parse location to record in Resolved AST
//                                nodes.
//   parse_multiple - if true, use AnalyzeNextStatement to analyze a sequence
//                    of statements from the same string.
//   default_timezone - default timezone to use for analysis.
//   run_unparser
//       - if true (default), runs the unparser on the resolved AST. Displays
//         a warning message if the unparsed sql is not valid.
//   unparser_positional_parameter_mode
//       - if "question_mark" (default), unparses positional parameters as the
//         ? character. If "named", unparses positional parameters as @param1,
//         @param2, etc. in accordance with their positions.
//   show_unparsed - if true, shows the unparsed sql of the resolved AST.
//   show_unparsed_resolved_ast_diff
//       - if true, shows both the original resolved AST and unparsed resolved
//         AST, if they are different.
//   language_features - comma-separated list of LanguageFeature enum names,
//                       indicating enabled features, without the
//                       FEATURE_ prefix.
//   enabled_ast_rewrites
//       - Specify a list of rewriters to run against this test statement. Each
//         list should start with one of the following:
//          * NONE: starts from an empty set of rewrites
//          * ALL: starts from all known rewrites
//          * DEFAULTS: starts from AnalyzerOptions::DefaultRewrites
//         Following the initial set keyword, a comma separated list of rewriter
//         names is accepted that can either be additions or subtractions from
//         the initial set. The writer names are the names from the
//         ResolvedASTRewrite enum without the REWRITE_ prefix. Addition cases
//         are prefixed by + and subtraction cases are prefixed by -. If the
//         final set of rewrites is empty, or if 'enabled_ast_rewrites' is not
//         set, then no ResolvedAST rewrite is printed in the test file.
//   in_scope_expression_column_name
//       - with mode=expression, add an in_scope_expression column with this
//         name in addition to the default set of named expression columns.
//   in_scope_expression_column_type
//       - with mode=expression and in_scope_expression_column_name, use this
//         type for an in_scope_expression_column in addition to the default
//         set of named expression columns.
//   product_mode
//       - "external" or "internal". Maps to ProductMode analyzer option.
//   statement_context
//       - "default" or "module". Maps to StatementContext analyzer option.
//   use_hints_allowlist
//       - if true, fill in AllowedHintsAndOptions in AnalyzerOptions with a
//         set of allowed hint/option names. (False by default)
//   use_catalog
//       - must be either "SampleCatalog" (default) to use the standard catalog
//         defined in sample_catalog.cc, or "SpecialCatalog" to use a hardcoded
//         catalog defined in special_catalog.cc to test anonymous / duplicated
//         column names.
//   error_message_mode
//       - "with_payload", "one_line", or "multi_line_with_caret".
//         Maps to ErrorMessageMode analyzer option.
//   ddl_pseudo_column_mode
//       - either "callback" or "list". Only "list" pseudo-columns are available
//         in Java-based analyzer tests. "list" is the default.
extern const char* const kAllowInternalError;
extern const char* const kAllowUndeclaredParameters;
extern const char* const kDefaultTimezone;
extern const char* const kExpectErrorLocation;
extern const char* const kInScopeExpressionColumnName;
extern const char* const kInScopeExpressionColumnType;
extern const char* const kCoercedQueryOutputTypes;
extern const char* const kLanguageFeatures;
extern const char* const kModeOption;
extern const char* const kParameterMode;
extern const char* const kParseMultiple;
extern const char* const kPositionalParameters;
extern const char* const kProductMode;
extern const char* const kParseLocationRecordType;
extern const char* const kCreateNewColumnForEachProjectedOutput;
extern const char* const kRunInJava;
extern const char* const kRunUnparser;
extern const char* const kShowExtractedTableNames;
extern const char* const kShowTableResolutionTime;
extern const char* const kShowResolvedAST;
extern const char* const kShowStrictMode;
extern const char* const kShowUnparsed;
extern const char* const kShowUnparsedResolvedASTDiff;
extern const char* const kStatementContext;
extern const char* const kSupportedStatementKinds;
extern const char* const kTestExtractTableNames;
extern const char* const kUnparserPositionalParameterMode;
extern const char* const kUseCatalog;
extern const char* const kRunDeserializer;
extern const char* const kUseHintsAllowlist;
extern const char* const kUseSharedIdSequence;
extern const char* const kEnableLiteralReplacement;
extern const char* const kErrorMessageMode;
extern const char* const kDdlPseudoColumnMode;
extern const char* const kPreserveColumnAliases;
extern const char* const kSupportedGenericEntityTypes;
extern const char* const kEnabledASTRewrites;
extern const char* const kCreateTableLikeNotScanned;
extern const char* const kPrivilegeRestrictionTableNotScanned;

void RegisterAnalyzerTestOptions(
    file_based_test_driver::TestCaseOptions* test_case_options);

void SerializeAnalyzerTestOptions(
    const file_based_test_driver::TestCaseOptions* options,
    AnalyzerTestCase* proto);

// Return a set of known parameters used in the analyzer tests.
std::vector<std::pair<std::string, const zetasql::Type*>> GetQueryParameters(
    TypeFactory* type_factory);

// Returns a collection of positional parameters used in the analyzer tests.
std::vector<const zetasql::Type*> GetPositionalQueryParameters(
    TypeFactory* type_factory);

// Returns a collection of LanguageFeatures that must be enabled for the
// provided 'test_case_options'.
absl::StatusOr<LanguageOptions::LanguageFeatureSet> GetRequiredLanguageFeatures(
    const file_based_test_driver::TestCaseOptions& test_case_options);

// A map-like type where keys are a canonicalized version of the string that
// apperas in the kEnabledASTRewewrites and ASTRewriteSet is the set of rewrites
// implied by that string. We use a vector to preserve insertion order. A
// linked_hash_map would be ideal, but the absl container libraries lack that
// particular data structure.
using AnalyzerTestRewriteGroups =
    std::vector<std::pair<std::string, AnalyzerOptions::ASTRewriteSet>>;

// Returns a list of rewrite rule sets to enable accoring to the provided
// 'test_case_options'. The result of applying each set is printed in the test
// file. If 'enabled_ast_rewrites' is not set or set to empty string, the
// returned vector is empty. A 'NONE' group, or any sets of rules specified by
// 'test_case_options' that are empty, is excluded from the returned groups.
// Thus [enabled_ast_rewrites=NONE] is equivalent to not setting the option.
// [enabled_ast_rewrites=NONE|ALL] is equivalent to [enabled_ast_rewrites=ALL].
// The NONE group is most useful when modified as in "NONE,+ANONYMIZATION". All
// errors are returned as StatusCode::kInternal because this this a testonly
// function.
absl::StatusOr<AnalyzerTestRewriteGroups> GetEnabledRewrites(
    const file_based_test_driver::TestCaseOptions& test_case_options);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_ANALYZER_TEST_OPTIONS_H_
