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

// Implements ReplaceLiteralsByParameters method declared in analyzer.h.
// Tested by parse_locations.test.

#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/node_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// Compares ResolvedLiterals by parse location. The ParseLocationRange must be
// set on all compared literals.
struct LiteralParseLocationComparator {
  bool operator()(const ResolvedLiteral* l1, const ResolvedLiteral* l2)
      const {
    DCHECK(l1->GetParseLocationRangeOrNULL() != nullptr);
    DCHECK(l2->GetParseLocationRangeOrNULL() != nullptr);
    return l1->GetParseLocationRangeOrNULL()->start() <
           l2->GetParseLocationRangeOrNULL()->start();
  }
};

// Returns true if the given literals occur at the same location and have the
// same value (and hence type). Such literals are created in analytical
// functions. The ParseLocationRange must be set on all compared literals.
static bool IsSameLiteral(const ResolvedLiteral* a, const ResolvedLiteral* b) {
  DCHECK(a->GetParseLocationRangeOrNULL() != nullptr);
  DCHECK(b->GetParseLocationRangeOrNULL() != nullptr);
  const ParseLocationRange& location_a = *a->GetParseLocationRangeOrNULL();
  const ParseLocationRange& location_b = *b->GetParseLocationRangeOrNULL();
  return location_a == location_b && a->value() == b->value();
}

static std::string GenerateParameterName(
    const ResolvedLiteral* literal, const AnalyzerOptions& analyzer_options,
    int* index) {
  std::string type_name = Type::TypeKindToString(
      literal->type()->kind(), analyzer_options.language().product_mode());
  std::string param_name;
  const QueryParametersMap& parameters = analyzer_options.query_parameters();
  do {
    // User parameters are less likely to start with underscores.
    param_name = absl::StrCat("_p", (*index)++, "_", type_name);
  } while (zetasql_base::ContainsKey(parameters, absl::AsciiStrToLower(param_name)));
  return param_name;
}

absl::Status ReplaceLiteralsByParameters(
    const std::string& sql,
    const absl::node_hash_set<std::string>& option_names_to_ignore,
    const AnalyzerOptions& analyzer_options,
    const AnalyzerOutput* analyzer_output, LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql) {
  CHECK(analyzer_output != nullptr);
  literal_map->clear();
  generated_parameters->clear();
  result_sql->clear();

  // Collect all <literals> that are for options (hints) that have parse
  // locations.
  std::vector<const ResolvedNode*> option_nodes;
  analyzer_output->resolved_statement()->GetDescendantsWithKinds(
      {RESOLVED_OPTION}, &option_nodes);
  std::unordered_set<const ResolvedLiteral*> ignore_options_literals;
  for (const ResolvedNode* node : option_nodes) {
    const ResolvedOption* option = node->GetAs<ResolvedOption>();
    if (option->value()->node_kind() == RESOLVED_LITERAL &&
        option->value()->GetParseLocationRangeOrNULL() != nullptr) {
      const ResolvedLiteral* option_literal =
          option->value()->GetAs<ResolvedLiteral>();
      if (zetasql_base::ContainsKey(option_names_to_ignore, option->name())) {
        ignore_options_literals.insert(option_literal);
      }
    }
  }

  // Collect all <literals> that have a parse location.
  std::vector<const ResolvedNode*> literal_nodes;
  analyzer_output->resolved_statement()->GetDescendantsWithKinds(
      {RESOLVED_LITERAL}, &literal_nodes);
  std::vector<const ResolvedLiteral*> literals;
  for (const ResolvedNode* node : literal_nodes) {
    const ResolvedLiteral* literal = node->GetAs<ResolvedLiteral>();
    if (literal->GetParseLocationRangeOrNULL() != nullptr &&
        !zetasql_base::ContainsKey(ignore_options_literals, literal)) {
      literals.push_back(literal);
    }
  }
  std::sort(literals.begin(), literals.end(), LiteralParseLocationComparator());

  // <literals> are ordered by parse location. The loop below constructs
  // <result_sql> by appending the <replacement> string for each encountered
  // literal.
  ParseLocationTranslator translator(sql);
  int prefix_offset = 0;  // Offset in <sql> of the text preceding the literal.
  int parameter_index = 0;  // Index used to generate unique parameter names.
  std::string parameter_name;  // Most recently used parameter name.
  for (int i = 0; i < literals.size(); ++i) {
    const ResolvedLiteral* literal = literals[i];
    const ParseLocationRange* location = literal->GetParseLocationRangeOrNULL();
    ZETASQL_RET_CHECK(location != nullptr);
    const int first_offset = location->start().GetByteOffset();
    const int last_offset = location->end().GetByteOffset();
    ZETASQL_RET_CHECK(first_offset >= 0 && last_offset > first_offset &&
              last_offset <= sql.length());
    // Since literals are ordered by location, literals representing the same
    // input location are guaranteed to be consecutive.
    if (i > 0 && IsSameLiteral(literal, literals[i - 1])) {
      // Each occurrence of a literal maps to the same parameter name.
      ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(literal_map, literal, parameter_name));
      continue;
    }
    ZETASQL_RET_CHECK(prefix_offset == 0 || prefix_offset < last_offset)
        << "Parse locations of literals are broken:"
        << "\nQuery: " << sql
        << "\nResolved AST: "
        << analyzer_output->resolved_statement()->DebugString();
    parameter_name = GenerateParameterName(
        literal, analyzer_options, &parameter_index);

    absl::StrAppend(result_sql,
                    sql.substr(prefix_offset, first_offset - prefix_offset),
                    "@", parameter_name);
    // Add a space after the parameter name if the original literal was followed
    // by a character that can occur in an identifier or begin of a hint. This
    // is required in expressions like x='foobar'AND <other condition>.
    if (last_offset < sql.size()) {
      char ch = sql[last_offset];
      if (absl::ascii_isalnum(ch) || ch == '_' || ch == '@') {
        absl::StrAppend(result_sql, " ");
      }
    }

    ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(literal_map, literal, parameter_name));
    ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(generated_parameters, parameter_name,
                                      literal->value()))
        << parameter_name;
    prefix_offset = last_offset;
  }
  absl::StrAppend(result_sql, sql.substr(prefix_offset));
  return absl::OkStatus();
}

absl::Status ReplaceLiteralsByParameters(
    const std::string& sql, const AnalyzerOptions& analyzer_options,
    const AnalyzerOutput* analyzer_output, LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql) {
  return ReplaceLiteralsByParameters(
      sql, /*option_names_to_ignore=*/{}, analyzer_options, analyzer_output,
      literal_map, generated_parameters, result_sql);
}

}  // namespace zetasql
