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

#ifndef ZETASQL_PARSER_PARSER_INTERNAL_H_
#define ZETASQL_PARSER_PARSER_INTERNAL_H_

#include <cctype>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/errors.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/bison_parser.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/strings.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

// Shorthand to call parser->CreateASTNode<>(). The "node_type" must be a
// AST... class from the zetasql namespace. The "..." are the arguments to
// BisonParser::CreateASTNode<>().
#define MAKE_NODE(node_type, ...) \
  parser->CreateASTNode<zetasql::node_type>(__VA_ARGS__)

#define PARSER_LA_IS_EMPTY() next_symbol_.symbol == noToken

// Signals the token disambiguation buffer that a new statement is starting.
// The second argument indicates whether the parser lookahead buffer is
// populated or not.
#define OVERRIDE_NEXT_TOKEN_LOOKBACK(expected_token, lookback_token)           \
  do {                                                                         \
    ZETASQL_RETURN_IF_ERROR(OverrideNextTokenLookback(tokenizer, PARSER_LA_IS_EMPTY(), \
                                              TokenKinds::expected_token,      \
                                              TokenKinds::lookback_token));    \
  } while (0)

// Overrides the lookback token kind to be `new_token_kind` for the most
// recently returned token by the lookahead transformer. `location` is the error
// location to report when the override fails.
#define OVERRIDE_CURRENT_TOKEN_LOOKBACK(location, new_token_kind)             \
  do {                                                                        \
    ZETASQL_RET_CHECK(PARSER_LA_IS_EMPTY()) << "The parser lookahead buffer must be " \
                                       "empty to override the current token"; \
    ZETASQL_RETURN_IF_ERROR(                                                          \
        OverrideCurrentTokenLookback(tokenizer, TokenKinds::new_token_kind)); \
  } while (0)

namespace zetasql {
// Forward declarations to avoid an interface and a v-table lookup on every
// token.
namespace parser {
class LookaheadTransformer;
}  // namespace parser

namespace parser_internal {

// Forward declarations of wrappers so that the generated parser can call the
// lookahead transformer without an interface and a v-table lookup on every
// token.
using zetasql::parser::BisonParserMode;
using zetasql::parser::LookaheadTransformer;

void SetForceTerminate(LookaheadTransformer*, int*);
void PushBisonParserMode(LookaheadTransformer*, BisonParserMode);
void PopBisonParserMode(LookaheadTransformer*);
int GetNextToken(LookaheadTransformer*, absl::string_view*,
                 ParseLocationRange*);
absl::Status OverrideNextTokenLookback(LookaheadTransformer*, bool,
                                       parser::Token, parser::Token);
absl::Status OverrideCurrentTokenLookback(LookaheadTransformer*, parser::Token);

enum class NotKeywordPresence { kPresent, kAbsent };

enum class AllOrDistinctKeyword {
  kAll,
  kDistinct,
  kNone,
};

enum class PrecedingOrFollowingKeyword { kPreceding, kFollowing };

enum class ShiftOperator { kLeft, kRight };

enum class TableOrTableFunctionKeywords {
  kTableKeyword,
  kTableAndFunctionKeywords
};

enum class ImportType {
  kModule,
  kProto,
};

enum class IndexTypeKeywords {
  kNone,
  kSearch,
  kVector,
};

struct PivotOrUnpivotClause {
  ASTPivotClause* pivot_clause;
  ASTUnpivotClause* unpivot_clause;
  ASTAlias* alias;
};

struct ClausesFollowingFrom {
  ASTNode* where;
  ASTNode* group_by;
  ASTNode* having;
  ASTNode* qualify;
  ASTNode* window;
};

struct GeneratedOrDefaultColumnInfo {
  ASTExpression* default_expression;
  ASTGeneratedColumnInfo* generated_column_info;
};

struct ExternalTableWithClauses {
  ASTWithPartitionColumnsClause* with_partition_columns_clause;
  ASTWithConnectionClause* with_connection_clause;
};

struct LanguageOrRemoteWithConnection {
  ASTIdentifier* language;
  bool is_remote;
  ASTWithConnectionClause* with_connection_clause;
};

struct LanguageOptionsSet {
  ASTIdentifier* language;
  ASTNode* options;
};

struct OptionsBodySet {
  ASTNode* options;
  ASTNode* body;
};

struct BeginEndBlockOrLanguageAsCode {
  ASTScript* body;
  ASTIdentifier* language;
  ASTNode* code;
};

struct PathExpressionWithScope {
  ASTExpression* maybe_dashed_path_expression;
  bool is_temp_table;
};

struct ColumnMatchSuffix {
  ASTSetOperationColumnMatchMode* column_match_mode;
  ASTColumnList* column_list;
};

struct QueryOrReplicaSourceInfo {
  ASTQuery* query;
  ASTPathExpression* replica_source;
};

struct GroupByPreamble {
  ASTNode* hint;
  bool and_order_by;
};

struct GroupByModifier {
  ASTNode* group_by;
  ASTNode* having_expr;
};

struct CreateIndexStatementSuffix {
  ASTNode* partition_by;
  ASTNode* options_list;
  ASTNode* spanner_index_innerleaving_clause;
};

// This node is used for temporarily aggregating together components of an
// identifier that are separated by various characters, such as slash ("/"),
// dash ("-"), and colon (":") to enable supporting table paths of the form:
// /span/nonprod-test:db.Table without any escaping.  This node exists
// temporarily to hold intermediate values, and will not be part of the final
// parse tree.
class SeparatedIdentifierTmpNode final : public zetasql::ASTNode {
 public:
  static inline constexpr zetasql::ASTNodeKind kConcreteNodeKind =
      zetasql::AST_FAKE;

  SeparatedIdentifierTmpNode() : ASTNode(kConcreteNodeKind) {}
  void Accept(zetasql::ParseTreeVisitor* visitor, void* data) const override {
    ABSL_LOG(FATAL)  // Crash OK
        << "SeparatedIdentifierTmpNode does not support Accept";
  }
  absl::StatusOr<zetasql::VisitResult> Accept(
      zetasql::NonRecursiveParseTreeVisitor* visitor) const override {
    ABSL_LOG(FATAL)  // Crash OK
        << "SeparatedIdentifierTmpNode does not support Accept";
  }
  // This is used to represent an unquoted full identifier path that may contain
  // slashes ("/"), dashes ('-'), and colons (":"). This requires special
  // handling because of the ambiguity in the lexer between an identifier and a
  // number. For example:
  // /span/nonprod-5:db-3.Table
  // The lexer takes this to be
  // /,span,/,nonprod,-,5,:,db,-,3.,Table
  // Where tokens like 3. are treated as a FLOATING_POINT_LITERAL, so the
  // natural path separator "." is lost. For more information on this, see the
  // 'slashed_identifier' rule.

  // We represent this as a list of one or more 'PathParts' which are
  // implicitly separated by a dot ('.'). Each may be composed of one or more
  // 'IdParts' which is a list of the tokens that compose a single component of
  // the path (a single identifier) including any slashes, dashes, and/or
  // colons.
  // Thus, the example string above would be represented as the following:
  // {{"/", "span", "/", "nonprod", "-", "5", ":", "db", "-", "3"}, {"Table"}}

  // In order to save memory, these all contain string_view entries (backed by
  // the parser's copy of the input sql).
  // This also uses inlined vectors, because we rarely expect more than a few
  // entries at either level.
  // Note, in the event the size is large, this will allocate directly to the
  // heap, rather than into the arena.
  using IdParts = std::vector<absl::string_view>;
  using PathParts = std::vector<IdParts>;

  void set_path_parts(PathParts path_parts) {
    path_parts_ = std::move(path_parts);
  }

  PathParts&& release_path_parts() { return std::move(path_parts_); }
  absl::Status InitFields() final {
    {
      FieldLoader fl(this);  // Triggers check that there were no children.
      return fl.Finalize();
    }
  }

  // Returns a vector of identifier ASTNodes from `raw_parts`.
  // `raw_parts` represents a path as a list of lists. Each sublist contains the
  // raw components of an identifier. To form an ASTPathExpression, we
  // concatenate the components of each sublist together to form a single
  // identifier and return a list of these identifiers, which can be used to
  // build an ASTPathExpression.
  template <typename Location>
  static inline absl::StatusOr<std::vector<zetasql::ASTNode*>> BuildPathParts(
      const Location& bison_location, PathParts raw_parts,
      zetasql::parser::BisonParser* parser) {
    if (raw_parts.empty()) {
      return absl::InvalidArgumentError(
          "Internal error: Empty slashed path expression");
    }
    std::vector<zetasql::ASTNode*> parts;
    for (int i = 0; i < raw_parts.size(); ++i) {
      SeparatedIdentifierTmpNode::IdParts& raw_id_parts = raw_parts[i];
      if (raw_id_parts.empty()) {
        return absl::InvalidArgumentError(
            "Internal error: Empty dashed identifier part");
      }
      // Trim trailing "." which is leftover from lexing float literals
      // like a/1.b -> {"a", "/", "1.", "b"}
      for (int j = 0; j < raw_id_parts.size(); ++j) {
        absl::string_view& dash_part = raw_id_parts[j];
        if (absl::EndsWith(dash_part, ".")) {
          dash_part.remove_suffix(1);
        }
      }
      parts.push_back(parser->MakeIdentifier(bison_location,
                                             absl::StrJoin(raw_id_parts, "")));
    }
    return parts;
  }

 private:
  PathParts path_parts_;
};

template <typename SemanticType>
inline int zetasql_bison_parserlex(SemanticType* yylval,
                                     ParseLocationRange* yylloc,
                                     LookaheadTransformer* tokenizer) {
  ABSL_DCHECK(tokenizer != nullptr);
  absl::string_view text;
  int token = GetNextToken(tokenizer, &text, yylloc);
  yylval->string_view = {text.data(), text.length()};
  return token;
}

// Adds 'children' to 'node' and then returns 'node'.
template <typename ASTNodeType>
inline ASTNodeType* WithExtraChildren(
    ASTNodeType* node, absl::Span<zetasql::ASTNode* const> children) {
  for (zetasql::ASTNode* child : children) {
    if (child != nullptr) {
      node->AddChild(child);
    }
  }
  return node;
}

// Returns the first location in 'locations' that is not empty. If none of the
// locations are nonempty, returns the first location.
template <typename Location>
inline Location FirstNonEmptyLocation(const Location& a, const Location& b) {
  if (a.start().GetByteOffset() != a.end().GetByteOffset()) {
    return a;
  }
  if (b.start().GetByteOffset() != b.end().GetByteOffset()) {
    return b;
  }
  return a;
}

template <typename Location, typename... MoreLocations>
inline Location NonEmptyRangeLocation(const Location& first_location,
                                      const MoreLocations&... locations) {
  std::optional<Location> range;
  for (const Location& location : {first_location, locations...}) {
    if (location.start().GetByteOffset() != location.end().GetByteOffset()) {
      if (!range.has_value()) {
        range = location;
      } else {
        if (location.start().GetByteOffset() < range->start().GetByteOffset()) {
          range->set_start(location.start());
        }
        if (location.end().GetByteOffset() > range->end().GetByteOffset()) {
          range->set_end(location.end());
        }
      }
    }
  }
  if (range.has_value()) {
    return *range;
  }
  return first_location;
}
inline bool IsUnparenthesizedNotExpression(zetasql::ASTNode* node) {
  using zetasql::ASTUnaryExpression;
  const ASTUnaryExpression* expr = node->GetAsOrNull<ASTUnaryExpression>();
  return expr != nullptr && !expr->parenthesized() &&
         expr->op() == ASTUnaryExpression::NOT;
}

// Makes a zero-length location range: [point, point).
// This is to simulate a required AST node whose child nodes are all optional.
// The location range of the node when all children are unspecified is an empty
// range.
template <typename LocationPoint>
inline ParseLocationRange LocationFromOffset(const LocationPoint& point) {
  return ParseLocationRange(point, point);
}

// Returns true if the given text can be an unquoted identifier or a reserved
// keyword. This is useful for cases that can accept any reserved keyword as
// identifier without quoting, such as a macro name.
inline absl::Status IsIdentifierOrKeyword(absl::string_view text) {
  ABSL_DCHECK(!text.empty());
  if (text.front() == '`') {
    // This is a quoted identifier. No other token coming from the lexer &
    // lookahead transformer starts with a backtick, not even error recovery
    // ones (i.e. tokens defined to capture some wrong pattern and provide a
    // better context or error message).
    std::string str;
    std::string error_string;
    int error_offset;
    return ParseGeneralizedIdentifier(text, &str, &error_string, &error_offset);
  }

  // Not a quoted identifier, so this must be a valid identifier or a keyword
  // (regardless of reserved or not). The regex is [A-Z_][A-Z_0-9]* (case-
  // insensitive, see the lexer rules). The first character must be an
  // underscore or alpha.
  if (text.front() != '_' && !std::isalpha(text.front())) {
    return MakeSqlError() << "Expected macro name";
  }

  // After the first, we only accept an alphanumerical or an underscore.
  for (int i = 1; i < text.length(); ++i) {
    if (text[i] != '_' && !std::isalnum(text[i])) {
      return MakeSqlError() << "Expected macro name";
    }
  }
  return absl::OkStatus();
}

inline ASTTableExpression* MaybeApplyPivotOrUnpivot(
    ASTTableExpression* table_expr, ASTPivotClause* pivot_clause,
    ASTUnpivotClause* unpivot_clause) {
  ABSL_DCHECK(pivot_clause == nullptr || unpivot_clause == nullptr)
      << "pivot_clause and unpivot_clause cannot both be non-null";
  if (pivot_clause != nullptr) {
    return WithExtraChildren(table_expr, {pivot_clause});
  }

  if (unpivot_clause != nullptr) {
    return WithExtraChildren(table_expr, {unpivot_clause});
  }

  return table_expr;
}

// `Location` is needed because when `left` or `right` is a parenthesized
// expression, their location doesn't include those parens, so `(left.start,
// right.end)` does not cover the full range.
template <typename Location>
inline ASTRowPatternExpression* MakeOrCombineRowPatternOperation(
    const ASTRowPatternOperation::OperationType op, parser::BisonParser* parser,
    Location location, ASTRowPatternExpression* left,
    ASTRowPatternExpression* right) {
  if (left->node_kind() == AST_ROW_PATTERN_OPERATION &&
      left->GetAsOrDie<ASTRowPatternOperation>()->op_type() == op &&
      !left->parenthesized()) {
    return parser->WithEndLocation(WithExtraChildren(left, {right}), location);
  } else {
    // if `left` is an unparenthesized empty pattern, its location will still
    // be the end offset of the last token, outside of @$.
    // Adjust its location to the start of the `|`.
    if (left->node_kind() == AST_EMPTY_ROW_PATTERN && !left->parenthesized()) {
      left->set_start_location(location.start());
      left->set_end_location(location.start());
    }

    auto* new_root = MAKE_NODE(ASTRowPatternOperation, location, {left, right});
    new_root->set_op_type(op);
    return new_root;
  }
}

inline void SetGqlSubqueryQueryFields(ASTQuery* query) {
  // Graph subquery are braced {} not parenthesized (), so we unset for unparser
  // to skip the parenthesize printing
  query->set_is_nested(false);
  query->set_parenthesized(false);
}

// Constructs a graph subquery based on operators and an optional graph
// reference.
template <typename Location>
inline ASTQuery* MakeGraphSubquery(ASTGqlOperatorList* ops,
                                   ASTPathExpression* graph,
                                   zetasql::parser::BisonParser* parser,
                                   const Location& loc) {
  auto* graph_table = MAKE_NODE(ASTGraphTableQuery, loc, {graph, ops});
  auto* graph_query = MAKE_NODE(ASTGqlQuery, loc, {graph_table});
  auto* query = MAKE_NODE(ASTQuery, loc, {graph_query});
  SetGqlSubqueryQueryFields(query);
  return query;
}

// Constructs a GQL path pattern graph subquery based on a path pattern and
// an optional graph reference.
template <typename Location>
inline ASTExpressionSubquery* MakeGqlExistsGraphPatternSubquery(
    ASTGraphPattern* graph_pattern, ASTPathExpression* graph, ASTNode* hint,
    zetasql::parser::BisonParser* parser, const Location& loc) {
  auto* path_pattern_query =
      MAKE_NODE(ASTGqlGraphPatternQuery, loc, {graph, graph_pattern});
  auto* query = MAKE_NODE(ASTQuery, loc, {path_pattern_query});
  SetGqlSubqueryQueryFields(query);
  auto* subquery = MAKE_NODE(ASTExpressionSubquery, loc, {hint, query});
  subquery->set_modifier(zetasql::ASTExpressionSubquery::EXISTS);
  return subquery;
}

// Constructs a GQL linear ops graph subquery based on an op list and
// an optional graph reference.
template <typename Location>
inline ASTExpressionSubquery* MakeGqlExistsLinearOpsSubquery(
    ASTGqlOperatorList* op_list, ASTPathExpression* graph, ASTNode* hint,
    zetasql::parser::BisonParser* parser, const Location& loc) {
  auto* linear_ops_query =
      MAKE_NODE(ASTGqlLinearOpsQuery, loc, {graph, op_list});
  auto* query = MAKE_NODE(ASTQuery, loc, {linear_ops_query});
  SetGqlSubqueryQueryFields(query);
  auto* subquery = MAKE_NODE(ASTExpressionSubquery, loc, {hint, query});
  subquery->set_modifier(zetasql::ASTExpressionSubquery::EXISTS);
  return subquery;
}

template <typename Location>
inline zetasql::ASTGraphLabelExpression* MakeOrCombineGraphLabelOperation(
    const zetasql::ASTGraphLabelOperation::OperationType label_op,
    zetasql::parser::BisonParser* parser, const Location& location,
    zetasql::ASTGraphLabelExpression* left,
    zetasql::ASTGraphLabelExpression* right) {
  if (left->node_kind() == zetasql::AST_GRAPH_LABEL_OPERATION &&
      left->GetAsOrDie<zetasql::ASTGraphLabelOperation>()->op_type() ==
          label_op &&
      !left->parenthesized()) {
    // Embrace and extend left's ASTNode to flatten a series of `label_op`.
    return parser->WithEndLocation(WithExtraChildren(left, {right}), location);
  } else {
    auto* new_root = MAKE_NODE(ASTGraphLabelOperation, location, {left, right});
    new_root->set_op_type(label_op);
    return new_root;
  }
}

template <typename Location>
inline zetasql::ASTGraphEdgePattern* MakeGraphEdgePattern(
    zetasql::parser::BisonParser* parser,
    zetasql::ASTGraphElementPatternFiller* filler,
    zetasql::ASTGraphEdgePattern::EdgeOrientation orientation,
    const Location& location) {
  auto* edge_pattern = MAKE_NODE(ASTGraphEdgePattern, location, {filler});
  edge_pattern->set_orientation(orientation);
  return edge_pattern;
}

template <typename Location>
inline zetasql::ASTGraphElementLabelAndPropertiesList*
MakeGraphElementLabelAndPropertiesListImplicitDefaultLabel(
    zetasql::parser::BisonParser* parser,
    zetasql::ASTGraphProperties* properties, const Location& location) {
  auto* label_properties = MAKE_NODE(ASTGraphElementLabelAndProperties,
                                     location, {nullptr, properties});
  auto* label_properties_list = MAKE_NODE(ASTGraphElementLabelAndPropertiesList,
                                          location, {label_properties});
  return label_properties_list;
}

// Returns true if any node in the query subtree has a LockMode node.
inline bool HasLockMode(const zetasql::ASTNode* node) {
  if (node != nullptr) {
    // Use queue based BFS traversal to avoid stack overflow issues.
    std::queue<const zetasql::ASTNode*> node_queue;
    node_queue.push(node);

    while (!node_queue.empty()) {
      const zetasql::ASTNode* current_node = node_queue.front();
      node_queue.pop();

      if (current_node->node_kind() == zetasql::AST_LOCK_MODE) {
        return true;
      } else {
        for (int i = 0; i < current_node->num_children(); ++i) {
          if (current_node->child(i) != nullptr) {
            node_queue.push(current_node->child(i));
          }
        }
      }
    }
  }
  return false;
}

using zetasql::ASTInsertStatement;

}  // namespace parser_internal
}  // namespace zetasql

#endif  // ZETASQL_PARSER_PARSER_INTERNAL_H_
