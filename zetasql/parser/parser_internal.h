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

#include <optional>
#include <utility>
#include <vector>

#include "zetasql/parser/bison_parser.h"
#include "zetasql/parser/flex_tokenizer.h"
#include "zetasql/parser/location.hh"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/statement_properties.h"
#include "zetasql/public/strings.h"
#include "zetasql/base/case.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"

// Shorthand to call parser->CreateASTNode<>(). The "node_type" must be a
// AST... class from the zetasql namespace. The "..." are the arguments to
// BisonParser::CreateASTNode<>().
#define MAKE_NODE(node_type, ...) \
  parser->CreateASTNode<zetasql::node_type>(__VA_ARGS__);

// Generates a parse error with message 'msg' (which must be a string
// expression) at bison location 'location', and aborts the parser.
#define YYERROR_AND_ABORT_AT(location, msg) \
  do {                                      \
    error(location, (msg));                 \
    YYABORT;                                \
  } while (0)

// Generates a parse error of the form "Unexpected X", where X is a description
// of the current token, at bison location 'location', and aborts the parser.
#define YYERROR_UNEXPECTED_AND_ABORT_AT(location) \
  do {                                            \
    error(location, "");                          \
    YYABORT;                                      \
  } while (0)

#define CHECK_LABEL_SUPPORT(node, location)                                   \
  if (node != nullptr && (!parser->language_options().LanguageFeatureEnabled( \
                             zetasql::FEATURE_V_1_3_SCRIPT_LABEL))) {       \
    YYERROR_AND_ABORT_AT(location, "Script labels are not supported");        \
  }

#define CHECK_END_LABEL_VALID(label_node, label_location, end_label_node,      \
                              end_label_location)                              \
  if (end_label_node != nullptr &&                                             \
      !end_label_node->GetAsIdString().CaseEquals(                             \
          label_node->GetAsIdString())) {                                      \
    YYERROR_AND_ABORT_AT(end_label_location,                                   \
                         absl::StrCat("Mismatched end label; expected ",       \
                                      label_node->GetAsStringView(), ", got ", \
                                      end_label_node->GetAsStringView()));     \
  }

// Generates a parse error if there are spaces between location <left> and
// location <right>.
// For example, this is used when we composite multiple existing tokens to
// match a complex symbol without reserving it as a new token.
#define YYERROR_AND_ABORT_AT_WHITESPACE(left, right)                         \
  if (parser->HasWhitespace(left, right)) {                                  \
    YYERROR_AND_ABORT_AT(                                                    \
        left, absl::StrCat("Syntax error: Unexpected whitespace between \"", \
                           parser->GetInputText(left), "\" and \"",          \
                           parser->GetInputText(right), "\""));              \
  }

namespace zetasql {

namespace parser_internal {
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
  static inline absl::StatusOr<std::vector<zetasql::ASTNode*>> BuildPathParts(
      const zetasql_bison_parser::location& bison_location,
      PathParts raw_parts, zetasql::parser::BisonParser* parser) {
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
inline int zetasql_bison_parserlex(
    SemanticType* yylval, zetasql_bison_parser::location* yylloc,
    zetasql::parser::ZetaSqlFlexTokenizer* tokenizer) {
  ABSL_DCHECK(tokenizer != nullptr);
  return tokenizer->GetNextTokenFlex(yylloc);
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
inline zetasql_bison_parser::location FirstNonEmptyLocation(
    absl::Span<const zetasql_bison_parser::location> locations) {
  for (const zetasql_bison_parser::location& location : locations) {
    if (location.begin.column != location.end.column) {
      return location;
    }
  }
  return locations[0];
}

inline zetasql_bison_parser::location NonEmptyRangeLocation(
    absl::Span<const zetasql_bison_parser::location> locations) {
  std::optional<zetasql_bison_parser::location> range;
  for (const zetasql_bison_parser::location& location : locations) {
    if (location.begin.column != location.end.column) {
      if (!range.has_value()) {
        range = location;
      } else {
        if (location.begin.column < range->begin.column) {
          range->begin = location.begin;
        }
        if (location.end.column > range->end.column) {
          range->end = location.end;
        }
      }
    }
  }
  if (range.has_value()) {
    return *range;
  }
  return locations[0];
}
inline bool IsUnparenthesizedNotExpression(zetasql::ASTNode* node) {
  using zetasql::ASTUnaryExpression;
  const ASTUnaryExpression* expr = node->GetAsOrNull<ASTUnaryExpression>();
  return expr != nullptr && !expr->parenthesized() &&
         expr->op() == ASTUnaryExpression::NOT;
}

using zetasql::ASTInsertStatement;

}  // namespace parser_internal
}  // namespace zetasql

#endif  // ZETASQL_PARSER_PARSER_INTERNAL_H_
