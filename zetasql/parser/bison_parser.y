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

%code requires {
// Bison parser for ZetaSQL. This works in conjunction with
// zetasql::parser::BisonParser.
//
// To debug the state machine in case of conflicts, run (locally):
// $ bison bison_parser.y -r all --report-file=$HOME/bison_report.txt
// (Do NOT set the --report-file to a path on citc, because then the file will
// be truncated at 1MB for some reason.)

#include "zetasql/parser/location.hh"
#include "zetasql/parser/bison_parser.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/join_proccessor.h"
#include "zetasql/parser/statement_properties.h"
#include "zetasql/public/strings.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/strings/match.h"
#include "absl/strings/str_join.h"

#define YYINITDEPTH 50

// Shorthand to call parser->CreateASTNode<>(). The "node_type" must be a
// AST... class from the zetasql namespace. The "..." are the arguments to
// BisonParser::CreateASTNode<>().
#define MAKE_NODE(node_type, ...) \
    parser->CreateASTNode<zetasql::node_type>(__VA_ARGS__);

enum class NotKeywordPresence {
  kPresent,
  kAbsent
};

enum class AllOrDistinctKeyword {
  kAll,
  kDistinct,
  kNone,
};

enum class PrecedingOrFollowingKeyword {
  kPreceding,
  kFollowing
};

enum class ShiftOperator {
  kLeft,
  kRight
};

enum class TableOrTableFunctionKeywords {
  kTableKeyword,
  kTableAndFunctionKeywords
};

enum class ImportType {
  kModule,
  kProto,
};

// This node is used for temporarily aggregating together components of dashed
// identifiers ('a-b-1.c-2.d').  This node exists temporarily to hold
// intermediate values, and will not be part of the final parse tree.
class DashedIdentifierTmpNode final : public zetasql::ASTNode {
 public:
  static constexpr zetasql::ASTNodeKind kConcreteNodeKind =
      zetasql::AST_FAKE;

  DashedIdentifierTmpNode() : ASTNode(kConcreteNodeKind) {}
  void Accept(zetasql::ParseTreeVisitor* visitor, void* data) const override {
    LOG(FATAL) << "DashedIdentifierTmpNode does not support Accept";
  }
  zetasql_base::StatusOr<zetasql::VisitResult> Accept(
      zetasql::NonRecursiveParseTreeVisitor* visitor) const override {
    LOG(FATAL) << "DashedIdentifierTmpNode does not support Accept";
  }
  // This is used to represent an unquoted full identifier path that may contain
  // dashes ('-'). This requires special handling because of the ambiguity
  // in the lexer between an identifier and a number. For example:
  // outer-table-1.subtable-2.inner-3
  // The lexer takes this to be
  // outer,-,table,-,1.,subtable,-,2.,inner,-,3
  // For more information on this, see the 'dashed_identifier' rule.

  // We represent this as a list of one or more 'PathParts' which are
  // implicitly separated by a dot ('.'). Each may be composed of one or more
  // 'DashParts' which are implicitly separated by a dash ('-').
  // Thus, the example string above would be represented as the following:
  // {{"outer", "table", "1"}, {"subtable", "2"}, {"inner", "3"}}

  // In order to save memory, these all contain string_view entries (backed by
  // the parser's copy of the input sql).
  // This also uses inlined vectors, because we rarely expect more than a few
  // entries at either level.
  // Note, in the event the size is large, this will allocate directly to the
  // heap, rather than into the arena.
  using DashParts = absl::InlinedVector<absl::string_view, 2>;
  using PathParts = absl::InlinedVector<DashParts, 2>;

  void set_path_parts(PathParts path_parts) {
    path_parts_ = std::move(path_parts);
  }

  PathParts&& release_path_parts() {
    return std::move(path_parts_);
  }
  void InitFields() final {
    {
      FieldLoader fl(this);  // Triggers check that there were no children.
    }
  }

 private:
  PathParts path_parts_;
};

}

%defines
%skeleton "lalr1.cc"
%define parse.error verbose
%define api.parser.class {BisonParserImpl}

// This uses a generated "position" and "location" class, where "location" is a
// range of positions. "position" keeps track of the file name, line and column.
// We use only the column fields, and we use them to store byte offsets in the
// input.
%locations

// Bison doesn't support nested namespaces for this, so we can't use
// zetasql::parser.
%name-prefix "zetasql_bison_parser"

// Parameters for the parser. The tokenizer gets passed through into the lexer
// as well, so it is declared with "%lex-param" too.
%lex-param {zetasql::parser::ZetaSqlFlexTokenizer* tokenizer}
%parse-param {zetasql::parser::ZetaSqlFlexTokenizer* tokenizer}
%parse-param {zetasql::parser::BisonParser* parser}
%parse-param {zetasql::ASTNode** ast_node_result}
%parse-param {zetasql::parser::ASTStatementProperties*
                  ast_statement_properties}
%parse-param {std::string* error_message}
%parse-param {zetasql::ParseLocationPoint* error_location}
%parse-param {bool* move_error_location_past_whitespace}
%parse-param {int* statement_end_byte_offset}

// AMBIGUOUS CASES
// ===============
//
// AMBIGOUS CASE 1: EXPRESSION SUBQUERY
// ------------------------------------
// There is a known ambiguous case causing 2 shift/reduce conflicts:
//
//   "((SELECT ...))"
//
// This has two different possible interpretations:
// (a) a parenthesized expression containing an expression subquery.
// (b) an expression subquery containing a parenthesized query.
//
// Sometimes there is a resolution, if there is an expression-only or
// subquery-only continuation, e.g.:
//
//   "((SELECT ...) + ...)" => interpretation (a)
//   "((SELECT ...) UNION ALL ...)" => interpretation (b)
//
// However, this resolution always comes *after* parsing the entire subquery.
// Even if we assume there is always a resolution of the ambiguity, the only way
// to parse this in an LALR(1) parser such as Bison is by either having a single
// parse tree for the entire path, and deciding after the fact, *or* ensuring
// that the stack contents and states between the two paths are *exactly
// compatible*. We have chosen to take the second path.
//
// Here's how it works. The rules below the "query" rule tree are replicated
// into "*_maybe_expression". The only difference for most of the rules is that
// the $1 rule component is also a "_maybe_expression" rule variant, if and only
// if it could possibly resolve to a "query_primary". The "query_primary" rule's
// "maybe_expression" variant replaces the parenthesized query "(" query ")" by
// simply "expression", which itself resolves to "(" query_maybe_expression ")"
// via the "bare_expression_subquery" rule. The net effect is that the parser's
// state is compatible with both a parenthesized expression and a parenthesized
// query, even with multiple levels of parenthesization. For instance, the stack
// may contain this:
//
//   "(" "(" "(" <select> ")"
//
// Up until this point there is no reason for the parser to do any reduction
// that forces the choice. The first two parentheses could be the start of a
// "parenthesized_expression" or of a "query_maybe_expression". The third
// level of parentheses has no ambiguity (it can no longer be a
// "parenthesized_expression"). However, the first expected item in the
// "query_maybe_expression" rule tree is an "expression", so it gets resolved as
// a subquery expression anyway:
//
//   "(" "(" "(" <select> ")"
//   ...
//   "(" "(" <bare_expression_subquery>
//   "(" "(" <expression>
//
// Then assume the lookahead is ")" (i.e., the query is "(((<select>)))"). Then
// this happens:
//
//   "(" "(" <expression> LOOKAHEAD ")"
//
// At this point, the choice *is* ambiguous. There are two rules that may be
// applied:
//
//   (1) parenthesized_expression: "(" expression ")"
//   (2) query_primary_maybe_expression: expression
//
// The first rule is active because "(" "(" may be the start of a parenthesized
// expression. The second rule is active because "(" may be the start of a
// bare_expression_subquery, which is defined as:
//
//   (3) bare_expression_subquery: "(" query_maybe_expression ")"
//
// Now here's where the trick happens: only rules (1) and (2) are active, not
// rule (3) (because the "query_maybe_expression" isn't on the stack yet). And
// the choice here is between shifting ")" onto the stack (choosing rule (1)) or
// to reduce (rule (2)). The default resolution for shift/reduce conflicts is to
// shift, so the parser will choose rule (1) here, removing the ambiguity! So
// we get these steps:
//
//   "(" "(" <expression> ")"
//   "(" <parenthesized_expression>
//   "(" <expression>
//
// At this point, if the lookahead is again ")", then we get the exact same
// scenario and resolution:
//
//   "(" <expression> LOOKAHEAD ")"
//   "(" <expression> ")"
//   <parenthesized_expression>
//
// And the whole thing is resolved as two layers of parenthesized_expressions
// wrapping a bare_expression_subquery inside. This is also how the JavaCC
// parser resolves this. (Note that "parenthesized_expression" only marks its
// contained expression as parenthesized, so the resulting AST will have no
// layers of parentheses.)
//
// If, however, the last lookahead was not ")" but "UNION", the following
// would happen:
//
//   "(" <expression> LOOKAHEAD "UNION"
//
// Here, rule (1) would not be active, leaving only rule (2). Hence, the
// <expression> is reduced to a <query_primary_maybe_expression>:
//
//   "(" <query_primary_maybe_expression> LOOKAHEAD "UNION"
//
// The rule for <query_primary_maybe_expression> then has to backtrack and
// account for the fact that its input is an expression, not a query! So it
// checks that its input is a subquery expression, extracts the query, and turns
// it into a parenthesized query instead. There is a possible input where this
// would trigger an error:
//
//   (((1+2)) UNION ALL ...)
//
// Because there is no guarantee that the first parenthesized
// query_maybe_expression is actually a subquery!
//
//
// AMBIGOUS CASE 2: INSERT ... VALUES
// ----------------------------------
// The second known ambiguous case is INSERT ... VALUES. Since "values" can be
// used as an identifier, in a query like "INSERT mytable values (...", "values"
// can be a path expression. Technically this should not be ambiguous because
// the first element in this example is a table and the second cannot be
// anything else but VALUES. However, the optional "REPLACE" and "UPDATE"
// keywords at the beginning of the INSERT statement can also be used as
// identifiers, which means the grammar must parse at least three identifiers
// at the start of INSERT: one for replace/update, one for the target path, and
// one for VALUES.
//
// This case is responsible for one shift/reduce conflict. When "VALUES" is
// followed by "(", the grammar does not reduce "VALUES" to
// keyword_as_identifier, and instead shifts the "(" and treats "VALUES" as a
// keyword. See "insert_statement" for more comments on why this cannot be
// easily solved in any other way.
//
//
// AMBIGUOUS CASE 3: SAFE_CAST(...)
// --------------------------------
// The SAFE_CAST keyword is non-reserved and can be used as an identifier. This
// causes one shift/reduce conflict between keyword_as_identifier and the rule
// that starts with "SAFE_CAST" "(". It is resolved in favor of the SAFE_CAST(
// rule, which is the desired behavior.
//
//
// AMBIGUOUS CASE 4: CREATE TABLE FUNCTION
// ---------------------------------------
// ZetaSQL now supports statements of type CREATE TABLE FUNCTION <name> to
// generate new table-valued functions with user-defined names. It also
// supports statements of type CREATE TABLE <name> to generate tables. In the
// latter case, the table name can be any identifier, including FUNCTION, so
// the parser encounters a shift/reduce conflict when the CREATE TABLE FUNCTION
// tokens are pushed onto the stack. By default, the parser chooses to shift,
// favoring creating a new table-valued function. The user may workaround this
// limitation by surrounding the FUNCTION token in backticks.
// This case is responsible for 3 shift/reduce conflicts:
// 1. The separate parser rules for CREATE EXTERNAL TABLE and CREATE EXTERNAL
//    TABLE FUNCTION encounter a shift/reduce conflict.
// 2. The separate parser rules for CREATE TABLE AS and CREATE TABLE FUNCTION
//    encounter a shift/reduce confict.
// 3. The separate next_statement_kind rules for CREATE TABLE AS and CREATE
//    TABLE FUNCTION encounter a shift/reduce confict.
//
//
// AMBIGUOUS CASE 5: CREATE TABLE CONSTRAINTS
// ------------------------------------------
// The CREATE TABLE rules for the PRIMARY KEY and FOREIGN KEY constraints have
// 2 shift/reduce conflicts, one for each constraint. PRIMARY and FOREIGN can
// be used as keywords for constraint definitions and as identifiers for column
// names. Bison can either shift the PRIMARY or FOREIGN keywords and use them
// for constraint definitions, or it can reduce them as identifiers and use
// them for column definitions. By default Bison shifts them. If the next token
// is KEY, Bison proceeds to reduce table_constraint_definition; otherwise, it
// reduces PRIMARY or FOREIGN as identifier and proceeds to reduce
// table_column_definition. Note that this grammar reports a syntax error when
// using PRIMARY KEY or FOREIGN KEY as column definition name and type pairs.
//
// AMBIGUOUS CASE 6: REPLACE_FIELDS(...)
// --------------------------------
// The REPLACE_FIELDS keyword is non-reserved and can be used as an identifier.
// This causes a shift/reduce conflict between keyword_as_identifier and the
// rule that starts with "REPLACE_FIELDS" "(". It is resolved in favor of the
// REPLACE_FIELDS( rule, which is the desired behavior.
//
// AMBIGUOUS CASE 7: Procedure parameter list in CREATE PROCEDURE
// -------------------------------------------------------------
// With rule procedure_parameter being:
// [<mode>] <identifier> <type>
// Optional <mode> can be non-reserved word OUT or INOUT, which can also be
// used as <identifier>. This causes 4 shift/reduce conflicts:
//   ( OUT
//   ( INOUT
//   , OUT
//   , INOUT
// By default, Bison chooses to "shift" and always treat OUT/INOUT as <mode>.
// In order to use OUT/INOUT as identifier, it needs to be escaped with
// backticks.
//
// AMBIGUOUS CASE 8: CREATE TABLE GENERATED
// -------------------------------------------------------------
// The GENERATED column is a non-reserved keyword, so when a generated column
// is defined with "<name> [<type>] GENERATED [ON WRITE] AS ()", we have a
// shift/reduce conflict, not knowing whether the word GENERATED is an
// identifier from <type> or the keyword GENERATED because <type> is missing.
// By default, Bison chooses "shift", treating GENERATED as a keyword. To use it
// as an identifier, it needs to be escaped with backticks.
//
// Total expected shift/reduce conflicts as described above:
//   2: EXPRESSION SUBQUERY
//   1: INSERT VALUES
//   1: SAFE CAST
//   3: CREATE TABLE FUNCTION
//   2: CREATE TABLE CONSTRAINTS
//   1: REPLACE FIELDS
//   4: CREATE PROCEDURE
//   1: CREATE TABLE GENERATED
//   1: CREATE EXTERNAL TABLE FUNCTION
//   1: DESCRIPTOR
%expect 17

%union {
  bool boolean;
  int64_t int64_val;
  zetasql::TypeKind type_kind;
  zetasql::ASTFunctionCall::NullHandlingModifier null_handling_modifier;
  zetasql::ASTWindowFrame::FrameUnit frame_unit;
  zetasql::ASTTemplatedParameterType::TemplatedTypeKind
      templated_parameter_kind;
  zetasql::ASTBinaryExpression::Op binary_op;
  zetasql::ASTUnaryExpression::Op unary_op;
  zetasql::ASTSetOperation::OperationType set_operation_type;
  zetasql::ASTJoin::JoinType join_type;
  zetasql::ASTJoin::JoinHint join_hint;
  zetasql::ASTSampleSize::Unit sample_size_unit;
  zetasql::ASTInsertStatement::InsertMode insert_mode;
  zetasql::ASTNodeKind ast_node_kind;
  NotKeywordPresence not_keyword_presence;
  AllOrDistinctKeyword all_or_distinct_keyword;
  zetasql::SchemaObjectKind schema_object_kind_keyword;
  PrecedingOrFollowingKeyword preceding_or_following_keyword;
  TableOrTableFunctionKeywords table_or_table_function_keywords;
  ShiftOperator shift_operator;
  ImportType import_type;
  zetasql::ASTCreateStatement::Scope create_scope;
  zetasql::ASTCreateStatement::SqlSecurity sql_security;
  zetasql::ASTForeignKeyReference::Match foreign_key_match;
  zetasql::ASTForeignKeyActions::Action foreign_key_action;
  zetasql::ASTFunctionParameter::ProcedureParameterMode parameter_mode;
  zetasql::ASTCreateFunctionStmtBase::DeterminismLevel determinism_level;

  // Not owned. The allocated nodes are all owned by the parser.
  // Nodes should use the most specific type available.
  zetasql::ASTForeignKeyReference* foreign_key_reference;
  zetasql::ASTSetOperation* query_set_operation;
  zetasql::ASTInsertValuesRowList* insert_values_row_list;
  zetasql::ASTQuery* query;
  zetasql::ASTExpression* expression;
  zetasql::ASTExpressionSubquery* expression_subquery;
  zetasql::ASTFunctionCall* function_call;
  zetasql::ASTIdentifier* identifier;
  zetasql::ASTInsertStatement* insert_statement;
  zetasql::ASTNode* node;
  zetasql::ASTStatementList* statement_list;
  DashedIdentifierTmpNode* dashed_identifier;
}
// YYEOF is a special token used to indicate the end of the input. It's alias
// defaults to "end of file", but "end of input" is more appropriate for us.
%token YYEOF 0 "end of input"

// Literals and identifiers. String, bytes and identifiers are not unescaped by
// the tokenizer. This is done in the parser so that we can give better error
// messages, pinpointing specific error locations in the token. This is really
// helpful for e.g. invalid escape codes.
%token STRING_LITERAL "string literal"
%token BYTES_LITERAL "bytes literal"
%token INTEGER_LITERAL "integer literal"
%token FLOATING_POINT_LITERAL "floating point literal"
%token IDENTIFIER "identifier"

// Comments. They are only returned if the tokenizer is run in a special comment
// preserving mode. They are not returned by the tokenizer when used with the
// Bison parser.
%token COMMENT "comment"

// Operators and punctuation. All punctuation must be referenced as "x", not
// 'x', or else bison will complain.  The corresponding token codes for single
// character punctuation are 'x' (i.e., the character code).
%token '*' "*"
%token ',' ","
%token ';' ";"
%token '(' "("
%token ')' ")"
%token '=' "="
%token KW_NOT_EQUALS_C_STYLE "!="
%token KW_NOT_EQUALS_SQL_STYLE "<>"
%token '<' "<"
%token KW_LESS_EQUALS "<="
%token '>' ">"
%token KW_GREATER_EQUALS ">="
%token '|' "|"
%token '^' "^"
%token '&' "&"
%token '[' "["
%token ']' "]"
%token '@' "@"
%token KW_DOUBLE_AT "@@"
%token KW_CONCAT_OP "||"
%token '+' "+"
%token '-' "-"
%token '/' "/"
%token '~' "~"
%token '.' "."
%token KW_DOT_STAR ".*"
%token KW_OPEN_HINT "@{"
%token '}' "}"
%token '?' "?"
%token KW_OPEN_INTEGER_HINT "@n"
%token KW_SHIFT_LEFT "<<"
%token KW_SHIFT_RIGHT ">>"
%token KW_NAMED_ARGUMENT_ASSIGNMENT "=>"

// These are not used in the grammar. They are here for parity with the JavaCC
// tokenizer.
%token ':' ":"
%token '{' "{"

// Precedence for operator tokens. We use operator precedence parsing because it
// is *much* faster than recursive productions (~2x speedup). The operator
// precedence is defined by the order of the declarations here, with tokens
// specified in the same declaration having the same precedence.
//
// The fake DOUBLE_AT_PRECEDENCE symbol is introduced to resolve a shift/reduce
// conflict in the system_variable_expression rule. A potentially ambiguous
// input is "@@a.b". Without modifying the rule's precedence, this could be
// parsed as a system variable named "a" of type STRUCT or as a system variable
// named "a.b".
%left ".*"
%left "OR"
%left "AND"
%left UNARY_NOT_PRECEDENCE
%nonassoc "=" "<>" ">" "<" ">=" "<=" "!=" "LIKE" "IN" "BETWEEN" "IS" "NOT for BETWEEN/IN/LIKE"
%left "|"
%left "^"
%left "&"
%left "<<" ">>"
%left "+" "-"
%left "||"
%left "*" "/"
%left UNARY_PRECEDENCE  // For all unary operators
%precedence DOUBLE_AT_PRECEDENCE // Needs to appear before "."
%left PRIMARY_PRECEDENCE "(" ")" "[" "]" "." // For ., .(...), [], etc.

%code {

inline int zetasql_bison_parserlex(
    zetasql_bison_parser::BisonParserImpl::semantic_type* yylval,
    zetasql_bison_parser::location* yylloc,
    zetasql::parser::ZetaSqlFlexTokenizer* tokenizer) {
  DCHECK(tokenizer != nullptr);
  return tokenizer->GetNextTokenFlex(yylloc);
}

// Generates a parse error with message 'msg' (which must be a string
// expression) at bison location 'location', and aborts the parser.
#define YYERROR_AND_ABORT_AT(location, msg) \
    do { \
      error(location, (msg)); \
      YYABORT; \
    } while (0)

// Generates a parse error of the form "Unexpected X", where X is a description
// of the current token, at bison location 'location', and aborts the parser.
#define YYERROR_UNEXPECTED_AND_ABORT_AT(location) \
    do { \
      error(location, ""); \
      YYABORT; \
    } while (0)

#define WithStartLocation(node, location) \
  parser->WithStartLocation(node, location)

#define WithEndLocation(node, location) \
  parser->WithEndLocation(node, location)

#define WithLocation(node, location) \
  parser->WithLocation(node, location)

// Adds 'children' to 'node' and then returns 'node'.
template <typename ASTNodeType>
ASTNodeType* WithExtraChildren(
    ASTNodeType* node,
    absl::Span<zetasql::ASTNode* const> children) {
  for (zetasql::ASTNode* child : children) {
    if (child != nullptr) {
      node->AddChild(child);
    }
  }
  return node;
}

// Returns the first location in 'locations' that is not empty. If none of the
// locations are nonempty, returns the first location.
static zetasql_bison_parser::location FirstNonEmptyLocation(
    absl::Span<const zetasql_bison_parser::location> locations) {
  for (const zetasql_bison_parser::location& location : locations) {
    if (location.begin.column != location.end.column) {
      return location;
    }
  }
  return locations[0];
}

static bool IsUnparenthesizedNotExpression(zetasql::ASTNode* node) {
  using zetasql::ASTUnaryExpression;
  const ASTUnaryExpression* expr =
      node->GetAsOrNull<ASTUnaryExpression>();
  return expr != nullptr && !expr->parenthesized() &&
         expr->op() == ASTUnaryExpression::NOT;
}

using zetasql::ASTInsertStatement;
using zetasql::ASTCreateFunctionStmtBase;

}

// KEYWORDS
// --------
//
// To add a keyword:
// 1. Add a rule to flex_tokenizer.l.
// 2. Add the keyword to the array in keywords.cc, with the appropriate class.
// 3. If the keyword can be used as an identifier, add it to the
//    "keyword_as_identifier" production in the grammar.
// 4. If the keyword is reserved, add it to the "reserved_keyword_rule"
//    production in the grammar.
// BEGIN_RESERVED_KEYWORDS -- Do not remove this!
%token KW_ALL "ALL"
%token KW_AND "AND"
%token KW_AND_FOR_BETWEEN "AND for BETWEEN"
%token KW_ANY "ANY"
%token KW_ARRAY "ARRAY"
%token KW_AS "AS"
%token KW_ASC "ASC"
%token KW_ASSERT_ROWS_MODIFIED "ASSERT_ROWS_MODIFIED"
%token KW_AT "AT"
%token KW_BETWEEN "BETWEEN"
%token KW_BY "BY"
%token KW_CASE "CASE"
%token KW_CAST "CAST"
%token KW_COLLATE "COLLATE"
%token KW_CREATE "CREATE"
%token KW_CROSS "CROSS"
%token KW_CURRENT "CURRENT"
%token KW_DEFAULT "DEFAULT"
%token KW_DEFINE "DEFINE"
%token KW_DESC "DESC"
%token KW_DISTINCT "DISTINCT"
%token KW_ELSE "ELSE"
%token KW_END "END"
%token KW_ENUM "ENUM"
// Except is used in two locations of the language. And when the parser is
// exploding the rules it detects two rules can be used for the same syntax.
// See flex_tokenizer.l where it defines this special token that is only
// generated for an EXCEPT when there is a conflict involved.
//
// This is a special token that is only generated for an EXCEPT that is followed
// by a hint, ALL or DISTINCT.
%token KW_EXCEPT_IN_SET_OP "EXCEPT in set operation"
%token KW_EXCEPT "EXCEPT"
%token KW_EXISTS "EXISTS"
%token KW_EXTRACT "EXTRACT"
%token KW_FALSE "FALSE"
%token KW_FOLLOWING "FOLLOWING"
%token KW_FROM "FROM"
%token KW_FULL "FULL"
%token KW_GROUP "GROUP"
%token KW_GROUPING "GROUPING"
%token KW_HASH "HASH"
%token KW_HAVING "HAVING"
%token KW_IF "IF"
%token KW_IGNORE "IGNORE"
%token KW_IN "IN"
%token KW_INNER "INNER"
%token KW_INTERSECT "INTERSECT"
%token KW_INTERVAL "INTERVAL"
%token KW_INTO "INTO"
%token KW_IS "IS"
%token KW_JOIN "JOIN"
%token KW_LEFT "LEFT"
%token KW_LIKE "LIKE"
%token KW_LIMIT "LIMIT"
%token KW_LOOKUP "LOOKUP"
%token KW_MERGE "MERGE"
%token KW_NATURAL "NATURAL"
%token KW_NEW "NEW"
%token KW_NO "NO"
%token KW_NOT "NOT"
%token KW_NULL "NULL"
%token KW_NULLS "NULLS"
%token KW_ON "ON"
%token KW_OR "OR"
%token KW_ORDER "ORDER"
%token KW_OUTER "OUTER"
%token KW_OVER "OVER"
%token KW_PARTITION "PARTITION"
%token KW_PRECEDING "PRECEDING"
%token KW_PROTO "PROTO"
%token KW_RANGE "RANGE"
%token KW_RECURSIVE "RECURSIVE"
%token KW_RESPECT "RESPECT"
%token KW_RIGHT "RIGHT"
%token KW_ROLLUP "ROLLUP"
%token KW_ROWS "ROWS"
%token KW_SELECT "SELECT"
%token KW_SET "SET"
%token KW_STRUCT "STRUCT"
%token KW_TABLESAMPLE "TABLESAMPLE"
%token KW_THEN "THEN"
%token KW_TO "TO"
%token KW_TRUE "TRUE"
%token KW_UNBOUNDED "UNBOUNDED"
%token KW_UNION "UNION"
%token KW_USING "USING"
%token KW_WHEN "WHEN"
%token KW_WHERE "WHERE"
%token KW_WINDOW "WINDOW"
%token KW_WITH "WITH"
%token KW_UNNEST "UNNEST"

// These keywords may not be used in the grammar currently but are reserved
// for future use.
%token KW_CONTAINS "CONTAINS"
%token KW_CUBE "CUBE"
%token KW_ESCAPE "ESCAPE"
%token KW_EXCLUDE "EXCLUDE"
%token KW_FETCH "FETCH"
%token KW_FOR "FOR"
%token KW_GROUPS "GROUPS"
%token KW_LATERAL "LATERAL"
%token KW_OF "OF"
%token KW_SOME "SOME"
%token KW_TREAT "TREAT"
%token KW_WITHIN "WITHIN"
// END_RESERVED_KEYWORDS -- Do not remove this!

// This is a different token because using KW_NOT for BETWEEN/IN/LIKE would
// confuse the operator precedence parsing. Boolean NOT has a different
// precedence than NOT BETWEEN/IN/LIKE.
%token KW_NOT_FOR_BETWEEN_IN_LIKE "NOT for BETWEEN/IN/LIKE"

// Non-reserved keywords.  These can also be used as identifiers.
// These must all be listed explicitly in the "keyword_as_identifier" rule
// below. Do NOT include keywords in this list that are conditionally generated.
// They go in a separate list below this one.
// BEGIN_NON_RESERVED_KEYWORDS -- Do not remove this!
%token KW_ABORT "ABORT"
%token KW_ACCESS "ACCESS"
%token KW_ACTION "ACTION"
%token KW_ADD "ADD"
%token KW_AGGREGATE "AGGREGATE"
%token KW_ALTER "ALTER"
%token KW_ASSERT "ASSERT"
%token KW_BATCH "BATCH"
%token KW_BEGIN "BEGIN"
%token KW_BIGNUMERIC "BIGNUMERIC"
%token KW_BREAK "BREAK"
%token KW_CALL "CALL"
%token KW_CASCADE "CASCADE"
%token KW_CHECK "CHECK"
%token KW_CLUSTER "CLUSTER"
%token KW_COLUMN "COLUMN"
%token KW_COMMIT "COMMIT"
%token KW_CONNECTION "CONNECTION"
%token KW_CONTINUE "CONTINUE"
%token KW_CONSTANT "CONSTANT"
%token KW_CONSTRAINT "CONSTRAINT"
%token KW_DATA "DATA"
%token KW_DATABASE "DATABASE"
%token KW_DATE "DATE"
%token KW_DATETIME "DATETIME"
%token KW_DECLARE "DECLARE"
%token KW_DEFINER "DEFINER"
%token KW_DELETE "DELETE"
%token KW_DESCRIBE "DESCRIBE"
%token KW_DESCRIPTOR "DESCRIPTOR"
%token KW_DETERMINISTIC "DETERMINISTIC"
%token KW_DO "DO"
%token KW_DROP "DROP"
%token KW_ENFORCED "ENFORCED"
%token KW_ELSEIF "ELSEIF"
%token KW_EXECUTE "EXECUTE"
%token KW_EXPLAIN "EXPLAIN"
%token KW_EXPORT "EXPORT"
%token KW_EXTERNAL "EXTERNAL"
%token KW_FILTER "FILTER"
%token KW_FILL "FILL"
%token KW_FIRST "FIRST"
%token KW_FOREIGN "FOREIGN"
%token KW_FUNCTION "FUNCTION"
%token KW_GENERATED "GENERATED"
%token KW_GRANT "GRANT"
%token KW_HIDDEN "HIDDEN"
%token KW_IMMEDIATE "IMMEDIATE"
%token KW_IMMUTABLE "IMMUTABLE"
%token KW_IMPORT "IMPORT"
%token KW_INDEX "INDEX"
%token KW_INOUT "INOUT"
%token KW_INSERT "INSERT"
%token KW_INVOKER "INVOKER"
%token KW_ITERATE "ITERATE"
%token KW_ISOLATION "ISOLATION"
%token KW_KEY "KEY"
%token KW_LANGUAGE "LANGUAGE"
%token KW_LAST "LAST"
%token KW_LEAVE "LEAVE"
%token KW_LEVEL "LEVEL"
%token KW_LOOP "LOOP"
%token KW_MATCH "MATCH"
%token KW_MATCHED "MATCHED"
%token KW_MATERIALIZED "MATERIALIZED"
%token KW_MAX "MAX"
%token KW_MESSAGE "MESSAGE"
%token KW_MIN "MIN"
%token KW_MODEL "MODEL"
%token KW_MODULE "MODULE"
%token KW_NUMERIC "NUMERIC"
%token KW_OFFSET "OFFSET"
%token KW_ONLY "ONLY"
%token KW_OPTIONS "OPTIONS"
%token KW_OUT "OUT"
%token KW_PERCENT "PERCENT"
%token KW_POLICIES "POLICIES"
%token KW_POLICY "POLICY"
%token KW_PRIMARY "PRIMARY"
%token KW_PRIVATE "PRIVATE"
%token KW_PRIVILEGES "PRIVILEGES"
%token KW_PROCEDURE "PROCEDURE"
%token KW_PUBLIC "PUBLIC"
%token KW_RAISE "RAISE"
%token KW_READ "READ"
%token KW_REFERENCES "REFERENCES"
%token KW_RENAME "RENAME"
%token KW_REPEATABLE "REPEATABLE"
%token KW_REPLACE "REPLACE"
%token KW_REPLACE_FIELDS "REPLACE_FIELDS"
%token KW_RESTRICT "RESTRICT"
%token KW_RETURN "RETURN"
%token KW_RETURNS "RETURNS"
%token KW_REVOKE "REVOKE"
%token KW_ROLLBACK "ROLLBACK"
%token KW_ROW "ROW"
%token KW_RUN "RUN"
%token KW_SAFE_CAST "SAFE_CAST"
%token KW_SECURITY "SECURITY"
%token KW_SHOW "SHOW"
%token KW_SIMPLE "SIMPLE"
%token KW_SOURCE "SOURCE"
%token KW_SQL "SQL"
%token KW_STABLE "STABLE"
%token KW_START "START"
%token KW_STORED "STORED"
%token KW_STORING "STORING"
%token KW_SYSTEM "SYSTEM"
%token KW_SYSTEM_TIME "SYSTEM_TIME"
%token KW_TABLE "TABLE"
%token KW_TARGET "TARGET"
%token KW_TRANSFORM "TRANSFORM"
%token KW_TEMP "TEMP"
%token KW_TEMPORARY "TEMPORARY"
%token KW_TIME "TIME"
%token KW_TIMESTAMP "TIMESTAMP"
%token KW_TRANSACTION "TRANSACTION"
%token KW_TRUNCATE "TRUNCATE"
%token KW_TYPE "TYPE"
%token KW_UNIQUE "UNIQUE"
%token KW_UPDATE "UPDATE"
%token KW_VALUE "VALUE"
%token KW_VALUES "VALUES"
%token KW_VOLATILE "VOLATILE"
%token KW_VIEW "VIEW"
%token KW_VIEWS "VIEWS"
%token KW_WEIGHT "WEIGHT"
%token KW_WHILE "WHILE"
%token KW_WRITE "WRITE"
%token KW_ZONE "ZONE"
%token KW_EXCEPTION "EXCEPTION"
%token KW_ERROR "ERROR"
// END_NON_RESERVED_KEYWORDS -- Do not remove this!

// This is not a keyword token. It represents all identifiers that are
// CURRENT_* functions for date/time.
%token KW_CURRENT_DATETIME_FUNCTION

%token MODE_STATEMENT
%token MODE_SCRIPT
%token MODE_NEXT_STATEMENT
%token MODE_NEXT_SCRIPT_STATEMENT
%token MODE_NEXT_STATEMENT_KIND
%token MODE_EXPRESSION
%token MODE_TYPE

// All nonterminals that return nodes.
%type <node> abort_batch_statement
%type <node> alter_statement
%type <expression> analytic_function_call_expression
%type <node> array_column_schema_inner
%type <expression> array_constructor
%type <expression> array_constructor_prefix
%type <expression> array_constructor_prefix_no_expressions
%type <node> array_type
%type <node> as_query
%type <node> as_sql_function_body_or_string
%type <node> assert_statement
%type <expression_subquery> bare_expression_subquery
%type <node> begin_statement
%type <expression> bignumeric_literal
%type <expression> boolean_literal
%type <expression> bytes_literal
%type <node> call_statement
%type <node> call_statement_with_args_prefix
%type <expression> case_expression
%type <expression> case_expression_prefix
%type <expression> case_no_value_expression_prefix
%type <expression> case_value_expression_prefix
%type <expression> cast_expression
%type <expression> cast_int_literal_or_parameter
%type <node> cluster_by_clause_prefix_no_hint
%type <node> column_list
%type <node> column_list_prefix
%type <node> column_schema_inner
%type <node> commit_statement
%type <node> connection_clause
%type <node> create_constant_statement
%type <node> create_database_statement
%type <node> create_function_statement
%type <node> create_procedure_statement
%type <node> create_row_access_policy_grant_to_clause
%type <node> create_row_access_policy_statement
%type <node> create_external_table_statement
%type <node> create_external_table_function_statement
%type <node> create_index_statement
%type <node> create_table_function_statement
%type <node> create_model_statement
%type <node> create_table_statement
%type <node> create_view_statement
%type <expression> date_or_time_literal
%type <node> define_table_statement
%type <node> delete_statement
%type <node> describe_info
%type <node> describe_statement
%type <node> dml_statement
%type <node> drop_all_row_access_policies_statement
%type <node> drop_statement
%type <node> explain_statement
%type <node> export_data_statement
%type <expression> expression
%type <node> grant_to_clause
%type <node> index_storing_expression_list_prefix
%type <node> index_storing_expression_list
%type <expression> expression_or_default
%type <expression_subquery> expression_subquery
%type <expression> extract_expression
%type <expression> extract_expression_base
%type <node> field_schema
%type <node> filter_using_clause
%type <expression> floating_point_literal
%type <node> foreign_key_column_attribute
%type <foreign_key_reference> foreign_key_reference
%type <node> from_clause_contents
%type <expression> function_call_argument
%type <function_call> function_call_expression
%type <function_call> function_call_expression_base
%type <function_call> function_call_expression_with_args_prefix
%type <node> function_declaration
%type <node> function_name_from_keyword
%type <node> function_parameter
%type <node> function_parameters
%type <node> function_parameters_prefix
%type <node> procedure_parameter
%type <node> procedure_parameters
%type <node> procedure_parameters_prefix
%type <determinism_level> opt_determinism_level
%type <parameter_mode> opt_procedure_parameter_mode
%type <expression> generalized_path_expression
%type <expression> maybe_dashed_generalized_path_expression
%type <node> grant_statement
%type <node> grantee_list
%type <node> group_by_clause_prefix
%type <node> grouping_item
%type <node> hint
%type <node> hint_entry
%type <node> hint_with_body
%type <node> hint_with_body_prefix
%type <identifier> identifier
%type <identifier> identifier_in_hints
%type <node> if_statement
%type <node> elseif_clauses
%type <node> execute_immediate
%type <node> opt_execute_into_clause
%type <node> opt_execute_using_clause
%type <node> execute_using_argument
%type <node> execute_using_argument_list
%type <node> opt_elseif_clauses
%type <node> begin_end_block
%type <node> opt_exception_handler
%type <node> if_statement_unclosed
%type <node> break_statement
%type <node> continue_statement
%type <node> return_statement
%type <node> loop_statement
%type <node> while_statement
%type <node> import_statement
%type <node> variable_declaration
%type <node> opt_default_expression
%type <node> identifier_list
%type <node> set_statement
%type <node> index_order_by
%type <node> index_order_by_prefix
%type <node> index_storing_list
%type <node> index_unnest_expression_list
%type <node> in_list_two_or_more_prefix
%type <insert_statement> insert_statement
%type <insert_statement> insert_statement_prefix
%type <insert_values_row_list> insert_values_list
%type <node> insert_values_row
%type <node> insert_values_row_prefix
%type <expression> int_literal_or_parameter
%type <expression> integer_literal
%type <node> join
%type <node> join_input
%type <node> merge_action
%type <node> merge_insert_value_list_or_source_row
%type <node> merge_source
%type <node> merge_statement
%type <node> merge_statement_prefix
%type <node> merge_when_clause
%type <node> merge_when_clause_list
%type <node> model_clause
%type <node> module_statement
%type <node> nested_dml_statement
%type <expression> new_constructor
%type <node> new_constructor_arg
%type <expression> new_constructor_prefix
%type <expression> new_constructor_prefix_no_arg
%type <node> next_statement
%type <node> next_script_statement
%type <expression> null_literal
%type <node> opt_null_order
%type <expression> numeric_literal
%type <node> on_clause
%type <node> opt_and_expression
%type <node> opt_as_alias
%type <node> opt_as_alias_with_required_as
%type <node> opt_as_or_into_alias
%type <node> opt_as_query
%type <node> opt_as_query_or_string
%type <node> opt_as_sql_function_body_or_string
%type <node> opt_assert_rows_modified
%type <node> opt_cluster_by_clause_no_hint
%type <node> opt_collate_clause
%type <node> opt_column_list
%type <node> opt_constraint_identity
%type <node> opt_create_row_access_policy_grant_to_clause
%type <create_scope> opt_create_scope
%type <node> opt_at_system_time
%type <node> opt_description
%type <node> opt_else
%type <node> opt_foreign_key_actions
%type <node> opt_from_clause
%type <expression> opt_from_path_expression
%type <node> opt_function_parameters
%type <node> opt_function_returns
%type <node> opt_group_by_clause
%type <node> opt_having_clause
%type <node> opt_having_modifier
%type <node> opt_hint
%type <identifier> opt_identifier
%type <node> opt_index_storing_list
%type <node> opt_index_unnest_expression_list
%type <node> opt_language
%type <node> opt_like_string_literal
%type <node> opt_limit_offset_clause
%type <node> opt_on_or_using_clause_list
%type <node> on_or_using_clause_list
%type <node> on_or_using_clause
%type <expression> on_path_expression
%type <node> opt_options_list
%type <node> opt_order_by_clause
%type <node> opt_partition_by_clause
%type <node> opt_partition_by_clause_no_hint
%type <node> opt_repeatable_clause
%type <node> opt_returns
%type <sql_security> opt_sql_security_clause
%type <sql_security> sql_security_clause_kind
%type <node> opt_sample_clause
%type <node> opt_sample_clause_suffix
%type <node> opt_select_as_clause
%type <node> opt_table_element_list
%type <node> opt_transaction_mode_list
%type <node> opt_transform_clause
%type <node> opt_where_clause
%type <node> opt_where_expression
%type <node> opt_window_clause
%type <node> opt_window_frame_clause
%type <node> opt_with_offset_and_alias
%type <node> opt_with_offset_or_sample_clause
%type <node> options_entry
%type <node> options_list
%type <node> options_list_prefix
%type <node> order_by_clause_prefix
%type <node> ordering_expression
%type <expression> named_parameter_expression
%type <expression> parameter_expression
%type <expression> parenthesized_expression
%type <expression> system_variable_expression
%type <node> parenthesized_in_rhs
%type <node> partition_by_clause_prefix
%type <node> partition_by_clause_prefix_no_hint
%type <expression> path_expression
%type <dashed_identifier> dashed_identifier
%type <expression> dashed_path_expression
%type <expression> maybe_dashed_path_expression
%type <node> path_expression_or_string
%type <expression> possibly_cast_int_literal_or_parameter
%type <node> possibly_empty_column_list
%type <node> privilege
%type <node> privilege_list
%type <node> privilege_name
%type <node> privileges
%type <query> query
%type <node> query_maybe_expression
%type <node> query_primary
%type <node> query_primary_maybe_expression
%type <node> query_primary_or_set_operation
%type <node> query_primary_or_set_operation_maybe_expression
%type <node> query_set_operation
%type <node> query_set_operation_maybe_expression
%type <query_set_operation> query_set_operation_prefix
%type <query_set_operation> query_set_operation_prefix_maybe_expression
%type <node> query_statement
%type <node> repeatable_clause
%type <expression> generalized_extension_path
%type <node> replace_fields_arg
%type <expression> replace_fields_prefix
%type <expression> replace_fields_expression
%type <node> rename_statement
%type <node> revoke_statement
%type <node> rollback_statement
%type <node> rollup_list
%type <node> row_access_policy_alter_action
%type <node> row_access_policy_alter_action_list
%type <node> run_batch_statement
%type <node> sample_clause
%type <node> sample_size
%type <expression> sample_size_value
%type <node> select
%type <node> select_column
%type <node> select_list
%type <node> select_list_prefix
%type <node> show_statement
%type <identifier> show_target
%type <node> simple_column_schema_inner
%type <node> sql_function_body
%type <node> star_except_list
%type <node> star_except_list_prefix
%type <node> star_modifiers
%type <node> star_modifiers_with_replace_prefix
%type <node> star_replace_item
%type <node> start_batch_statement
%type <node> sql_statement
%type <node> sql_statement_body
%type <statement_list> statement_list
%type <node> script
%type <statement_list> non_empty_statement_list
%type <statement_list> unterminated_non_empty_statement_list
%type <expression> string_literal
%type <expression> string_literal_or_parameter
%type <expression> struct_constructor
%type <node> struct_constructor_arg
%type <expression> struct_constructor_prefix_with_keyword
%type <expression> struct_constructor_prefix_with_keyword_no_arg
%type <expression> struct_constructor_prefix_without_keyword
%type <node> struct_column_field
%type <node> struct_column_schema_inner
%type <node> struct_column_schema_prefix
%type <node> struct_field
%type <node> struct_type
%type <node> struct_type_prefix
%type <node> table_clause
%type <node> table_column_definition
%type <node> table_column_schema
%type <node> table_constraint_definition
%type <node> table_constraint_spec
%type <node> table_element
%type <node> table_element_list
%type <node> table_element_list_prefix
%type <node> table_path_expression
%type <node> table_path_expression_base
%type <node> table_primary
%type <node> table_subquery
%type <node> templated_parameter_type
%type <node> terminated_statement
%type <node> transaction_mode
%type <node> transaction_mode_list
%type <node> truncate_statement
%type <node> tvf
%type <node> tvf_argument
%type <node> tvf_prefix
%type <node> tvf_prefix_no_args
%type <node> type
%type <node> type_or_tvf_schema
%type <node> tvf_schema
%type <node> tvf_schema_column
%type <node> tvf_schema_prefix
%type <node> type_name
%type <node> unnest_expression
%type <node> unnest_expression_with_opt_alias_and_offset
%type <node> unterminated_statement
%type <node> unterminated_sql_statement
%type <node> unterminated_script_statement
%type <node> update_item
%type <node> update_item_list
%type <node> update_set_value
%type <node> update_statement
%type <node> using_clause
%type <node> using_clause_prefix
%type <node> window_clause_prefix
%type <node> window_definition
%type <node> window_frame_bound
%type <node> window_specification
%type <node> with_clause
%type <node> with_clause_entry
%type <node> with_clause_with_trailing_comma
%type <node> opt_with_connection_clause
%type <node> alter_action_list
%type <node> alter_action
%type <node> named_argument
%type <node> column_position
%type <node> opt_column_position
%type <expression> fill_using_expression
%type <expression> opt_fill_using_expression

%type <all_or_distinct_keyword> all_or_distinct
%type <all_or_distinct_keyword> opt_all_or_distinct
%type <schema_object_kind_keyword> schema_object_kind

%type <not_keyword_presence> between_operator
%type <not_keyword_presence> in_operator
%type <not_keyword_presence> is_operator
%type <not_keyword_presence> like_operator

%type <preceding_or_following_keyword> preceding_or_following

%type <shift_operator> shift_operator

%type <import_type> import_type

%type <table_or_table_function_keywords> table_or_table_function
%type <boolean> opt_access
%type <boolean> opt_aggregate
%type <boolean> opt_asc_or_desc
%type <boolean> opt_filter
%type <boolean> opt_if_exists
%type <boolean> opt_if_not_exists
%type <boolean> opt_natural
%type <boolean> opt_not_aggregate
%type <boolean> opt_or_replace
%type <boolean> opt_recursive
%type <boolean> opt_stored
%type <boolean> opt_unique
%type <node> primary_key_column_attribute
%type <node> hidden_column_attribute
%type <node> not_null_column_attribute
%type <node> column_attribute
%type <node> column_attributes
%type <node> generated_column_info
%type <node> opt_column_attributes
%type <node> opt_field_attributes
%type <node> opt_generated_column_info
%type <node> raise_statement

%type <binary_op> additive_operator
%type <binary_op> comparative_operator
%type <join_hint> join_hint
%type <join_type> join_type
%type <binary_op> multiplicative_operator
%type <ast_node_kind> next_statement_kind
%type <ast_node_kind> next_statement_kind_parenthesized_select
%type <ast_node_kind> next_statement_kind_without_hint
%type <ast_node_kind> next_statement_kind_create_modifiers
%type <set_operation_type> query_set_operation_type
%type <sample_size_unit> sample_size_unit
%type <insert_mode> unambiguous_or_ignore_replace_update
%type <unary_op> unary_operator

%type <type_kind> date_or_time_literal_kind

%type <null_handling_modifier> opt_null_handling_modifier
%type <frame_unit> frame_unit
%type <templated_parameter_kind> templated_parameter_kind

%type <foreign_key_match> opt_foreign_key_match
%type <foreign_key_match> foreign_key_match_mode
%type <foreign_key_action> foreign_key_action
%type <foreign_key_action> opt_foreign_key_on_delete
%type <foreign_key_action> opt_foreign_key_on_update
%type <foreign_key_action> foreign_key_on_delete
%type <foreign_key_action> foreign_key_on_update

%type <boolean> opt_constraint_enforcement
%type <boolean> constraint_enforcement

%type <node> descriptor_column_list
%type <node> descriptor_column
%type <node> descriptor_argument

%start start_mode
%%

start_mode:
    MODE_STATEMENT sql_statement { *ast_node_result = $2; }
    | MODE_SCRIPT script { *ast_node_result = $2; }
    | MODE_NEXT_STATEMENT next_statement { *ast_node_result = $2; }
    | MODE_NEXT_SCRIPT_STATEMENT next_script_statement { *ast_node_result = $2; }
    | MODE_NEXT_STATEMENT_KIND next_statement_kind
      { ast_statement_properties->node_kind = $2; }
    | MODE_EXPRESSION expression { *ast_node_result = $2; }
    | MODE_TYPE type { *ast_node_result = $2; }
    ;


opt_semicolon: ";" | /* Nothing */ ;

sql_statement:
    unterminated_sql_statement opt_semicolon
      {
        $$ = $1;
      }
    ;

next_script_statement:
    unterminated_statement ";"
      {
        // The semicolon marks the end of the statement.
        tokenizer->SetForceTerminate();
        *statement_end_byte_offset = @2.end.column;
        $$ = $1;
      }
    | unterminated_statement
      {
        // There's no semicolon. That means we have to be at EOF.
        *statement_end_byte_offset = -1;
        $$ = $1;
      }
    ;

next_statement:
    unterminated_sql_statement ";"
      {
        // The semicolon marks the end of the statement.
        tokenizer->SetForceTerminate();
        *statement_end_byte_offset = @2.end.column;
        $$ = $1;
      }
    | unterminated_sql_statement
      {
        // There's no semicolon. That means we have to be at EOF.
        *statement_end_byte_offset = -1;
        $$ = $1;
      }
    ;

unterminated_statement:
  unterminated_sql_statement
  | unterminated_script_statement
  ;

unterminated_sql_statement:
    sql_statement_body
    | hint sql_statement_body
      {
        $$ = MAKE_NODE(ASTHintedStatement, @$, {$1, $2});
      }
    ;

unterminated_script_statement:
    if_statement
    | begin_end_block
    | variable_declaration
    | while_statement
    | loop_statement
    | break_statement
    | continue_statement
    | return_statement
    | raise_statement
    ;

terminated_statement:
    unterminated_statement ";"
      {
        $$ = $1;
      }
    ;

sql_statement_body:
    query_statement
    | alter_statement
    | assert_statement
    | dml_statement
    | merge_statement
    | truncate_statement
    | begin_statement
    | set_statement
    | commit_statement
    | start_batch_statement
    | run_batch_statement
    | abort_batch_statement
    | create_constant_statement
    | create_database_statement
    | create_function_statement
    | create_procedure_statement
    | create_index_statement
    | create_row_access_policy_statement
    | create_external_table_statement
    | create_external_table_function_statement
    | create_model_statement
    | create_table_function_statement
    | create_table_statement
    | create_view_statement
    | define_table_statement
    | describe_statement
    | execute_immediate
    | explain_statement
    | export_data_statement
    | grant_statement
    | rename_statement
    | revoke_statement
    | rollback_statement
    | show_statement
    | drop_all_row_access_policies_statement
    | drop_statement
    | call_statement
    | import_statement
    | module_statement
    ;

query_statement:
    query
      {
        $$ = MAKE_NODE(ASTQueryStatement, @$, {$1});
      }
    ;

alter_action:
    "SET" "OPTIONS" options_list
      {
        $$ = MAKE_NODE(ASTSetOptionsAction, @$, {$3});
      }
    | "ADD" table_constraint_spec
      {
        $$ = MAKE_NODE(ASTAddConstraintAction, @$, {$2});
      }
    | "ADD" "CONSTRAINT" opt_if_not_exists identifier table_constraint_spec
      {
        auto* constraint = $5;
        constraint->AddChild($4);
        WithStartLocation(constraint, @4);
        auto* node = MAKE_NODE(ASTAddConstraintAction, @$, {constraint});
        node->set_is_if_not_exists($3);
        $$ = node;
      }
    | "DROP" "CONSTRAINT" opt_if_exists identifier
      {
        auto* node =
          MAKE_NODE(ASTDropConstraintAction, @$, {$4});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "CONSTRAINT" opt_if_exists identifier constraint_enforcement
      {
        auto* node =
          MAKE_NODE(ASTAlterConstraintEnforcementAction, @$, {$4});
        node->set_is_if_exists($3);
        node->set_is_enforced($5);
        $$ = node;
      }
    | "ALTER" "CONSTRAINT" opt_if_exists identifier "SET" "OPTIONS" options_list
      {
        auto* node =
          MAKE_NODE(ASTAlterConstraintSetOptionsAction, @$, {$4, $7});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ADD" "COLUMN" opt_if_not_exists table_column_definition
          opt_column_position opt_fill_using_expression
      {
        auto* node = MAKE_NODE(ASTAddColumnAction, @$, {$4, $5, $6});
        node->set_is_if_not_exists($3);
        $$ = node;
      }
    | "DROP" "COLUMN" opt_if_exists identifier
      {
        auto* node = MAKE_NODE(ASTDropColumnAction, @$, {$4});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "COLUMN" identifier "SET" "DATA" "TYPE" column_schema_inner
      {
        $$ = MAKE_NODE(ASTAlterColumnTypeAction, @$, {$3, $7});
      }
    | "ALTER" "COLUMN" identifier "SET" "OPTIONS" options_list
      {
        $$ = MAKE_NODE(ASTAlterColumnOptionsAction, @$, {$3, $6});
      }
    ;

alter_action_list:
    alter_action
      {
        $$ = MAKE_NODE(ASTAlterActionList, @$, {$1});
      }
    | alter_action_list "," alter_action
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

row_access_policy_alter_action:
    grant_to_clause
    | "FILTER" "USING" "(" expression ")"
      {
        zetasql::ASTFilterUsingClause* node = MAKE_NODE(
            ASTFilterUsingClause, @$, {$4});
        node->set_has_filter_keyword(true);
        $$ = node;
      }
    | "REVOKE" "FROM" "(" grantee_list ")"
      {
        $$ = MAKE_NODE(ASTRevokeFromClause, @$, {$4});
      }
    | "REVOKE" "FROM" "ALL"
      {
        zetasql::ASTRevokeFromClause* node = MAKE_NODE(
            ASTRevokeFromClause, @$);
        node->set_is_revoke_from_all(true);
        $$ = node;
      }
    | "RENAME" "TO" identifier
      {
        $$ = MAKE_NODE(ASTRenameToClause, @$, {$3});
      }
    ;

// This is split up the other ALTER actions since the alter actions for ROW
// ACCESS POLICY are only used by ROW ACCESS POLICY at the moment.
row_access_policy_alter_action_list:
    row_access_policy_alter_action
      {
        $$ = MAKE_NODE(ASTAlterActionList, @$, {$1});
      }
    | row_access_policy_alter_action_list "," row_access_policy_alter_action
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

// Note - this excludes the following objects:
// - ROW ACCESS POLICY for tactical reasons, since the production rules for
//   ALTER and DROP require very different syntax for ROW ACCESS POLICY as
//   compared to other object kinds.  So we do not want to match
//   ROW ACCESS POLICY here.
// - TABLE and TABLE FUNCTION, since we use different production for table path
//   expressions (one which may contain dashes).
schema_object_kind:
    "AGGREGATE" "FUNCTION"
      { $$ = zetasql::SchemaObjectKind::kAggregateFunction; }
    | "CONSTANT"
      { $$ = zetasql::SchemaObjectKind::kConstant; }
    | "DATABASE"
      { $$ = zetasql::SchemaObjectKind::kDatabase; }
    | "EXTERNAL" "TABLE"
      { $$ = zetasql::SchemaObjectKind::kExternalTable; }
    | "FUNCTION"
      { $$ = zetasql::SchemaObjectKind::kFunction; }
    | "INDEX"
      { $$ = zetasql::SchemaObjectKind::kIndex; }
    | "MATERIALIZED" "VIEW"
      { $$ = zetasql::SchemaObjectKind::kMaterializedView; }
    | "MODEL"
      { $$ = zetasql::SchemaObjectKind::kModel; }
    | "PROCEDURE"
      { $$ = zetasql::SchemaObjectKind::kProcedure; }
    | "VIEW"
      { $$ = zetasql::SchemaObjectKind::kView; }
    ;

alter_statement:
    "ALTER" table_or_table_function opt_if_exists maybe_dashed_path_expression
      alter_action_list
      {
        if ($2 == TableOrTableFunctionKeywords::kTableAndFunctionKeywords) {
          YYERROR_AND_ABORT_AT(@2, "ALTER TABLE FUNCTION is not supported");

        }
        zetasql::ASTAlterTableStatement* node = MAKE_NODE(
          ASTAlterTableStatement, @$, {$4, $5});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" schema_object_kind opt_if_exists path_expression
      alter_action_list
      {
        zetasql::ASTAlterStatementBase* node = nullptr;
        // Only ALTER DATABASE, TABLE, VIEW, and MATERIALIZED VIEW are currently
        // supported.
        if ($2 == zetasql::SchemaObjectKind::kDatabase) {
          node = MAKE_NODE(ASTAlterDatabaseStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kView) {
          node = MAKE_NODE(ASTAlterViewStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kMaterializedView) {
          node = MAKE_NODE(ASTAlterMaterializedViewStatement, @$);
        } else {
          YYERROR_AND_ABORT_AT(@2, absl::StrCat("ALTER ", absl::AsciiStrToUpper(
            parser->GetInputText(@2)), " is not supported"));
        }
        node->set_is_if_exists($3);
        node->AddChildren({$4, $5});
        $$ = WithLocation(node, @$);
      }
    | "ALTER" "ROW" "ACCESS" "POLICY" opt_if_exists identifier "ON"
      path_expression row_access_policy_alter_action_list
      {
        zetasql::ASTAlterRowAccessPolicyStatement* node = MAKE_NODE(
            ASTAlterRowAccessPolicyStatement, @$, {$6, $8, $9});
        node->set_is_if_exists($5);
        $$ = node;
      }
    | "ALTER" "ALL" "ROW" "ACCESS" "POLICIES" "ON" path_expression
      row_access_policy_alter_action
      {
        $$ = MAKE_NODE(ASTAlterAllRowAccessPoliciesStatement, @$, {$7, $8});
      }
    ;

opt_transform_clause:
    "TRANSFORM" "(" select_list ")"
      {
        $$ = MAKE_NODE(ASTTransformClause, @$, {$3})
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

assert_statement:
    "ASSERT" expression opt_description
      {
        $$ = MAKE_NODE(ASTAssertStatement, @$, {$2, $3});
      }
    ;

opt_description:
    "AS" string_literal
      {
        $$ = $2;
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

transaction_mode:
    "READ" "ONLY"
      {
        auto* node = MAKE_NODE(ASTTransactionReadWriteMode, @$, {});
        node->set_mode(zetasql::ASTTransactionReadWriteMode::READ_ONLY);
        $$ = node;
      }
    | "READ" "WRITE"
      {
        auto* node = MAKE_NODE(ASTTransactionReadWriteMode, @$, {});
        node->set_mode(zetasql::ASTTransactionReadWriteMode::READ_WRITE);
        $$ = node;
      }
    | "ISOLATION" "LEVEL" identifier
      {
        $$ = MAKE_NODE(ASTTransactionIsolationLevel, @$, {$3});
      }
    | "ISOLATION" "LEVEL" identifier identifier
      {
        $$ = MAKE_NODE(ASTTransactionIsolationLevel, @$, {$3, $4});
      }
    ;

transaction_mode_list:
    transaction_mode
      {
        $$ = MAKE_NODE(ASTTransactionModeList, @$, {$1});
      }
    | transaction_mode_list "," transaction_mode
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

opt_transaction_mode_list:
    transaction_mode_list
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

begin_statement:
    begin_transaction_keywords opt_transaction_mode_list
      {
        $$ = MAKE_NODE(ASTBeginStatement, @$, {$2});
      }
    ;

begin_transaction_keywords:
    "START" transaction_keyword
    | "BEGIN" opt_transaction_keyword
    ;

transaction_keyword:
    "TRANSACTION"
    ;

opt_transaction_keyword:
    transaction_keyword
    | /* Nothing */
    ;

set_statement:
    "SET" "TRANSACTION" transaction_mode_list
      {
        $$ = MAKE_NODE(ASTSetTransactionStatement, @$, {$3});
      }
    | "SET" identifier "=" expression
    {
      $$ = MAKE_NODE(ASTSingleAssignment, @$, {$2, $4});
    }
    | "SET" named_parameter_expression "=" expression
    {
      $$ = MAKE_NODE(ASTParameterAssignment, @$, {$2, $4});
    }
    | "SET" system_variable_expression "=" expression
    {
      $$ = MAKE_NODE(ASTSystemVariableAssignment, @$, {$2, $4});
    }
    | "SET" "(" identifier_list ")" "=" expression
    {
      $$ = MAKE_NODE(ASTAssignmentFromStruct, @$, {$3, $6});
    }
    | "SET" "(" ")"
    {
      // Provide improved error message for an empty variable list.
      YYERROR_AND_ABORT_AT(@3,
        "Parenthesized SET statement requires a variable list");
    }
    | "SET" identifier "," identifier_list "="
    {
      // Provide improved error message for missing parentheses around a
      // list of multiple variables.
      YYERROR_AND_ABORT_AT(@2,
        "Using SET with multiple variables requires parentheses around the "
        "variable list");
    }
    ;

commit_statement:
    "COMMIT" opt_transaction_keyword
      {
        $$ = MAKE_NODE(ASTCommitStatement, @$, {});
      }
    ;

rollback_statement:
    "ROLLBACK" opt_transaction_keyword
      {
        $$ = MAKE_NODE(ASTRollbackStatement, @$, {});
      }
    ;

start_batch_statement:
    "START" "BATCH" opt_identifier
      {
        $$ = MAKE_NODE(ASTStartBatchStatement, @$, {$3});
      }
    ;

run_batch_statement:
    "RUN" "BATCH"
      {
        $$ = MAKE_NODE(ASTRunBatchStatement, @$, {});
      }
    ;

abort_batch_statement:
    "ABORT" "BATCH"
      {
        $$ = MAKE_NODE(ASTAbortBatchStatement, @$, {});
      }
    ;

create_constant_statement:
    "CREATE" opt_or_replace opt_create_scope "CONSTANT" opt_if_not_exists
    path_expression "=" expression
      {
        auto* create = MAKE_NODE(ASTCreateConstantStatement, @$, {$6, $8});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($5);
        $$ = create;
      }
    ;

create_database_statement:
    "CREATE" "DATABASE" path_expression opt_options_list
      {
        $$ = MAKE_NODE(ASTCreateDatabaseStatement, @$, {$3, $4});
      }
    ;

create_function_statement:
    "CREATE" opt_or_replace opt_create_scope opt_aggregate
        "FUNCTION" opt_if_not_exists function_declaration opt_function_returns
        opt_sql_security_clause opt_determinism_level opt_language
        as_sql_function_body_or_string opt_options_list
      {
        auto* create = MAKE_NODE(ASTCreateFunctionStatement, @$,
                                 {$7, $8, $11, $12, $13});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_aggregate($4);
        create->set_is_if_not_exists($6);
        create->set_sql_security($9);
        create->set_determinism_level($10);
        $$ = create;
      }
    | "CREATE" opt_or_replace opt_create_scope opt_aggregate
        "FUNCTION" opt_if_not_exists function_declaration opt_function_returns
        opt_sql_security_clause opt_determinism_level opt_language "OPTIONS" options_list
        opt_as_sql_function_body_or_string
      {
        auto* create = MAKE_NODE(ASTCreateFunctionStatement, @$,
                                 {$7, $8, $11, $14, $13});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_aggregate($4);
        create->set_is_if_not_exists($6);
        create->set_sql_security($9);
        create->set_determinism_level($10);
        $$ = create;
      }
    | "CREATE" opt_or_replace opt_create_scope opt_aggregate
        "FUNCTION" opt_if_not_exists function_declaration opt_function_returns
        opt_sql_security_clause opt_determinism_level opt_language
      {
        auto* create = MAKE_NODE(ASTCreateFunctionStatement, @$,
                                 {$7, $8, $11, nullptr, nullptr});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_aggregate($4);
        create->set_is_if_not_exists($6);
        create->set_sql_security($9);
        create->set_determinism_level($10);
        $$ = create;
      }
    ;

// Returns true if AGGREGATE is present, false otherwise.
opt_aggregate:
    "AGGREGATE" { $$ = true; }
    | /* Nothing */ { $$ = false; }
    ;

// Returns true if NOT AGGREGATE is present, false otherwise.
opt_not_aggregate:
    "NOT" "AGGREGATE" { $$ = true; }
    | /* Nothing */ { $$ = false; }
    ;

function_declaration:
    path_expression function_parameters
      {
        $$ = MAKE_NODE(ASTFunctionDeclaration, @$, {$1, $2});
      }
    ;

function_parameter:
    identifier type_or_tvf_schema opt_as_alias_with_required_as
    opt_not_aggregate
      {
        auto* parameter = MAKE_NODE(ASTFunctionParameter, @$, {$1, $2, $3});
        parameter->set_is_not_aggregate($4);
        $$ = parameter;
      }
    | type_or_tvf_schema opt_as_alias_with_required_as opt_not_aggregate
      {
        auto* parameter = MAKE_NODE(ASTFunctionParameter, @$, {$1, $2});
        parameter->set_is_not_aggregate($3);
        $$ = parameter;
      }
    ;

function_parameters_prefix:
    "(" function_parameter
      {
        $$ = MAKE_NODE(ASTFunctionParameters, @$, {$2});
      }
    | function_parameters_prefix "," function_parameter
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

function_parameters:
    function_parameters_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        $$ = MAKE_NODE(ASTFunctionParameters, @$);
      }
    ;

create_procedure_statement:
    "CREATE" opt_or_replace opt_create_scope "PROCEDURE" opt_if_not_exists
    path_expression procedure_parameters opt_options_list begin_end_block
      {
        auto* create =
            MAKE_NODE(ASTCreateProcedureStatement, @$, {$6, $7, $8, $9});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($5);
        $$ = create;
      }
    ;

procedure_parameters_prefix:
    "(" procedure_parameter
      {
        $$ = MAKE_NODE(ASTFunctionParameters, @$, {$2});
      }
    | procedure_parameters_prefix "," procedure_parameter
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

procedure_parameters:
    procedure_parameters_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        $$ = MAKE_NODE(ASTFunctionParameters, @$);
      }
    ;

procedure_parameter_termination:
    ")"
    | ","
    ;

procedure_parameter:
    opt_procedure_parameter_mode identifier type_or_tvf_schema
      {
        auto* parameter = MAKE_NODE(ASTFunctionParameter, @$, {$2, $3});
        parameter->set_procedure_parameter_mode($1);
        $$ = parameter;
      }
    | opt_procedure_parameter_mode identifier procedure_parameter_termination
      {
        // There may be 3 cases causing this error:
        // 1. OUT int32_t where mode is empty and intended identifier name is
        //    "OUT"
        // 2. OUT int32_t where mode is OUT and identifier is missing
        // 3. OUT param_a where type is missing
        YYERROR_AND_ABORT_AT(@3,
                             "Syntax error: Unexpected end of parameter."
                             " Parameters should be in the format "
                             "[<parameter mode>] <parameter name> <type>. "
                             "If IN/OUT/INOUT is intended to be the name of a "
                             "parameter, it must be escaped with backticks"
                             );
      }
    ;

opt_procedure_parameter_mode:
    "IN" {$$ = ::zetasql::ASTFunctionParameter::ProcedureParameterMode::IN;}
    | "OUT"
      {$$ = ::zetasql::ASTFunctionParameter::ProcedureParameterMode::OUT;}
    | "INOUT"
      {$$ = ::zetasql::ASTFunctionParameter::ProcedureParameterMode::INOUT;}
    | /* nothing */
      {$$ = ::zetasql::ASTFunctionParameter::ProcedureParameterMode::NOT_SET;}
    ;

opt_returns:
    "RETURNS" type_or_tvf_schema
      {
        if ($2->node_kind() == zetasql::AST_TEMPLATED_PARAMETER_TYPE) {
          // TODO: Note that the official design supports this
          // feature. A reasonable use-case is named templated types here: e.g.
          // CREATE FUNCTION f(arg ANY TYPE T) RETURNS T AS ...
          YYERROR_AND_ABORT_AT(
              @2,
              "Syntax error: Templated types are not allowed in the "
              "RETURNS clause");
        }
        $$ = $2;
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

opt_function_returns:
    opt_returns
      {
        if ($1 != nullptr &&
            $1->node_kind() == zetasql::AST_TVF_SCHEMA) {
            YYERROR_AND_ABORT_AT(@1, "Syntax error: Unexpected TABLE");
        }
        $$ = $1;
      }
    ;

opt_determinism_level:
    "DETERMINISTIC" {$$ = ASTCreateFunctionStmtBase::DETERMINISTIC;}
    | "NOT" "DETERMINISTIC"
      {$$ = ASTCreateFunctionStmtBase::NOT_DETERMINISTIC;}
    | "IMMUTABLE"
      {$$ = ASTCreateFunctionStmtBase::IMMUTABLE;}
    | "STABLE"
      {$$ = ASTCreateFunctionStmtBase::STABLE;}
    | "VOLATILE"
      {$$ = ASTCreateFunctionStmtBase::VOLATILE;}
    | /* Nothing */
      {$$ = ASTCreateFunctionStmtBase::DETERMINISM_UNSPECIFIED;}
    ;


opt_language:
    "LANGUAGE" identifier
      {
        $$ = $2;
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

opt_sql_security_clause:
    "SQL" "SECURITY" sql_security_clause_kind { $$ = $3; }
    | /* Nothing */
      {
        $$ = zetasql::ASTCreateStatement::SQL_SECURITY_UNSPECIFIED;
      }
    ;

sql_security_clause_kind:
  "INVOKER" { $$ = zetasql::ASTCreateStatement::SQL_SECURITY_INVOKER; }
  | "DEFINER" { $$ = zetasql::ASTCreateStatement::SQL_SECURITY_DEFINER; }

as_sql_function_body_or_string:
    "AS" sql_function_body
      {
        $$ = $2;
      }
    | "AS" string_literal
      {
        $$ = $2;
      }
    ;

opt_as_sql_function_body_or_string:
    as_sql_function_body_or_string
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

path_expression_or_string:
    path_expression
      {
        $$ = $1;
      }
    | string_literal
      {
        $$ = $1;
      }
    ;

sql_function_body:
    "(" expression ")"
      {
        $$ = MAKE_NODE(ASTSqlFunctionBody, @$, {$2});
      }
    | "(" "SELECT"
      {
        YYERROR_AND_ABORT_AT(
        @2,
        "The body of each CREATE FUNCTION statement is an expression, not a "
        "query; to use a query as an expression, the query must be wrapped "
        "with additional parentheses to make it a scalar subquery expression");
      }
    ;

grant_to_clause:
    "GRANT" "TO" "(" grantee_list ")"
      {
        zetasql::ASTGrantToClause* grant_to =
            MAKE_NODE(ASTGrantToClause, @$, {$4});
        grant_to->set_has_grant_keyword_and_parens(true);
        $$ = grant_to;
      }

create_row_access_policy_grant_to_clause:
    grant_to_clause
    | "TO" grantee_list
      {
        zetasql::ASTGrantToClause* grant_to =
            MAKE_NODE(ASTGrantToClause, @$, {$2});
        grant_to->set_has_grant_keyword_and_parens(false);
        $$ = grant_to;
      }

opt_create_row_access_policy_grant_to_clause:
    create_row_access_policy_grant_to_clause
      {
        $$ = $1;
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

/* Returns true if FILTER is present. */
opt_filter:
    "FILTER"
      {
        $$ = true;
      }
    | /* Nothing */
      {
        $$ = false;
      }
    ;

filter_using_clause:
    opt_filter "USING" "(" expression ")"
      {
        zetasql::ASTFilterUsingClause* filter_using =
            MAKE_NODE(ASTFilterUsingClause, @$, {$4});
        filter_using->set_has_filter_keyword($1);
        $$ = filter_using;
      }

create_row_access_policy_statement:
    "CREATE" opt_or_replace "ROW" opt_access "POLICY" opt_if_not_exists
        opt_identifier "ON" path_expression
        opt_create_row_access_policy_grant_to_clause filter_using_clause
      {
        zetasql::ASTCreateRowAccessPolicyStatement* create =
            MAKE_NODE(ASTCreateRowAccessPolicyStatement, @$, {$7, $9, $10, $11});
        create->set_is_or_replace($2);
        create->set_is_if_not_exists($6);
        create->set_has_access_keyword($4);
        $$ = create;
      }
    ;

create_external_table_statement:
    "CREATE" opt_or_replace opt_create_scope "EXTERNAL"
    "TABLE" opt_if_not_exists path_expression
    opt_table_element_list opt_partition_by_clause_no_hint
    opt_cluster_by_clause_no_hint opt_options_list
      {
        if ($11 == nullptr) {
          YYERROR_AND_ABORT_AT(
              @11,
              "Syntax error: Expected keyword OPTIONS");
        }
        auto* create =
            MAKE_NODE(ASTCreateExternalTableStatement, @$,
            {$7, $8, $9, $10, $11});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($6);
        $$ = create;
      }
    ;

// This rule encounters a shift/reduce conflict with
// 'create_external_table_statement' as noted in AMBIGUOUS CASE 4 in the
// file-level comment. The syntax of this rule and
// 'create_external_table_statement' must be kept the same until the "TABLE"
// keyword, so that parser can choose between these two rules based on the
// "FUNCTION" keyword conflict.
create_external_table_function_statement:
    "CREATE" opt_or_replace opt_create_scope "EXTERNAL" "TABLE" "FUNCTION"
      {
        YYERROR_AND_ABORT_AT(
        @4,
        "Syntax error: CREATE EXTERNAL TABLE FUNCTION is not supported");
      }
    ;

create_index_statement:
  "CREATE" opt_or_replace opt_unique "INDEX" opt_if_not_exists
    path_expression "ON" path_expression opt_as_alias opt_index_unnest_expression_list index_order_by opt_index_storing_list opt_options_list
      {
        auto* create =
          MAKE_NODE(ASTCreateIndexStatement, @$, {$6, $8, $9, $10, $11, $12, $13});
        create->set_is_or_replace($2);
        create->set_is_unique($3);
        create->set_is_if_not_exists($5);
        $$ = create;
      }
    ;

// This rule encounters a shift/reduce conflict with 'create_table_statement'
// as noted in AMBIGUOUS CASE 4 in the file-level comment. The syntax of this
// rule and 'create_table_statement' must be kept the same until the "TABLE"
// keyword, so that parser can choose between these two rules based on the
// "FUNCTION" keyword conflict.
create_table_function_statement:
    "CREATE" opt_or_replace opt_create_scope "TABLE" "FUNCTION"
    opt_if_not_exists path_expression opt_function_parameters opt_returns
    opt_sql_security_clause opt_options_list opt_language opt_as_query_or_string
      {
        if ($8 == nullptr) {
            // Missing function argument list.
            YYERROR_AND_ABORT_AT(@8, "Syntax error: Expected (");
        }
        if ($9 != nullptr  &&
            $9->node_kind() != zetasql::AST_TVF_SCHEMA) {
          YYERROR_AND_ABORT_AT(@9, "Syntax error: Expected keyword TABLE");
        }
        // Build the create table function statement.
        auto* fn_decl = MAKE_NODE(ASTFunctionDeclaration, @7, @8, {$7, $8});
        auto* create = MAKE_NODE(ASTCreateTableFunctionStatement, @$,
                                 {fn_decl, $9, $11, $12, $13});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($6);
        create->set_sql_security($10);
        $$ = create;
      }
    ;

// This rule encounters a shift/reduce conflict with
// 'create_table_function_statement' as noted in AMBIGOUS CASE 4 in the
// file-level comment. The syntax of this rule and
// 'create_table_function_statement' must be kept the same until the "TABLE"
// keyword, so that parser can choose between these two rules based on the
// "FUNCTION" keyword conflict.
create_table_statement:
    "CREATE" opt_or_replace opt_create_scope "TABLE" opt_if_not_exists
    maybe_dashed_path_expression opt_table_element_list
    opt_partition_by_clause_no_hint opt_cluster_by_clause_no_hint
    opt_options_list opt_as_query
      {
        zetasql::ASTCreateStatement* create =
            MAKE_NODE(ASTCreateTableStatement, @$, {$6, $7, $8, $9, $10, $11});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($5);
        $$ = create;
      }
    ;

create_model_statement:
    "CREATE" opt_or_replace opt_create_scope "MODEL" opt_if_not_exists
    path_expression opt_transform_clause opt_options_list opt_as_query
      {
        zetasql::ASTCreateStatement* create =
            MAKE_NODE(ASTCreateModelStatement, @$, {$6, $7, $8, $9});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($5);
        $$ = create;
      }
    ;

opt_table_element_list:
    table_element_list
    | /* Nothing */ { $$ = nullptr; }
    ;

table_element_list:
    table_element_list_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
      ;

table_element_list_prefix:
    "(" table_element
      {
        $$ = MAKE_NODE(ASTTableElementList, @$, {$2});
      }
    | table_element_list_prefix "," table_element
      {
        $$ = WithExtraChildren($1, {$3});
      }
    | table_element_list_prefix ","
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

// The table_element grammar includes 2 shift/reduce conflicts in its
// table_constraint_definition rule. See the file header comment for
// AMBIGUOUS CASE 5: CREATE TABLE CONSTRAINTS.
//
// The table elements for the CREATE TABLE statement include a mix of column
// definitions and constraint definitions (such as foreign key, primary key,
// or check constraint). Most keywords in these definitions are
// context-sensitive and may also be used as identifiers.
//
// A number of strategies are used to disambiguate the grammar. Definitions
// starting with constraint keywords and tokens, such as "PRIMARY" "KEY",
// "FOREIGN" "KEY", and "CHECK" "(", are parsed as table constraints.
// Also, definitions with a reserved keyword, such as ARRAY, for the second
// token are unambiguously parsed as column definitions.
//
// Definitions prefixed with two identifiers are potentially either a column
// definition or a named constraint (e.g. CONSTRAINT name FOREIGN KEY). We
// cannot use 'CONSTRAINT identifier' in the grammar without an explosion of
// Bison shift/reduce conflicts with 'identifier identifier' in the column
// definition rules. Instead, constraint names are parsed as
// 'identifier identifier', manually checking if the first identifier is
// "CONSTRAINT".
//
// Lastly, the third token of a table element definition is always a reserved
// keyword (reserved_keyword_rule), a non-reserved keyword
// (keyword_as_identifier), or the "." in a path expression. The third token is
// never an IDENTIFIER. This enables the grammar to unambigously distinguish
// between named foreign key constraints and column definition attributes. The
// only requirement is that the third component of all table element rules,
// direct and indirect, is a keyword or symbol (i.e., a string literal, such as
// "HIDDEN", "FOREIGN" or ".").
//
table_element:
    table_column_definition
    | table_constraint_definition
    ;

table_column_definition:
    identifier table_column_schema opt_column_attributes opt_options_list
      {
        auto* schema = WithEndLocation(WithExtraChildren($2, {$3, $4}), @$);
        $$ = MAKE_NODE(ASTColumnDefinition, @$, {$1, schema});
      }
    ;

table_column_schema:
    column_schema_inner opt_generated_column_info
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$2}), @$);
      }
    | generated_column_info
      {
        $$ = MAKE_NODE(ASTInferredTypeColumnSchema, @$, {$1});
      }
    ;

simple_column_schema_inner:
    path_expression
      {
        $$ = MAKE_NODE(ASTSimpleColumnSchema, @$, {$1});
      }
    ;

array_column_schema_inner:
    "ARRAY" "<" field_schema ">"
      {
        $$ = MAKE_NODE(ASTArrayColumnSchema, @$, {$3});
      }
    ;

struct_column_field:
    // Unnamed fields cannot have OPTIONS annotation, because OPTIONS is not
    // a reserved keyword. More specifically, both
    //  field_schema
    // and
    //  column_schema_inner "OPTIONS" options_list
    // will result in conflict; even if we increase %expect, the parser favors
    // the last rule and fails when it encounters "(" after "OPTIONS".
    //
    // We could replace this rule with
    //   column_schema_inner
    //   | column_schema_inner not_null_column_attribute opt_options_list
    // without conflict, but it would be inconsistent to allow
    // STRUCT<INT64 NOT NULL OPTIONS()> while disallowing
    // STRUCT<INT64 OPTIONS()>.
    //
    // For a similar reason, the only supported field attribute is NOT NULL,
    // which have reserved keywords.
    column_schema_inner opt_field_attributes
      {
        auto* schema = WithEndLocation(WithExtraChildren($1, {$2}), @$);
        $$ = MAKE_NODE(ASTStructColumnField, @$, {schema});
      }
    | identifier field_schema
      {
        $$ = MAKE_NODE(ASTStructColumnField, @$, {$1, $2});
      }
    ;

struct_column_schema_prefix:
    "STRUCT" "<" struct_column_field
      {
        $$ = MAKE_NODE(ASTStructColumnSchema, @$, {$3});
      }
    | struct_column_schema_prefix "," struct_column_field
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

// This node does not apply WithEndLocation. column_schema and field_schema do.
struct_column_schema_inner:
    "STRUCT" "<" ">"
      {
        $$ = MAKE_NODE(ASTStructColumnSchema, @$);
      }
    | struct_column_schema_prefix ">"
    ;

column_schema_inner:
    simple_column_schema_inner
    | array_column_schema_inner
    | struct_column_schema_inner
    ;

opt_stored:
  "STORED"
    {
      $$ = true;
    }
  | /* nothing */
    {
      $$ = false;
    }
  ;

generated_column_info:
  "AS" "(" expression ")" opt_stored
    {
      auto* column = MAKE_NODE(ASTGeneratedColumnInfo, @$, {$3});
      column->set_is_stored($5);
      $$ = column;
    }
    // Since GENERATED is not a reserved keyword, this rule causes a
    // shift/reduce conflict with keyword_as_identifier. See more detailed
    // explanation at the "AMBIGUOUS CASES" section at the top of this file.
  | "GENERATED" "AS" "(" expression ")" opt_stored
    {
      auto* column = MAKE_NODE(ASTGeneratedColumnInfo, @$, {$4});
      column->set_is_stored($6);
      $$ = column;
    }
    // Explicitly writing ON WRITE here, because if we wrote a separate
    // opt_on_write grammar rule, we'd get a reduce/reduce conflict.
  | "GENERATED" "ON" "WRITE" "AS" "(" expression ")" opt_stored
    {
      auto* column = MAKE_NODE(ASTGeneratedColumnInfo, @$, {$6});
      column->set_is_stored($8);
      column->set_is_on_write(true);
      $$ = column;
    }
  ;

opt_generated_column_info:
  generated_column_info
  | /* nothing */ { $$ = nullptr;}
  ;

field_schema: column_schema_inner opt_field_attributes opt_options_list
    {
      $$ = WithEndLocation(WithExtraChildren($1, {$2, $3}), @$);
    }
    ;

primary_key_column_attribute:
  "PRIMARY" "KEY"
    {
      $$ = MAKE_NODE(ASTPrimaryKeyColumnAttribute, @$, {});
    }
  ;

foreign_key_column_attribute:
  opt_constraint_identity foreign_key_reference
    {
      auto* node = MAKE_NODE(ASTForeignKeyColumnAttribute, @$, {$1, $2});
      $$ = WithStartLocation(node, FirstNonEmptyLocation({@1, @2}));
    }
  ;

hidden_column_attribute:
  "HIDDEN"
    {
      $$ = MAKE_NODE(ASTHiddenColumnAttribute, @$, {});
    }
  ;

not_null_column_attribute:
  "NOT" "NULL"
    {
      $$ = MAKE_NODE(ASTNotNullColumnAttribute, @$, {});
    }
  ;

column_attribute:
  primary_key_column_attribute
  | foreign_key_column_attribute
  | hidden_column_attribute
  | not_null_column_attribute
  ;

// Conceptually, a foreign key column reference is defined by this rule:
//
//   opt_constraint_identity foreign_key_reference opt_constraint_enforcement
//
// However, the trailing opt_constraint_enforcement leads to a potential syntax
// error for a valid column definition:
//
//   a INT64 REFERENCES t (a) NOT NULL
//
// If foreign_key_reference included opt_constraint_enforcement, Bison's
// bottom-up evaluation would want to bind NOT to ENFORCED. For the example
// above, it would fail on NULL with a syntax error.
//
// The workaround is to hoist NOT ENFORCED to the same level in the grammar as
// NOT NULL. This forces Bison to defer shift/reduce decisions for NOT until it
// evaluates the next token, either ENFORCED or NULL.
column_attributes:
    column_attribute
      {
        $$ = MAKE_NODE(ASTColumnAttributeList, @$, {$1});
      }
    | column_attributes column_attribute
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$2}), @$);
      }
    | column_attributes constraint_enforcement
      {
        auto* last = $1->mutable_child($1->num_children() - 1);
        if (last->node_kind() != zetasql::AST_FOREIGN_KEY_COLUMN_ATTRIBUTE
          && last->node_kind() != zetasql::AST_PRIMARY_KEY_COLUMN_ATTRIBUTE) {
          YYERROR_AND_ABORT_AT(@2,
              "Syntax error: Unexpected constraint enforcement clause");
        }
        // Update the node's location to include constraint_enforcement.
        last = WithEndLocation(last, @$);
        if (last->node_kind() == zetasql::AST_FOREIGN_KEY_COLUMN_ATTRIBUTE) {
          int index = last->find_child_index(
              zetasql::AST_FOREIGN_KEY_REFERENCE);
          if (index == -1) {
            YYERROR_AND_ABORT_AT(@2,
                "Internal Error: Expected foreign key reference");
          }
          zetasql::ASTForeignKeyReference* reference =
              last->mutable_child(index)
                  ->GetAsOrDie<zetasql::ASTForeignKeyReference>();
          reference->set_enforced($2);
        } else {
          zetasql::ASTPrimaryKeyColumnAttribute* primary_key =
              last->GetAsOrDie<zetasql::ASTPrimaryKeyColumnAttribute>();
          primary_key->set_enforced($2);
        }
        $$ = WithEndLocation($1, @$);
      }
    ;

opt_column_attributes:
    column_attributes
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_field_attributes:
  not_null_column_attribute
    {
      $$ = MAKE_NODE(ASTColumnAttributeList, @$, {$1});
    }
  | /* Nothing */ { $$ = nullptr; }
  ;

column_position:
    "PRECEDING" identifier
      {
        auto* pos = MAKE_NODE(ASTColumnPosition, @$, {$2});
        pos->set_type(zetasql::ASTColumnPosition::PRECEDING);
        $$ = pos;
      }
    | "FOLLOWING" identifier
      {
        auto* pos = MAKE_NODE(ASTColumnPosition, @$, {$2});
        pos->set_type(zetasql::ASTColumnPosition::FOLLOWING);
        $$ = pos;
      }
    ;

opt_column_position:
    column_position
    | /* Nothing */ { $$ = nullptr; }
    ;

fill_using_expression:
    "FILL" "USING" expression
      {
        $$ = $3;
      }
    ;

opt_fill_using_expression:
    fill_using_expression
    | /* Nothing */ { $$ = nullptr; }
    ;

table_constraint_spec:
    "CHECK" "(" expression ")" opt_constraint_enforcement opt_options_list
      {
        auto* node = MAKE_NODE(ASTCheckConstraint, @$, {$3, $6});
        node->set_is_enforced($5);
        $$ = node;
      }
    | "FOREIGN" "KEY" column_list foreign_key_reference
        opt_constraint_enforcement opt_options_list
      {
        zetasql::ASTForeignKeyReference* foreign_key_ref = $4;
        foreign_key_ref->set_enforced($5);
        $$ = MAKE_NODE(ASTForeignKey, @$, {$3, $4, $6});
      }
    ;

// This rule produces 2 shift/reduce conflicts and requires manual parsing of
// named constraints. See table_element for details.
table_constraint_definition:
    "PRIMARY" "KEY" possibly_empty_column_list opt_constraint_enforcement
    opt_options_list
      {
        zetasql::ASTPrimaryKey* node = MAKE_NODE(ASTPrimaryKey, @$, {$3, $5});
        node->set_enforced($4);
        $$ = node;
      }
    | table_constraint_spec
    | identifier identifier table_constraint_spec
      {
        auto* node = $3;
        absl::string_view constraint = parser->GetInputText(@1);
        if (!zetasql_base::CaseEqual(constraint, "CONSTRAINT")) {
          if (node->node_kind() == zetasql::AST_CHECK_CONSTRAINT) {
            YYERROR_AND_ABORT_AT(
              @1,
              "Syntax error: Expected CONSTRAINT for check constraint "
              "definition. Check constraints on columns are not supported. "
              "Define check constraints as table elements instead");
          } else if (node->node_kind() == zetasql::AST_FOREIGN_KEY) {
            YYERROR_AND_ABORT_AT(@1,
              "Syntax error: Expected CONSTRAINT for foreign key definition");
          } else {
            YYERROR_AND_ABORT_AT(@$,
              "Syntax error: Unkown table constraint type");
          }
        }
        node->AddChild($2);
        $$ = WithLocation(node, @$);
      }
    ;

// Foreign key enforcement is parsed separately in order to avoid ambiguities
// in the grammar. See column_attributes for details.
foreign_key_reference:
    "REFERENCES" path_expression column_list opt_foreign_key_match
        opt_foreign_key_actions
      {
        auto* reference = MAKE_NODE(ASTForeignKeyReference, @$, {$2, $3, $5});
        reference->set_match($4);
        $$ = reference;
      }
    ;

opt_foreign_key_match:
    "MATCH" foreign_key_match_mode { $$ = $2; }
    | /* Nothing */ { $$ = zetasql::ASTForeignKeyReference::SIMPLE; }
    ;

foreign_key_match_mode:
    "SIMPLE" { $$ = zetasql::ASTForeignKeyReference::SIMPLE; }
    | "FULL" { $$ = zetasql::ASTForeignKeyReference::FULL; }
    | "NOT" "DISTINCT" { $$ = zetasql::ASTForeignKeyReference::NOT_DISTINCT; }
    ;

opt_foreign_key_actions:
    foreign_key_on_update opt_foreign_key_on_delete
      {
        auto* actions = MAKE_NODE(ASTForeignKeyActions, @$, {});
        actions->set_update_action($1);
        actions->set_delete_action($2);
        $$ = actions;
      }
    | foreign_key_on_delete opt_foreign_key_on_update
      {
        auto* actions = MAKE_NODE(ASTForeignKeyActions, @$, {});
        actions->set_delete_action($1);
        actions->set_update_action($2);
        $$ = actions;
      }
    | /* Nothing */
      {
        $$ = MAKE_NODE(ASTForeignKeyActions, @$, {});
      }
    ;

opt_foreign_key_on_update:
    foreign_key_on_update
    | /* Nothing */ { $$ = zetasql::ASTForeignKeyActions::NO_ACTION; }
    ;

opt_foreign_key_on_delete:
    foreign_key_on_delete
    | /* Nothing */ { $$ = zetasql::ASTForeignKeyActions::NO_ACTION; }
    ;

foreign_key_on_update:
    "ON" "UPDATE" foreign_key_action { $$ = $3; }
    ;

foreign_key_on_delete:
    "ON" "DELETE" foreign_key_action { $$ = $3; }
    ;

foreign_key_action:
    "NO" "ACTION" { $$ = zetasql::ASTForeignKeyActions::NO_ACTION; }
    | "RESTRICT" { $$ = zetasql::ASTForeignKeyActions::RESTRICT; }
    | "CASCADE" { $$ = zetasql::ASTForeignKeyActions::CASCADE; }
    | "SET" "NULL" { $$ = zetasql::ASTForeignKeyActions::SET_NULL; }
    ;

opt_constraint_identity:
    "CONSTRAINT" identifier { $$ = $2; }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_constraint_enforcement:
    constraint_enforcement
    | /* Nothing */ { $$ = true; }
    ;

constraint_enforcement:
    "ENFORCED" { $$ = true; }
    | "NOT" "ENFORCED" { $$ = false; }
    ;

// Matches either "TABLE" or "TABLE FUNCTION'. This encounters a shift/reduce
// conflict as noted in AMBIGUOUS CASE 4 in the file-level comment.
table_or_table_function:
    "TABLE" "FUNCTION"
      {
        $$ = TableOrTableFunctionKeywords::kTableAndFunctionKeywords;
      }
    | "TABLE"
      {
        $$ = TableOrTableFunctionKeywords::kTableKeyword;
      }
    ;

tvf_schema_column:
    identifier type
      {
        $$ = MAKE_NODE(ASTTVFSchemaColumn, @$, {$1, $2});
      }
    | type
      {
        $$ = MAKE_NODE(ASTTVFSchemaColumn, @$, {nullptr, $1});
      }
    ;

tvf_schema_prefix:
    "TABLE" "<" tvf_schema_column
      {
        auto* create = MAKE_NODE(ASTTVFSchema, @$, {$3});
        $$ = create;
      }
    | tvf_schema_prefix "," tvf_schema_column
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

tvf_schema:
    tvf_schema_prefix ">"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

opt_recursive: "RECURSIVE" { $$ = true; }
  | /* Nothing */ { $$ = false; }
  ;

create_view_statement:
    "CREATE" opt_or_replace opt_create_scope opt_recursive "VIEW"
    opt_if_not_exists maybe_dashed_path_expression opt_sql_security_clause
    opt_options_list as_query
      {
        auto* create = MAKE_NODE(ASTCreateViewStatement, @$, {$7, $9, $10});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_recursive($4);
        create->set_is_if_not_exists($6);
        create->set_sql_security($8);
        $$ = create;
      }
    |
    "CREATE" opt_or_replace "MATERIALIZED" opt_recursive "VIEW"
    opt_if_not_exists maybe_dashed_path_expression opt_sql_security_clause
    opt_partition_by_clause_no_hint opt_cluster_by_clause_no_hint
    opt_options_list as_query
      {
        auto* create = MAKE_NODE(
          ASTCreateMaterializedViewStatement, @$, {$7, $9, $10, $11, $12});
        create->set_is_or_replace($2);
        create->set_recursive($4);
        create->set_scope(zetasql::ASTCreateStatement::DEFAULT_SCOPE);
        create->set_is_if_not_exists($6);
        create->set_sql_security($8);
        $$ = create;
      }
    ;

as_query:
    "AS" query { $$ = $2; }

opt_as_query:
    as_query { $$ = $1; }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_as_query_or_string :
    as_query { $$ = $1; }
    | "AS" string_literal { $$ = $2; }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_if_not_exists:
    "IF" "NOT" "EXISTS" { $$ = true; }
    | /* Nothing */ { $$ = false; }
    ;

describe_statement:
    describe_keyword describe_info
      {
        $$ = WithStartLocation($2, @$);
      }
    ;

describe_info:
    identifier path_expression opt_from_path_expression
      {
        $$ = MAKE_NODE(ASTDescribeStatement, @$, {$1, $2, $3});
      }
    | path_expression opt_from_path_expression
      {
        $$ = MAKE_NODE(ASTDescribeStatement, @$, {nullptr, $1, $2});
      }
    ;

opt_from_path_expression:
    "FROM" path_expression
      {
        $$ = $2;
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

explain_statement:
    "EXPLAIN" unterminated_sql_statement
      {
        $$ = MAKE_NODE(ASTExplainStatement, @$, {$2});
      }
    ;

export_data_statement:
    "EXPORT" "DATA" opt_with_connection_clause opt_options_list "AS" query
      {
        $$ = MAKE_NODE(ASTExportDataStatement, @$, {$3, $4, $6});
      }
    ;

grant_statement:
    "GRANT" privileges "ON" identifier path_expression "TO" grantee_list
      {
        $$ = MAKE_NODE(ASTGrantStatement, @$, {$2, $4, $5, $7});
      }
    | "GRANT" privileges "ON" path_expression "TO" grantee_list
      {
        $$ = MAKE_NODE(ASTGrantStatement, @$, {$2, $4, $6});
      }
    ;

revoke_statement:
    "REVOKE" privileges "ON" identifier path_expression "FROM" grantee_list
      {
        $$ = MAKE_NODE(ASTRevokeStatement, @$, {$2, $4, $5, $7});
      }
    | "REVOKE" privileges "ON" path_expression "FROM" grantee_list
      {
        $$ = MAKE_NODE(ASTRevokeStatement, @$, {$2, $4, $6});
      }
    ;

privileges:
    "ALL" opt_privileges_keyword
      {
        $$ = MAKE_NODE(ASTPrivileges, @$, {});
      }
    |  privilege_list
      {
        $$ = $1;
      }
    ;

opt_privileges_keyword:
    KW_PRIVILEGES
    | /* Nothing */
    ;

privilege_list:
    privilege
      {
        $$ = MAKE_NODE(ASTPrivileges, @$, {$1});
      }
    | privilege_list "," privilege
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

privilege:
    privilege_name opt_column_list
      {
        $$ = MAKE_NODE(ASTPrivilege, @$, {$1, $2});
      }
    ;

privilege_name:
    identifier
      {
        $$ = $1;
      }
    | KW_SELECT
      {
        // The SELECT keyword is allowed to be a privilege name.
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    ;

rename_statement:
    "RENAME" identifier path_expression "TO" path_expression
      {
        $$ = MAKE_NODE(ASTRenameStatement, @$, {$2, $3, $5});
      }
    ;

import_statement:
    "IMPORT" import_type path_expression_or_string
    opt_as_or_into_alias opt_options_list
      {
        auto* import = MAKE_NODE(ASTImportStatement, @$, {$3, $4, $5});
        switch ($2) {
          case ImportType::kModule:
            import->set_import_kind(zetasql::ASTImportStatement::MODULE);
            break;
          case ImportType::kProto:
            import->set_import_kind(zetasql::ASTImportStatement::PROTO);
            break;
        }
        $$ = import;
      }
    ;

module_statement:
    "MODULE" path_expression opt_options_list
      {
        $$ = MAKE_NODE(ASTModuleStatement, @$, {$2, $3});
      }
    ;

index_order_by_prefix:
    "(" ordering_expression
      {
        $$ = MAKE_NODE(ASTIndexItemList, @$, {$2});
      }
    | index_order_by_prefix "," ordering_expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

index_order_by:
    index_order_by_prefix ")"
    {
      $$ = WithEndLocation($1, @$);
    }
    ;

index_unnest_expression_list:
   unnest_expression_with_opt_alias_and_offset
     {
       $$ = MAKE_NODE(ASTIndexUnnestExpressionList, @$, {$1});
     }
   |
   index_unnest_expression_list unnest_expression_with_opt_alias_and_offset
     {
       $$ = WithExtraChildren($1, {$2});
     }
   ;

opt_index_unnest_expression_list:
   index_unnest_expression_list
   |  /* Nothing */ { $$ = nullptr; }
   ;

index_storing_expression_list_prefix:
    "(" expression
      {
        $$ = MAKE_NODE(ASTIndexStoringExpressionList, @$, {$2});
      }
    | index_storing_expression_list_prefix "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

index_storing_expression_list:
    index_storing_expression_list_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

index_storing_list:
  "STORING" index_storing_expression_list {
    $$ = $2;
  }
  ;

opt_index_storing_list:
   index_storing_list
   | /* Nothing */ { $$ = nullptr; }
   ;

column_list_prefix:
    "(" identifier
      {
        $$ = MAKE_NODE(ASTColumnList, @$, {$2});
      }
    | column_list_prefix "," identifier
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

column_list:
    column_list_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

opt_column_list:
    column_list
    | /* Nothing */ { $$ = nullptr; }
    ;

possibly_empty_column_list:
    column_list_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        $$ = nullptr;
      }
    ;

grantee_list:
    string_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTGranteeList, @$, {$1});
      }
    | grantee_list "," string_literal_or_parameter
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

show_statement:
    "SHOW" show_target opt_from_path_expression opt_like_string_literal
      {
        $$ = MAKE_NODE(ASTShowStatement, @$, {$2, $3, $4});
      }
    ;

show_target:
  "MATERIALIZED" "VIEWS"
    {
      $$ = parser->MakeIdentifier(@$, "MATERIALIZED VIEWS");
    }
  | identifier
    {
      $$ = $1;
    }
  ;

opt_like_string_literal:
    "LIKE" string_literal
      {
        $$ = $2;
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

// Returns AllOrDistinctKeyword::kDistinct for DISTINCT, kAllKeyword for ALL.
all_or_distinct:
    "ALL" { $$ = AllOrDistinctKeyword::kAll; }
    | "DISTINCT" { $$ = AllOrDistinctKeyword::kDistinct; }
    ;

// Returns the token for a set operation as expected by
// ASTSetOperation::op_type().
query_set_operation_type:
    "UNION"
      {
        $$ = zetasql::ASTSetOperation::UNION;
      }
    | "EXCEPT in set operation"
      {
        $$ = zetasql::ASTSetOperation::EXCEPT;
      }
    | "INTERSECT"
      {
        $$ = zetasql::ASTSetOperation::INTERSECT;
      }
    ;

// Careful: Keep in sync with "query_primary_or_set_operation_maybe_expression"!
query_primary_or_set_operation:
    query_primary
    | query_set_operation
    ;

// "query_primary_or_set_operation" variant to resolve AMBIGUOUS CASE described
// above. Keep in sync with "query_primary_or_set_operation".
// Difference from query_primary_or_set_operation: where the other rule allows
// prefix (query), this rule uses "expression" to parse the same thing instead,
// and then errors out if the parsed entity is not an expression subquery.
//
// Careful: Keep in sync with "query_primary_or_set_operation"!
query_primary_or_set_operation_maybe_expression:
    query_primary_maybe_expression
    | query_set_operation_maybe_expression
    ;

// We don't use an opt_with_clause for the first element because it causes
// shift/reduce conflicts.
//
// Careful: Keep in sync with "query_maybe_expression"!
query:
    with_clause query_primary_or_set_operation opt_order_by_clause
    opt_limit_offset_clause
      {
        $$ = MAKE_NODE(ASTQuery, @$, {$1, $2, $3, $4});
      }
    | with_clause_with_trailing_comma "SELECT"
      {
        // TODO: Consider pointing the error location at the comma
        // instead of at the SELECT.
        YYERROR_AND_ABORT_AT(@2,
                             "Syntax error: Trailing comma after the WITH "
                             "clause before the SELECT clause is not allowed");
      }
    | query_primary_or_set_operation opt_order_by_clause
      opt_limit_offset_clause
      {
        $$ = MAKE_NODE(ASTQuery, @$, {$1, $2, $3});
      }
    ;

// "query" variant to resolve AMBIGUOUS CASE above.
// Difference from "query": where the other rule allows prefix (query), this
// rule uses "expression" to parse the same thing instead, and then errors out
// if the parsed entity is not an expression subquery.
//
// Careful: keep in sync with "query"!
query_maybe_expression:
    with_clause query_primary_or_set_operation_maybe_expression
    opt_order_by_clause opt_limit_offset_clause
      {
        $$ = MAKE_NODE(ASTQuery, @$, {$1, $2, $3, $4});
      }
    | with_clause_with_trailing_comma "SELECT"
      {
        // TODO: Consider pointing the error location at the comma
        // instead of at the SELECT.
        YYERROR_AND_ABORT_AT(@2,
                             "Syntax error: Trailing comma after the WITH "
                             "clause before the SELECT clause is not allowed");
      }
    | query_primary_or_set_operation_maybe_expression opt_order_by_clause
      opt_limit_offset_clause
      {
        $$ = MAKE_NODE(ASTQuery, @$, {$1, $2, $3});
      }
    ;

// This rule allows combining multiple query_primaries with set operations
// as long as all the set operations are identical. It is written to allow
// different set operations grammatically, but it generates an error if
// the set operations in an unparenthesized sequence are different.
// We have no precedence rules for associativity between different set
// operations but parentheses are supported to disambiguate.
//
// Careful: keep in sync with "query_set_operation_prefix_maybe_expression"!
query_set_operation_prefix:
    query_primary query_set_operation_type opt_hint all_or_distinct
    query_primary
      {
        auto* set_op = MAKE_NODE(ASTSetOperation, @$, {$3, $1, $5});
        set_op->set_op_type($2);
        set_op->set_distinct($4 == AllOrDistinctKeyword::kDistinct);
        $$ = set_op;
      }
    | query_set_operation_prefix query_set_operation_type opt_hint all_or_distinct
      query_primary
      {
        zetasql::ASTSetOperation* set_op = $1;
        if (set_op->op_type() != $2 ||
            set_op->distinct() != ($4 == AllOrDistinctKeyword::kDistinct)) {
          YYERROR_AND_ABORT_AT(
              @2,
              "Syntax error: Different set operations cannot be used in the "
              "same query without using parentheses for grouping");
        }
        if (/*hint*/$3) {
          YYERROR_AND_ABORT_AT(
              @3,
              "Syntax error: Hints on set operations must appear on the first "
              " operation.");
        }
        $$ = WithExtraChildren(set_op, {$5});
      }
    ;

// Careful: keep in sync with "query_set_operation_maybe_expression"!
query_set_operation:
   query_set_operation_prefix
     {
       $$ = WithEndLocation($1, @$);
     }
   ;

// "query_set_operation_prefix" variant to resolve AMBIGUOUS CASE described
// above.
// Difference from query_set_operation_prefix: where the other rule allows
// prefix (query), this rule uses "expression" to parse the same thing instead,
// and then errors out if the parsed entity is not an expression subquery.
//
// Careful: keep in sync with "query_set_operation_prefix"!
query_set_operation_prefix_maybe_expression:
    query_primary_maybe_expression query_set_operation_type all_or_distinct
    query_primary
      {
        auto* set_op = MAKE_NODE(ASTSetOperation, @$, {$1, $4});
        set_op->set_op_type($2);
        set_op->set_distinct($3 == AllOrDistinctKeyword::kDistinct);
        $$ = set_op;
      }
    | query_set_operation_prefix_maybe_expression query_set_operation_type
    all_or_distinct query_primary
      {
        zetasql::ASTSetOperation* set_op = $1;
        if (set_op->op_type() != $2 ||
            set_op->distinct() != ($3 == AllOrDistinctKeyword::kDistinct)) {
          YYERROR_AND_ABORT_AT(
              @2,
              "Syntax error: Different set operations cannot be used in the "
              "same query without using parentheses for grouping");
        }
        $$ = WithExtraChildren(set_op, {$4});
      }
    ;

// "query_set_operation" variant to resolve AMBIGUOUS CASE described above.
// Difference from query_set_operation: where the other rule allows prefix
// (query), this rule uses "expression" to parse the same thing instead, and
// then errors out if the parsed entity is not an expression subquery.
//
// Careful: keep in sync with "query_set_operation"!
query_set_operation_maybe_expression:
   query_set_operation_prefix_maybe_expression
     {
       $$ = WithEndLocation($1, @$);
     }
   ;

// Careful: keep in sync with "query_primary_maybe_expression"!
query_primary:
    select
    | "(" query ")"
      {
        zetasql::ASTQuery* query = $2;
        query->set_parenthesized(true);
        $$ = query;
      }
    ;

// "query_primary" variant to resolve AMBIGUOUS CASE described above.
//
// This uses "expression" instead of "(" query ")", and then errors out if it
// does not return an expression subquery. See AMBIGUOUS CASE above for an
// explanation of what this does.
//
// Careful: keep in sync with "query_primary"!
query_primary_maybe_expression:
    select
    | expression
      {
        if ($1->node_kind() != zetasql::AST_EXPRESSION_SUBQUERY) {
          // We could give an error at the end of the expression, because that's
          // where the context turns the expression into an argument of a
          // relational set operator. However, there are cases where this is
          // triggered where the following token really can't be recognized by
          // the user as a relational operator, even though it does force the
          // interpretation to be "query". So we point at the beginning of the
          // expression instead, to be on the safe side.
          // TODO: This is not ideal. Make a better error message.
          YYERROR_AND_ABORT_AT(
              @$,
              "Syntax error: Parenthesized expression cannot be parsed as an "
              "expression, struct constructor, or subquery");
        }
        zetasql::ASTQuery* query =
            $1->GetAsOrDie<zetasql::ASTExpressionSubquery>()
              ->GetMutableQueryChildInternal();
        if (query == nullptr) {
          YYERROR_AND_ABORT_AT(
              @1,
              "Internal error: expected query as child of subquery");
        }
        query->set_parenthesized(true);
        $$ = query;
      }
    ;

select:
    "SELECT" opt_hint
    placeholder
    opt_all_or_distinct
    opt_select_as_clause select_list opt_from_clause opt_where_clause
    opt_group_by_clause opt_having_clause opt_window_clause
      {
        auto* select =
            MAKE_NODE(ASTSelect, @$, {$2,
                                      $5, $6, $7, $8, $9, $10, $11});
        select->set_distinct($4 == AllOrDistinctKeyword::kDistinct);
        $$ = select;
      }
    | "SELECT" opt_hint
      placeholder
      opt_all_or_distinct
      opt_select_as_clause "FROM"
      {
        YYERROR_AND_ABORT_AT(
            @6,
            "Syntax error: SELECT list must not be empty");
      }
    ;

// AS STRUCT, AS VALUE, or AS <path expression>. This needs some special
// handling because VALUE is a valid path expression.
opt_select_as_clause:
    "AS" "STRUCT"
      {
         auto* select_as = MAKE_NODE(ASTSelectAs, @$);
         select_as->set_as_mode(zetasql::ASTSelectAs::STRUCT);
         $$ = select_as;
      }
    | "AS" path_expression
      {
        // "VALUE" is a valid identifier, so it can be a valid path expression.
        // But AS VALUE has a special meaning as a SELECT statement mode. We
        // handle it here, but only when VALUE is used without backquotes. With
        // backquotes the `VALUE` is treated like a regular path expression.
        bool is_value = false;
        if ($2->num_children() == 1) {
          if (zetasql_base::CaseEqual(parser->GetInputText(@2), "VALUE")) {
            auto* select_as = MAKE_NODE(ASTSelectAs, @$);
            select_as->set_as_mode(zetasql::ASTSelectAs::VALUE);
            $$ = select_as;
            is_value = true;
          }
        }
        if (!is_value) {
          auto* select_as = MAKE_NODE(ASTSelectAs, @$, {$2});
          select_as->set_as_mode(zetasql::ASTSelectAs::TYPE_NAME);
          $$ = select_as;
        }
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

extra_identifier_in_hints_name:
    "HASH"
    | "PROTO"
    | "PARTITION"
    ;

identifier_in_hints:
    identifier
    | extra_identifier_in_hints_name
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    ;

hint_entry:
    identifier_in_hints "=" expression
      {
        $$ = MAKE_NODE(ASTHintEntry, @$, {$1, $3});
      }
    | identifier_in_hints "." identifier_in_hints "=" expression
      {
        $$ = MAKE_NODE(ASTHintEntry, @$, {$1, $3, $5});
      }
    ;

hint_with_body_prefix:
    KW_OPEN_INTEGER_HINT integer_literal "@{" hint_entry
      {
        $$ = MAKE_NODE(ASTHint, @$, {$2, $4});
      }
    | "@{" hint_entry
      {
        $$ = MAKE_NODE(ASTHint, @$, {$2});
      }
    | hint_with_body_prefix "," hint_entry
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

hint_with_body:
    hint_with_body_prefix "}"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

// We can have "@<int>", "@<int> @{hint_body}", or "@{hint_body}". The case
// where both @<int> and @{hint_body} are present is covered by
// hint_with_body_prefix.
hint:
    KW_OPEN_INTEGER_HINT integer_literal
      {
        $$ = MAKE_NODE(ASTHint, @$, {$2});
      }
    | hint_with_body
    ;

// This returns an AllOrDistinctKeyword to indicate what was present.
opt_all_or_distinct:
    "ALL" { $$ = AllOrDistinctKeyword::kAll; }
    | "DISTINCT" { $$ = AllOrDistinctKeyword::kDistinct; }
    | /* Nothing */ { $$ = AllOrDistinctKeyword::kNone; }
    ;

select_list_prefix:
    select_column
      {
        $$ = MAKE_NODE(ASTSelectList, @$, {$1});
      }
    | select_list_prefix "," select_column
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

select_list:
    select_list_prefix
      {
        $$ = WithEndLocation($1, @$);
      }
    |
    select_list_prefix ","
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

star_except_list_prefix:
    "EXCEPT" "(" identifier
      {
        $$ = MAKE_NODE(ASTStarExceptList, @$, {$3});
      }
    | star_except_list_prefix "," identifier
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

star_except_list:
    star_except_list_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

star_replace_item:
    expression "AS" identifier
      {
        $$ = MAKE_NODE(ASTStarReplaceItem, @$, {$1, $3});
      }
    ;

star_modifiers_with_replace_prefix:
   star_except_list "REPLACE" "(" star_replace_item
      {
        $$ = MAKE_NODE(ASTStarModifiers, @$, {$1, $4});
      }
   | "REPLACE" "(" star_replace_item
     {
       $$ = MAKE_NODE(ASTStarModifiers, @$, {$3});
     }
   | star_modifiers_with_replace_prefix "," star_replace_item
     {
       $$ = WithExtraChildren($1, {$3});
     }
   ;

star_modifiers:
    star_except_list
      {
        $$ = MAKE_NODE(ASTStarModifiers, @$, {$1});
      }
    | star_modifiers_with_replace_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

select_column:
    expression
      {
        $$ = MAKE_NODE(ASTSelectColumn, @$, {$1});
      }
    | expression "AS" identifier
      {
        auto* alias = MAKE_NODE(ASTAlias, @2, @3, {$3});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {$1, alias});
      }
    | expression identifier
      {
        auto* alias = MAKE_NODE(ASTAlias, @2, {$2});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {$1, alias});
      }
    | expression ".*"
      {
        auto* dot_star = MAKE_NODE(ASTDotStar, @2, {$1});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {dot_star});
      }
    | expression ".*" star_modifiers
      {
        auto* dot_star_with_modifiers =
            MAKE_NODE(ASTDotStarWithModifiers, @2, @3, {$1, $3});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {dot_star_with_modifiers});
      }
    | "*"
      {
        auto* star = MAKE_NODE(ASTStar, @$);
        star->set_image("*");
        $$ = MAKE_NODE(ASTSelectColumn, @$, {star});
      }
    | "*" star_modifiers
      {
        auto* star_with_modifiers = MAKE_NODE(ASTStarWithModifiers, @$, {$2});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {star_with_modifiers});
      }
    ;

opt_as_alias:
    opt_as identifier
      {
        $$ = MAKE_NODE(ASTAlias, FirstNonEmptyLocation({@1, @2}), @2, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_as_alias_with_required_as:
    "AS" identifier
      {
        $$ = MAKE_NODE(ASTAlias, @$, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_as_or_into_alias:
    "AS" identifier
      {
        $$ = MAKE_NODE(ASTAlias, @$, {$2});
      }
    | "INTO" identifier
      {
        $$ = MAKE_NODE(ASTIntoAlias, @$, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_as:
    KW_AS
    | /* Nothing */
    ;

// Returns true for "NATURAL", false for not-natural.
opt_natural:
    "NATURAL" { $$ = true; }
    | /* Nothing */ { $$ = false; }
    ;

opt_outer: "OUTER" | /* Nothing */ ;

int_literal_or_parameter:
    integer_literal
    | parameter_expression
    | system_variable_expression;

cast_int_literal_or_parameter:
    "CAST" "(" int_literal_or_parameter "AS" type ")"
      {
        $$ = MAKE_NODE(ASTCastExpression, @$, {$3, $5});
      }
    ;

// TODO: If we update the literal productions to include
// CASTed literals, then we should update this.
possibly_cast_int_literal_or_parameter:
    cast_int_literal_or_parameter
    | int_literal_or_parameter
    ;

repeatable_clause:
    "REPEATABLE" "(" possibly_cast_int_literal_or_parameter ")"
      {
        $$ = MAKE_NODE(ASTRepeatableClause, @$, {$3});
      }
    ;

sample_size_value:
    possibly_cast_int_literal_or_parameter
    | floating_point_literal
    ;

// Returns the TABLESAMPLE size unit as expected by ASTSampleClause::set_unit().
sample_size_unit:
    "ROWS" { $$ = zetasql::ASTSampleSize::ROWS; }
    | "PERCENT" { $$ = zetasql::ASTSampleSize::PERCENT; }
    ;

sample_size:
    sample_size_value sample_size_unit opt_partition_by_clause_no_hint
      {
        auto* sample_size = MAKE_NODE(ASTSampleSize, @$, {$1, $3});
        sample_size->set_unit($2);
        $$ = sample_size;
      }
    ;

opt_repeatable_clause:
    repeatable_clause
    | /* Nothing */ { $$ = nullptr; }
    ;

// It doesn't appear to be possible to consolidate the rules without introducing
// a shift/reduce or a reduce/reduce conflict related to REPEATABLE.
opt_sample_clause_suffix:
    repeatable_clause
      {
        $$ = MAKE_NODE(ASTSampleSuffix, @$, {nullptr, $1});
      }
    | "WITH" "WEIGHT" opt_repeatable_clause
      {
        auto* with_weight = MAKE_NODE(ASTWithWeight, @$, {});
        $$ = MAKE_NODE(ASTSampleSuffix, @$, {with_weight, $3});
      }
    | "WITH" "WEIGHT" identifier opt_repeatable_clause
      {
        auto* alias = MAKE_NODE(ASTAlias, @3, {$3});
        auto* with_weight = MAKE_NODE(ASTWithWeight, @$, {alias});
        $$ = MAKE_NODE(ASTSampleSuffix, @$, {with_weight, $4});
      }
    | "WITH" "WEIGHT" "AS" identifier opt_repeatable_clause
      {
        auto* alias = MAKE_NODE(ASTAlias, @3, @4, {$4});
        auto* with_weight = MAKE_NODE(ASTWithWeight, @$, {alias});
        $$ = MAKE_NODE(ASTSampleSuffix, @$, {with_weight, $5});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

sample_clause:
    "TABLESAMPLE" identifier "(" sample_size ")" opt_sample_clause_suffix
      {
        $$ = MAKE_NODE(ASTSampleClause, @$, {$2, $4, $6});
      }
    ;

opt_sample_clause:
    sample_clause
    | /* Nothing */ { $$ = nullptr; }
    ;

table_subquery:
    "(" query ")" opt_as_alias opt_sample_clause
      {
        zetasql::ASTQuery* query = $2;
        query->set_is_nested(true);
        $$ = MAKE_NODE(ASTTableSubquery, @$, {$2, $4, $5});
      }
    ;

table_clause:
    "TABLE" tvf
      {
        $$ = MAKE_NODE(ASTTableClause, @$, {$2});
      }
    | "TABLE" path_expression
      {
        $$ = MAKE_NODE(ASTTableClause, @$, {$2});
      }
    ;

model_clause:
    "MODEL" path_expression
      {
        $$ = MAKE_NODE(ASTModelClause, @$, {$2});
      }
    ;

connection_clause:
    "CONNECTION" path_expression
      {
        $$ = MAKE_NODE(ASTConnectionClause, @$, {$2});
      }
    ;

descriptor_column:
    identifier
      {
        $$ = MAKE_NODE(ASTDescriptorColumn, @$, {$1, nullptr});
      }
    ;

descriptor_column_list:
    descriptor_column
      {
        $$ = MAKE_NODE(ASTDescriptorColumnList, @$, {$1});
      }
    | descriptor_column_list "," descriptor_column
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

descriptor_argument:
    "DESCRIPTOR" "(" descriptor_column_list ")"
      {
        $$ = MAKE_NODE(ASTDescriptor, @$, {$3});
      }
    ;

tvf_argument:
    expression
      {
        $$ = MAKE_NODE(ASTTVFArgument, @$, {$1});
      }
    | descriptor_argument
      {
        $$ = MAKE_NODE(ASTTVFArgument, @$, {$1});
      }
    | table_clause
      {
        $$ = MAKE_NODE(ASTTVFArgument, @$, {$1});
      }
    | model_clause
      {
        $$ = MAKE_NODE(ASTTVFArgument, @$, {$1});
      }
    | connection_clause
      {
        $$ = MAKE_NODE(ASTTVFArgument, @$, {$1});
      }
    | named_argument
      {
        $$ = MAKE_NODE(ASTTVFArgument, @$, {$1});
      }
    | "(" table_clause ")"
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Table arguments for table-valued function "
            "calls written as \"TABLE path\" must not be enclosed in "
            "parentheses. To fix this, replace (TABLE path) with TABLE path");
      }
    | "(" model_clause ")"
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Model arguments for table-valued function "
            "calls written as \"MODEL path\" must not be enclosed in "
            "parentheses. To fix this, replace (MODEL path) with MODEL path");
      }
    | "(" connection_clause ")"
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Connection arguments for table-valued function "
            "calls written as \"CONNECTION path\" must not be enclosed in "
            "parentheses. To fix this, replace (CONNECTION path) with "
            "CONNECTION path");
      }
    | "(" named_argument ")"
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Named arguments for table-valued function "
            "calls written as \"name => value\" must not be enclosed in "
            "parentheses. To fix this, replace (name => value) with "
            "name => value");
      }
    | "SELECT"
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Each subquery argument for table-valued function "
            "calls must be enclosed in parentheses. To fix this, replace "
            "SELECT... with (SELECT...)");
      }
    | "WITH"
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Each subquery argument for table-valued function "
            "calls must be enclosed in parentheses. To fix this, replace "
            "WITH... with (WITH...)");
      }
    ;

tvf_prefix_no_args:
    path_expression "("
      {
        $$ = MAKE_NODE(ASTTVF, @$, {$1});
      }
    | "IF" "("
      {
        auto* identifier = parser->MakeIdentifier(@1, parser->GetInputText(@1));
        auto* path_expression = MAKE_NODE(ASTPathExpression, @1, {identifier});
        $$ = MAKE_NODE(ASTTVF, @$, {path_expression});
      }
    ;

tvf_prefix:
    tvf_prefix_no_args tvf_argument
      {
        $$ = WithExtraChildren($1, {$2});
      }
    | tvf_prefix "," tvf_argument
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

tvf:
    tvf_prefix_no_args ")" opt_hint opt_as_alias opt_sample_clause
      {
        $$ = WithExtraChildren(WithEndLocation($1, @$), {$3, $4, $5});
      }
    | tvf_prefix ")" opt_hint opt_as_alias opt_sample_clause
      {
        $$ = WithExtraChildren(WithEndLocation($1, @$), {$3, $4, $5});
      }
    ;

opt_with_offset_or_sample_clause:
    "WITH" "OFFSET" opt_as_alias
      {
        $$ = MAKE_NODE(ASTWithOffset, @$, {$3});
      }
    | sample_clause
    | /* Nothing */ { $$ = nullptr; }
    ;

table_path_expression_base:
    unnest_expression
    | maybe_dashed_path_expression { $$ = $1; }
    | path_expression "["
      {
        YYERROR_AND_ABORT_AT(
            @2,
            "Syntax error: Array element access is not allowed in the FROM "
            "clause without UNNEST; Use UNNEST(<expression>)");
      }
    | path_expression "." "("
      {
        YYERROR_AND_ABORT_AT(
            @3,
            "Syntax error: Generalized field access is not allowed in the FROM "
            "clause without UNNEST; Use UNNEST(<expression>)");
      }
    | unnest_expression "["
      {
        YYERROR_AND_ABORT_AT(
            @2,
            "Syntax error: Array element access is not allowed in the FROM "
            "clause without UNNEST; Use UNNEST(<expression>)");
      }
    | unnest_expression "." "("
      {
        YYERROR_AND_ABORT_AT(
            @3,
            "Syntax error: Generalized field access is not allowed in the FROM "
            "clause without UNNEST; Use UNNEST(<expression>)");
      }
    ;

table_path_expression:
    table_path_expression_base opt_hint opt_as_alias opt_at_system_time
    opt_with_offset_or_sample_clause
      {
        $$ = MAKE_NODE(ASTTablePathExpression, @$, {$1, $2, $3, $4, $5});
      }
    ;

table_primary:
    tvf
    | table_path_expression
    | "(" join ")" opt_sample_clause
      {
        zetasql::parser::ErrorInfo error_info;
        auto node = zetasql::parser::TransformJoinExpression(
          $2, parser, &error_info);
        if (node == nullptr) {
          YYERROR_AND_ABORT_AT(error_info.location, error_info.message);
        }

        $$ = MAKE_NODE(ASTParenthesizedJoin, @$, {node, $4});
      }
    | table_subquery
    ;

opt_at_system_time:
    "FOR" "SYSTEM" "TIME" "AS" "OF" expression
      {
        $$ = MAKE_NODE(ASTForSystemTime, @$, {$6})
      }
    | "FOR" "SYSTEM_TIME" "AS" "OF" expression
      {
        $$ = MAKE_NODE(ASTForSystemTime, @$, {$5})
      }

    | /* Nothing */ { $$ = nullptr; }
    ;

on_clause:
    "ON" expression
      {
        $$ = MAKE_NODE(ASTOnClause, @$, {$2});
      }
    ;

using_clause_prefix:
    "USING" "(" identifier
      {
        $$ = MAKE_NODE(ASTUsingClause, @$, {$3});
      }
    | using_clause_prefix "," identifier
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

using_clause:
    using_clause_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

opt_on_or_using_clause_list:
    on_or_using_clause_list
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

on_or_using_clause_list:
    on_or_using_clause
      {
        $$ = MAKE_NODE(ASTOnOrUsingClauseList, @$, {$1});
      }
    | on_or_using_clause_list on_or_using_clause
      {
        if (parser->language_options() != nullptr &&
            parser->language_options()->LanguageFeatureEnabled(
               zetasql::FEATURE_V_1_3_ALLOW_CONSECUTIVE_ON)) {
          $$ = WithEndLocation(WithExtraChildren($1, {$2}), @$);
        } else {
          YYERROR_AND_ABORT_AT(
              @2,
              absl::StrCat(
                  "Syntax error: Expected end of input but got keyword ",
                  ($2->node_kind() == zetasql::AST_ON_CLAUSE
                       ? "ON" : "USING")));
        }
      }
    ;

on_or_using_clause:
    on_clause
    | using_clause

// Returns the join type id. Returns 0 to indicate "just a join".
join_type:
    "CROSS" { $$ = zetasql::ASTJoin::CROSS; }
    | "FULL" opt_outer { $$ = zetasql::ASTJoin::FULL; }
    | "INNER" { $$ = zetasql::ASTJoin::INNER; }
    | "LEFT" opt_outer { $$ = zetasql::ASTJoin::LEFT; }
    | "RIGHT" opt_outer { $$ = zetasql::ASTJoin::RIGHT; }
    | /* Nothing */  { $$ = zetasql::ASTJoin::DEFAULT_JOIN_TYPE; }
    ;

// Return the join hint token as expected by ASTJoin::set_join_hint().
join_hint:
    "HASH" { $$ = zetasql::ASTJoin::HASH; }
    | "LOOKUP" { $$ = zetasql::ASTJoin::LOOKUP; }
    | /* Nothing */ { $$ = zetasql::ASTJoin::NO_JOIN_HINT; }
    ;

join_input: join | table_primary ;

// This is only used for parenthesized joins. Unparenthesized joins in the FROM
// clause are directly covered in from_clause_contents. These rules are separate
// because the FROM clause also allows comma joins, while parenthesized joins do
// not.
// Note that if there are consecutive ON/USING clauses, then this ASTJoin tree
// must be processed by TransformJoinExpression in the rule table_primary before
// the final AST is returned.
join:
    join_input opt_natural join_type join_hint "JOIN" opt_hint table_primary
    opt_on_or_using_clause_list
      {
        zetasql::parser::ErrorInfo error_info;
        auto node = zetasql::parser::JoinRuleAction(
            FirstNonEmptyLocation({@2, @3, @4, @5}), @$,
            $1, $2, $3, $4, $6, $7, $8, parser, &error_info);
        if (node == nullptr) {
          YYERROR_AND_ABORT_AT(error_info.location, error_info.message);
        }

        $$ = node;
      }
    ;

from_clause_contents:
    table_primary
    | from_clause_contents "," table_primary
      {
        zetasql::parser::ErrorInfo error_info;
        auto node = zetasql::parser::CommaJoinRuleAction(
            @2, @3, $1, $3, parser, &error_info);
        if (node == nullptr) {
          YYERROR_AND_ABORT_AT(error_info.location, error_info.message);
        }

        $$ = node;
      }
    | from_clause_contents opt_natural join_type join_hint "JOIN" opt_hint
      table_primary opt_on_or_using_clause_list
      {
        // Give an error if we have a RIGHT or FULL JOIN following a comma
        // join since our left-to-right binding would violate the standard.
        // See (broken link).
        if (($3 == zetasql::ASTJoin::FULL ||
             $3 == zetasql::ASTJoin::RIGHT) &&
            $1->node_kind() == zetasql::AST_JOIN) {
          const auto* join_input = $1->GetAsOrDie<zetasql::ASTJoin>();
          while (true) {
            if (join_input->join_type() == zetasql::ASTJoin::COMMA) {
              YYERROR_AND_ABORT_AT(
                  @3,
                  absl::StrCat("Syntax error: ",
                               ($3 == zetasql::ASTJoin::FULL
                                    ? "FULL" : "RIGHT"),
                               " JOIN must be parenthesized when following a "
                               "comma join.  Also, if the preceding comma join "
                               "is a correlated CROSS JOIN that unnests an "
                               "array, then CROSS JOIN syntax must be used in "
                               "place of the comma join"));
            }
            if (join_input->child(0)->node_kind() == zetasql::AST_JOIN) {
              // Look deeper only if the left input is an unparenthesized join.
              join_input =
                  join_input->child(0)->GetAsOrDie<zetasql::ASTJoin>();
            } else {
              break;
            }
          }
        }

        zetasql::parser::ErrorInfo error_info;
        auto node = zetasql::parser::JoinRuleAction(
            FirstNonEmptyLocation({@2, @3, @4, @5}), @$,
            $1, $2, $3, $4, $6, $7, $8,
            parser, &error_info);
        if (node == nullptr) {
          YYERROR_AND_ABORT_AT(error_info.location, error_info.message);
        }

        $$ = node;
      }
    | "@"
      {
        YYERROR_AND_ABORT_AT(
            @1, "Query parameters cannot be used in place of table names");
      }
    | "?"
      {
        YYERROR_AND_ABORT_AT(
            @1, "Query parameters cannot be used in place of table names");
      }
    | KW_DOUBLE_AT
      {
        YYERROR_AND_ABORT_AT(
            @1, "System variables cannot be used in place of table names");
      }
    ;

opt_from_clause:
    "FROM" from_clause_contents
      {
        zetasql::parser::ErrorInfo error_info;
        auto node = zetasql::parser::TransformJoinExpression(
          $2, parser, &error_info);
        if (node == nullptr) {
          YYERROR_AND_ABORT_AT(error_info.location, error_info.message);
        }

        $$ = MAKE_NODE(ASTFromClause, @$, {node});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_where_clause:
    "WHERE" expression
      {
        $$ = MAKE_NODE(ASTWhereClause, @$, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

rollup_list:
    "ROLLUP" "(" expression
      {
        $$ = MAKE_NODE(ASTRollup, @$, {$3});
      }
    | rollup_list "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

grouping_item:
    expression
      {
        $$ = MAKE_NODE(ASTGroupingItem, @$, {$1});
      }
    | rollup_list ")"
      {
        $1 = WithEndLocation($1, @$);
        $$ = MAKE_NODE(ASTGroupingItem, @$, {$1});
      }
    ;

group_by_clause_prefix:
    "GROUP" opt_hint "BY" grouping_item
      {
        $$ = MAKE_NODE(ASTGroupBy, @$, {$2, $4});
      }
    | group_by_clause_prefix "," grouping_item
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

opt_group_by_clause:
    group_by_clause_prefix
      {
        $$ = WithEndLocation($1, @$);
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_having_clause:
    "HAVING" expression
      {
        $$ = MAKE_NODE(ASTHaving, @$, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

window_definition:
    identifier "AS" window_specification
      {
        $$ = MAKE_NODE(ASTWindowDefinition, @$, {$1, $3});
      }
    ;

window_clause_prefix:
    "WINDOW" window_definition
      {
        $$ = MAKE_NODE(ASTWindowClause, @$, {$2});
      }
    | window_clause_prefix "," window_definition
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

opt_window_clause:
    window_clause_prefix
      {
        $$ = WithEndLocation($1, @$);
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_limit_offset_clause:
    "LIMIT" possibly_cast_int_literal_or_parameter
    "OFFSET" possibly_cast_int_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTLimitOffset, @$, {$2, $4});
      }
    | "LIMIT" possibly_cast_int_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTLimitOffset, @$, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_having_modifier:
    "HAVING" "MAX" expression
      {
        auto* modifier = MAKE_NODE(ASTHavingModifier, @$, {$3});
        modifier->set_modifier_kind(
            zetasql::ASTHavingModifier::ModifierKind::MAX);
        $$ = modifier;
      }
    | "HAVING" "MIN" expression
      {
        auto* modifier = MAKE_NODE(ASTHavingModifier, @$, {$3});
        modifier->set_modifier_kind(
            zetasql::ASTHavingModifier::ModifierKind::MIN);
        $$ = modifier;
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

opt_null_handling_modifier:
    "IGNORE" "NULLS"
      {
        $$ = zetasql::ASTFunctionCall::IGNORE_NULLS;
      }
    | "RESPECT" "NULLS"
      {
        $$ = zetasql::ASTFunctionCall::RESPECT_NULLS;
      }
    | /* Nothing */
      {
        $$ = zetasql::ASTFunctionCall::DEFAULT_NULL_HANDLING;
      }
    ;

with_clause_entry:
    identifier "AS" "(" query ")"
      {
        $$ = MAKE_NODE(ASTWithClauseEntry, @$, {$1, $4});
      }
    ;

with_clause:
    "WITH" with_clause_entry
      {
        $$ = MAKE_NODE(ASTWithClause, @$, {$2});
        $$ = WithEndLocation($$, @$);
      }
    | "WITH" "RECURSIVE" with_clause_entry
      {
        zetasql::ASTWithClause* with_clause =
            MAKE_NODE(ASTWithClause, @$, {$3})
        with_clause = WithEndLocation(with_clause, @$);
        with_clause->set_recursive(true);
        $$ = with_clause;
      }
    | with_clause "," with_clause_entry
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

opt_with_connection_clause:
    "WITH" connection_clause
      {
        $$ = MAKE_NODE(ASTWithConnectionClause, @$, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

with_clause_with_trailing_comma:
    with_clause ","
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

// Returns true for DESC, false for ASC (which is the default).
opt_asc_or_desc:
    "ASC" { $$ = false; }
    | "DESC" { $$ = true; }
    | /* Nothing */ { $$ = false; }
    ;

opt_null_order:
    "NULLS" "FIRST"
      {
        auto* null_order = MAKE_NODE(ASTNullOrder, @$, {});
        null_order->set_nulls_first(true);
        $$ = null_order;
      }
    | "NULLS" "LAST"
      {
        auto* null_order = MAKE_NODE(ASTNullOrder, @$, {});
        null_order->set_nulls_first(false);
        $$ = null_order;
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

string_literal_or_parameter:
    string_literal
    | parameter_expression
    | system_variable_expression;

opt_collate_clause:
    "COLLATE" string_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTCollate, @$, {$2});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

ordering_expression:
    expression opt_collate_clause opt_asc_or_desc opt_null_order
      {
        auto* ordering_expr =
            MAKE_NODE(ASTOrderingExpression, @$, {$1, $2, $4});
        ordering_expr->set_descending($3);
        $$ = ordering_expr;
      }
    ;

order_by_clause_prefix:
    "ORDER" opt_hint "BY" ordering_expression
      {
        $$ = MAKE_NODE(ASTOrderBy, @$, {$2, $4});
      }
    | order_by_clause_prefix "," ordering_expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

opt_order_by_clause:
    order_by_clause_prefix
      {
        $$ = WithEndLocation($1, @$);
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

// This rule contains a hack to work around the fact that (query) and
// (expression) are ambiguous. The reason they are ambiguous is that
// all ZetaSQL queries can be parenthesized. So you can write something like
// IN ((SELECT 1)), which could be an IN subquery that happens to be
// parenthesized, or it could be a subquery expression.
// See also http://b/29441587#comment3.
parenthesized_in_rhs:
    bare_expression_subquery
      {
        zetasql::ASTExpressionSubquery* subquery = $1;
        zetasql::ASTQuery* query =
            subquery->GetMutableQueryChildInternal();
        if (query == nullptr) {
          YYERROR_AND_ABORT_AT(
              @1,
              "Internal error: expected query child of subquery");
        }
        $$ = query;
      }
    | "(" expression ")"
      {
        if ($2->node_kind() == zetasql::AST_EXPRESSION_SUBQUERY) {
          auto* subquery = $2->GetAsOrDie<zetasql::ASTExpressionSubquery>();
          if (subquery->modifier() ==
                  zetasql::ASTExpressionSubquery::Modifier::NONE) {
            // To match the JavaCC parser, we prefer interpretating IN ((query))
            // as IN (query) with a parenthesized query, not a value IN list
            // containing a scalar expression query.
            // Return the contained ASTQuery, wrapped in another ASTQuery to
            // replace the parentheses.
            zetasql::ASTQuery* query =
                subquery->GetMutableQueryChildInternal();
            if (query == nullptr) {
              YYERROR_AND_ABORT_AT(
                  @2,
                  "Internal error: expected query child of parenthesized"
                  " subquery");
            }
            query->set_parenthesized(true);
            $$ = MAKE_NODE(ASTQuery, @2, {query});
          } else {
            // The expression subquery is an EXISTS or ARRAY subquery, which
            // is a scalar expression and is not interpreted as a Query.  Treat
            // this as an InList with a single element.
            // Don't include the parentheses in the location, to match the
            // JavaCC parser.
            $$ = MAKE_NODE(ASTInList, @2, {$2});
          }
        } else {
          // Don't include the parentheses in the location, to match the JavaCC
          // parser.
          $$ = MAKE_NODE(ASTInList, @2, {$2});
        }
      }
    | in_list_two_or_more_prefix ")"
      {
        // Don't include the ")" in the location, to match the JavaCC parser.
        // TODO: Fix that.
        $$ = WithEndLocation($1, @1);
      }
    ;

in_list_two_or_more_prefix:
    "(" expression "," expression
      {
        // The JavaCC parser doesn't include the opening "(" in the location
        // for some reason. TODO: Correct this after JavaCC is gone.
        $$ = MAKE_NODE(ASTInList, @2, @4, {$2, $4});
      }
    | in_list_two_or_more_prefix "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

unnest_expression:
    "UNNEST" "(" expression ")"
      {
        $$ = MAKE_NODE(ASTUnnestExpression, @$, {$3});
      }
    | "UNNEST" "(" "SELECT"
      {
        YYERROR_AND_ABORT_AT(
        @3,
        "The argument to UNNEST is an expression, not a query; to use a query "
        "as an expression, the query must be wrapped with additional "
        "parentheses to make it a scalar subquery expression");
      }
    ;

unnest_expression_with_opt_alias_and_offset:
    unnest_expression opt_as_alias opt_with_offset_and_alias
      {
        $$ = MAKE_NODE(ASTUnnestExpressionWithOptAliasAndOffset, @$,
                       {$1, $2, $3});
      }
    ;

// This rule returns the JavaCC operator id for the operator.
comparative_operator:
    "=" { $$ = zetasql::ASTBinaryExpression::EQ; }
    | "!=" { $$ = zetasql::ASTBinaryExpression::NE; }
    | "<>" { $$ = zetasql::ASTBinaryExpression::NE2; }
    | "<" { $$ = zetasql::ASTBinaryExpression::LT; }
    | "<=" { $$ = zetasql::ASTBinaryExpression::LE; }
    | ">" { $$ = zetasql::ASTBinaryExpression::GT; }
    | ">=" { $$ = zetasql::ASTBinaryExpression::GE; };

additive_operator:
    "+" { $$ = zetasql::ASTBinaryExpression::PLUS; }
    | "-" { $$ = zetasql::ASTBinaryExpression::MINUS; }
    ;

multiplicative_operator:
    "*" { $$ = zetasql::ASTBinaryExpression::MULTIPLY; }
    | "/" { $$ = zetasql::ASTBinaryExpression::DIVIDE; }
    ;

// Returns ShiftOperator to indicate the operator type.
shift_operator:
    "<<" { $$ = ShiftOperator::kLeft; }
    | ">>" { $$ = ShiftOperator::kRight; }
    ;

// Returns ImportType to indicate the import object type.
import_type:
    "MODULE" { $$ = ImportType::kModule; }
    | "PROTO" { $$ = ImportType::kProto; }
    ;

// Returns NotKeywordPresence to indicate whether NOT was present.
like_operator:
    "LIKE" { $$ = NotKeywordPresence::kAbsent; } %prec "LIKE"
    | "NOT for BETWEEN/IN/LIKE" "LIKE"
      {
        @$ = @2;  // Error messages should point at the "LIKE".
        $$ = NotKeywordPresence::kPresent;
      } %prec "LIKE"
    ;

// Returns NotKeywordPresence to indicate whether NOT was present.
between_operator:
    "BETWEEN"
      {
        $$ = NotKeywordPresence::kAbsent;
      } %prec "BETWEEN"
    | "NOT for BETWEEN/IN/LIKE" "BETWEEN"
      {
        @$ = @2;  // Error messages should point at the "BETWEEN".
        $$ = NotKeywordPresence::kPresent;
      } %prec "BETWEEN"
    ;

// Returns NotKeywordPresence to indicate whether NOT was present.
in_operator:
    "IN" { $$ = NotKeywordPresence::kAbsent; } %prec "IN"
    | "NOT for BETWEEN/IN/LIKE" "IN"
      {
        @$ = @2;  // Error messages should point at the "IN".
        $$ = NotKeywordPresence::kPresent;
      } %prec "IN"
    ;

// Returns NotKeywordPresence to indicate whether NOT was present.
is_operator:
    "IS" { $$ = NotKeywordPresence::kAbsent; } %prec "IS"
    | "IS" "NOT" { $$ = NotKeywordPresence::kPresent; } %prec "IS"
    ;

unary_operator:
    "+"
      {
        $$ = zetasql::ASTUnaryExpression::PLUS;
      } %prec UNARY_PRECEDENCE
    | "-"
      {
        $$ = zetasql::ASTUnaryExpression::MINUS;
      } %prec UNARY_PRECEDENCE
    | "~"
      {
        $$ = zetasql::ASTUnaryExpression::BITWISE_NOT;
      } %prec UNARY_PRECEDENCE
    ;

// TODO: Consider inlining the most common of these productions.
// Inlining saves significant resources while parsing.
expression:
    null_literal
    | boolean_literal
    | string_literal
    | bytes_literal
    | integer_literal
    | numeric_literal
    | bignumeric_literal
    | floating_point_literal
    | date_or_time_literal
    | parameter_expression
    | system_variable_expression
    | array_constructor
    | new_constructor
    | case_expression
    | cast_expression
    | extract_expression
    | replace_fields_expression
    | function_call_expression { $$ = $1; }
    | analytic_function_call_expression
    | identifier
      {
        // The path expression is extended by the "." identifier rule below.
        $$ = MAKE_NODE(ASTPathExpression, @$, {$1});

        // This could be a bare reference to a CURRENT_* date/time function.
        // Those functions can be called without arguments, but they should
        // still be parsed as function calls. We only parse them as such when
        // the identifiers are not backquoted, i.e., when they are used as
        // keywords. The backquoted versions are treated like regular
        // identifiers.
        // GetInputText() returns the backquotes if they are in the input.
        absl::string_view raw_input = parser->GetInputText(@1);
        // Quick check to filter out certain non-matches.
        if (zetasql_base::CaseEqual(raw_input.substr(0, 8), "current_")) {
          absl::string_view remainder = raw_input.substr(8);
          if (zetasql_base::CaseEqual(remainder, "time") ||
              zetasql_base::CaseEqual(remainder, "date") ||
              zetasql_base::CaseEqual(remainder, "datetime") ||
              zetasql_base::CaseEqual(remainder, "timestamp")) {
            auto* function_call = MAKE_NODE(ASTFunctionCall, @$, {$$});
            function_call->set_is_current_date_time_without_parentheses(true);
            $$ = function_call;
          }
        }
      }
    | parenthesized_expression
    | struct_constructor
    | expression_subquery
    | expression "[" expression "]" %prec PRIMARY_PRECEDENCE
      {
        $$ = MAKE_NODE(ASTArrayElement, @2, @4, {$1, $3});
      }
    | expression "." "(" path_expression ")"  %prec PRIMARY_PRECEDENCE
      {
        $$ = MAKE_NODE(ASTDotGeneralizedField, @2, @5, {$1, $4});
      }
    | expression "." identifier %prec PRIMARY_PRECEDENCE
      {
        // Note that if "expression" ends with an identifier, then the tokenizer
        // switches to IDENTIFIER_DOT mode before tokenizing $3. That means that
        // "identifier" here allows any non-reserved keyword to be used as an
        // identifier, as well as "identifiers" that start with a digit.

        // We try to build path expressions as long as identifiers are added.
        // As soon as a dotted path contains anything else, we use generalized
        // DotIdentifier.
        if ($1->node_kind() == zetasql::AST_PATH_EXPRESSION &&
            !$1->parenthesized()) {
          $$ = WithExtraChildren(WithEndLocation($1, @3), {$3});
        } else {
          $$ = MAKE_NODE(ASTDotIdentifier, @2, @3, {$1, $3});
        }
      }
    | expression "OR" expression %prec "OR"
      {
        if ($1->node_kind() == zetasql::AST_OR_EXPR &&
            !$1->parenthesized()) {
          // Embrace and extend $1's ASTNode.
          $$ = WithExtraChildren(WithEndLocation($1, @3), {$3});
        } else {
          $$ = MAKE_NODE(ASTOrExpr, @$, {$1, $3});
        }
      }
    | expression "AND" expression %prec "AND"
      {
        if ($1->node_kind() == zetasql::AST_AND_EXPR &&
            !$1->parenthesized()) {
          // Embrace and extend $1's ASTNode to flatten a series of ANDs.
          $$ = WithExtraChildren(WithEndLocation($1, @3), {$3});
        } else {
          $$ = MAKE_NODE(ASTAndExpr, @$, {$1, $3});
        }
      }
    | "NOT" expression %prec UNARY_NOT_PRECEDENCE
      {
        auto* not_expr = MAKE_NODE(ASTUnaryExpression, @$, {$2});
        not_expr->set_op(zetasql::ASTUnaryExpression::NOT);
        $$ = not_expr;
      }
    | expression like_operator expression %prec "LIKE"
        {
          // NOT has lower precedence but can be parsed unparenthesized in the
          // rhs because it is not ambiguous. This is not allowed.
          if (IsUnparenthesizedNotExpression($3)) {
            YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
          }
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(
                @2,
                "Syntax error: "
                "Expression to the left of LIKE must be parenthesized");
          }
          auto* binary_expression =
              MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
          binary_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          binary_expression->set_op(zetasql::ASTBinaryExpression::LIKE);
          $$ = binary_expression;
        }
    | expression in_operator opt_hint unnest_expression %prec "IN"
        {
          if ($3) {
            YYERROR_AND_ABORT_AT(@3,
                                 "Syntax error: HINTs cannot be specified on "
                                 "IN clause with UNNEST");
          }
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                 "Syntax error: Expression to the left of IN "
                                 "must be parenthesized");
          }
          auto* in_expression = MAKE_NODE(ASTInExpression, @2, @4, {$1, $4});
          in_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = in_expression;
        }
    | expression in_operator opt_hint parenthesized_in_rhs %prec "IN"
        {
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                "Syntax error: Expression to the left of IN "
                                "must be parenthesized");
          }
          zetasql::ASTInExpression* in_expression = nullptr;
          if ($4->node_kind() == zetasql::AST_QUERY)
          {
            in_expression = MAKE_NODE(ASTInExpression, @2, @4, {$1, $3, $4});
          }
          else
          {
            if($3)
            {
              YYERROR_AND_ABORT_AT(@3,
                                  "Syntax error: HINTs cannot be specified on "
                                  "IN clause with value list");
            }
            in_expression = MAKE_NODE(ASTInExpression, @2, @4, {$1, $4});
          }
          in_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = in_expression;
        }
    | expression between_operator
      expression "AND for BETWEEN" expression %prec "BETWEEN"
        {
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                 "Syntax error: Expression to the left of "
                                 "BETWEEN must be parenthesized");
          }
          // Test the middle operand for unparenthesized operators with lower
          // or equal precedence. These cases are unambiguous w.r.t. the
          // operator precedence parsing, but they are disallowed by the SQL
          // standard because it interprets precedence strictly, i.e., it allows
          // no nesting of operators with lower precedence even if it is
          // unambiguous.
          if (!$3->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@3,
                                 "Syntax error: Expression in BETWEEN must be "
                                 "parenthesized");
          }
          // NOT has lower precedence but can be parsed unparenthesized in the
          // rhs because it is not ambiguous. This is not allowed.
          if (IsUnparenthesizedNotExpression($5)) {
            YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
          }
          auto* between_expression =
              MAKE_NODE(ASTBetweenExpression, @2, @5, {$1, $3, $5});
          between_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = between_expression;
        }
    | expression is_operator null_literal %prec "IS"
        {
          // The Bison parser allows comparison expressions in the LHS, even
          // though these operators are at the same precedence level and are not
          // associative. Explicitly forbid this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                 "Syntax error: Expression to the left of IS "
                                 "must be parenthesized");
          }
          auto* binary_expression =
              MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
          binary_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          binary_expression->set_op(zetasql::ASTBinaryExpression::IS);
          $$ = binary_expression;
        }
    | expression is_operator boolean_literal %prec "IS"
        {
          // The Bison parser allows comparison expressions in the LHS, even
          // though these operators are at the same precedence level and are not
          // associative. Explicitly forbid this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                 "Syntax error: Expression to the left of IS "
                                 "must be parenthesized");
          }
          auto* binary_expression =
              MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
          binary_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          binary_expression->set_op(zetasql::ASTBinaryExpression::IS);
          $$ = binary_expression;
        }
    | expression comparative_operator expression %prec "="
        {
          // NOT has lower precedence but can be parsed unparenthesized in the
          // rhs because it is not ambiguous. This is not allowed. We don't have
          // to check for other expressions: other comparison expressions are
          // caught by the Bison grammar because "=" is %nonassoc, and AND and
          // OR will not be parsed as children of this rule because they have
          // lower precedence than "=".
          if (IsUnparenthesizedNotExpression($3)) {
            YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
          }
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                 "Syntax error: Expression to the left of "
                                 "comparison must be parenthesized");
          }
          auto* binary_expression =
              MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
          binary_expression->set_op($2);
          $$ = binary_expression;
        }
    | expression "|" expression
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* binary_expression =
            MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
        binary_expression->set_op(
            zetasql::ASTBinaryExpression::BITWISE_OR);
        $$ = binary_expression;
      }
    | expression "^" expression
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* binary_expression =
            MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
        binary_expression->set_op(
            zetasql::ASTBinaryExpression::BITWISE_XOR);
        $$ = binary_expression;
      }
    | expression "&" expression
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* binary_expression =
            MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
        binary_expression->set_op(
            zetasql::ASTBinaryExpression::BITWISE_AND);
        $$ = binary_expression;
      }
    | expression "||" expression
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. However, this is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* binary_expression =
            MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
        binary_expression->set_op(
            zetasql::ASTBinaryExpression::CONCAT_OP);
        $$ = binary_expression;
      }
    | expression shift_operator expression %prec "<<"
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* binary_expression =
            MAKE_NODE(ASTBitwiseShiftExpression, @2, @3, {$1, $3});
        binary_expression->set_is_left_shift($2 == ShiftOperator::kLeft);
        $$ = binary_expression;
      }
    | expression additive_operator expression %prec "+"
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* binary_expression =
            MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
        binary_expression->set_op($2);
        $$ = binary_expression;
      }
    | expression multiplicative_operator expression %prec "*"
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* binary_expression =
            MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
        binary_expression->set_op($2);
        $$ = binary_expression;
      }
    | unary_operator expression %prec UNARY_PRECEDENCE
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($2)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@2);
        }
        auto* expression =
            MAKE_NODE(ASTUnaryExpression, @$, {$2});
        expression->set_op($1);
        $$ = expression;
      }
    ;

// Note that the tokenizer will be in "DOT_IDENTIFIER" mode for all identifiers
// after the first dot. This allows path expressions like "foo.201601010" or
// "foo.all" to be written without backquoting, and we don't have to worry about
// this in the parser.
path_expression:
    identifier
      {
        $$ = MAKE_NODE(ASTPathExpression, @$, {$1});
      }
    | path_expression "." identifier
      {
        $$ = WithExtraChildren(WithEndLocation($1, @3), {$3});
      }
    ;

dashed_identifier:
    identifier "-" identifier
      {
        // a - b
        if (@1.end.column != @2.begin.column ||
            @2.end.column != @3.begin.column) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        absl::string_view id1 = parser->GetInputText(@1);
        absl::string_view id2 = parser->GetInputText(@3);
        if (id1[0] == '`' || id2[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        auto out = parser->CreateASTNode<DashedIdentifierTmpNode>(@1);
        out->set_path_parts({{id1, id2}});
        $$ = out;
      }
    | dashed_identifier "-" identifier
      {
        // a-b - c
        if (@1.end.column != @2.begin.column ||
            @2.end.column != @3.begin.column) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        DashedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        absl::string_view id2 = parser->GetInputText(@3);
        if (id2[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        // Add an extra sub-part to the ending dashed identifier.
        prev.back().push_back(id2);
        auto out = parser->CreateASTNode<DashedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }
    | identifier "-" INTEGER_LITERAL
      {
        // a - 5
        if (@1.end.column != @2.begin.column ||
            @2.end.column != @3.begin.column) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        absl::string_view id1 = parser->GetInputText(@1);
        absl::string_view id2 = parser->GetInputText(@3);
        if (id1[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        auto out = parser->CreateASTNode<DashedIdentifierTmpNode>(@1);
        out->set_path_parts({{id1, id2}});
        $$ = out;
      }
    | dashed_identifier "-" INTEGER_LITERAL
      {
        // a-b - 5
        if (@1.end.column != @2.begin.column ||
            @2.end.column != @3.begin.column) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        DashedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        absl::string_view id2 = parser->GetInputText(@3);
        prev.back().push_back(id2);
        auto out = parser->CreateASTNode<DashedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }
    | identifier '-' FLOATING_POINT_LITERAL identifier
      {
        // a - 1. b
        if (@1.end.column != @2.begin.column ||
            @2.end.column != @3.begin.column) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        absl::string_view id1 = parser->GetInputText(@1);
        absl::string_view id2 = parser->GetInputText(@3);
        absl::string_view id3 = parser->GetInputText(@4);
        if (id1[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        auto out = parser->CreateASTNode<DashedIdentifierTmpNode>(@1);
        // Here (and below) we need to handle the case where dot is lex'ed as
        // part of floating number as opposed to path delimiter. To parse it
        // correctly, we push the components separately (as string_view).
        // {{"a", "1"}, "b"}
        out->set_path_parts({{id1, id2}, {id3}});
        $$ = out;
      }
    | dashed_identifier '-' FLOATING_POINT_LITERAL identifier
      {
        // a-b - 1. c
        if (@1.end.column != @2.begin.column ||
            @2.end.column != @3.begin.column) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        DashedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        absl::string_view id1 = parser->GetInputText(@3);
        absl::string_view id2 = parser->GetInputText(@4);
        // This case is a continuation of an existing dashed_identifier `prev`,
        // followed by what the lexer believes is a floating point literal.
        // here: /*prev=*/={{"a", "b"}}
        // we append "1" to complete the dashed components, followed
        // by the identifier ("c") as {{"c"}}.
        // Thus, we end up with {{"a", "b", "1"}, {"c"}}
        prev.back().push_back(id1);
        prev.push_back({id2});
        auto out = parser->CreateASTNode<DashedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }

dashed_path_expression:
    dashed_identifier
      {
        // Dashed identifiers are represented by a temporary node, which
        // aggregates the raw components (as string_view backed by the
        // original query). The `dashed_identifier` rule will create raw parts
        // which we then essentially concatenate back together.
        DashedIdentifierTmpNode::PathParts raw_parts = $1->release_path_parts();
        std::vector<zetasql::ASTNode*> parts;
        for (int i = 0; i < raw_parts.size(); ++i) {
          DashedIdentifierTmpNode::DashParts& raw_dash_parts = raw_parts[i];
          if (raw_dash_parts.empty()) {
            YYERROR_AND_ABORT_AT(@1,
                "Internal error: Empty dashed identifier part");
          }
          for (int j =0; j< raw_dash_parts.size(); ++j) {
            absl::string_view &dash_part = raw_dash_parts[j];
            if (absl::EndsWith(dash_part, ".")) {
              dash_part.remove_suffix(1);
            }
          }
          parts.push_back(
              parser->MakeIdentifier(@1, absl::StrJoin(raw_dash_parts, "-")));
        }

        $$ = MAKE_NODE(ASTPathExpression, @$, std::move(parts))
      }
    | dashed_path_expression "." identifier
      {
        $$ = WithExtraChildren(WithEndLocation($1, @3), {$3});
      }
    ;

maybe_dashed_path_expression:
    path_expression { $$ = $1; }
    | dashed_path_expression
      {
        if (parser->language_options() != nullptr &&
            parser->language_options()->LanguageFeatureEnabled(
               zetasql::FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME)) {
          $$ = $1;
        } else {
          YYERROR_AND_ABORT_AT(
              @1,
              absl::StrCat(
                "Syntax error: Table name contains '-' character. "
                "It needs to be quoted: ",
                zetasql::ToIdentifierLiteral(
                  parser->GetInputText(@1), false)));
        }
      }
    ;

array_constructor_prefix_no_expressions:
    "ARRAY" "[" { $$ = MAKE_NODE(ASTArrayConstructor, @$); }
    | "[" { $$ = MAKE_NODE(ASTArrayConstructor, @$); }
    | array_type "["
      {
        $$ = MAKE_NODE(ASTArrayConstructor, @$, {$1});
      }
    ;

array_constructor_prefix:
    array_constructor_prefix_no_expressions expression
      {
        $$ = WithExtraChildren($1, {$2});
      }
    | array_constructor_prefix "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

array_constructor:
    array_constructor_prefix_no_expressions "]"
      {
        $$ = WithEndLocation($1, @$);
      }
    | array_constructor_prefix "]"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

date_or_time_literal_kind:
    "DATE" { $$ = zetasql::TYPE_DATE; }
    | "DATETIME" { $$ = zetasql::TYPE_DATETIME; }
    | "TIME" { $$ = zetasql::TYPE_TIME; }
    | "TIMESTAMP" { $$ = zetasql::TYPE_TIMESTAMP; }
    ;

date_or_time_literal:
    date_or_time_literal_kind string_literal
      {
        auto* literal = MAKE_NODE(ASTDateOrTimeLiteral, @$, {$2});
        literal->set_type_kind($1);
        $$ = literal;
      }
    ;

parameter_expression:
    named_parameter_expression
    | "?"
      {
        auto* parameter_expr = MAKE_NODE(ASTParameterExpr, @$, {});
        // Bison's algorithm guarantees that the "?" productions are reduced in
        // left-to-right order.
        parameter_expr->set_position(
          parser->GetNextPositionalParameterPosition());
        $$ = parameter_expr;
      }
    ;

named_parameter_expression:
    "@" identifier
      {
        $$ = MAKE_NODE(ASTParameterExpr, @$, {$2});
      }
    | "@" reserved_keyword_rule
      {
        zetasql::ASTIdentifier* reserved_keyword_identifier =
            parser->MakeIdentifier(@2, parser->GetInputText(@2));
        $$ = MAKE_NODE(ASTParameterExpr, @$, {reserved_keyword_identifier});
      }
    ;

type_name:
    path_expression
      {
        $$ = MAKE_NODE(ASTSimpleType, @$, {$1});
      }
    ;

array_type:
    "ARRAY" "<" type ">"
      {
        $$ = MAKE_NODE(ASTArrayType, @$, {$3});
      }
    ;

struct_field:
    identifier type
      {
        $$ = MAKE_NODE(ASTStructField, @$, {$1, $2});
      }
    | type
      {
        $$ = MAKE_NODE(ASTStructField, @$, {$1});
      }
    ;

struct_type_prefix:
    "STRUCT" "<" struct_field
      {
        $$ = MAKE_NODE(ASTStructType, @$, {$3});
      }
    | struct_type_prefix "," struct_field
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

struct_type:
    "STRUCT" "<" ">"
      {
        $$ = MAKE_NODE(ASTStructType, @$);
      }
    | struct_type_prefix ">"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

type: array_type | struct_type | type_name ;

templated_parameter_kind:
    "PROTO"
      {
        $$ = zetasql::ASTTemplatedParameterType::ANY_PROTO;
      }
    | "ENUM"
      {
        $$ = zetasql::ASTTemplatedParameterType::ANY_ENUM;
      }
    | "STRUCT"
      {
        $$ = zetasql::ASTTemplatedParameterType::ANY_STRUCT;
      }
    | "ARRAY"
      {
        $$ = zetasql::ASTTemplatedParameterType::ANY_ARRAY;
      }
    | identifier
      {
        const std::string templated_type_string = $1->GetAsString();
        if (zetasql_base::CaseEqual(templated_type_string, "TABLE")) {
          $$ = zetasql::ASTTemplatedParameterType::ANY_TABLE;
        } else if (zetasql_base::CaseEqual(templated_type_string, "TYPE")) {
          $$ = zetasql::ASTTemplatedParameterType::ANY_TYPE;
        } else {
          YYERROR_AND_ABORT_AT(@1,
                               "Syntax error: unexpected ANY template type");
        }
      }
    ;

templated_parameter_type:
    "ANY" templated_parameter_kind
      {
        auto* templated_parameter =
            MAKE_NODE(ASTTemplatedParameterType, @$, {});
        templated_parameter->set_kind($2);
        $$ = templated_parameter;
      }
    ;

type_or_tvf_schema: type | templated_parameter_type | tvf_schema;

new_constructor_prefix_no_arg:
    "NEW" type_name "("
      {
        $$ = MAKE_NODE(ASTNewConstructor, @$, {$2});
      }
    ;

new_constructor_arg:
    expression
      {
        $$ = MAKE_NODE(ASTNewConstructorArg, @$, {$1});
      }
    | expression "AS" identifier
      {
        $$ = MAKE_NODE(ASTNewConstructorArg, @$, {$1, $3});
      }
    | expression "AS" "(" path_expression ")"
      {
        // Do not parenthesize $4 because it is not really a parenthesized
        // path expression. The parentheses are just part of the syntax here.
        $$ = MAKE_NODE(ASTNewConstructorArg, @$, {$1, $4});
      }
    ;

new_constructor_prefix:
    new_constructor_prefix_no_arg new_constructor_arg
      {
        $$ = WithExtraChildren($1, {$2});
      }
    | new_constructor_prefix "," new_constructor_arg
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

new_constructor:
    new_constructor_prefix ")"
      {
        $$ = WithEndLocation($1, @2);
      }
    | new_constructor_prefix_no_arg ")"
      {
        $$ = WithEndLocation($1, @2);
      }
    ;

case_no_value_expression_prefix:
    "CASE" "WHEN" expression "THEN" expression
      {
        $$ = MAKE_NODE(ASTCaseNoValueExpression, @$, {$3, $5});
      }
    | case_no_value_expression_prefix "WHEN" expression "THEN" expression
      {
        $$ = WithExtraChildren($1, {$3, $5});
      }
    ;

case_value_expression_prefix:
    "CASE" expression "WHEN" expression "THEN" expression
      {
        $$ = MAKE_NODE(ASTCaseValueExpression, @$, {$2, $4, $6});
      }
    | case_value_expression_prefix "WHEN" expression "THEN" expression
      {
        $$ = WithExtraChildren($1, {$3, $5});
      }
    ;

case_expression_prefix:
    case_no_value_expression_prefix
    | case_value_expression_prefix
    ;

case_expression:
    case_expression_prefix "END"
      {
        $$ = WithEndLocation($1, @$);
      }
    | case_expression_prefix "ELSE" expression "END"
      {
        $$ = WithExtraChildren(WithEndLocation($1, @$), {$3});
      }
    ;

cast_expression:
    "CAST" "(" expression "AS" type ")"
      {
        auto* cast = MAKE_NODE(ASTCastExpression, @$, {$3, $5});
        cast->set_is_safe_cast(false);
        $$ = cast;
      }
    | "CAST" "(" "SELECT"
      {
        YYERROR_AND_ABORT_AT(
        @3,
        "The argument to CAST is an expression, not a query; to use a query "
        "as an expression, the query must be wrapped with additional "
        "parentheses to make it a scalar subquery expression");
      }
    // This rule causes a shift/reduce conflict with keyword_as_identifier. It
    // is resolved in favor of this rule, which is the desired behavior.
    | "SAFE_CAST" "(" expression "AS" type ")"
      {
        auto* cast = MAKE_NODE(ASTCastExpression, @$, {$3, $5});
        cast->set_is_safe_cast(true);
        $$ = cast;
      }
    | "SAFE_CAST" "(" "SELECT"
      {
        YYERROR_AND_ABORT_AT(
        @3,
        "The argument to SAFE_CAST is an expression, not a query; to use a "
        "query as an expression, the query must be wrapped with additional "
        "parentheses to make it a scalar subquery expression");
      }
    ;

extract_expression_base:
    "EXTRACT" "(" expression "FROM" expression
      {
        $$ = MAKE_NODE(ASTExtractExpression, @$, {$3, $5});
      }
    ;

extract_expression:
    extract_expression_base ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | extract_expression_base "AT" "TIME" "ZONE" expression ")"
      {
        $$ = WithExtraChildren(WithEndLocation($1, @$), {$5});
      }
    ;

replace_fields_arg:
    expression "AS" generalized_path_expression
      {
        $$ = MAKE_NODE(ASTReplaceFieldsArg, @$, {$1, $3});
      }
    | expression "AS" generalized_extension_path
      {
        $$ = MAKE_NODE(ASTReplaceFieldsArg, @$, {$1, $3});
      }
    ;

replace_fields_prefix:
    "REPLACE_FIELDS" "(" expression "," replace_fields_arg
      {
        $$ = MAKE_NODE(ASTReplaceFieldsExpression, @$, {$3, $5});
      }
    | replace_fields_prefix "," replace_fields_arg
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

replace_fields_expression:
    replace_fields_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

function_name_from_keyword:
    "IF"
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    | "GROUPING"
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    | "LEFT"
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    | "RIGHT"
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    ;

// These rules have "expression" as their first part rather than
// "path_expression". This is needed because the expression parser doesn't
// use the "path_expression" rule, and instead builds path expressions by
// starting with an identifier and then using .identifier followup rules to
// extend the path expression. If we were to use "path_expression" here, then
// the parser becomes ambiguous because it can parse the paths in two different
// ways, one using a sequence of expression parsing rules and one using a
// sequence of path_expression parsing rules. Instead, we use "expression" and
// error out if the expression is anything other than a path expression.
//
// One exception is made for CURRENT_DATE/TIMESTAMP/.. expressions, which are
// converted into function calls immediately when they are seen, even without
// parentheses. We allow them as input parameters so that parentheses can still
// be added to them after they are already parsed as function calls.
function_call_expression_base:
    expression "(" "DISTINCT" %prec PRIMARY_PRECEDENCE
      {
        if ($1->node_kind() == zetasql::AST_FUNCTION_CALL) {
          auto* function_call = $1->GetAsOrDie<zetasql::ASTFunctionCall>();
          if (function_call->parenthesized()) {
            YYERROR_AND_ABORT_AT(
                @2,
                "Syntax error: Function call cannot be applied to this "
                "expression. Function calls require a path, e.g. a.b.c()");
          } else if (
              function_call->is_current_date_time_without_parentheses()) {
            // This is a function call like "CURRENT_DATE", which does not
            // allow DISTINCT.
            // Note that we don't call this a "Syntax error" because it's really
            // a semantic error.
            YYERROR_AND_ABORT_AT(
                @3,
                absl::StrCat("DISTINCT not allowed for function ",
                             parser->GetInputText(@1)));
          } else {
            // TODO: Add test for this error.
            YYERROR_AND_ABORT_AT(
                @2,
                "Syntax error: Double function call parentheses");
          }
        } else if (
            $1->node_kind() != zetasql::AST_PATH_EXPRESSION ||
            $1->GetAsOrDie<zetasql::ASTPathExpression>()->parenthesized()) {
          YYERROR_AND_ABORT_AT(
              @2,
              "Syntax error: Function call cannot be applied to this "
              "expression. Function calls require a path, e.g. a.b.c()");
        } else {
          auto* function_call = MAKE_NODE(ASTFunctionCall, @$, {$1});
          function_call->set_distinct(true);
          $$ = function_call;
        }
      }
    | expression "(" %prec PRIMARY_PRECEDENCE
      {
        // TODO: Merge this with the other code path. We have to have
        // two separate productions to avoid an empty opt_distinct rule that
        // causes shift/reduce conflicts.
        if ($1->node_kind() == zetasql::AST_FUNCTION_CALL) {
          auto* function_call = $1->GetAsOrDie<zetasql::ASTFunctionCall>();
          if (function_call->parenthesized()) {
            YYERROR_AND_ABORT_AT(
                @2,
                "Syntax error: Function call cannot be applied to this "
                "expression. Function calls require a path, e.g. a.b.c()");
          } else if (
              function_call->is_current_date_time_without_parentheses()) {
            // This is a function call like "CURRENT_DATE" without parentheses.
            // Allow parentheses to be added to such a call at most once.
            function_call->set_is_current_date_time_without_parentheses(false);
            $$ = function_call;
          } else {
            // TODO: Add test for this error.
            YYERROR_AND_ABORT_AT(
                @2,
                "Syntax error: Double function call parentheses");
          }
        } else if (
            $1->node_kind() != zetasql::AST_PATH_EXPRESSION ||
            $1->GetAsOrDie<zetasql::ASTPathExpression>()->parenthesized()) {
          YYERROR_AND_ABORT_AT(
              @2,
              "Syntax error: Function call cannot be applied to this "
              "expression. Function calls require a path, e.g. a.b.c()");
        } else {
          auto* function_call = MAKE_NODE(ASTFunctionCall, @$, {$1});
          function_call->set_distinct(false);
          $$ = function_call;
        }
      }
    | function_name_from_keyword "(" %prec PRIMARY_PRECEDENCE
      {
        // IF and GROUPING can be function calls, but they are also keywords.
        // Treat them specially, and don't allow DISTINCT etc. since that only
        // applies to aggregate functions.
        auto* path_expression = MAKE_NODE(ASTPathExpression, @1, {$1});
        auto* function_call = MAKE_NODE(ASTFunctionCall, @$, {path_expression});
        function_call->set_distinct(false);
        $$ = function_call;
      }
    ;

function_call_argument:
    expression
    | "INTERVAL" expression identifier
      {
        $$ = MAKE_NODE(ASTIntervalExpr, @$, {$2, $3});
      }
    | "SELECT"
      {
        YYERROR_AND_ABORT_AT(
        @1,
        "Each function argument is an expression, not a query; to use a "
        "query as an expression, the query must be wrapped with additional "
        "parentheses to make it a scalar subquery expression");
      }
    | named_argument
    ;

named_argument:
    identifier KW_NAMED_ARGUMENT_ASSIGNMENT expression
      {
        $$ = MAKE_NODE(ASTNamedArgument, @$, {$1, $3});
      }
    ;

function_call_expression_with_args_prefix:
    function_call_expression_base function_call_argument
      {
        $$ = WithExtraChildren($1, {$2});
      }
    // The first argument may be a "*" instead of an expression. This is valid
    // for COUNT(*), which has no other arguments
    // The analyzer must validate the "*" is not used with other functions.
    | function_call_expression_base "*"
      {
        auto* star = MAKE_NODE(ASTStar, @2);
        star->set_image("*");
        $$ = WithExtraChildren($1, {star});
      }
    | function_call_expression_with_args_prefix "," function_call_argument
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

function_call_expression:
    // Empty argument list.
    function_call_expression_base opt_having_modifier opt_order_by_clause
      opt_limit_offset_clause ")"
      {
        $$ = WithExtraChildren(WithEndLocation($1, @$), {$2, $3, $4});
      }
    // Non-empty argument list.
    // opt_null_handling_modifier only appear here as they require at least
    // one argument.
    | function_call_expression_with_args_prefix opt_null_handling_modifier
      opt_having_modifier
      placeholder
      opt_order_by_clause
      opt_limit_offset_clause ")"
      {
        $1->set_null_handling_modifier($2);
        $$ = WithExtraChildren(WithEndLocation($1, @$), {
            $3,
            $5, $6});
      }
    ;

opt_identifier:
    identifier
    | /* Nothing */ { $$ = nullptr; }
    ;

partition_by_clause_prefix:
    "PARTITION" opt_hint "BY" expression
      {
        $$ = MAKE_NODE(ASTPartitionBy, @$, {$2, $4});
      }
    | partition_by_clause_prefix "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

opt_partition_by_clause:
    partition_by_clause_prefix { $$ = WithEndLocation($1, @$); }
    | /* Nothing */ { $$ = nullptr; }
    ;

partition_by_clause_prefix_no_hint:
    "PARTITION" "BY" expression
      {
        $$ = MAKE_NODE(ASTPartitionBy, @$, {nullptr, $3});
      }
    | partition_by_clause_prefix_no_hint "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

opt_partition_by_clause_no_hint:
    partition_by_clause_prefix_no_hint { $$ = WithEndLocation($1, @$); }
    | /* Nothing */ { $$ = nullptr; }
    ;

cluster_by_clause_prefix_no_hint:
    "CLUSTER" "BY" expression
      {
        $$ = MAKE_NODE(ASTClusterBy, @$, {$3});
      }
    | cluster_by_clause_prefix_no_hint "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

opt_cluster_by_clause_no_hint:
    cluster_by_clause_prefix_no_hint { $$ = WithEndLocation($1, @$); }
    | /* Nothing */ { $$ = nullptr; }
    ;

// Returns PrecedingOrFollowingKeyword to indicate which keyword was present.
preceding_or_following:
    "PRECEDING" { $$ = PrecedingOrFollowingKeyword::kPreceding; }
    | "FOLLOWING" { $$ = PrecedingOrFollowingKeyword::kFollowing; }
    ;

window_frame_bound:
    "UNBOUNDED" preceding_or_following
      {
        auto* frame = MAKE_NODE(ASTWindowFrameExpr, @$);
        frame->set_boundary_type(
            ($2 == PrecedingOrFollowingKeyword::kPreceding)
                ? zetasql::ASTWindowFrameExpr::UNBOUNDED_PRECEDING
                : zetasql::ASTWindowFrameExpr::UNBOUNDED_FOLLOWING);
        $$ = frame;
      }
    | "CURRENT" "ROW"
      {
        auto* frame = MAKE_NODE(ASTWindowFrameExpr, @$);
        frame->set_boundary_type(
            zetasql::ASTWindowFrameExpr::CURRENT_ROW);
        $$ = frame;
      }
    | expression preceding_or_following
      {
        auto* frame = MAKE_NODE(ASTWindowFrameExpr, @$, {$1});
        frame->set_boundary_type(
            ($2 == PrecedingOrFollowingKeyword::kPreceding)
                ? zetasql::ASTWindowFrameExpr::OFFSET_PRECEDING
                : zetasql::ASTWindowFrameExpr::OFFSET_FOLLOWING);
        $$ = frame;
      }
    ;

frame_unit:
    "ROWS" { $$ = zetasql::ASTWindowFrame::ROWS; }
    | "RANGE" { $$ = zetasql::ASTWindowFrame::RANGE; }
    ;

opt_window_frame_clause:
    frame_unit "BETWEEN" window_frame_bound "AND for BETWEEN" window_frame_bound
      {
        auto* frame = MAKE_NODE(ASTWindowFrame, @$, {$3, $5});
        frame->set_unit($1);
        $$ = frame;
      }
    | frame_unit window_frame_bound
      {
        auto* frame = MAKE_NODE(ASTWindowFrame, @$, {$2});
        frame->set_unit($1);
        $$ = frame;
      }
    | /* Nothing */ { $$ = nullptr; }

window_specification:
    identifier
      {
        $$ = MAKE_NODE(ASTWindowSpecification, @$, {$1});
      }
    | "(" opt_identifier opt_partition_by_clause opt_order_by_clause
          opt_window_frame_clause ")"
      {
        $$ = MAKE_NODE(ASTWindowSpecification, @$, {$2, $3, $4, $5});
      }
   ;

analytic_function_call_expression:
    function_call_expression "OVER" window_specification
      {
        $$ = MAKE_NODE(ASTAnalyticFunctionCall, @$, {$1, $3});
      }
    ;

parenthesized_expression:
    "(" expression ")"
      {
        $2->set_parenthesized(true);
        // Don't include the location in the parentheses. Semantic error
        // messages about this expression should point at the start of the
        // expression, not at the opening parentheses.
        $$ = $2;
      }
    ;

struct_constructor_prefix_with_keyword_no_arg:
    struct_type "("
      {
        $$ = MAKE_NODE(ASTStructConstructorWithKeyword, @$, {$1});
      }
    | "STRUCT" "("
      {
        $$ = MAKE_NODE(ASTStructConstructorWithKeyword, @$);
      }
    ;

struct_constructor_prefix_with_keyword:
    struct_constructor_prefix_with_keyword_no_arg struct_constructor_arg
      {
        $$ = WithExtraChildren($1, {$2});
      }
    | struct_constructor_prefix_with_keyword "," struct_constructor_arg
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

struct_constructor_arg:
    expression opt_as_alias_with_required_as
      {
        $$ = MAKE_NODE(ASTStructConstructorArg, @$, {$1, $2});
      }
    ;

struct_constructor_prefix_without_keyword:
    // STRUCTs with no prefix must have at least two expressions, otherwise
    // they're parsed as parenthesized expressions.
    "(" expression "," expression
      {
        $$ = MAKE_NODE(ASTStructConstructorWithParens, @$, {$2, $4});
      }
    | struct_constructor_prefix_without_keyword "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

struct_constructor:
    struct_constructor_prefix_with_keyword ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | struct_constructor_prefix_with_keyword_no_arg ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | struct_constructor_prefix_without_keyword ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

expression_subquery:
    "ARRAY" "(" query ")"
      {
        auto* subquery = MAKE_NODE(ASTExpressionSubquery, @$, {$3});
        subquery->set_modifier(zetasql::ASTExpressionSubquery::ARRAY);
        $$ = subquery;
      }
    | "EXISTS" opt_hint "(" query ")"
      {
        auto* subquery = MAKE_NODE(ASTExpressionSubquery, @$, {$2, $4});
        subquery->set_modifier(zetasql::ASTExpressionSubquery::EXISTS);
        $$ = subquery;
      }
    | bare_expression_subquery
    ;

bare_expression_subquery:
    "(" query_maybe_expression ")"
      {
        $$ = MAKE_NODE(ASTExpressionSubquery, @$, {$2});
      }
    ;

null_literal:
    "NULL"
      {
        auto* literal = MAKE_NODE(ASTNullLiteral, @1);
        // TODO: Migrate to absl::string_view or avoid having to
        // set this at all if the client isn't interested.
        literal->set_image(std::string(parser->GetInputText(@1)));
        $$ = literal;
      }
    ;

boolean_literal:
    "TRUE"
      {
        auto* literal = MAKE_NODE(ASTBooleanLiteral, @1);
        literal->set_value(true);
        // TODO: Migrate to absl::string_view or avoid having to
        // set this at all if the client isn't interested.
        literal->set_image(std::string(parser->GetInputText(@1)));
        $$ = literal;
      }
    | "FALSE"
      {
        auto* literal = MAKE_NODE(ASTBooleanLiteral, @1);
        literal->set_value(false);
        // TODO: Migrate to absl::string_view or avoid having to
        // set this at all if the client isn't interested.
        literal->set_image(std::string(parser->GetInputText(@1)));
        $$ = literal;
      }
    ;

string_literal:
    STRING_LITERAL
      {
        const absl::string_view input_text = parser->GetInputText(@1);
        std::string str;
        std::string error_string;
        int error_offset;
        const absl::Status parse_status = zetasql::ParseStringLiteral(
            input_text, &str, &error_string, &error_offset);
        if (!parse_status.ok()) {
          zetasql_bison_parser::location location = yyla.location;
          location.begin.column += error_offset;
          if (!error_string.empty()) {
            YYERROR_AND_ABORT_AT(location,
                                 absl::StrCat("Syntax error: ", error_string));
          }
          DLOG(FATAL) << "ParseStringLiteral did not return an error string";
          YYERROR_AND_ABORT_AT(location,
                               absl::StrCat("Syntax error: ",
                                            parse_status.message()));
        }

        auto* literal = MAKE_NODE(ASTStringLiteral, @1);
        literal->set_string_value(std::move(str));
        // TODO: Migrate to absl::string_view or avoid having to
        // set this at all if the client isn't interested.
        literal->set_image(std::string(input_text));
        $$ = literal;
      }
    ;

bytes_literal:
    BYTES_LITERAL
      {
        const absl::string_view input_text = parser->GetInputText(@1);
        std::string bytes;
        std::string error_string;
        int error_offset;
        const absl::Status parse_status = zetasql::ParseBytesLiteral(
            input_text, &bytes, &error_string, &error_offset);
        if (!parse_status.ok()) {
          zetasql_bison_parser::location location = yyla.location;
          location.begin.column += error_offset;
          if (!error_string.empty()) {
            YYERROR_AND_ABORT_AT(location,
                                 absl::StrCat("Syntax error: ", error_string));
          }
          DLOG(FATAL) << "ParseBytesLiteral did not return an error string";
          YYERROR_AND_ABORT_AT(location,
                               absl::StrCat("Syntax error: ",
                                            parse_status.message()));
        }

        // The identifier is parsed *again* in the resolver. The output of the
        // parser maintains the original image.
        // TODO: Fix this wasted work when the JavaCC parser is gone.
        auto* literal = MAKE_NODE(ASTBytesLiteral, @1);
        literal->set_bytes_value(std::move(bytes));
        // TODO: Migrate to absl::string_view or avoid having to
        // set this at all if the client isn't interested.
        literal->set_image(std::string(input_text));
        $$ = literal;
      }
    ;

integer_literal:
    INTEGER_LITERAL
      {
        auto* literal = MAKE_NODE(ASTIntLiteral, @1);
        literal->set_image(std::string(parser->GetInputText(@1)));
        $$ = literal;
      }
    ;

numeric_literal:
    "NUMERIC" STRING_LITERAL
      {
        auto* literal = MAKE_NODE(ASTNumericLiteral, @$);
        literal->set_image(std::string(parser->GetInputText(@2)));
        $$ = literal;
      }
    ;

bignumeric_literal:
    "BIGNUMERIC" STRING_LITERAL
      {
        auto* literal = MAKE_NODE(ASTBigNumericLiteral, @$);
        literal->set_image(std::string(parser->GetInputText(@2)));
        $$ = literal;
      }
    ;

floating_point_literal:
    FLOATING_POINT_LITERAL
      {
        auto* literal = MAKE_NODE(ASTFloatLiteral, @1);
        literal->set_image(std::string(parser->GetInputText(@1)));
        $$ = literal;
      }
    ;

identifier:
    IDENTIFIER
      {
        const absl::string_view identifier_text = parser->GetInputText(@1);
        // The tokenizer rule already validates that the identifier is valid,
        // except for backquoted identifiers.
        if (identifier_text[0] == '`') {
          std::string str;
          std::string error_string;
          int error_offset;
          const absl::Status parse_status =
              zetasql::ParseGeneralizedIdentifier(
                  identifier_text, &str, &error_string, &error_offset);
          if (!parse_status.ok()) {
            zetasql_bison_parser::location location = yyla.location;
            location.begin.column += error_offset;
            if (!error_string.empty()) {
              YYERROR_AND_ABORT_AT(location,
                                   absl::StrCat("Syntax error: ",
                                                error_string));
            }
            DLOG(FATAL) << "ParseIdentifier did not return an error string";
            YYERROR_AND_ABORT_AT(location,
                                 absl::StrCat("Syntax error: ",
                                              parse_status.message()));
          }
          $$ = parser->MakeIdentifier(@1, str);
        } else {
          $$ = parser->MakeIdentifier(@1, identifier_text);
        }
      }
    | keyword_as_identifier
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    ;

system_variable_expression:
    KW_DOUBLE_AT path_expression %prec DOUBLE_AT_PRECEDENCE
    {
      $$ = MAKE_NODE(ASTSystemVariableExpr, @$, {$2});
    }
    | KW_DOUBLE_AT reserved_keyword_rule
      {
        zetasql::ASTIdentifier* reserved_keyword_identifier =
            parser->MakeIdentifier(@2, parser->GetInputText(@2));
        zetasql::ASTPathExpression* path =
            MAKE_NODE(ASTPathExpression, @$, {reserved_keyword_identifier});
        $$ = MAKE_NODE(ASTSystemVariableExpr, @$, {path});
      }
    ;

// This production returns nothing -- the enclosing rule uses only the location
// of the keyword to retrieve the token image from the parser input.
reserved_keyword_rule:
    // WARNING: If you add something here, add it in the reserved token
    // list at the top.
    // BEGIN_RESERVED_KEYWORD_RULE -- Do not remove this!
    "ALL"
    | "AND"
    | "ANY"
    | "ARRAY"
    | "AS"
    | "ASC"
    | "ASSERT_ROWS_MODIFIED"
    | "AT"
    | "BETWEEN"
    | "BY"
    | "CASE"
    | "CAST"
    | "COLLATE"
    | "CREATE"
    | "CROSS"
    | "CURRENT"
    | "DEFAULT"
    | "DEFINE"
    | "DESC"
    | "DISTINCT"
    | "ELSE"
    | "END"
    | "ENUM"
    | "EXCEPT"
    | "EXISTS"
    | "EXTRACT"
    | "FALSE"
    | "FOLLOWING"
    | "FROM"
    | "FULL"
    | "GROUP"
    | "GROUPING"
    | "HASH"
    | "HAVING"
    | "IF"
    | "IGNORE"
    | "IN"
    | "INNER"
    | "INTERSECT"
    | "INTERVAL"
    | "INTO"
    | "IS"
    | "JOIN"
    | "LEFT"
    | "LIKE"
    | "LIMIT"
    | "LOOKUP"
    | "MERGE"
    | "NATURAL"
    | "NEW"
    | "NOT"
    | "NULL"
    | "NULLS"
    | "ON"
    | "OR"
    | "ORDER"
    | "OUTER"
    | "OVER"
    | "PARTITION"
    | "PRECEDING"
    | "PROTO"
    | "RANGE"
    | "RECURSIVE"
    | "RESPECT"
    | "RIGHT"
    | "ROLLUP"
    | "ROWS"
    | "SELECT"
    | "SET"
    | "STRUCT"
    | "TABLESAMPLE"
    | "THEN"
    | "TO"
    | "TRUE"
    | "UNBOUNDED"
    | "UNION"
    | "USING"
    | "WHEN"
    | "WHERE"
    | "WINDOW"
    | "WITH"
    | "UNNEST"
    | "CONTAINS"
    | "CUBE"
    | "ESCAPE"
    | "EXCLUDE"
    | "FETCH"
    | "FOR"
    | "GROUPS"
    | "LATERAL"
    | "NO"
    | "OF"
    | "SOME"
    | "TREAT"
    | "WITHIN"
    // END_RESERVED_KEYWORD_RULE -- Do not remove this!
    ;

// This includes non-reserved keywords that can also be used as identifiers.
// This production returns nothing -- the enclosing rule uses only the location
// of the keyword to retrieve the token image from the parser input.
keyword_as_identifier:
    // WARNING: If you add something here, add it in the non-reserved token list
    // at the top.
    // BEGIN_KEYWORD_AS_IDENTIFIER -- Do not remove this!
    "ABORT"
    | "ACCESS"
    | "ACTION"
    | "AGGREGATE"
    | "ADD"
    | "ALTER"
    | "ASSERT"
    | "BATCH"
    | "BEGIN"
    | "BIGNUMERIC"
    | "BREAK"
    | "CALL"
    | "CASCADE"
    | "CHECK"
    | "CLUSTER"
    | "COLUMN"
    | "COMMIT"
    | "CONNECTION"
    | "CONSTANT"
    | "CONSTRAINT"
    | "CONTINUE"
    | "DATA"
    | "DATABASE"
    | "DATE"
    | "DATETIME"
    | "DECLARE"
    | "DEFINER"
    | "DELETE"
    | "DESCRIBE"
    | "DETERMINISTIC"
    | "DO"
    | "DROP"
    | "ELSEIF"
    | "ENFORCED"
    | "ERROR"
    | "EXCEPTION"
    | "EXECUTE"
    | "EXPLAIN"
    | "EXPORT"
    | "EXTERNAL"
    | "FILTER"
    | "FILL"
    | "FIRST"
    | "FOREIGN"
    | "FUNCTION"
    | "GENERATED"
    | "GRANT"
    | "HIDDEN"
    | "IMMEDIATE"
    | "IMMUTABLE"
    | "IMPORT"
    | "INDEX"
    | "INSERT"
    | "INOUT"
    | "INVOKER"
    | "ISOLATION"
    | "ITERATE"
    | "KEY"
    | "LANGUAGE"
    | "LAST"
    | "LEAVE"
    | "LEVEL"
    | "LOOP"
    | "MATCH"
    | "MATCHED"
    | "MATERIALIZED"
    | "MAX"
    | "MESSAGE"
    | "MIN"
    | "MODEL"
    | "MODULE"
    | "NUMERIC"
    | "OFFSET"
    | "ONLY"
    | "OPTIONS"
    | "OUT"
    | "PERCENT"
    | "POLICIES"
    | "POLICY"
    | "PRIMARY"
    | "PRIVATE"
    | "PRIVILEGES"
    | "PROCEDURE"
    | "PUBLIC"
    | "RAISE"
    | "READ"
    | "REFERENCES"
    | "RENAME"
    | "REPEATABLE"
    | "REPLACE"
    | "REPLACE_FIELDS"
    | "RESTRICT"
    | "RETURNS"
    | "RETURN"
    | "REVOKE"
    | "ROLLBACK"
    | "ROW"
    | "RUN"
    | "SAFE_CAST"
    | "SECURITY"
    | "SHOW"
    | "SIMPLE"
    | "SOURCE"
    | "SQL"
    | "STABLE"
    | "START"
    | "STORED"
    | "STORING"
    | "SYSTEM"
    | "SYSTEM_TIME"
    | "TABLE"
    | "TARGET"
    | "TEMP"
    | "TEMPORARY"
    | "TIME"
    | "TIMESTAMP"
    | "TRANSACTION"
    | "TRANSFORM"
    | "TRUNCATE"
    | "TYPE"
    | "UNIQUE"
    | "UPDATE"
    | "VALUE"
    | "VALUES"
    | "VIEW"
    | "VIEWS"
    | "VOLATILE"
    | "WEIGHT"
    | "WHILE"
    | "WRITE"
    | "ZONE"
    | "DESCRIPTOR"
    // END_KEYWORD_AS_IDENTIFIER -- Do not remove this!
    ;

opt_or_replace: "OR" "REPLACE" { $$ = true; } | /* Nothing */ { $$ = false; } ;

opt_create_scope:
    "TEMP" { $$ = zetasql::ASTCreateStatement::TEMPORARY; }
    | "TEMPORARY" { $$ = zetasql::ASTCreateStatement::TEMPORARY; }
    | "PUBLIC" { $$ = zetasql::ASTCreateStatement::PUBLIC; }
    | "PRIVATE" { $$ = zetasql::ASTCreateStatement::PRIVATE; }
    | /* Nothing */ { $$ = zetasql::ASTCreateStatement::DEFAULT_SCOPE; }
    ;

opt_unique: "UNIQUE" { $$ = true; } | /* Nothing */ { $$ = false; } ;

describe_keyword: "DESCRIBE" | "DESC" ;

opt_hint:
    hint
    | /* Nothing */ { $$ = nullptr; }
    ;

options_entry:
    identifier_in_hints "=" expression
      {
        $$ = MAKE_NODE(ASTOptionsEntry, @$, {$1, $3});
      }
    ;

options_list_prefix:
    "(" options_entry
      {
        $$ = MAKE_NODE(ASTOptionsList, @$, {$2});
      }
    | options_list_prefix "," options_entry
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

options_list:
    options_list_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        $$ = MAKE_NODE(ASTOptionsList, @$);
      }
    ;

opt_options_list:
    "OPTIONS" options_list { $$ = $2; }
    | /* Nothing */ { $$ = nullptr; }
    ;

define_table_statement:
    "DEFINE" "TABLE" path_expression options_list
      {
        $$ = MAKE_NODE(ASTDefineTableStatement, @$, {$3, $4});
      }
    ;

dml_statement:
    insert_statement { $$ = $1; }
    | delete_statement
    | update_statement
    ;

opt_from_keyword: "FROM" | /* Nothing */ ;

opt_where_expression:
    "WHERE" expression
      {
        $$ = $2;
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

opt_assert_rows_modified:
    "ASSERT_ROWS_MODIFIED" possibly_cast_int_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTAssertRowsModified, @$, {$2});
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }

// Returns the JavaCC token code for IGNORE, REPLACE or UPDATE.
// This is what ASTInsertStatement::set_insert_mode expects.
// This does NOT recognize just "REPLACE" or "UPDATE" because that causes
// ambiguity: these keywords are also usable as identifers, so "INSERT REPLACE"
// could be insertion into a table named "replace" or it could be INSERT
// REPLACE. Instead, we recognize INSERT followed by an arbitrary identifier.
unambiguous_or_ignore_replace_update:
    "OR" "IGNORE" { $$ = zetasql::ASTInsertStatement::IGNORE; }
    | "IGNORE" { $$ = zetasql::ASTInsertStatement::IGNORE; }
    | "OR" "REPLACE"
      {
        $$ = zetasql::ASTInsertStatement::REPLACE;
      }
    | "OR" "UPDATE"
      {
        $$ = zetasql::ASTInsertStatement::UPDATE;
      }
    ;

// See comment for insert_statement.
insert_statement_prefix:
    "INSERT"
      {
        $$ = MAKE_NODE(ASTInsertStatement, @$);
      }
    | insert_statement_prefix unambiguous_or_ignore_replace_update
      {
        zetasql::ASTInsertStatement* insert = $1;
        if (insert->parse_progress() >=
            ASTInsertStatement::kSeenOrIgnoreReplaceUpdate) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@2);
        }
        insert->set_insert_mode($2);
        insert->set_parse_progress(
            ASTInsertStatement::kSeenOrIgnoreReplaceUpdate);
        $$ = insert;
      }
   | insert_statement_prefix "INTO" maybe_dashed_generalized_path_expression
      {
        zetasql::ASTInsertStatement* insert = $1;
        if (insert->parse_progress() >= ASTInsertStatement::kSeenTargetPath) {
          YYERROR_AND_ABORT_AT(
              @2, "Syntax error: Unexpected INSERT target name");
        }
        insert->set_parse_progress(
            ASTInsertStatement::kSeenTargetPath);
        $$ = WithExtraChildren(insert, {$3});
      }
    | insert_statement_prefix generalized_path_expression
      {
        zetasql::ASTInsertStatement* insert = $1;
        // Recognize REPLACE and UPDATE as keywords, but only if there was no
        // OR IGNORE/REPLACE/UPDATE before.
        bool is_or_replace_update = false;
        if (insert->parse_progress() <
            ASTInsertStatement::kSeenOrIgnoreReplaceUpdate) {
          absl::string_view path_expression_text = parser->GetInputText(@2);
          if (zetasql_base::CaseEqual(path_expression_text, "REPLACE")) {
            insert->set_insert_mode(
                zetasql::ASTInsertStatement::REPLACE);
            is_or_replace_update = true;
          } else if (zetasql_base::CaseEqual(path_expression_text, "UPDATE")) {
            insert->set_insert_mode(
                zetasql::ASTInsertStatement::UPDATE);
            is_or_replace_update = true;
          }
        }
        if (is_or_replace_update) {
          insert->set_parse_progress(
              ASTInsertStatement::kSeenOrIgnoreReplaceUpdate);
          $$ = insert;
        } else {
          if (insert->parse_progress() == ASTInsertStatement::kSeenTargetPath) {
            YYERROR_AND_ABORT_AT(
                 @2, "Syntax error: INSERT target cannot have an alias");
          }
          if (insert->parse_progress() > ASTInsertStatement::kSeenTargetPath) {
            YYERROR_AND_ABORT_AT(
                 @2, "Syntax error: Unexpected INSERT target name");
          }
          insert->set_parse_progress(
              ASTInsertStatement::kSeenTargetPath);
          $$ = WithExtraChildren(insert, {$2});
        }
      }
    | insert_statement_prefix column_list
      {
        zetasql::ASTInsertStatement* insert = $1;
        if (insert->parse_progress() >= ASTInsertStatement::kSeenColumnList) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected column list");
        }
        if (insert->parse_progress() < ASTInsertStatement::kSeenTargetPath) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Expecting INSERT target name");
        }
        insert->set_parse_progress(ASTInsertStatement::kSeenColumnList);
        $$ = WithExtraChildren(insert, {$2});
      }
    // This has a shift/reduce conflict with the "path_expression" rule above.
    // This rule wins because "VALUES" -> path_expression needs a reduction
    // while the "(" of insert_values_list requires a shift, and a shift/reduce
    // conflict is always resolved in favor of shifting.
    // See the comment for "insert_statement" for more context.
    // This rule also matches when "VALUES" is actually intended as a target
    // path, and the insert_values_list is actually a column list! This enables
    // statements such as INSERT VALUES (c1, c2) VALUES (3, 5), where the first
    // VALUES is the insert target path.
    | insert_statement_prefix "VALUES" insert_values_list
      {
        zetasql::ASTInsertStatement* insert = $1;
        $3 = WithStartLocation($3, @2);
        if (insert->parse_progress() < ASTInsertStatement::kSeenTargetPath) {
          // We haven't seen a target path yet. That means the "VALUES" should
          // be reinterpreted as a target path, and the insert_values_list as a
          // column list! We convert the already-parsed values list into the
          // intended column list.
          zetasql::ASTIdentifier* values_identifier =
              parser->MakeIdentifier(@2, parser->GetInputText(@2));
          auto* values_path_expression =
              MAKE_NODE(ASTPathExpression, @2, {values_identifier});
          insert->AddChild(values_path_expression);
          zetasql::ASTInsertValuesRowList* row_list = $3;
          if (row_list->num_children() == 0 ||
              row_list->child(0)->node_kind() !=
                  zetasql::AST_INSERT_VALUES_ROW) {
            YYERROR_AND_ABORT_AT(
                @3,
                "Internal error: values list is unexpected type");
          }
          auto* row =
              row_list->mutable_child(0)
                      ->GetAsOrDie<zetasql::ASTInsertValuesRow>();
          auto* column_list = MAKE_NODE(ASTColumnList, @3, {});
          for (int i = 0; i < row->num_children(); ++i) {
            zetasql::ASTNode* element = row->mutable_child(i);
            if (element->node_kind() != zetasql::AST_PATH_EXPRESSION) {
              if (element->node_kind() == zetasql::AST_DEFAULT_LITERAL) {
                YYERROR_AND_ABORT_AT(
                    parser->GetBisonLocation(element->GetParseLocationRange()),
                    "Syntax error: Expected column name, got keyword DEFAULT");
              }
              YYERROR_AND_ABORT_AT(
                  parser->GetBisonLocation(element->GetParseLocationRange()),
                  "Syntax error: Expected column name");
            }
            auto* path_expression =
                element->GetAsOrDie<zetasql::ASTPathExpression>();
            if (path_expression->num_children() != 1) {
              YYERROR_AND_ABORT_AT(
                  parser->GetBisonLocation(element->GetParseLocationRange()),
                  "Syntax error: Expected column name");
            }
            column_list->AddChild(path_expression->mutable_child(0));
          }
          if (row_list->num_children() > 1) {
            // There are multiple lists. Assume the user actually intended to
            // write VALUES but forgot to do so. Do this without checking the
            // first list for being correct as a column list, because we assume
            // that the user intended it as a VALUES list.
            YYERROR_AND_ABORT_AT(
                parser->GetBisonLocation(
                    row_list->child(1)->GetParseLocationRange()),
                "Syntax error: Unexpected multiple column lists");
          }
          insert->AddChild(column_list);
          insert->set_parse_progress(
              ASTInsertStatement::kSeenColumnList);
        } else if (insert->parse_progress() >=
                   ASTInsertStatement::kSeenValuesList) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected VALUES list");
        } else if (insert->parse_progress() <
                   ASTInsertStatement::kSeenTargetPath) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Expecting INSERT target name");
        } else {
          $$ = WithEndLocation(WithExtraChildren(insert, {$3}), @$);
          insert->set_parse_progress(
              ASTInsertStatement::kSeenValuesList);
        }
      }
    ;

// INSERT is extremely complicated to parse in an LALR(1) grammar. The
// complications are because of the following issues:
// (a) the "OR" in OR IGNORE/REPLACE/UPDATE is optional.
// (b) REPLACE and UPDATE can be used as identifiers.
// (c) VALUES can be used as an identifier as well.
// (d) All of the clauses and keywords except for the target path expression are
// optional.
// For instance, "INSERT replace values" could be a prefix meaning "INSERT INTO
// replace VALUES (..." or "INSERT OR REPLACE INTO values...". It ends up being
// unambiguous in the end, but the only strict way to parse it in Bison is by
// enumerating all combinations. We could use a tokenizer state for the
// first tokens to reduce the confusion, but unfortunately INSERT itself is
// also usable as an identifier, so we cannot simply switch to a different
// tokenizer state when we see INSERT! Otherwise something like
// "SELECT insert update" (which is valid and selects a column named "insert"
// with alias "update") would fail.
//
// The solution used here is to allow arbitrary combinations of the optional and
// mandatory components at the grammar level, and to keep track of the
// components that have been seen in the parse_progress() of the
// ASTInsertStatement that is being constructed. The validation is done in the
// parsing rules. All of this happens in insert_statement_prefix.
//
// Because "VALUES" is a non-reserved keyword, the "VALUES" rule in
// insert_statement_prefix is ambiguous with the "path_expression" rule. This
// is resolved in favor of "VALUES" by a shift/reduce conflict (reducing "VALUE"
// to keyword_as_identifier versus shifting the "(" that is required at the
// start of insert_values_list. There may be ways to avoid this conflict, but
// they are not very palatable:
//
// - Excluding "VALUES" from all of the path expressions in the
//   insert_statement_prefix. This would require us to duplicate the
//   path_expression and identifier productions to exclude this keyword. That
//   solution adds a maintenance burden. In addition, it would prevent the name
//   VALUES from being used as an insert target, which is unfortunate because
//   this may be a common name in nested INSERTs (inserting into a repeated
//   field named VALUES). In addition, VALUES is currently accepted for this
//   purpose by the JavaCC parser.
//
// - Not matching "VALUES" explicitly anymore, but matching it with a
//   path_expression in insert_statement_prefix. That would require adding a
//   rule for insert_values_list to insert_statement_prefix. Unfortunately that
//   adds another bunch of hard-to-resolve conflicts. For one thing, a VALUES
//   list and a column list have overlapping syntax, so we would have to parse
//   those in a single unified way (probably as a VALUES list) and then
//   disambiguate later in code. We already do that to resolve statements like
//   "INSERT VALUES (c1, c2)" However, insert_values_list also conflicts with
//   "query" (used in the insert_statement rule), because the values list can
//   contain expressions, which can contain expression subquries. Something like
//   ((SELECT 1)) could be a parenthesized query or a VALUES list containing a
//   scalar expression subquery. That ambiguity was solved for expression
//   subqueries using a shift/reduce conflict, and we would have to jump through
//   hoops to get the same effect here as well. That would be much harder to
//   understand than the simple shift/reduce conflict here.
insert_statement:
    insert_statement_prefix opt_assert_rows_modified
      {
        zetasql::ASTInsertStatement* insert = $1;
        if (insert->parse_progress() < ASTInsertStatement::kSeenTargetPath) {
          YYERROR_AND_ABORT_AT(@2,
                               "Syntax error: Expecting INSERT target name");
        }
        if (insert->parse_progress() < ASTInsertStatement::kSeenValuesList) {
          YYERROR_AND_ABORT_AT(@2,
                               "Syntax error: Expecting VALUES list or query");
        }
        $$ = WithEndLocation(WithExtraChildren(insert, {$2}), @$);
      }
    | insert_statement_prefix query opt_assert_rows_modified
      {
        zetasql::ASTInsertStatement* insert = $1;
        if (insert->parse_progress() < ASTInsertStatement::kSeenTargetPath) {
          YYERROR_AND_ABORT_AT(
               @2, "Syntax error: Expecting INSERT target name");
        }
        if (insert->parse_progress() >= ASTInsertStatement::kSeenValuesList) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected query");
        }
        $$ = WithEndLocation(WithExtraChildren(insert, {$2, $3}), @$);
      }
    ;

expression_or_default:
   expression
   | "DEFAULT"
     {
       $$ = MAKE_NODE(ASTDefaultLiteral, @$, {});
     }
   ;

insert_values_row_prefix:
    "(" expression_or_default
      {
        $$ = MAKE_NODE(ASTInsertValuesRow, @$, {$2});
      }
    | insert_values_row_prefix "," expression_or_default
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

insert_values_row:
    insert_values_row_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    ;

insert_values_list:
    insert_values_row
      {
        $$ = MAKE_NODE(ASTInsertValuesRowList, @$, {$1});
      }
    | insert_values_list "," insert_values_row
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

delete_statement:
    "DELETE" opt_from_keyword maybe_dashed_generalized_path_expression
    opt_as_alias opt_with_offset_and_alias opt_where_expression
    opt_assert_rows_modified
      {
        $$ = MAKE_NODE(ASTDeleteStatement, @$, {$3, $4, $5, $6, $7});
      }
    ;

opt_with_offset_and_alias:
    "WITH" "OFFSET" opt_as_alias
      {
        $$ = MAKE_NODE(ASTWithOffset, @$, {$3});
      }
    | /* Nothing */ { $$ = nullptr; }
    ;

update_statement:
    "UPDATE" maybe_dashed_generalized_path_expression opt_as_alias
    opt_with_offset_and_alias "SET" update_item_list opt_from_clause
    opt_where_expression opt_assert_rows_modified
      {
        $$ = MAKE_NODE(ASTUpdateStatement, @$, {$2, $3, $4, $6, $7, $8, $9});
      }
    ;

truncate_statement:
    "TRUNCATE" "TABLE" maybe_dashed_path_expression opt_where_expression
      {
        $$ = MAKE_NODE(ASTTruncateStatement, @$, {$3, $4});
      }
    ;

nested_dml_statement:
    "(" dml_statement ")"
      {
        $$ = $2;
      }
    ;

// A "generalized path expression" is a path expression that can contain
// generalized field accesses (e.g., "a.b.c.(foo.bar).d.e"). To avoid
// ambiguities in the grammar (particularly with INSERT), a generalized path
// must start with an identifier. The parse trees that result are consistent
// with the similar syntax in the <expression> rule.
generalized_path_expression:
    identifier
      {
        $$ = MAKE_NODE(ASTPathExpression, @$, {$1});
      }
    | generalized_path_expression "." generalized_extension_path
      {
        // Remove the parentheses from generalized_extension_path as they were
        // added to indicate the path corresponds to an extension field in the
        // resolver. It is implied that the path argument of
        // ASTDotGeneralizedField is an extension and thus parentheses are
        // automatically added when this node is unparsed.
        $3->set_parenthesized(false);
        $$ = MAKE_NODE(ASTDotGeneralizedField, @2, @3, {$1, $3});
      }
    | generalized_path_expression "." identifier
      {
        if ($1->node_kind() == zetasql::AST_PATH_EXPRESSION) {
          $$ = WithExtraChildren(WithEndLocation($1, @3), {$3});
        } else {
          $$ = MAKE_NODE(ASTDotIdentifier, @2, @3, {$1, $3});
        }
      }
    | generalized_path_expression "[" expression "]"
      {
        $$ = MAKE_NODE(ASTArrayElement, @2, @4, {$1, $3});
      }
    ;

maybe_dashed_generalized_path_expression:
  generalized_path_expression { $$ = $1; }
  // TODO: This is just a regular path expression, not generalized one
  // it doesn't allow extensions or array elements access. It is OK for now,
  // since this production is only used in INSERT INTO and UPDATE statements
  // which don't actually allow extensions or array element access anyway.
  | dashed_path_expression
    {
      if (parser->language_options() != nullptr &&
          parser->language_options()->LanguageFeatureEnabled(
             zetasql::FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME)) {
        $$ = $1;
      } else {
        YYERROR_AND_ABORT_AT(
            @1,
            absl::StrCat(
              "Syntax error: Table name contains '-' character. "
              "It needs to be quoted: ",
              zetasql::ToIdentifierLiteral(
                parser->GetInputText(@1), false)));
      }
    }
  ;

// A "generalized extension path" is similar to a "generalized path expression"
// in that they contain generalized field accesses. The primary difference is
// that a generalized extension path must start with a parenthesized path
// expression, where as a generalized path expression must start with an
// identifier. A generalized extension path allows field accesses of message
// extensions to be parsed.
generalized_extension_path:
    "(" path_expression ")"
      {
       $2->set_parenthesized(true);
       $$ = $2;
      }
    | generalized_extension_path "." "(" path_expression ")"
      {
        $$ = MAKE_NODE(ASTDotGeneralizedField, @2, @5, {$1, $4});
      }
    | generalized_extension_path "." identifier
      {
        $$ = MAKE_NODE(ASTDotIdentifier, @2, @3, {$1, $3});
      }
    ;

update_set_value:
    generalized_path_expression "=" expression_or_default
      {
        $$ = MAKE_NODE(ASTUpdateSetValue, @$, {$1, $3});
      }
    ;

update_item:
    update_set_value
      {
        $$ = MAKE_NODE(ASTUpdateItem, @$, {$1});
      }
    | nested_dml_statement
      {
        $$ = MAKE_NODE(ASTUpdateItem, @$, {$1});
      }
    ;

update_item_list:
   update_item
     {
       $$ = MAKE_NODE(ASTUpdateItemList, @$, {$1});
     }
   | update_item_list "," update_item
     {
       $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
     }
   ;

opt_into:
    "INTO"
    | /* Nothing */
    ;

opt_by_target:
    "BY" "TARGET"
    | /* Nothing */
    ;

opt_and_expression:
    "AND" expression
      {
        $$ = $2;
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

merge_insert_value_list_or_source_row:
    "VALUES" insert_values_row
      {
        $$ = $2;
      }
    | "ROW"
      {
        $$ = MAKE_NODE(ASTInsertValuesRow, @$, {});
      }
    ;

merge_action:
    "INSERT" opt_column_list merge_insert_value_list_or_source_row
      {
        auto* node = MAKE_NODE(ASTMergeAction, @$, {$2, $3});
        node->set_action_type(zetasql::ASTMergeAction::INSERT);
        $$ = node;
      }
    | "UPDATE" "SET" update_item_list
        {
          auto* node = MAKE_NODE(ASTMergeAction, @$, {$3});
          node->set_action_type(zetasql::ASTMergeAction::UPDATE);
          $$ = node;
        }
    | "DELETE"
        {
          auto* node = MAKE_NODE(ASTMergeAction, @$, {});
          node->set_action_type(zetasql::ASTMergeAction::DELETE);
          $$ = node;
        }
    ;

merge_when_clause:
    "WHEN" "MATCHED" opt_and_expression "THEN" merge_action
      {
        auto* node = MAKE_NODE(ASTMergeWhenClause, @$, {$3, $5});
        node->set_match_type(zetasql::ASTMergeWhenClause::MATCHED);
        $$ = node;
      }
    | "WHEN" "NOT" "MATCHED" opt_by_target opt_and_expression "THEN"
      merge_action
        {
          auto* node = MAKE_NODE(ASTMergeWhenClause, @$, {$5, $7});
          node->set_match_type(
              zetasql::ASTMergeWhenClause::NOT_MATCHED_BY_TARGET);
          $$ = node;
        }
    | "WHEN" "NOT" "MATCHED" "BY" "SOURCE" opt_and_expression "THEN"
      merge_action
      {
        auto* node = MAKE_NODE(ASTMergeWhenClause, @$, {$6, $8});
        node->set_match_type(
            zetasql::ASTMergeWhenClause::NOT_MATCHED_BY_SOURCE);
        $$ = node;
      }
    ;

merge_when_clause_list:
  merge_when_clause
    {
      $$ = MAKE_NODE(ASTMergeWhenClauseList, @$, {$1});
    }
  | merge_when_clause_list merge_when_clause
    {
      $$ = WithEndLocation(WithExtraChildren($1, {$2}), @$);
    }
  ;

// TODO: Consider allowing table_primary as merge_source, which
// requires agreement about spec change.
merge_source:
    table_path_expression
    | table_subquery
    ;

merge_statement_prefix:
  "MERGE" opt_into maybe_dashed_path_expression opt_as_alias
  "USING" merge_source "ON" expression
    {
      $$ = MAKE_NODE(ASTMergeStatement, @$, {$3, $4, $6, $8});
    }
  ;

merge_statement:
  merge_statement_prefix merge_when_clause_list
    {
      WithEndLocation(WithExtraChildren($1, {$2}), @$);
    }
  ;

call_statement_with_args_prefix:
    "CALL" path_expression "(" tvf_argument
      {
        $$ = MAKE_NODE(ASTCallStatement, @$, {$2, $4});
      }
    | call_statement_with_args_prefix "," tvf_argument
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

call_statement:
    call_statement_with_args_prefix ")"
      {
        $$ = WithEndLocation($1, @$);
      }
    | "CALL" path_expression "(" ")"
      {
        $$ = MAKE_NODE(ASTCallStatement, @$, {$2});
      }
    ;

opt_function_parameters:
    function_parameters
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

/* Returns true if IF EXISTS was specified. */
opt_if_exists:
    "IF" "EXISTS"
      {
        $$ = true;
      }
    | /* Nothing */
      {
        $$ = false;
      }
    ;

/* Returns true if ACCESS was specified. */
opt_access:
    "ACCESS"
      {
        $$ = true;
      }
    | /* Nothing */
      {
        $$ = false;
      }
    ;

// TODO: Make new syntax mandatory.
drop_all_row_access_policies_statement:
    "DROP" "ALL" "ROW" opt_access "POLICIES" "ON" path_expression
      {
        auto* drop_all = MAKE_NODE(ASTDropAllRowAccessPoliciesStatement, @$,
            {$7});
        drop_all->set_has_access_keyword($4);
        $$ = drop_all;
      }
    ;

on_path_expression:
    "ON" path_expression
      {
        $$ = $2;
      }
    ;

drop_statement:
    "DROP" "ROW" "ACCESS" "POLICY" opt_if_exists identifier on_path_expression
      {
        // This is a DROP ROW ACCESS POLICY statement.
        auto* drop_row_access_policy =
            MAKE_NODE(ASTDropRowAccessPolicyStatement, @$, {$6, $7});
        drop_row_access_policy->set_is_if_exists($5);
        $$ = drop_row_access_policy;
      }
    | "DROP" table_or_table_function opt_if_exists maybe_dashed_path_expression
      {
        if ($2 == TableOrTableFunctionKeywords::kTableAndFunctionKeywords) {
          // ZetaSQL does not (yet) support DROP TABLE FUNCTION,
          // though it should as per a recent spec.  Currently, table/aggregate
          // functions are dropped via simple DROP FUNCTION statements.
          YYERROR_AND_ABORT_AT(@2, absl::StrCat(
            "DROP TABLE FUNCTION is not supported, use DROP FUNCTION ",
            zetasql::ToIdentifierLiteral(parser->GetInputText(@4), false)));
        }
        auto* drop = MAKE_NODE(ASTDropStatement, @$, {$4});
        drop->set_schema_object_kind(zetasql::SchemaObjectKind::kTable);
        drop->set_is_if_exists($3);
        $$ = drop;
      }
    | "DROP" schema_object_kind opt_if_exists path_expression
      opt_function_parameters
      {
        // This is a DROP <object_type> <object_name> statement.
        if ($2 == zetasql::SchemaObjectKind::kAggregateFunction) {
          // ZetaSQL does not (yet) support DROP AGGREGATE FUNCTION,
          // though it should as per a recent spec.  Currently, table/aggregate
          // functions are dropped via simple DROP FUNCTION statements.
          YYERROR_AND_ABORT_AT(@2,
                               "DROP AGGREGATE FUNCTION is not "
                               "supported, use DROP FUNCTION");
        }
        if ($2 == zetasql::SchemaObjectKind::kFunction) {
            // If no function parameters are given, then all overloads of the
            // named function will be dropped. Note that "DROP FUNCTION FOO()"
            // will drop the zero-argument overload of foo(), rather than
            // dropping all overloads.
            auto* drop_function =
                MAKE_NODE(ASTDropFunctionStatement, @$, {$4, $5});
            drop_function->set_is_if_exists($3);
            $$ = drop_function;
        } else {
          if ($5 != nullptr) {
            YYERROR_AND_ABORT_AT(@5,
                                 "Syntax error: Parameters are only "
                                 "supported for DROP FUNCTION");
          }
          if ($2 == zetasql::SchemaObjectKind::kMaterializedView) {
            auto* drop_materialized_view =
                MAKE_NODE(ASTDropMaterializedViewStatement, @$, {$4});
            drop_materialized_view->set_is_if_exists($3);
            $$ = drop_materialized_view;
          } else {
            if ($5 != nullptr) {
              YYERROR_AND_ABORT_AT(@5,
                                   "Syntax error: Parameters are only "
                                   "supported for DROP FUNCTION");
            }
            auto* drop = MAKE_NODE(ASTDropStatement, @$, {$4});
            drop->set_schema_object_kind($2);
            drop->set_is_if_exists($3);
            $$ = drop;
          }
        }
      }
    ;

non_empty_statement_list:
    terminated_statement
      {
        $$ = MAKE_NODE(ASTStatementList, @$, {$1});
      }
    | non_empty_statement_list terminated_statement
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$2}), @$);
      };

unterminated_non_empty_statement_list:
    unterminated_statement
      {
        $$ = MAKE_NODE(ASTStatementList, @$, {$1});
      }
    | non_empty_statement_list unterminated_statement
      {
        $$ = WithEndLocation(WithExtraChildren($1, {$2}), @$);
      };

opt_execute_into_clause:
  KW_INTO identifier_list
    {
      $$ = MAKE_NODE(ASTExecuteIntoClause, @$, {$2});
    }
  | /* Nothing */
    {
      $$ = nullptr;
    }
  ;

execute_using_argument:
  expression KW_AS identifier
    {
      auto* alias = MAKE_NODE(ASTAlias, @3, @3, {$3});
      $$ = MAKE_NODE(ASTExecuteUsingArgument, @$, {$1, alias});
    }
  | expression
    {
      $$ = MAKE_NODE(ASTExecuteUsingArgument, @$, {$1, nullptr});
    }
  ;

// Returns ASTExecuteUsingClause to avoid an unneeded AST class for accumulating
// list values.
execute_using_argument_list:
  execute_using_argument
    {
      $$ = MAKE_NODE(ASTExecuteUsingClause, @$, {$1});
    }
  | execute_using_argument_list "," execute_using_argument
    {
      $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
    }
  ;

opt_execute_using_clause:
  KW_USING execute_using_argument_list
    {
      $$ = $2;
    }
  | /* Nothing */
    {
      $$ = nullptr;
    }
  ;

execute_immediate:
  KW_EXECUTE KW_IMMEDIATE expression opt_execute_into_clause
  opt_execute_using_clause
    {
      $$ = MAKE_NODE(ASTExecuteImmediateStatement, @$, {$3, $4, $5});
    }
  ;

script:
  non_empty_statement_list
  {
    $1->set_variable_declarations_allowed(true);
    $$ = MAKE_NODE(ASTScript, @$, {$1});
  }
  | unterminated_non_empty_statement_list
  {
    $1->set_variable_declarations_allowed(true);
    $$ = MAKE_NODE(ASTScript, @$, {$1});
  }
  | /* Nothing */
    {
      // Resolve to an empty script.
      zetasql::ASTStatementList* empty_stmt_list = MAKE_NODE(
          ASTStatementList, @$, {});
      $$ = MAKE_NODE(ASTScript, @$, {empty_stmt_list});
    }
  ;

statement_list:
    non_empty_statement_list
    {
      $$ = $1;
    }
  | /* Nothing */
    {
      // Resolve to an empty statement list.
      $$ = MAKE_NODE(ASTStatementList, @$, {});
    }
  ;

opt_else:
    "ELSE" statement_list
      {
        $$ = $2;
      }
    | /* Nothing */
      {
        $$ = nullptr;
      }
    ;

elseif_clauses:
  "ELSEIF" expression "THEN" statement_list
  {
    zetasql::ASTElseifClause* elseif_clause = MAKE_NODE(
        ASTElseifClause, @$, {$2, $4});
    $$ = MAKE_NODE(ASTElseifClauseList, @$, {elseif_clause});
  }
  | elseif_clauses "ELSEIF" expression "THEN" statement_list
  {
    zetasql::ASTElseifClause* elseif_clause = MAKE_NODE(
        ASTElseifClause, @2, {$3, $5});
    $$ = WithEndLocation(WithExtraChildren(
        $1, {WithEndLocation(elseif_clause, @$)}), @$);
  };

opt_elseif_clauses:
  elseif_clauses
    {
      $$ = $1;
    }
  | /*nothing*/
    {
      $$ = nullptr;
    }
  ;

if_statement_unclosed:
    "IF" expression "THEN" statement_list opt_elseif_clauses opt_else
      {
        $$ = MAKE_NODE(ASTIfStatement, @$, {$2, $4, $5, $6});
      }
    ;

if_statement:
    if_statement_unclosed
    "END" "IF"
      {
        $$ = WithEndLocation($1, @$);
      }
    |
    if_statement_unclosed error
      {
        // This rule produces an error for any IF statement not closed with END
        // IF. Without it, the error would indicate that the parser expected the
        // END keyword without explicitly referencing END IF.
        YYERROR_AND_ABORT_AT(@2, "Syntax error: Expected END IF");
      }
    ;

begin_end_block:
    "BEGIN" statement_list opt_exception_handler "END" {
      $2->set_variable_declarations_allowed(true);
      $$ = MAKE_NODE(ASTBeginEndBlock, @$, {$2, $3});
    }
    ;

opt_exception_handler:
    "EXCEPTION" "WHEN" "ERROR" "THEN" statement_list {
      zetasql::ASTExceptionHandler* handler = MAKE_NODE(
          ASTExceptionHandler, @2, {$5});
      $$ = MAKE_NODE(ASTExceptionHandlerList, @1, {handler});
    }
    | /* Nothing */
    {
      $$ = nullptr;
    }
    ;

opt_default_expression:
    "DEFAULT" expression
    {
      $$ = $2;
    }
    | /* Nothing */
    {
      $$ = nullptr;
    }
    ;

identifier_list:
    identifier
    {
      $$ = MAKE_NODE(ASTIdentifierList, @$, {$1});
    }
    | identifier_list "," identifier
    {
      $$ = WithEndLocation(WithExtraChildren($1, {$3}), @$);
    }
    ;

variable_declaration:
    "DECLARE" identifier_list type opt_default_expression
    {
      $$ = MAKE_NODE(ASTVariableDeclaration, @$, {$2, $3, $4});
    }
    |
    "DECLARE" identifier_list "DEFAULT" expression
    {
      $$ = MAKE_NODE(ASTVariableDeclaration, @$, {$2, nullptr, $4});
    }
    ;

loop_statement:
    "LOOP" statement_list "END" "LOOP"
    {
      $$ = MAKE_NODE(ASTWhileStatement, @$, {$2});
    }
    ;

while_statement:
    "WHILE" expression "DO" statement_list "END" "WHILE"
    {
      $$ = MAKE_NODE(ASTWhileStatement, @$, {$2, $4});
    }
    ;

break_statement:
    "BREAK"
    {
      zetasql::ASTBreakStatement* stmt = MAKE_NODE(ASTBreakStatement, @$, {});
      stmt->set_keyword(zetasql::ASTBreakContinueStatement::BREAK);
      $$ = stmt;
    }
    | "LEAVE"
    {
      zetasql::ASTBreakStatement* stmt = MAKE_NODE(ASTBreakStatement, @$, {});
      stmt->set_keyword(zetasql::ASTBreakContinueStatement::LEAVE);
      $$ = stmt;
    }
    ;

continue_statement:
    "CONTINUE"
    {
      zetasql::ASTContinueStatement* stmt = MAKE_NODE(ASTContinueStatement,
          @$, {});
      stmt->set_keyword(zetasql::ASTBreakContinueStatement::CONTINUE);
      $$ = stmt;
    }
    | "ITERATE"
    {
      zetasql::ASTContinueStatement* stmt = MAKE_NODE(ASTContinueStatement,
          @$, {});
      stmt->set_keyword(zetasql::ASTBreakContinueStatement::ITERATE);
      $$ = stmt;
    }
    ;

// TODO: add expression to RETURN as defined in
// (broken link) section "RETURN Statement".
return_statement:
    "RETURN"
    {
      $$ = MAKE_NODE(ASTReturnStatement, @$, {});
    }
    ;

raise_statement:
    "RAISE"
    {
      $$ = MAKE_NODE(ASTRaiseStatement, @$);
    }
    | "RAISE" "USING" "MESSAGE" "=" expression
    {
      $$ = MAKE_NODE(ASTRaiseStatement, @$, {$5});
    };

next_statement_kind:
    opt_hint next_statement_kind_without_hint
      {
        ast_statement_properties->statement_level_hints = $1;
        // The parser will complain about the remainder of the input if we let
        // the tokenizer continue to produce tokens, because we don't have any
        // grammar for the rest of the input.
        tokenizer->SetForceTerminate();
        $$ = $2;
      }
    ;

next_statement_kind_parenthesized_select:
    "(" next_statement_kind_parenthesized_select { $$ = $2; }
    | "SELECT" { $$ = zetasql::ASTQueryStatement::kConcreteNodeKind; }
    | "WITH" { $$ = zetasql::ASTQueryStatement::kConcreteNodeKind; }
    ;

next_statement_kind_table:
    "TABLE"
      {
        // Set statement properties node_kind before finishing parsing, so that
        // in the case of a syntax error after "TABLE", ParseNextStatementKind()
        // still returns ASTCreateTableStatement::kConcreteNodeKind.
        ast_statement_properties->node_kind =
            zetasql::ASTCreateTableStatement::kConcreteNodeKind;
      }
    ;

next_statement_kind_create_table_opt_as_or_semicolon:
    "AS" { ast_statement_properties->is_create_table_as_select = true; }
    | ";"
    | /* nothing */
    ;

next_statement_kind_create_modifiers:
    opt_or_replace opt_create_scope
      {
        ast_statement_properties->create_scope = $2;
      }

next_statement_kind_without_hint:
    "EXPLAIN" { $$ = zetasql::ASTExplainStatement::kConcreteNodeKind; }
    | next_statement_kind_parenthesized_select
    | "DEFINE" "TABLE"
      { $$ = zetasql::ASTDefineTableStatement::kConcreteNodeKind; }
    | "EXECUTE" "IMMEDIATE"
      { $$ = zetasql::ASTExecuteImmediateStatement::kConcreteNodeKind; }
    | "EXPORT" "DATA"
      { $$ = zetasql::ASTExportDataStatement::kConcreteNodeKind; }
    | "INSERT" { $$ = zetasql::ASTInsertStatement::kConcreteNodeKind; }
    | "UPDATE" { $$ = zetasql::ASTUpdateStatement::kConcreteNodeKind; }
    | "DELETE" { $$ = zetasql::ASTDeleteStatement::kConcreteNodeKind; }
    | "MERGE" { $$ = zetasql::ASTMergeStatement::kConcreteNodeKind; }
    | describe_keyword
      { $$ = zetasql::ASTDescribeStatement::kConcreteNodeKind; }
    | "SHOW" { $$ = zetasql::ASTShowStatement::kConcreteNodeKind; }
    | "DROP" "ALL" "ROW" opt_access "POLICIES"
      {
        $$ = zetasql::ASTDropAllRowAccessPoliciesStatement::kConcreteNodeKind;
      }
    | "DROP" "ROW" "ACCESS" "POLICY"
      { $$ = zetasql::ASTDropRowAccessPolicyStatement::kConcreteNodeKind; }
    | "DROP" table_or_table_function
      { $$ = zetasql::ASTDropStatement::kConcreteNodeKind; }
    | "DROP" schema_object_kind
      {
        switch ($2) {
          case zetasql::SchemaObjectKind::kFunction:
            $$ = zetasql::ASTDropFunctionStatement::kConcreteNodeKind;
            break;
          case zetasql::SchemaObjectKind::kMaterializedView:
            $$ = zetasql::ASTDropMaterializedViewStatement::kConcreteNodeKind;
            break;
          default:
            $$ = zetasql::ASTDropStatement::kConcreteNodeKind;
            break;
        }
      }
    | "GRANT" { $$ = zetasql::ASTGrantStatement::kConcreteNodeKind; }
    | "REVOKE" { $$ = zetasql::ASTRevokeStatement::kConcreteNodeKind; }
    | "RENAME" { $$ = zetasql::ASTRenameStatement::kConcreteNodeKind; }
    | "START" { $$ = zetasql::ASTBeginStatement::kConcreteNodeKind; }
    | "BEGIN" { $$ = zetasql::ASTBeginStatement::kConcreteNodeKind; }
    | "SET" "TRANSACTION" identifier
      { $$ = zetasql::ASTSetTransactionStatement::kConcreteNodeKind; }
    | "SET" identifier "="
      { $$ = zetasql::ASTSingleAssignment::kConcreteNodeKind; }
    | "SET" named_parameter_expression "="
      { $$ = zetasql::ASTParameterAssignment::kConcreteNodeKind; }
    | "SET" system_variable_expression "="
      { $$ = zetasql::ASTSystemVariableAssignment::kConcreteNodeKind; }
    | "SET" "("
      { $$ = zetasql::ASTAssignmentFromStruct::kConcreteNodeKind; }
    | "COMMIT" { $$ = zetasql::ASTCommitStatement::kConcreteNodeKind; }
    | "ROLLBACK" { $$ = zetasql::ASTRollbackStatement::kConcreteNodeKind; }
    | "START" "BATCH"
      { $$ = zetasql::ASTStartBatchStatement::kConcreteNodeKind; }
    | "RUN" "BATCH" { $$ = zetasql::ASTRunBatchStatement::kConcreteNodeKind; }
    | "ABORT" "BATCH"
      { $$ = zetasql::ASTAbortBatchStatement::kConcreteNodeKind; }
    | "ALTER" "DATABASE"
      { $$ = zetasql::ASTAlterDatabaseStatement::kConcreteNodeKind; }
    | "ALTER" "TABLE"
      { $$ = zetasql::ASTAlterTableStatement::kConcreteNodeKind; }
    | "ALTER" "ROW"
      { $$ = zetasql::ASTAlterRowAccessPolicyStatement::kConcreteNodeKind; }
    | "ALTER" "ALL" "ROW" "ACCESS" "POLICIES"
      { $$ =
          zetasql::ASTAlterAllRowAccessPoliciesStatement::kConcreteNodeKind; }
    | "ALTER" "VIEW"
      { $$ = zetasql::ASTAlterViewStatement::kConcreteNodeKind; }
    | "ALTER" "MATERIALIZED" "VIEW"
      { $$ = zetasql::ASTAlterMaterializedViewStatement::kConcreteNodeKind; }
    | "CREATE" "DATABASE"
      { $$ = zetasql::ASTCreateDatabaseStatement::kConcreteNodeKind; }
    | "CREATE" next_statement_kind_create_modifiers opt_aggregate
      "CONSTANT"
      {
        $$ = zetasql::ASTCreateConstantStatement::kConcreteNodeKind;
      }
    | "CREATE" next_statement_kind_create_modifiers opt_aggregate
      "FUNCTION"
      {
        $$ = zetasql::ASTCreateFunctionStatement::kConcreteNodeKind;
      }
    | "CREATE" next_statement_kind_create_modifiers "PROCEDURE"
      {
        $$ = zetasql::ASTCreateProcedureStatement::kConcreteNodeKind;
      }
    | "CREATE" opt_or_replace opt_unique "INDEX"
      { $$ = zetasql::ASTCreateIndexStatement::kConcreteNodeKind; }
    | "CREATE" next_statement_kind_create_modifiers
      next_statement_kind_table opt_if_not_exists
      maybe_dashed_path_expression opt_table_element_list
      opt_partition_by_clause_no_hint opt_cluster_by_clause_no_hint
      opt_options_list next_statement_kind_create_table_opt_as_or_semicolon
      {
        $$ = zetasql::ASTCreateTableStatement::kConcreteNodeKind;
      }
    | "CREATE" next_statement_kind_create_modifiers "MODEL"
      {
        $$ = zetasql::ASTCreateModelStatement::kConcreteNodeKind;
      }
    | "CREATE" next_statement_kind_create_modifiers "TABLE"
      "FUNCTION"
      {
        $$ = zetasql::ASTCreateTableFunctionStatement::kConcreteNodeKind;
      }
    | "CREATE" next_statement_kind_create_modifiers "EXTERNAL"
      {
        $$ = zetasql::ASTCreateExternalTableStatement::kConcreteNodeKind;
      }
    | "CREATE" opt_or_replace "ROW" opt_access "POLICY"
      { $$ = zetasql::ASTCreateRowAccessPolicyStatement::kConcreteNodeKind; }
    | "CREATE" next_statement_kind_create_modifiers opt_recursive "VIEW"
      {
        $$ = zetasql::ASTCreateViewStatement::kConcreteNodeKind;
      }
    | "CREATE" opt_or_replace "MATERIALIZED" opt_recursive "VIEW"
      { $$ = zetasql::ASTCreateMaterializedViewStatement::kConcreteNodeKind; }
    | "CALL"
      { $$ = zetasql::ASTCallStatement::kConcreteNodeKind; }
    | "RETURN"
      { $$ = zetasql::ASTReturnStatement::kConcreteNodeKind; }
    | "IMPORT"
      { $$ = zetasql::ASTImportStatement::kConcreteNodeKind; }
    | "MODULE"
      { $$ = zetasql::ASTModuleStatement::kConcreteNodeKind; }
    | "ASSERT"
      { $$ = zetasql::ASTAssertStatement::kConcreteNodeKind; }
    | "TRUNCATE"
      { $$ = zetasql::ASTTruncateStatement::kConcreteNodeKind; }
    ;

placeholder:;

%%

void zetasql_bison_parser::BisonParserImpl::error(
    const zetasql_bison_parser::location& loc,
    const std::string& msg) {
  *error_message = msg;
  *error_location = zetasql::ParseLocationPoint::FromByteOffset(
      parser->filename().ToStringView(), loc.begin.column);
}
