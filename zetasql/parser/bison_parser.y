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

%code requires {
// Bison parser for ZetaSQL. This works in conjunction with
// zetasql::parser::BisonParser.
//
// To debug the state machine in case of conflicts, run (locally):
// $ bison bison_parser.y -Wprecedence -Wcounterexamples -b tmp_prefix -r all \
//     --report-file=$HOME/bison_report.txt
// (Do NOT set the --report-file to a path on citc, because then the file will
// be truncated at 1MB for some reason.)

#include <cstdint>

#include "zetasql/parser/bison_parser.h"
#include "zetasql/parser/join_processor.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser_internal.h"
#include "zetasql/parser/statement_properties.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/strings.h"
#include "zetasql/base/case.h"
#include "absl/memory/memory.h"
#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_format.h"
#include "absl/status/status.h"

#define YYINITDEPTH 50
#ifndef YYDEBUG
#define YYDEBUG 0
#endif
}

%defines
%skeleton "lalr1.cc"
%define parse.error verbose
%define api.parser.class {BisonParserImpl}
%define api.location.type {zetasql::ParseLocationRange}

%initial-action
{
#if YYDEBUG
   set_debug_level(absl::GetFlag(FLAGS_zetasql_bison_parserdebug));
#endif
}

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
%lex-param {zetasql::parser::LookaheadTransformer* tokenizer}
%parse-param {zetasql::parser::LookaheadTransformer* tokenizer}
%parse-param {zetasql::parser::BisonParser* parser}
%parse-param {zetasql::ASTNode** ast_node_result}
%parse-param {zetasql::parser::ASTStatementProperties*
                  ast_statement_properties}
%parse-param {std::string* error_message}
%parse-param {zetasql::ParseLocationPoint* error_location}
%parse-param {int* statement_end_byte_offset}

%union {
  bool boolean;
  int64_t int64_val;
  struct {
    const char* str;
    size_t len;
  } string_view;
  const char* string_constant;
  zetasql::TypeKind type_kind;
  zetasql::ASTFunctionCall::NullHandlingModifier null_handling_modifier;
  zetasql::ASTWindowFrame::FrameUnit frame_unit;
  zetasql::ASTTemplatedParameterType::TemplatedTypeKind
      templated_parameter_kind;
  zetasql::ASTBinaryExpression::Op binary_op;
  zetasql::ASTUnaryExpression::Op unary_op;
  zetasql::ASTOptionsEntry::AssignmentOp options_assignment_op;
  zetasql::ASTJoin::JoinType join_type;
  zetasql::ASTJoin::JoinHint join_hint;
  zetasql::ASTSampleSize::Unit sample_size_unit;
  zetasql::ASTInsertStatement::InsertMode insert_mode;
  zetasql::ASTNodeKind ast_node_kind;
  zetasql::ASTUnpivotClause::NullFilter opt_unpivot_nulls_filter;
  zetasql::parser_internal::NotKeywordPresence not_keyword_presence;
  zetasql::parser_internal::AllOrDistinctKeyword all_or_distinct_keyword;
  zetasql::SchemaObjectKind schema_object_kind_keyword;
  zetasql::parser_internal::PrecedingOrFollowingKeyword
      preceding_or_following_keyword;
  zetasql::parser_internal::TableOrTableFunctionKeywords
      table_or_table_function_keywords;
  zetasql::parser_internal::IndexTypeKeywords
      index_type_keywords;
  zetasql::parser_internal::ShiftOperator shift_operator;
  zetasql::parser_internal::ImportType import_type;
  zetasql::ASTAuxLoadDataStatement::InsertionMode insertion_mode;
  zetasql::ASTCreateStatement::Scope create_scope;
  zetasql::ASTCreateStatement::SqlSecurity sql_security;
  zetasql::ASTCreateStatement::SqlSecurity external_security;
  zetasql::ASTDropStatement::DropMode drop_mode;
  zetasql::ASTForeignKeyReference::Match foreign_key_match;
  zetasql::ASTForeignKeyActions::Action foreign_key_action;
  zetasql::ASTFunctionParameter::ProcedureParameterMode parameter_mode;
  zetasql::ASTCreateFunctionStmtBase::DeterminismLevel determinism_level;
  zetasql::ASTGeneratedColumnInfo::StoredMode stored_mode;
  zetasql::ASTGeneratedColumnInfo::GeneratedMode generated_mode;
  zetasql::ASTOrderingExpression::OrderingSpec ordering_spec;
  zetasql::ASTSelectWith* select_with;
  zetasql::ASTSetOperationColumnMatchMode* column_match_mode;
  zetasql::ASTSetOperationColumnPropagationMode* column_propagation_mode;

  // Not owned. The allocated nodes are all owned by the parser.
  // Nodes should use the most specific type available.
  zetasql::ASTForeignKeyReference* foreign_key_reference;
  zetasql::ASTSetOperation* query_set_operation;
  zetasql::ASTInsertValuesRowList* insert_values_row_list;
  zetasql::ASTQuery* query;
  zetasql::ASTExpression* expression;
  zetasql::ASTExpressionSubquery* expression_subquery;
  zetasql::ASTPathExpression* path_expression;
  zetasql::ASTFunctionCall* function_call;
  zetasql::ASTAlias* alias;
  zetasql::ASTIdentifier* identifier;
  zetasql::ASTInsertStatement* insert_statement;
  zetasql::ASTNode* node;
  zetasql::ASTStatementList* statement_list;
  zetasql::parser_internal::SeparatedIdentifierTmpNode* slashed_identifier;
  zetasql::ASTPivotClause* pivot_clause;
  zetasql::ASTUnpivotClause* unpivot_clause;
  zetasql::ASTMatchRecognizeClause* match_recognize_clause;
  zetasql::ASTRowPatternExpression* row_pattern_expression;
  zetasql::ASTSetOperationType* set_operation_type;
  zetasql::ASTSetOperationAllOrDistinct* set_operation_all_or_distinct;
  zetasql::ASTBytesLiteral* bytes_literal;
  zetasql::ASTBytesLiteralComponent* bytes_literal_component;
  zetasql::ASTStringLiteral* string_literal;
  zetasql::ASTStringLiteralComponent* string_literal_component;
  zetasql::ASTPipeOperator* pipe_operator;
  zetasql::ASTSampleClause* sample_clause;
  struct {
    zetasql::ASTPivotClause* pivot_clause;
    zetasql::ASTUnpivotClause* unpivot_clause;
    zetasql::ASTAlias* alias;
  } pivot_or_unpivot_clause_and_alias;
  struct {
    zetasql::ASTMatchRecognizeClause* match_recognize_clause;
    zetasql::ASTSampleClause* sample_clause;
  } match_recognize_and_sample_clauses;
  struct {
    zetasql::ASTNode* where;
    zetasql::ASTNode* group_by;
    zetasql::ASTNode* having;
    zetasql::ASTNode* qualify;
    zetasql::ASTNode* window;
  } clauses_following_from;
  struct {
    zetasql::ASTExpression* default_expression;
    zetasql::ASTGeneratedColumnInfo* generated_column_info;
  } generated_or_default_column_info;
  struct {
    zetasql::ASTWithPartitionColumnsClause* with_partition_columns_clause;
    zetasql::ASTWithConnectionClause* with_connection_clause;
  } external_table_with_clauses;
  struct {
    zetasql::ASTIdentifier* language;
    bool is_remote;
    zetasql::ASTWithConnectionClause* with_connection_clause;
  } language_or_remote_with_connection;
  struct {
    zetasql::ASTIdentifier* language;
    zetasql::ASTNode* options;
  } language_options_set;
  struct {
    zetasql::ASTNode* options;
    zetasql::ASTNode* body;
  } options_body_set;
  struct {
    zetasql::ASTScript* body;
    zetasql::ASTIdentifier* language;
    zetasql::ASTNode* code;
  } begin_end_block_or_language_as_code;
  struct {
    zetasql::ASTExpression* maybe_dashed_path_expression;
    bool is_temp_table;
  } path_expression_with_scope;
  struct {
    zetasql::ASTSetOperationColumnMatchMode* column_match_mode;
    zetasql::ASTColumnList* column_list;
  } column_match_suffix;
  struct {
    zetasql::ASTQuery* query;
    zetasql::ASTPathExpression* replica_source;
  } query_or_replica_source_info;
  struct {
    zetasql::ASTNode* hint;
    bool and_order_by;
  } group_by_preamble;
  zetasql::ASTStructBracedConstructor* struct_braced_constructor;
  zetasql::ASTBracedConstructor* braced_constructor;
  zetasql::ASTBracedConstructorField* braced_constructor_field;
  zetasql::ASTBracedConstructorFieldValue* braced_constructor_field_value;

  struct {
    zetasql::ASTNode* partition_by;
    zetasql::ASTNode* options_list;
    zetasql::ASTNode* spanner_index_innerleaving_clause;
  } create_index_statement_suffix;
}
// YYEOF is a special token used to indicate the end of the input. It's alias
// defaults to "end of file", but "end of input" is more appropriate for us.
%token YYEOF 0 "end of input"

// These tokens are only used by the macro expander
%token DOLLAR_SIGN "$"
%token MACRO_INVOCATION "macro invocation"
%token MACRO_ARGUMENT_REFERENCE "macro argument reference"

// Literals and identifiers. String, bytes and identifiers are not unescaped by
// the tokenizer. This is done in the parser so that we can give better error
// messages, pinpointing specific error locations in the token. This is really
// helpful for e.g. invalid escape codes.
%token STRING_LITERAL "string literal"
%token BYTES_LITERAL "bytes literal"
%token INTEGER_LITERAL "integer literal"
%token FLOATING_POINT_LITERAL "floating point literal"
%token <string_view> IDENTIFIER "identifier"
%token BACKSLASH      // Only for lenient macro expansion

// Script labels. This is set apart from IDENTIFIER for two reasons:
// - Identifiers should still be disallowed at statement beginnings in all
//   other cases.
// - (Unreserved) Keywords don't need to be recognized as labels since
//   flex_tokenizer.l takes care of that.
%token SCRIPT_LABEL

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
%token KW_ADD_ASSIGN "+="
%token KW_SUB_ASSIGN "-="
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
%token KW_OPEN_HINT "@ for hint"
%token '}' "}"
%token '?' "?"
// This is the opening `@` in a standalone integer hint.
%token KW_OPEN_INTEGER_HINT "@n"
// This is the opening `@` in an integer hint followed by a @{...} hint.
%token OPEN_INTEGER_PREFIX_HINT
%token KW_SHIFT_LEFT "<<"
%token KW_SHIFT_RIGHT ">>"
%token KW_NAMED_ARGUMENT_ASSIGNMENT "=>"
%token KW_LAMBDA_ARROW "->"
%token KW_PIPE "|>"

// These are not used in the grammar. They are here for parity with the JavaCC
// tokenizer.
%token ':' ":"
%token '{' "{"

// Precedence for operator tokens. We use operator precedence parsing because it
// is *much* faster than recursive productions (~2x speedup). The operator
// precedence is defined by the order of the declarations here, with tokens
// specified in the same declaration having the same precedence.
//
// Precedences are a total order, so resolving any conflict using precedence has
// non-local effects. Only use precedences that are widely globally accepted,
// like multiplication binding tighter than addition.
//
// The fake DOUBLE_AT_PRECEDENCE symbol is introduced to resolve a shift/reduce
// conflict in the system_variable_expression rule. A potentially ambiguous
// input is "@@a.b". Without modifying the rule's precedence, this could be
// parsed as a system variable named "a" of type STRUCT or as a system variable
// named "a.b" (the ZetaSQL language chooses the latter).
%left "OR"
%left "AND"
%precedence UNARY_NOT_PRECEDENCE
%nonassoc "=" "<>" ">" "<" ">=" "<=" "!=" "LIKE" "IN" "DISTINCT" "BETWEEN" "IS" "NOT_SPECIAL" "+=" "-="
%left "|"
%left "^"
%left "&"
%left "<<" ">>"
%left "+" "-"
%left "||"
%left "*" "/"
%precedence UNARY_PRECEDENCE  // For all unary operators
%precedence DOUBLE_AT_PRECEDENCE // Needs to appear before "."

// We need "." to have high precedence for generalised names, but giving ( and [
// a precedence leads to burying some complex shift-reduce conflicts.
%left PRIMARY_PRECEDENCE "(" "[" "."

%code {
// NOYACC-START
#if YYDEBUG
ABSL_FLAG(bool, zetasql_bison_parserdebug, true, "Print traces for the ZetaSQL parser.");
#endif
// NOYACC-END

using namespace zetasql::parser_internal;
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

// This sentinal allocates an integer smaller than all the values used for
// reserved keywords. Together with SENTINEL_RESERVED_KW_END, a simple integer
// comparison can efficiently identify a token as a reserved keyword. This token
// is not produced by the lexer.
%token SENTINEL_RESERVED_KW_START
// TODO: Use the SENTINEL tokens instead of this comment.
// BEGIN_RESERVED_KEYWORDS -- Do not remove this!
%token KW_ALL "ALL"
%token KW_AND "AND"
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
%token KW_DEFINE_FOR_MACROS "DEFINE for macros"
%token KW_DEFINE "DEFINE"
%token KW_DESC "DESC"
%token KW_DISTINCT "DISTINCT"
%token KW_ELSE "ELSE"
%token KW_END "END"
%token KW_ENUM "ENUM"
%token KW_EXCEPT "EXCEPT"
%token KW_EXISTS "EXISTS"
%token KW_EXTRACT "EXTRACT"
%token KW_FALSE "FALSE"
%token KW_FOLLOWING "FOLLOWING"
%token KW_FOR "FOR"
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
%token KW_MATCH_RECOGNIZE_RESERVED
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
%token KW_GROUPS "GROUPS"
%token KW_LATERAL "LATERAL"
%token KW_OF "OF"
%token KW_SOME "SOME"
%token KW_TREAT "TREAT"
%token KW_WITHIN "WITHIN"
%token KW_QUALIFY_RESERVED
// END_RESERVED_KEYWORDS -- Do not remove this!
%token SENTINEL_RESERVED_KW_END  // See comment on SENTINEL_RESERVED_KW_END


// These tokens should not appear in the grammar rules. They are used in actions
// to provide context to the DisambiguationLexer layer so it can more
// effectively understand the context of previously seen tokens.
%token SENTINEL_LB_TOKEN_START
// Used to look back at a token that was guaranteed to be the opening token of
// a block of statements. This is particularly used in the "script" rules.
%token LB_OPEN_STATEMENT_BLOCK
// Used to look back to see that the previous token was KW_BEGIN and that it is
// known to be the first token in a statement. That could mean the opening
// BEGIN of a BEGIN...END statement block or it could mean the first keyword of
// BEGIN TRANSACTION.
%token LB_BEGIN_AT_STATEMENT_START
// Used to look back at an EXPLAIN token that was used at the start of a SQL
// statement to introduce EXPLAIN {stmt}.
%token LB_EXPLAIN_SQL_STATEMENT
// Used to lookback to see whether the previous hint is a statement-level hint.
%token LB_END_OF_STATEMENT_LEVEL_HINT
// Represents a "." token used in a path expression, as opposed to the "." in
// floating point literals. This token is only used as a lookback by the
// lookahead_transformer.
%token LB_DOT_IN_PATH_EXPRESSION
// Used to lookback to know whether the previous token was the paren before a
// nested DML statement.
%token LB_OPEN_NESTED_DML
// Two tokens that help the lookahead_transformer locate type templates.
%token LB_OPEN_TYPE_TEMPLATE
%token LB_CLOSE_TYPE_TEMPLATE
// Used as a lookback override in SELECT WITH <identifier> OPTIONS. See below
// for more details.
%token LB_WITH_IN_SELECT_WITH_OPTIONS
%token SENTINEL_LB_TOKEN_END

// The tokens in this section are reserved in the sense they cannot be used as
// identifiers in the parser. They are not produced directly by the lexer
// though, they are produced by disambiguation transformations after the main
// lexer.

%token KW_WITH_STARTING_WITH_GROUP_ROWS "WITH starting WITH GROUP ROWS"
%token KW_WITH_STARTING_WITH_EXPRESSION "WITH starting with expression"
%token KW_EXCEPT_IN_SET_OP "EXCEPT in set operation"
%token KW_FULL_IN_SET_OP
%token KW_LEFT_IN_SET_OP
%token KW_REPLACE_AFTER_INSERT
%token KW_UPDATE_AFTER_INSERT
// This is a different token because using KW_NOT for BETWEEN/IN/LIKE would
// confuse the operator precedence parsing. Boolean NOT has a different
// precedence than NOT BETWEEN/IN/LIKE.
%token KW_NOT_SPECIAL "NOT_SPECIAL"

// This token is used alongside LB_WITH_IN_SELECT_WITH_OPTIONS in The
// SELECT WITH <identifier> OPTIONS construct.
// There is a true ambiguity in the query prefix when using SELECT WITH OPTIONS.
// For example,
//   SELECT {opt_hint} WITH modification_kind OPTIONS(a = 1) alias, * FROM ...
// has two valid parses.
// 1. `OPTIONS(a = 1)` is an options list associated with WITH modification_kind
//    and `alias` is the first column in the select list
// 2. `OPTIONS(a = 1)` is a function call, `a = 1` is an expression computing
//    the argument, and `alias` is the column alias.
// This is a true ambiguity. Both parses are valid and no ammount of lookahead
// will eliminate the ambiguity. Our intention has been to force parse #1.
//
// A solution based on LALR(1) parser's "prefer shift" rule that chooses to
// shift options rather than reduce column name is not ideal because it causes
// collateral damage. Consider:
// * SELECT WITH mod options, more_columns...
//
// In this case, options cannot be an option list because it's not followed by
// `(`. LALR(1) shift/reduce resolution can't see that `(` because it takes two
// lookaheads. Instead, we use the disambiguation layer to solve this using
// two lookaheads.
//
// `LB_WITH_IN_SELECT_WITH_OPTIONS` is used exclusively as a lookback override.
// The override is triggered by the parser before consuming the `WITH` token
// in the context of SELECT WITH. Then token disambiguation layer looks for this
// window:
//   lookback1 : LB_WITH_IN_SELECT_WITH_OPTIONS
//   token     : IDENTIFER
//   lookahead1: KW_OPTIONS
//   lookahead2: '('
// When it sees this window it changes lookahead1 to
// `KW_OPTIONS_IN_SELECT_WITH_OPTIONS` so that there is no ambiguity in the
// parser.
%token KW_OPTIONS_IN_SELECT_WITH_OPTIONS

// A special token to indicate that an integer or floating point literal is
// immediately followed by an identifier without space, for example 123abc.
//
// This token is only used by the lookahead_transformer under the `kTokenizer`
// and `kTokenizerPreserveComments` mode to prevent the callers of
// GetParseTokens() to blindly inserting whitespaces between "123" and "abc".
// For example, when formatting "SELECT 123abc", which is invalid, the formatted
// SQL should not become "SELECT 123 abc", which is valid.
%token INVALID_LITERAL_PRECEDING_IDENTIFIER_NO_SPACE

// The following two tokens will be converted into INTEGER_LITERAL by the
// lookahead_transformer, and the parser should not use them directly.
%token DECIMAL_INTEGER_LITERAL
%token HEX_INTEGER_LITERAL

// Represents an exponent part without a sign used in a float literal, for
// example the "e10" in "1.23e10". It will gets fused into a floating point
// literal or becomes an identifier by the lookahead_transformer, and the parser
// should not use it directly.
%token EXP_IN_FLOAT_NO_SIGN

// Represents the exponent part "E" used in a float literal, for example the "e"
// in "1.23e+10". It will gets fused into a floating point literal or becomes an
// identifier by the lookahead_transformer, and the parser should not use it
// directly.
%token STANDALONE_EXPONENT_SIGN "e"

// Non-reserved keywords.  These can also be used as identifiers.
// These must all be listed explicitly in the "keyword_as_identifier" rule
// below. Do NOT include keywords in this list that are conditionally generated.
// They go in a separate list below this one.
//
// This sentinal allocates an integer smaller than all the values used for
// reserved keywords. Together with SENTINEL_RESERVED_KW_END, a simple integer
// comparison can efficiently identify a token as a reserved keyword. This token
// is not produced by the lexer.
%token SENTINEL_NONRESERVED_KW_START
// TODO: Use the SENTINEL tokens instead of this comment.
// BEGIN_NON_RESERVED_KEYWORDS -- Do not remove this!
%token KW_ABORT "ABORT"
%token KW_ACCESS "ACCESS"
%token KW_ACTION "ACTION"
%token KW_ADD "ADD"
%token KW_AGGREGATE "AGGREGATE"
%token KW_ALTER "ALTER"
%token KW_ALWAYS "ALWAYS"
%token KW_ANALYZE "ANALYZE"
%token KW_APPROX "APPROX"
%token KW_ARE "ARE"
%token KW_ASSERT "ASSERT"
%token KW_BATCH "BATCH"
%token KW_BEGIN "BEGIN"
%token KW_BIGDECIMAL "BIGDECIMAL"
%token KW_BIGNUMERIC "BIGNUMERIC"
%token KW_BREAK "BREAK"
%token KW_CALL "CALL"
%token KW_CASCADE "CASCADE"
%token KW_CHECK "CHECK"
%token KW_CLAMPED "CLAMPED"
%token KW_CLONE "CLONE"
%token KW_COPY "COPY"
%token KW_CLUSTER "CLUSTER"
%token KW_COLUMN "COLUMN"
%token KW_COLUMNS "COLUMNS"
%token KW_COMMIT "COMMIT"
%token KW_CONNECTION "CONNECTION"
%token KW_CONTINUE "CONTINUE"
%token KW_CONSTANT "CONSTANT"
%token KW_CONSTRAINT "CONSTRAINT"
%token KW_CYCLE "CYCLE"
%token KW_DATA "DATA"
%token KW_DATABASE "DATABASE"
%token KW_DATE "DATE"
%token KW_DATETIME "DATETIME"
%token KW_DECIMAL "DECIMAL"
%token KW_DECLARE "DECLARE"
%token KW_DEFINER "DEFINER"
%token KW_DELETE "DELETE"
%token KW_DELETION "DELETION"
%token KW_DEPTH "DEPTH"
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
%token KW_EXTEND "EXTEND"
%token KW_EXTERNAL "EXTERNAL"
%token KW_FILES "FILES"
%token KW_FILTER "FILTER"
%token KW_FILL "FILL"
%token KW_FIRST "FIRST"
%token KW_FOREIGN "FOREIGN"
%token KW_FORMAT "FORMAT"
%token KW_FUNCTION "FUNCTION"
%token KW_GENERATED "GENERATED"
%token KW_GRANT "GRANT"
%token KW_GROUP_ROWS "GROUP_ROWS"
%token KW_HIDDEN "HIDDEN"
%token KW_IDENTITY "IDENTITY"
%token KW_IMMEDIATE "IMMEDIATE"
%token KW_IMMUTABLE "IMMUTABLE"
%token KW_IMPORT "IMPORT"
%token KW_INCLUDE "INCLUDE"
%token KW_INCREMENT "INCREMENT"
%token KW_INDEX "INDEX"
%token KW_INOUT "INOUT"
%token KW_INPUT "INPUT"
%token KW_INSERT "INSERT"
%token KW_INVOKER "INVOKER"
%token KW_ITERATE "ITERATE"
%token KW_ISOLATION "ISOLATION"
%token KW_JSON "JSON"
%token KW_KEY "KEY"
%token KW_LANGUAGE "LANGUAGE"
%token KW_LAST "LAST"
%token KW_LEAVE "LEAVE"
%token KW_LEVEL "LEVEL"
%token KW_LOAD "LOAD"
%token KW_LOOP "LOOP"
%token KW_MACRO "MACRO"
%token KW_MAP "MAP"
%token KW_MATCH "MATCH"
%token KW_MATCH_RECOGNIZE_NONRESERVED
%token KW_MATCHED "MATCHED"
%token KW_MATERIALIZED "MATERIALIZED"
%token KW_MAX "MAX"
%token KW_MAXVALUE "MAXVALUE"
%token KW_MEASURES "MEASURES"
%token KW_MESSAGE "MESSAGE"
%token KW_METADATA "METADATA"
%token KW_MIN "MIN"
%token KW_MINVALUE "MINVALUE"
%token KW_MODEL "MODEL"
%token KW_MODULE "MODULE"
%token KW_NUMERIC "NUMERIC"
%token KW_OFFSET "OFFSET"
%token KW_ONLY "ONLY"
%token KW_OPTIONS "OPTIONS"
%token KW_OUT "OUT"
%token KW_OUTPUT "OUTPUT"
%token KW_OVERWRITE "OVERWRITE"
%token KW_PARTITIONS "PARTITIONS"
%token KW_PATTERN "PATTERN"
%token KW_PERCENT "PERCENT"
%token KW_PIVOT "PIVOT"
%token KW_POLICIES "POLICIES"
%token KW_POLICY "POLICY"
%token KW_PRIMARY "PRIMARY"
%token KW_PRIVATE "PRIVATE"
%token KW_PRIVILEGE "PRIVILEGE"
%token KW_PRIVILEGES "PRIVILEGES"
%token KW_PROCEDURE "PROCEDURE"
%token KW_PROJECT "PROJECT"
%token KW_PUBLIC "PUBLIC"
%token KW_QUALIFY_NONRESERVED
%token KW_RAISE "RAISE"
%token KW_READ "READ"
%token KW_REFERENCES "REFERENCES"
%token KW_REMOTE "REMOTE"
%token KW_REMOVE "REMOVE"
%token KW_RENAME "RENAME"
%token KW_REPEAT "REPEAT"
%token KW_REPEATABLE "REPEATABLE"
%token KW_REPLACE "REPLACE"
%token KW_REPLACE_FIELDS "REPLACE_FIELDS"
%token KW_REPLICA "REPLICA"
%token KW_REPORT "REPORT"
%token KW_RESTRICT "RESTRICT"
%token KW_RESTRICTION "RESTRICTION"
%token KW_RETURN "RETURN"
%token KW_RETURNS "RETURNS"
%token KW_REVOKE "REVOKE"
%token KW_ROLLBACK "ROLLBACK"
%token KW_ROW "ROW"
%token KW_RUN "RUN"
%token KW_SAFE_CAST "SAFE_CAST"
%token KW_SCHEMA "SCHEMA"
%token KW_SEARCH "SEARCH"
%token KW_SECURITY "SECURITY"
%token KW_SEQUENCE "SEQUENCE"
%token KW_SETS "SETS"
%token KW_SHOW "SHOW"
%token KW_SIMPLE "SIMPLE"
%token KW_SNAPSHOT "SNAPSHOT"
%token KW_SOURCE "SOURCE"
%token KW_SQL "SQL"
%token KW_STABLE "STABLE"
%token KW_START "START"
%token KW_STATIC_DESCRIBE "STATIC_DESCRIBE"
%token KW_STORED "STORED"
%token KW_STORING "STORING"
%token KW_SYSTEM "SYSTEM"
%token KW_SYSTEM_TIME "SYSTEM_TIME"
%token KW_TABLE "TABLE"
%token KW_TABLES "TABLES"
%token KW_TARGET "TARGET"
%token KW_TRANSFORM "TRANSFORM"
%token KW_TEMP "TEMP"
%token KW_TEMPORARY "TEMPORARY"
%token KW_TIME "TIME"
%token KW_TIMESTAMP "TIMESTAMP"
%token KW_TRANSACTION "TRANSACTION"
%token KW_TRUNCATE "TRUNCATE"
%token KW_TYPE "TYPE"
%token KW_UNDROP "UNDROP"
%token KW_UNIQUE "UNIQUE"
%token KW_UNKNOWN "UNKNOWN"
%token KW_UNPIVOT "UNPIVOT"
%token KW_UNTIL "UNTIL"
%token KW_UPDATE "UPDATE"
%token KW_VALUE "VALUE"
%token KW_VALUES "VALUES"
%token KW_VECTOR "VECTOR"
%token KW_VOLATILE "VOLATILE"
%token KW_VIEW "VIEW"
%token KW_VIEWS "VIEWS"
%token KW_WEIGHT "WEIGHT"
%token KW_WHILE "WHILE"
%token KW_WRITE "WRITE"
%token KW_ZONE "ZONE"
%token KW_EXCEPTION "EXCEPTION"
%token KW_ERROR "ERROR"
%token KW_CORRESPONDING "CORRESPONDING"
%token KW_STRICT "STRICT"

// Spanner-specific keywords
%token KW_INTERLEAVE "INTERLEAVE"
%token KW_NULL_FILTERED "NULL_FILTERED"
%token KW_PARENT "PARENT"

// END_NON_RESERVED_KEYWORDS -- Do not remove this!
%token SENTINEL_NONRESERVED_KW_END

// This is not a keyword token. It represents all identifiers that are
// CURRENT_* functions for date/time.
%token KW_CURRENT_DATETIME_FUNCTION

// When in parser mode kMacroBody, any token other than YYEOF or ';' will be
// emitted as MACRO_BODY_TOKEN. This prevents the parser from needing to
// enumerate all token kinds to implement the macro body rule.
%token MACRO_BODY_TOKEN

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
%type <node> analyze_statement
%type <node> any_some_all
%type <node> array_column_schema_inner
%type <expression> array_constructor
%type <expression> array_constructor_prefix
%type <expression> array_constructor_prefix_no_expressions
%type <node> array_type
%type <query_or_replica_source_info> query_or_replica_source
%type <node> as_query
%type <node> as_sql_function_body_or_string
%type <node> assert_statement
%type <node> begin_statement
%type <node> aux_load_data_from_files_options_list
%type <node> aux_load_data_statement
%type <node> opt_load_data_partitions_clause
%type <node> load_data_partitions_clause
%type <path_expression_with_scope> maybe_dashed_path_expression_with_scope
%type <expression> bignumeric_literal
%type <expression> boolean_literal
%type <bytes_literal> bytes_literal
%type <bytes_literal_component> bytes_literal_component
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
%type <node> create_connection_statement
%type <node> create_database_statement
%type <node> create_function_statement
%type <node> create_procedure_statement
%type <node> create_privilege_restriction_statement
%type <node> create_row_access_policy_grant_to_clause
%type <node> create_row_access_policy_statement
%type <node> create_external_table_statement
%type <node> create_external_table_function_statement
%type <node> create_index_statement
%type <node> create_schema_statement
%type <node> create_external_schema_statement
%type <node> create_table_function_statement
%type <node> create_model_statement
%type <node> create_snapshot_statement
%type <node> create_table_statement
%type <node> create_view_statement
%type <node> create_entity_statement
%type <node> cube_list
%type <node> undrop_statement
%type <node> column_with_options
%type <node> column_with_options_list
%type <node> column_with_options_list_prefix
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
%type <node> export_model_statement
%type <node> export_metadata_statement
%type <expression> expression
                   unparenthesized_expression_higher_prec_than_and
                   expression_higher_prec_than_and
                   parenthesized_expression_not_a_query
                   expression_maybe_parenthesized_not_a_query
                   and_expression
                   or_expression
%type <node> expression_with_opt_alias
%type <node> unnest_expression_prefix
%type <node> generic_entity_type_unchecked
%type <node> generic_entity_type
%type <node> generic_sub_entity_type
%type <identifier> sub_entity_type_identifier
%type <node> grant_to_clause
%type <node> restrict_to_clause
%type <node> opt_restrict_to_clause
%type <node> index_storing_expression_list_prefix
%type <node> index_storing_expression_list
%type <expression> interval_expression
%type <expression> expression_or_default
%type <expression_subquery> expression_subquery_with_keyword
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
%type <expression> function_call_expression_with_clauses
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
%type <node> grantee_list_with_parens_prefix
%type <boolean> opt_and_order
%type <node> group_by_clause_prefix
%type <node> group_by_all
%type <group_by_preamble> group_by_preamble
%type <node> opt_selection_item_order
%type <node> opt_grouping_item_order
%type <node> grouping_item
%type <node> grouping_set
%type <node> grouping_set_list
%type <node> hint
%type <node> statement_level_hint
%type <node> hint_entry
%type <node> hint_with_body
%type <node> hint_with_body_prefix
%type <identifier> identifier
%type <identifier> token_identifier
%type <identifier> label
%type <identifier> identifier_in_hints
%type <node> if_statement
%type <node> elseif_clauses
%type <node> when_then_clauses
%type <node> case_statement
%type <node> opt_expression
%type <node> execute_immediate
%type <node> opt_execute_into_clause
%type <node> opt_execute_using_clause
%type <node> execute_using_argument
%type <node> execute_using_argument_list
%type <expression> expression_or_proto
%type <node> opt_elseif_clauses
%type <node> begin_end_block
%type <node> opt_exception_handler
%type <node> if_statement_unclosed
%type <node> break_statement
%type <node> continue_statement
%type <node> return_statement
%type <node> loop_statement
%type <node> while_statement
%type <node> until_clause
%type <node> repeat_statement
%type <node> for_in_statement
%type <node> import_statement
%type <node> variable_declaration
%type <node> opt_default_expression
%type <node> identifier_list
%type <node> path_expression_list
%type <node> path_expression_list_with_opt_parens
%type <node> path_expression_list_prefix
%type <node> path_expression_list_with_parens
%type <node> opt_path_expression_list_with_parens
%type <node> set_statement
%type <node> index_order_by_and_options
%type <node> index_all_columns
%type <node> index_order_by_and_options_prefix
%type <node> index_storing_list
%type <node> index_unnest_expression_list
%type <node> in_list_two_or_more_prefix
%type <node> clone_data_source
%type <node> clone_data_source_list
%type <node> clone_data_statement
%type <node> copy_data_source
%type <insert_statement> insert_statement
%type <insert_statement> insert_statement_prefix
%type <insert_values_row_list> insert_values_list
%type <node> insert_values_or_query
%type <node> insert_values_row
%type <node> insert_values_row_prefix
%type <expression> int_literal_or_parameter
%type <expression> opt_int_literal_or_parameter
%type <expression> integer_literal
%type <node> join
%type <node> join_input
%type <expression> json_literal
%type <expression> lambda_argument
%type <node> lambda_argument_list
%type <node> define_macro_statement
%type <node> macro_body
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
%type <braced_constructor_field_value> braced_constructor_field_value
%type <braced_constructor_field> braced_constructor_field
%type <braced_constructor_field> braced_constructor_extension
%type <braced_constructor> braced_constructor_start
%type <braced_constructor> braced_constructor_prefix
%type <braced_constructor> braced_constructor
%type <expression> braced_new_constructor
%type <expression> struct_braced_constructor
%type <node> next_statement
%type <node> next_script_statement
%type <expression> null_literal
%type <node> null_order
%type <node> opt_null_order
%type <expression> numeric_literal
%type <node> on_clause
%type <node> opt_and_expression
%type <alias> opt_as_alias
%type <alias> opt_as_alias_with_required_as
%type <node> opt_as_or_into_alias
%type <node> opt_as_string_or_integer
%type <node> opt_as_query
%type <node> opt_as_query_or_string
%type <node> opt_as_query_or_aliased_query_list
%type <node> opt_as_sql_function_body_or_string
%type <node> opt_as_code
%type <node> opt_assert_rows_modified
%type <node> opt_clamped_between_modifier
%type <node> opt_clone_table
%type <node> opt_column_with_options_list
%type <node> opt_copy_table
%type <column_match_suffix> opt_column_match_suffix
%type <column_propagation_mode> opt_corresponding_outer_mode
%type <column_propagation_mode> opt_strict
%type <node> set_operation_metadata
%type <node> opt_cluster_by_clause_no_hint
%type <node> opt_with_report_modifier
%type <node> collate_clause
%type <node> opt_collate_clause
%type <node> opt_default_collate_clause
%type <node> opt_column_list
%type <node> opt_constraint_identity
%type <node> opt_create_row_access_policy_grant_to_clause
%type <create_scope> opt_create_scope
%type <node> opt_at_system_time
%type <node> opt_description
%type <expression> opt_at_time_zone
%type <node> opt_format
%type <drop_mode> opt_drop_mode
%type <insertion_mode> append_or_overwrite
%type <expression> sequence_arg
%type <node> opt_else
%type <external_table_with_clauses> opt_external_table_with_clauses
%type <node> opt_on_path_expression
%type <node> opt_foreign_key_actions
%type <node> opt_from_clause
%type <node> from_clause
%type <expression> opt_from_path_expression
%type <node> opt_function_parameters
%type <node> opt_function_returns
%type <node> group_by_clause
%type <node> opt_group_by_clause
%type <node> opt_group_by_clause_with_opt_comma
%type <node> generic_entity_body
%type <node> opt_generic_entity_body
%type <node> having_clause
%type <node> opt_having_clause
%type <node> opt_having_or_group_by_modifier
%type <node> opt_hint
%type <identifier> opt_identifier
%type <node> opt_index_storing_list
%type <node> opt_index_unnest_expression_list
%type <node> opt_input_output_clause
%type <identifier> language
%type <identifier> opt_language
%type <language_or_remote_with_connection> opt_language_or_remote_with_connection
%type <language_options_set> unordered_language_options
%type <options_body_set> unordered_options_body
%type <node> opt_like_string_literal
%type <node> opt_like_path_expression
%type <node> limit_offset_clause
%type <node> opt_limit_offset_clause
%type <node> opt_on_or_using_clause_list
%type <node> on_or_using_clause_list
%type <node> on_or_using_clause
%type <node> opt_on_or_using_clause
%type <expression> on_path_expression
%type <node> options
%type <node> opt_options_list
%type <node> opt_order_by_clause
%type <node> opt_over_clause
%type <node> opt_partition_by_clause
%type <node> opt_partition_by_clause_no_hint
%type <node> opt_qualify_clause
%type <node> qualify_clause_nonreserved
%type <node> qualify_clause_reserved
%type <node> opt_qualify_clause_reserved
%type <node> opt_repeatable_clause
%type <language_or_remote_with_connection> opt_remote_with_connection_clause
%type <node> opt_returns
%type <node> opt_returning_clause
%type <sql_security> opt_sql_security_clause
%type <sql_security> sql_security_clause_kind
%type <external_security> opt_external_security_clause
%type <external_security> external_security_clause_kind
%type <node> opt_table_and_column_info_list
%type <node> opt_with_report_format
%type <node> pivot_value
%type <node> unpivot_in_item
%type <node> pivot_value_list
%type <node> unpivot_in_item_list
%type <node> unpivot_in_item_list_prefix
%type <pivot_clause> pivot_clause
%type <unpivot_clause> unpivot_clause
%type <node> pivot_expression
%type <node> pivot_expression_list
%type <match_recognize_clause> match_recognize_clause
// Use a combined struct to make sure they always are applied together, to avoid
// forgetting one of them.
%type <match_recognize_and_sample_clauses> opt_match_recognize_and_sample_clauses
%type <match_recognize_clause> opt_match_recognize_clause
%type <row_pattern_expression> row_pattern_expr
%type <row_pattern_expression> row_pattern_concatenation
%type <row_pattern_expression> row_pattern_factor
%type <sample_clause> opt_sample_clause
%type <node> opt_sample_clause_suffix
%type <node> opt_select_as_clause
%type <node> opt_table_element_list
%type <node> opt_transaction_mode_list
%type <node> opt_transform_clause
%type <node> opt_ttl_clause
%type <clauses_following_from> opt_clauses_following_from
%type <clauses_following_from> opt_clauses_following_where
%type <clauses_following_from> opt_clauses_following_group_by
%type <node> where_clause
%type <node> opt_where_clause
%type <node> opt_where_expression
%type <node> opt_window_clause
%type <node> opt_window_frame_clause
%type <node> opt_with_group_rows
%type <node> opt_with_offset_and_alias
%type <node> options_entry
%type <node> options_list
%type <node> options_list_prefix
%type <node> order_by_clause_prefix
%type <node> order_by_clause
%type <node> order_by_clause_with_opt_comma
%type <node> ordering_expression
%type <node> column_ordering_and_options_expr
%type <node> all_column_column_options
%type <node> opt_with_column_options
%type <expression> named_parameter_expression
%type <expression> parameter_expression
%type <expression> system_variable_expression
%type <node> parenthesized_in_rhs
%type <node> parenthesized_anysomeall_list_in_rhs
%type <node> partition_by_clause_prefix
%type <node> partition_by_clause_prefix_no_hint
%type <path_expression> path_expression
%type <slashed_identifier> dashed_identifier
%type <slashed_identifier> slashed_identifier
%type <expression> slashed_path_expression
%type <expression> dashed_path_expression
%type <expression> maybe_dashed_path_expression
%type <expression> maybe_slashed_or_dashed_path_expression
%type <node> path_expression_or_string
%type <expression> path_expression_or_default
%type <expression> possibly_cast_int_literal_or_parameter
%type <node> possibly_empty_grantee_list
%type <node> primary_key_or_table_constraint_spec
%type <node> primary_key_element
%type <node> primary_key_element_list
%type <node> primary_key_element_list_prefix
%type <node> primary_key_spec
%type <node> privilege
%type <node> privilege_list
%type <node> privilege_name
%type <node> privileges
%type <query> query parenthesized_query
%type <node> query_primary
%type <node> query_primary_or_set_operation
%type <node> query_set_operation
%type <query_set_operation> query_set_operation_prefix
%type <node> query_statement
%type <expression> range_literal
%type <node> range_type
%type <node> raw_type
%type <node> raw_column_schema_inner
%type <language_or_remote_with_connection> remote_with_connection_clause
%type <node> repeatable_clause
%type <expression> generalized_extension_path
%type <node> replace_fields_arg
%type <expression> replace_fields_prefix
%type <expression> replace_fields_expression
%type <node> rename_statement
%type <node> revoke_statement
%type <node> rollback_statement
%type <node> rollup_list
%type <node> privilege_restriction_alter_action
%type <node> privilege_restriction_alter_action_list
%type <node> row_access_policy_alter_action
%type <node> row_access_policy_alter_action_list
%type <node> run_batch_statement
%type <sample_clause> sample_clause
%type <node> sample_size
%type <expression> sample_size_value
%type <node> select
%type <node> select_clause
%type <node> select_column
%type <node> select_column_expr
%type <node> select_column_expr_with_as_alias
%type <node> select_column_dot_star
%type <node> select_column_star
%type <node> select_list
%type <node> select_list_prefix
%type <node> select_list_prefix_with_as_aliases
%type <node> with_expression_variable
%type <node> with_expression_variable_prefix
%type <expression> with_expression
%type <node> show_statement
%type <identifier> show_target
%type <expression> signed_numerical_literal
%type <node> simple_column_schema_inner
%type <string_literal_component> string_literal_component
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
%type <statement_list> unterminated_non_empty_statement_list
%type <statement_list> unterminated_non_empty_top_level_statement_list
%type <string_literal> string_literal
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
%type <node> function_type_prefix
%type <node> function_type
%type <node> table_clause
%type <node> table_column_definition
%type <node> table_column_schema
%type <node> table_constraint_definition
%type <node> table_constraint_spec
%type <node> table_element
%type <node> table_element_list
%type <node> table_element_list_prefix
%type <node> table_and_column_info
%type <node> table_and_column_info_list
%type <node> table_path_expression
%type <node> table_path_expression_base
%type <node> table_primary
%type <node> table_subquery
%type <node> templated_parameter_type
%type <node> transaction_mode
%type <node> transaction_mode_list
%type <node> truncate_statement
%type <node> tvf_with_suffixes
%type <node> tvf
%type <node> tvf_argument
%type <node> tvf_prefix
%type <node> tvf_prefix_no_args
%type <node> type
%type <expression> type_parameter
%type <node> type_parameters_prefix
%type <node> opt_type_parameters
%type <node> type_or_tvf_schema
%type <node> tvf_schema
%type <node> tvf_schema_column
%type <node> tvf_schema_prefix
%type <node> type_name
%type <node> unnest_expression
%type <node> unnest_expression_with_opt_alias_and_offset
%type <node> unterminated_statement
             unterminated_sql_statement
             unterminated_script_statement
             unterminated_unlabeled_script_statement
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
%type <node> opt_with_clause
%type <node> with_clause_with_trailing_comma
%type <node> with_connection_clause
%type <node> aliased_query
%type <node> aliased_query_list
%type <node> aliased_query_modifiers
%type <node> possibly_unbounded_int_literal_or_parameter
%type <node> recursion_depth_modifier
%type <node> opt_with_connection_clause
%type <node> alter_action_list
%type <node> alter_action
%type <expression> named_argument
%type <expression> opt_array_zip_mode
%type <node> column_position
%type <node> opt_column_position
%type <expression> fill_using_expression
%type <expression> opt_fill_using_expression
%type <set_operation_all_or_distinct> all_or_distinct
%type <all_or_distinct_keyword> opt_all_or_distinct
%type <schema_object_kind_keyword> schema_object_kind
%type <node> range_column_schema_inner
%type <node> map_type

%type <not_keyword_presence> between_operator
%type <not_keyword_presence> in_operator
%type <not_keyword_presence> is_operator
%type <not_keyword_presence> like_operator
%type <not_keyword_presence> distinct_operator

%type <preceding_or_following_keyword> preceding_or_following

%type <shift_operator> shift_operator

%type <import_type> import_type

%type <stored_mode> stored_mode
%type <generated_mode> generated_mode

%type <table_or_table_function_keywords> table_or_table_function
%type <index_type_keywords> index_type
%type <index_type_keywords> opt_index_type
%type <boolean> opt_access
%type <boolean> opt_aggregate
%type <ordering_spec> asc_or_desc
%type <ordering_spec> opt_asc_or_desc
%type <boolean> opt_filter
%type <boolean> opt_if_exists
%type <boolean> opt_if_not_exists
%type <boolean> opt_natural
%type <boolean> opt_not_aggregate
%type <boolean> opt_or_replace
%type <boolean> opt_overwrite
%type <boolean> opt_recursive
%type <boolean> opt_unique
%type <select_with> opt_select_with;
%type <node> primary_key_column_attribute
%type <node> hidden_column_attribute
%type <node> not_null_column_attribute
%type <node> column_attribute
%type <node> column_attributes
%type <node> generated_column_info
%type <boolean> invalid_generated_column
%type <node> default_column_info
%type <boolean> invalid_default_column
%type <generated_or_default_column_info> opt_column_info
%type <node> opt_column_attributes
%type <node> opt_field_attributes
%type <node> raise_statement

%type <node> identity_column_info
%type <node> opt_start_with
%type <node> opt_increment_by
%type <node> opt_maxvalue
%type <node> opt_minvalue
%type <boolean> opt_cycle

%type <string_constant> bad_keyword_after_from_query
%type <string_constant> bad_keyword_after_from_query_allows_parens
%type <query> query_without_pipe_operators
%type <pipe_operator> pipe_operator
%type <pipe_operator> pipe_where_clause
%type <pipe_operator> pipe_select_clause
%type <pipe_operator> pipe_limit_offset_clause
%type <pipe_operator> pipe_order_by_clause
%type <pipe_operator> pipe_extend_clause
%type <node> pipe_selection_item
%type <node> pipe_selection_item_list
%type <node> pipe_selection_item_list_no_comma
%type <node> pipe_selection_item_with_order
%type <node> pipe_selection_item_list_no_comma_with_order
%type <node> pipe_selection_item_list_with_order
%type <node> pipe_selection_item_list_with_order_or_empty
%type <pipe_operator> pipe_rename_item
%type <pipe_operator> pipe_rename_item_list
%type <pipe_operator> pipe_rename
%type <pipe_operator> pipe_aggregate_clause
%type <pipe_operator> pipe_group_by
%type <pipe_operator> pipe_set_operation_clause
%type <pipe_operator> pipe_join
%type <pipe_operator> pipe_call
%type <pipe_operator> pipe_window_clause
%type <pipe_operator> pipe_distinct
%type <pipe_operator> pipe_tablesample
%type <pipe_operator> pipe_as
%type <pipe_operator> pipe_static_describe
%type <pipe_operator> pipe_assert_base
%type <pipe_operator> pipe_assert
%type <node> identifier_in_pipe_drop
%type <node> identifier_list_in_pipe_drop
%type <pipe_operator> pipe_drop
%type <node> pipe_set_item
%type <pipe_operator> pipe_set_item_list
%type <pipe_operator> pipe_set
%type <pipe_operator> pipe_pivot
%type <pipe_operator> pipe_unpivot

%type <binary_op> additive_operator
%type <binary_op> comparative_operator
%type <options_assignment_op> options_assignment_operator
%type <join_hint> join_hint
%type <join_type> join_type
%type <opt_unpivot_nulls_filter> opt_unpivot_nulls_filter
%type <binary_op> multiplicative_operator
%type <ast_node_kind> next_statement_kind
%type <ast_node_kind> next_statement_kind_parenthesized_select
%type <ast_node_kind> next_statement_kind_without_hint
%type <ast_node_kind> next_statement_kind_create_modifiers
%type <set_operation_type> query_set_operation_type
%type <sample_size_unit> sample_size_unit
%type <insert_mode> opt_or_ignore_replace_update
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
%type <node> with_partition_columns_clause
%type <pivot_or_unpivot_clause_and_alias> opt_pivot_or_unpivot_clause_and_alias
%type <begin_end_block_or_language_as_code> begin_end_block_or_language_as_code

// Spanner-specific non-terminals
%type <boolean> opt_spanner_null_filtered
%type <node> spanner_index_interleave_clause
%type <node> opt_spanner_interleave_in_parent_clause
%type <node> opt_spanner_generated_or_default
%type <node> opt_spanner_not_null_attribute
%type <node> opt_spanner_table_options
%type <node> spanner_alter_column_action
%type <node> spanner_generated_or_default
%type <node> spanner_set_on_delete_action
%type <node> spanner_primary_key

%type <create_index_statement_suffix> opt_create_index_statement_suffix
// End of Spanner-specific non-terminals

// AMBIGUOUS CASES
// ===============
//
// AMBIGUOUS CASE 1: SAFE_CAST(...)
// --------------------------------
// The SAFE_CAST keyword is non-reserved and can be used as an identifier. This
// causes one shift/reduce conflict between keyword_as_identifier and the rule
// that starts with "SAFE_CAST" "(". It is resolved in favor of the SAFE_CAST(
// rule, which is the desired behavior.
//
//
// AMBIGUOUS CASE 2: CREATE TABLE FUNCTION
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
// AMBIGUOUS CASE 3: CREATE TABLE CONSTRAINTS
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
// AMBIGUOUS CASE 4: REPLACE_FIELDS(...)
// --------------------------------
// The REPLACE_FIELDS keyword is non-reserved and can be used as an identifier.
// This causes a shift/reduce conflict between keyword_as_identifier and the
// rule that starts with "REPLACE_FIELDS" "(". It is resolved in favor of the
// REPLACE_FIELDS( rule, which is the desired behavior.
//
// AMBIGUOUS CASE 5: Procedure parameter list in CREATE PROCEDURE
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
// AMBIGUOUS CASE 6: CREATE TABLE GENERATED
// -------------------------------------------------------------
// The GENERATED keyword is non-reserved, so when a generated column is defined
// with "<name> [<type>] GENERATED AS ()", we have a shift/reduce conflict, not
// knowing whether the word GENERATED is an identifier from <type> or the
// keyword GENERATED because <type> is missing. By default, Bison chooses
// "shift", treating GENERATED as a keyword. To use it as an identifier, it
// needs to be escaped with backticks.
//
// AMBIGUOUS CASE 7: DESCRIPTOR(...)
// --------------------------------
// The DESCRIPTOR keyword is non-reserved and can be used as an identifier. This
// causes one shift/reduce conflict between keyword_as_identifier and the rule
// that starts with "DESCRIPTOR" "(". It is resolved in favor of DESCRIPTOR(
// rule, which is the desired behavior.
//
// AMBIGUOUS CASE 8: ANALYZE OPTIONS(...)
// --------------------------------
// The OPTIONS keyword is non-reserved and can be used as an identifier.
// This causes a shift/reduce conflict between keyword_as_identifier and the
// rule that starts with "ANALYZE"  "OPTIONS" "(". It is resolved in favor of
// the OPTIONS( rule, which is the desired behavior.
//
// AMBIGUOUS CASE 9: SELECT * FROM T QUALIFY
// --------------------------------
// The QUALIFY keyword is non-reserved and can be used as an identifier.
// This causes a shift/reduce conflict between keyword_as_identifier and the
// rule that starts with "QUALIFY". It is resolved in favor of the QUALIFY rule,
// which is the desired behavior. Currently this is only used to report
// error messages to user when QUALIFY clause is used without
// WHERE/GROUP BY/HAVING.
//
// AMBIGUOUS CASE 10: ALTER COLUMN
// --------------------------------
// Spanner DDL compatibility extensions provide support for Spanner flavor of
// ALTER COLUMN action, which expects full column definition instead of
// sub-action. Column type identifier in this definition causes 2 shift/reduce
// conflicts with
//   ALTER COLUMN... DROP DEFAULT
//   ALTER COLUMN... DROP NOT NULL actions
// In both cases when encountering DROP, bison might either choose to shift
// (e.g. interpret DROP as keyword and proceed with one of the 2 rules above),
// or reduce DROP as type identifier in Spanner-specific rule. Bison chooses to
// shift, which is a desired behavior.
//
// AMBIGUOUS CASE 11: SEQUENCE CLAMPED
// ----------------------------------
// MyFunction(SEQUENCE clamped)
// Resolve to a function call passing a SEQUENCE input argument type.
//
// MyFunction(sequence clamped between x and y)
// Resolve to a function call passing a column 'sequence' modified
// with "clamped between x and y".
//
// Bison favors reducing the 2nd form to an error, so we add a lexer rule to
// force SEQUENCE followed by clamped to resolve to an identifier.
// So bison still thinks there is a conflict but the lexer
// will _never_ produce:
// ... KW_SEQUENCE KW_CLAMPED ...
// it instead produces
// ... IDENTIFIER KW_CLAMPED
// Which will resolve toward the second form
// (sequence clamped between x and y) correctly, and the first form (
// sequence clamped) will result in an error.
//
// In other contexts, CLAMPED will also act as an identifier via the
// keyword_as_identifier rule.
//
// If the user wants to reference a sequence called 'clamped', they must
// identifier quote it (SEQUENCE `clamped`);
//
// Total expected shift/reduce conflicts as described above:
//   1: SAFE CAST
//   3: CREATE TABLE FUNCTION
//   2: CREATE TABLE CONSTRAINTS
//   1: REPLACE FIELDS
//   4: CREATE PROCEDURE
//   1: CREATE TABLE GENERATED
//   1: CREATE EXTERNAL TABLE FUNCTION
//   1: DESCRIPTOR
//   1: ANALYZE
//   5: QUALIFY
//   2: ALTER COLUMN
//   1: SUM(SEQUENCE CLAMPED BETWEEN x and y)
%expect 23

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


opt_semicolon: ";" | %empty ;

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
        SetForceTerminate(tokenizer, statement_end_byte_offset);
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
        SetForceTerminate(tokenizer, statement_end_byte_offset);
        $$ = $1;
      }
    | unterminated_sql_statement
      {
        // There's no semicolon. That means we have to be at EOF.
        *statement_end_byte_offset = -1;
        $$ = $1;
      }
    ;

// This rule exists to run an action before parsing a statement irrespective
// of whether or not the statement is a sql statement or a script statement.
// This shape is recommended in the Bison manual.
// See https://www.gnu.org/software/bison/manual/bison.html#Midrule-Conflicts
//
// We override the lookback of BEGIN here, and not locally in begin_end_block
// to avoid shift/reduce conflicts with the BEGIN TRANSACTION statement. The
// alternative lookback in this case meerly asserts that BEGIN is a keyword
// at the beginning of a statement, which is true both for being end blocks
// and for BEGIN TRANSACTION.
pre_statement:
    %empty
    { OVERRIDE_NEXT_TOKEN_LOOKBACK(KW_BEGIN, LB_BEGIN_AT_STATEMENT_START); }

unterminated_statement:
  pre_statement unterminated_sql_statement[stmt]
    {
      @$ = @stmt;
      $$ = $stmt;
    }
  | pre_statement unterminated_script_statement[stmt]
    {
      @$ = @stmt;
      $$ = $stmt;
    }
  ;

statement_level_hint:
  hint
    {
      OVERRIDE_CURRENT_TOKEN_LOOKBACK(@hint, LB_END_OF_STATEMENT_LEVEL_HINT);
      $$ = $hint;
    }
  ;

unterminated_sql_statement:
    sql_statement_body
    | statement_level_hint[hint] sql_statement_body
      {
        $$ = MAKE_NODE(ASTHintedStatement, @$, {$hint, $sql_statement_body});
      }
    | "DEFINE" "MACRO"[kw_macro]
      {
        if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_4_SQL_MACROS)) {
          YYERROR_AND_ABORT_AT(@kw_macro, "Macros are not supported");
        }
        // Rule to capture wrong usage, where DEFINE MACRO is resulting from
        // expanding other macros, instead of being original user input.
        YYERROR_AND_ABORT_AT(
          @kw_macro,
          "Syntax error: DEFINE MACRO statements cannot be composed from other "
          "expansions");
      }
    | statement_level_hint[hint] "DEFINE"[kw_define] "MACRO"
      {
        YYERROR_AND_ABORT_AT(
          @hint, "Hints are not allowed on DEFINE MACRO statements.");
      }
    | statement_level_hint[hint] KW_DEFINE_FOR_MACROS "MACRO"
      {
        // This is here for extra future-proofing. We should never hit this
        // codepath, because the expander should only generate
        // KW_DEFINE_FOR_MACROS when it's the first token in the statement,
        // ignoring comments.
        ABSL_DLOG(FATAL) << "KW_DEFINE_FOR_MACROS should only appear as the "
                       "first token in a statement.";
        YYERROR_AND_ABORT_AT(
          @hint, "Hints are not allowed on DEFINE MACRO statements");
      }
    ;

unterminated_unlabeled_script_statement:
    begin_end_block
    | while_statement
    | loop_statement
    | repeat_statement
    | for_in_statement
    ;

unterminated_script_statement:
    if_statement
    | case_statement
    | variable_declaration
    | break_statement
    | continue_statement
    | return_statement
    | raise_statement
    | unterminated_unlabeled_script_statement
    | label ":"
      // We override the lookback of BEGIN here, and not locally in
      // begin_end_block to avoid shift/reduce conflicts with the
      // BEGIN TRANSACTION statement in the higher level unterminated_statement
      // rule. In this case we provide the LB_OPEN_STATEMENT_BLOCK lookback
      // because only BEGIN of a statement block can follow a label. If
      // BEGIN TRANSACTION is changed to accept a label, then we want to change
      // this lookback to `LB_BEGIN_AT_STATEMENT_START`.
      { OVERRIDE_NEXT_TOKEN_LOOKBACK(KW_BEGIN, LB_OPEN_STATEMENT_BLOCK); }
      unterminated_unlabeled_script_statement[stmt] opt_identifier[end_label]
      {
        CHECK_LABEL_SUPPORT($label, @label);
        if ($end_label != nullptr &&
            !$end_label->GetAsIdString().CaseEquals($label->GetAsIdString())) {
          YYERROR_AND_ABORT_AT(
              @end_label,
              absl::StrCat("Mismatched end label; expected ",
                           $label->GetAsStringView(), ", got ",
                           $end_label->GetAsStringView()));
        }
        auto label = MAKE_NODE(ASTLabel, @label, {$label});
        $stmt->AddChildFront(label);
        $$ = parser->WithLocation($stmt, @$);
      }
    ;

sql_statement_body:
    query_statement
    | alter_statement
    | analyze_statement
    | assert_statement
    | aux_load_data_statement
    | clone_data_statement
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
    | create_connection_statement
    | create_database_statement
    | create_function_statement
    | create_procedure_statement
    | create_index_statement
    | create_privilege_restriction_statement
    | create_row_access_policy_statement
    | create_external_table_statement
    | create_external_table_function_statement
    | create_model_statement
    | create_schema_statement
    | create_external_schema_statement
    | create_snapshot_statement
    | create_table_function_statement
    | create_table_statement
    | create_view_statement
    | create_entity_statement
    | define_macro_statement
    | define_table_statement
    | describe_statement
    | execute_immediate
    | explain_statement
    | export_data_statement
    | export_model_statement
    | export_metadata_statement
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
    | undrop_statement
    ;

define_macro_statement:
    // Use a special version of KW_DEFINE which indicates that this macro
    // definition was "original" (i.e., not expanded from other macros), and
    // is top-level (i.e., not nested under other statements or blocks like IF).
    KW_DEFINE_FOR_MACROS "MACRO"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_4_SQL_MACROS)) {
          YYERROR_AND_ABORT_AT(@2, "Macros are not supported");
        }
        PushBisonParserMode(tokenizer,
            zetasql::parser::BisonParserMode::kMacroBody);
      }
      MACRO_BODY_TOKEN[name] macro_body[body]
      {
        absl::string_view name = parser->GetInputText(@name);
        absl::Status is_identifier_or_keyword =
          IsIdentifierOrKeyword(name);
        if (!is_identifier_or_keyword.ok()) {
         YYERROR_AND_ABORT_AT(@name,
           absl::StrCat("Syntax error: ", is_identifier_or_keyword.message()));
       }

        if (name.front() == '`') {
          name = name.substr(1, name.length()-2);
        }

        PopBisonParserMode(tokenizer);
        $$ = MAKE_NODE(ASTDefineMacroStatement,
                       @$,
                       {parser->MakeIdentifier(@name, name), $body});
      }
    ;

// We are using the tokenizer to find the end of the DEFINE MACRO statement.
// We need to store the body. Ideally, we would keep the tokens to avoid having
// to re-tokenize the body when processing an invocation of this macro.
// However, current frameworks and APIs represent macros as strings. More
// importantly, comments may still be needed as they are used by some
// environments as a workaround for the lack of annotations. Consequently,
// after finding the full macro_body, we discard the tokens, and just store the
// input text, including whitespace and comments. When the environment has been
// upgraded to store tokens (which would require us to standardize token kinds
// and codes since they will be stored externally), we can store the tokens
// themselves.
macro_body:
    %empty
      {
        auto* macro_body = MAKE_NODE(ASTMacroBody, @$);
        macro_body->set_image("");
        $$ = macro_body;
      }
    | macro_token_list
      {
        auto* macro_body = MAKE_NODE(ASTMacroBody, @$);
        macro_body->set_image(std::string(parser->GetInputText(@1)));
        $$ = macro_body;
      }
    ;

macro_token_list:
    MACRO_BODY_TOKEN
    | macro_token_list MACRO_BODY_TOKEN
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
    | "SET" "AS" generic_entity_body[body]
      // See (broken link)
      {
        $$ = MAKE_NODE(ASTSetAsAction, @$, {$body});
      }
    | "ADD" table_constraint_spec
      {
        $$ = MAKE_NODE(ASTAddConstraintAction, @$, {$2});
      }
    | "ADD" primary_key_spec
      {
        $$ = MAKE_NODE(ASTAddConstraintAction, @$, {$2});
      }
    | "ADD" "CONSTRAINT" opt_if_not_exists identifier
        primary_key_or_table_constraint_spec
      {
        auto* constraint = $5;
        constraint->AddChild($4);
        parser->WithStartLocation(constraint, @4);
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
    | "DROP" "PRIMARY" "KEY" opt_if_exists
      {
        auto* node = MAKE_NODE(ASTDropPrimaryKeyAction, @$, {});
        node->set_is_if_exists($4);
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
    | "RENAME" "COLUMN" opt_if_exists identifier "TO" identifier
      {
        auto* node = MAKE_NODE(ASTRenameColumnAction, @$, {$4, $6});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "COLUMN" opt_if_exists identifier "SET" "DATA" "TYPE"
          field_schema
      {
        auto* node = MAKE_NODE(ASTAlterColumnTypeAction, @$, {$4, $8});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "COLUMN" opt_if_exists identifier "SET" "OPTIONS" options_list
      {
        auto* node = MAKE_NODE(ASTAlterColumnOptionsAction, @$, {$4, $7});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "COLUMN" opt_if_exists identifier "SET" "DEFAULT" expression
      {
        auto* node = MAKE_NODE(ASTAlterColumnSetDefaultAction, @$,{$4, $7});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "COLUMN" opt_if_exists identifier "DROP" "DEFAULT"
      {
        auto* node = MAKE_NODE(ASTAlterColumnDropDefaultAction, @$, {$4});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "COLUMN" opt_if_exists identifier "DROP" "NOT" "NULL"
      {
        auto* node = MAKE_NODE(ASTAlterColumnDropNotNullAction, @$, {$4});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" "COLUMN" opt_if_exists identifier "DROP" "GENERATED"
      {
        auto* node =
            MAKE_NODE(ASTAlterColumnDropGeneratedAction, @$, {$identifier});
        node->set_is_if_exists($opt_if_exists);
        $$ = node;
      }
    | "RENAME" "TO" path_expression
      {
        $$ = MAKE_NODE(ASTRenameToClause, @$, {$3});
      }
    | "SET" "DEFAULT" collate_clause
      {
        $$ = MAKE_NODE(ASTSetCollateClause, @$, {$3});
      }
    | "ADD" "ROW" "DELETION" "POLICY" opt_if_not_exists "(" expression ")"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_4_TTL)) {
          YYERROR_AND_ABORT_AT(@2,
            "ADD ROW DELETION POLICY clause is not supported.");
        }
        auto* node = MAKE_NODE(ASTAddTtlAction, @$, {$7});
        node->set_is_if_not_exists($5);
        $$ = node;
      }
    | "REPLACE" "ROW" "DELETION" "POLICY" opt_if_exists "(" expression ")"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_4_TTL)) {
          YYERROR_AND_ABORT_AT(@2,
            "REPLACE ROW DELETION POLICY clause is not supported.");
        }
        auto* node = MAKE_NODE(ASTReplaceTtlAction, @$, {$7});
        node->set_is_if_exists($5);
        $$ = node;
      }
    | "DROP" "ROW" "DELETION" "POLICY" opt_if_exists
      {
        if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_4_TTL)) {
          YYERROR_AND_ABORT_AT(@2,
            "DROP ROW DELETION POLICY clause is not supported.");
        }
        auto* node = MAKE_NODE(ASTDropTtlAction, @$, {});
        node->set_is_if_exists($5);
        $$ = node;
      }
    | "ALTER" generic_sub_entity_type opt_if_exists identifier alter_action
      {
        auto* node = MAKE_NODE(ASTAlterSubEntityAction, @$, {$2, $4, $5});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ADD" generic_sub_entity_type opt_if_not_exists identifier
      opt_options_list
      {
        auto* node = MAKE_NODE(ASTAddSubEntityAction, @$, {$2, $4, $5});
        node->set_is_if_not_exists($3);
        $$ = node;
      }
    | "DROP" generic_sub_entity_type opt_if_exists identifier
      {
        auto* node = MAKE_NODE(ASTDropSubEntityAction, @$, {$2, $4});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | spanner_alter_column_action
    | spanner_set_on_delete_action
    ;

alter_action_list:
    alter_action
      {
        $$ = MAKE_NODE(ASTAlterActionList, @$, {$1});
      }
    | alter_action_list "," alter_action
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

// This is split up from the other ALTER actions since the alter actions for
// PRIVILEGE RESTRICTION are only used by PRIVILEGE RESTRICTION at the moment.
privilege_restriction_alter_action:
    restrict_to_clause
    | "ADD" opt_if_not_exists possibly_empty_grantee_list
      {
        auto* node = MAKE_NODE(ASTAddToRestricteeListClause, @$, {$3});
        node->set_is_if_not_exists($2);
        $$ = node;
      }
    | "REMOVE" opt_if_exists possibly_empty_grantee_list
      {
        auto* node = MAKE_NODE(
            ASTRemoveFromRestricteeListClause, @$, {$3}
        );
        node->set_is_if_exists($2);
        $$ = node;
      }
    ;

// This is split up from the other ALTER actions since the alter actions for
// PRIVILEGE RESTRICTION are only used by PRIVILEGE RESTRICTION at the moment.
privilege_restriction_alter_action_list:
    privilege_restriction_alter_action
      {
        $$ = MAKE_NODE(ASTAlterActionList, @$, {$1});
      }
    | privilege_restriction_alter_action_list ","
    privilege_restriction_alter_action
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

// This is split up from the other ALTER actions since the alter actions for
// ROW ACCESS POLICY are only used by ROW ACCESS POLICY at the moment.
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
        zetasql::ASTPathExpression* id =
            MAKE_NODE(ASTPathExpression, @3, {$3});
        $$ = MAKE_NODE(ASTRenameToClause, @$, {id});
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
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

// Note - this excludes the following objects:
// - ROW ACCESS POLICY for tactical reasons, since the production rules for
//   ALTER and DROP require very different syntax for ROW ACCESS POLICY as
//   compared to other object kinds.  So we do not want to match
//   ROW ACCESS POLICY here.
// - TABLE, TABLE FUNCTION, and SNAPSHOT TABLE since we use different production
//   for table path expressions (one which may contain dashes).
// - SEARCH INDEX since the DROP SEARCH INDEX has an optional ON <table> clause.
// - VECTOR INDEX since the DROP VECTOR INDEX has an optional ON <table> clause.
schema_object_kind:
    "AGGREGATE" "FUNCTION"
      { $$ = zetasql::SchemaObjectKind::kAggregateFunction; }
    | "APPROX" "VIEW"
      { $$ = zetasql::SchemaObjectKind::kApproxView; }
    | "CONNECTION"
      { $$ = zetasql::SchemaObjectKind::kConnection; }
    | "CONSTANT"
      { $$ = zetasql::SchemaObjectKind::kConstant; }
    | "DATABASE"
      { $$ = zetasql::SchemaObjectKind::kDatabase; }
    | "EXTERNAL" table_or_table_function {
        if ($2 == TableOrTableFunctionKeywords::kTableAndFunctionKeywords) {
            YYERROR_AND_ABORT_AT(@1,
               "EXTERNAL TABLE FUNCTION is not supported");
        } else {
           $$ = zetasql::SchemaObjectKind::kExternalTable;
        }
      }
    | "EXTERNAL" "SCHEMA"
      { $$ = zetasql::SchemaObjectKind::kExternalSchema; }
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
    | "SCHEMA"
      { $$ = zetasql::SchemaObjectKind::kSchema; }
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
        // Only ALTER DATABASE, SCHEMA, TABLE, VIEW, MATERIALIZED VIEW,
        // APPROX VIEW and MODEL are currently supported.
        if ($2 == zetasql::SchemaObjectKind::kApproxView) {
          node = MAKE_NODE(ASTAlterApproxViewStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kConnection) {
          node = MAKE_NODE(ASTAlterConnectionStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kDatabase) {
          node = MAKE_NODE(ASTAlterDatabaseStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kSchema) {
          node = MAKE_NODE(ASTAlterSchemaStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kExternalSchema) {
          node = MAKE_NODE(ASTAlterExternalSchemaStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kView) {
          node = MAKE_NODE(ASTAlterViewStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kMaterializedView) {
          node = MAKE_NODE(ASTAlterMaterializedViewStatement, @$);
        } else if ($2 == zetasql::SchemaObjectKind::kModel) {
          node = MAKE_NODE(ASTAlterModelStatement, @$);
        } else {
          YYERROR_AND_ABORT_AT(@2, absl::StrCat("ALTER ", absl::AsciiStrToUpper(
            parser->GetInputText(@2)), " is not supported"));
        }
        node->set_is_if_exists($3);
        node->AddChildren({$4, $5});
        $$ = parser->WithLocation(node, @$);
      }
    | "ALTER" generic_entity_type opt_if_exists path_expression
      alter_action_list
      {
        auto* node = MAKE_NODE(ASTAlterEntityStatement, @$, {$2, $4, $5});
        node->set_is_if_exists($3);
        $$ = node;
      }
    | "ALTER" generic_entity_type opt_if_exists alter_action_list
      {
        if (parser->language_options().LanguageFeatureEnabled(
               zetasql::FEATURE_ALLOW_MISSING_PATH_EXPRESSION_IN_ALTER_DDL)) {
          auto* node = MAKE_NODE(ASTAlterEntityStatement, @$, {$2, nullptr, $4});
          node->set_is_if_exists($3);
          $$ = node;
        } else {
          // alter_action_list always starts with a keyword
          YYERROR_AND_ABORT_AT(
              @4, absl::StrCat("Syntax error: Unexpected keyword ",
                               parser->GetFirstTokenOfNode(@4)));
        }
      }
    | "ALTER" "PRIVILEGE" "RESTRICTION" opt_if_exists
      "ON" privilege_list "ON" identifier path_expression
      privilege_restriction_alter_action_list
      {
        auto* alter_privilege_restriction = MAKE_NODE(
            ASTAlterPrivilegeRestrictionStatement, @$, {$6, $8, $9, $10});
        alter_privilege_restriction->set_is_if_exists($4);
        $$ = alter_privilege_restriction;
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

// Uses table_element_list to reduce redundancy.
// However, constraints clauses are not allowed.
opt_input_output_clause:
    "INPUT" table_element_list "OUTPUT" table_element_list
      {
        auto* input = $2->GetAsOrDie<zetasql::ASTTableElementList>();
        if (input->HasConstraints()) {
          YYERROR_AND_ABORT_AT(@2,
                "Syntax error: Element list contains unexpected constraint");
        }
        auto* output = $4->GetAsOrDie<zetasql::ASTTableElementList>();
        if (output->HasConstraints()) {
          YYERROR_AND_ABORT_AT(@4,
                "Syntax error: Element list contains unexpected constraint");
        }
        $$ = MAKE_NODE(ASTInputOutputClause, @$, {$2, $4});
      }
    | %empty { $$ = nullptr; }
    ;

opt_transform_clause:
    "TRANSFORM" "(" select_list ")"
      {
        $$ = MAKE_NODE(ASTTransformClause, @$, {$3})
      }
    | %empty { $$ = nullptr; }
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
    | %empty
      {
        $$ = nullptr;
      }
    ;

analyze_statement:
    "ANALYZE" opt_options_list opt_table_and_column_info_list
      {
        $$ = MAKE_NODE(ASTAnalyzeStatement, @$, {$2, $3});
      }
    ;

opt_table_and_column_info_list:
    table_and_column_info_list
    | %empty { $$ = nullptr; }
    ;

table_and_column_info_list:
    table_and_column_info
      {
        $$ = MAKE_NODE(ASTTableAndColumnInfoList, @$, {$1});
      }
    | table_and_column_info_list "," table_and_column_info
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

table_and_column_info:
    maybe_dashed_path_expression opt_column_list
      {
        $$ = MAKE_NODE(ASTTableAndColumnInfo, @$, {$1, $2});
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
    | %empty
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
    | %empty
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

unordered_options_body:
    options opt_as_sql_function_body_or_string[body]
      {
        $$.options = $options;
        $$.body = $body;
      }
    | as_sql_function_body_or_string[body] opt_options_list[options]
      {
        if ($options != nullptr) {
          parser->AddWarning(parser->GenerateWarning(
              "The preferred style places the OPTIONS clause before the "
              "function body.",
              (@options).start().GetByteOffset()));
        }
        $$.options = $options;
        $$.body = $body;
      }
    | %empty
      {
        $$.options = nullptr;
        $$.body = nullptr;
      }
    ;

create_function_statement:
    // The preferred style is LANGUAGE OPTIONS BODY but LANGUAGE BODY OPTIONS
    // is allowed for backwards compatibility (with a deprecation warning).
    "CREATE" opt_or_replace opt_create_scope opt_aggregate
        "FUNCTION" opt_if_not_exists function_declaration opt_function_returns
        opt_sql_security_clause opt_determinism_level
        opt_language_or_remote_with_connection[language]
        unordered_options_body[uob]
      {
        auto* create = MAKE_NODE(
            ASTCreateFunctionStatement, @$,
            {$function_declaration, $opt_function_returns, $language.language,
             $language.with_connection_clause, $uob.body, $uob.options});
        create->set_is_or_replace($opt_or_replace);
        create->set_scope($opt_create_scope);
        create->set_is_aggregate($opt_aggregate);
        create->set_is_if_not_exists($opt_if_not_exists);
        create->set_sql_security($opt_sql_security_clause);
        create->set_determinism_level($opt_determinism_level);
        create->set_is_remote($language.is_remote);
        $$ = create;
      }
    ;

// Returns true if AGGREGATE is present, false otherwise.
opt_aggregate:
    "AGGREGATE" { $$ = true; }
    | %empty { $$ = false; }
    ;

// Returns true if NOT AGGREGATE is present, false otherwise.
opt_not_aggregate:
    "NOT" "AGGREGATE" { $$ = true; }
    | %empty { $$ = false; }
    ;

function_declaration:
    path_expression function_parameters
      {
        $$ = MAKE_NODE(ASTFunctionDeclaration, @$, {$1, $2});
      }
    ;

function_parameter:
    identifier type_or_tvf_schema opt_as_alias_with_required_as
      opt_default_expression opt_not_aggregate
      {
        auto* parameter = MAKE_NODE(ASTFunctionParameter, @$, {$1, $2, $3, $4});
        parameter->set_is_not_aggregate($5);
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        $$ = MAKE_NODE(ASTFunctionParameters, @$);
      }
    ;

begin_end_block_or_language_as_code:
    begin_end_block
      {
        zetasql::ASTStatementList* stmt_list = MAKE_NODE(
            ASTStatementList, @1, {$1});
        zetasql::ASTScript* body = MAKE_NODE(ASTScript, @1, {stmt_list});
        $$.body = body;
        $$.language = nullptr;
        $$.code = nullptr;
      }
    | "LANGUAGE" identifier opt_as_code
      {
        if (parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_NON_SQL_PROCEDURE)) {
          $$.body = nullptr;
          $$.language = $2;
          $$.code = $3;
        } else {
          YYERROR_AND_ABORT_AT(@1, "LANGUAGE is not supported");
        }
      }
    ;

opt_external_security_clause:
    "EXTERNAL" "SECURITY" external_security_clause_kind { $$ = $3; }
    | %empty
      {
        $$ = zetasql::ASTCreateStatement::SQL_SECURITY_UNSPECIFIED;
      }
    ;

external_security_clause_kind:
  "INVOKER" { $$ = zetasql::ASTCreateStatement::SQL_SECURITY_INVOKER; }
  | "DEFINER" { $$ = zetasql::ASTCreateStatement::SQL_SECURITY_DEFINER; }

create_procedure_statement:
    "CREATE" opt_or_replace opt_create_scope "PROCEDURE" opt_if_not_exists
    path_expression procedure_parameters opt_external_security_clause
    opt_with_connection_clause opt_options_list
    begin_end_block_or_language_as_code
    {
      auto* create =
          MAKE_NODE(ASTCreateProcedureStatement, @$,
                    {$6, $7, $10, $11.body, $9, $11.language, $11.code});
      create->set_is_or_replace($2);
      create->set_scope($3);
      create->set_is_if_not_exists($5);
      create->set_external_security($8);
      $$ = create;
    }

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
        $$ = parser->WithEndLocation($1, @$);
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
    | %empty
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
    | %empty
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
    "DETERMINISTIC" {$$ = zetasql::ASTCreateFunctionStmtBase::DETERMINISTIC;}
    | "NOT" "DETERMINISTIC"
      {$$ = zetasql::ASTCreateFunctionStmtBase::NOT_DETERMINISTIC;}
    | "IMMUTABLE"
      {$$ = zetasql::ASTCreateFunctionStmtBase::IMMUTABLE;}
    | "STABLE"
      {$$ = zetasql::ASTCreateFunctionStmtBase::STABLE;}
    | "VOLATILE"
      {$$ = zetasql::ASTCreateFunctionStmtBase::VOLATILE;}
    | %empty
      {$$ = zetasql::ASTCreateFunctionStmtBase::DETERMINISM_UNSPECIFIED;}
    ;

language:
    "LANGUAGE" identifier
      {
        $$ = $2;
      }
    ;

opt_language:
    language
      {
        $$ = $language;
      }
    | %empty
      {
        $$ = nullptr;
      }
    ;

remote_with_connection_clause:
    "REMOTE" opt_with_connection_clause
      {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_REMOTE_FUNCTION)) {
          YYERROR_AND_ABORT_AT(@1, "Keyword REMOTE is not supported");
        }

        $$.language = nullptr;
        $$.is_remote = true;
        if ($2 == nullptr) {
          $$.with_connection_clause = nullptr;
        } else {
          $$.with_connection_clause =
              $2->GetAsOrDie<zetasql::ASTWithConnectionClause>();
        }
      }
    | with_connection_clause
      {
        $$.language = nullptr;
        $$.is_remote = false;
        if ($1 == nullptr) {
          $$.with_connection_clause = nullptr;
        } else {
          if (!parser->language_options().LanguageFeatureEnabled(
                  zetasql::FEATURE_V_1_3_REMOTE_FUNCTION) &&
              !parser->language_options().LanguageFeatureEnabled(
                  zetasql::FEATURE_V_1_4_CREATE_FUNCTION_LANGUAGE_WITH_CONNECTION)) {
            YYERROR_AND_ABORT_AT(@1, "WITH CONNECTION clause is not supported");
          }
          $$.with_connection_clause =
              $1->GetAsOrDie<zetasql::ASTWithConnectionClause>();
        }
      }
    ;

opt_remote_with_connection_clause:
    remote_with_connection_clause
      {
        $$ = $1;
      }
    | %empty
      {
        $$.language = nullptr;
        $$.is_remote = false;
        $$.with_connection_clause = nullptr;
      }
    ;


opt_language_or_remote_with_connection:
    "LANGUAGE" identifier opt_remote_with_connection_clause
      {
        $$ = $3;
        $$.language = $2;
      }
    | remote_with_connection_clause opt_language
      {
        $$ = $1;
        $$.language = $2;
      }
    |  %empty
      {
        $$.language = nullptr;
        $$.is_remote = false;
        $$.with_connection_clause = nullptr;
      }
    ;


opt_sql_security_clause:
    "SQL" "SECURITY" sql_security_clause_kind { $$ = $3; }
    | %empty
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
    | %empty
      {
        $$ = nullptr;
      }
    ;

opt_as_code:
    "AS" string_literal
      {
        $$ = $2;
      }
    | %empty
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

path_expression_or_default:
    path_expression
      {
        $$ = $1;
      }
    | "DEFAULT"
      {
        $$ = MAKE_NODE(ASTDefaultLiteral, @$, {});
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

// Parens are required for statements where this clause can be one of many
// actions, so that it's unambiguous where the restrictee list ends and the next
// action begins.
restrict_to_clause:
    "RESTRICT" "TO" possibly_empty_grantee_list
      {
        zetasql::ASTRestrictToClause* node =
            MAKE_NODE(ASTRestrictToClause, @$, {$3});
        $$ = node;
      }
    ;

opt_restrict_to_clause:
    restrict_to_clause
      {
        $$ = $1;
      }
    | %empty
      {
        $$ = nullptr;
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
    | %empty
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
    | %empty
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

create_privilege_restriction_statement:
    "CREATE" opt_or_replace "PRIVILEGE" "RESTRICTION" opt_if_not_exists
    "ON" privilege_list "ON" identifier path_expression
    opt_restrict_to_clause
      {
        zetasql::ASTCreatePrivilegeRestrictionStatement* node =
            MAKE_NODE(ASTCreatePrivilegeRestrictionStatement, @$,
                      {$7, $9, $10, $11});
        node->set_is_or_replace($2);
        node->set_is_if_not_exists($5);
        $$ = node;
      }
    ;

create_row_access_policy_statement:
    "CREATE" opt_or_replace "ROW" opt_access "POLICY" opt_if_not_exists
        opt_identifier "ON" path_expression
        opt_create_row_access_policy_grant_to_clause filter_using_clause
      {
        zetasql::ASTPathExpression* opt_path_expression =
            $7 == nullptr ? nullptr : MAKE_NODE(ASTPathExpression, @7, {$7});
        zetasql::ASTCreateRowAccessPolicyStatement* create =
            MAKE_NODE(ASTCreateRowAccessPolicyStatement, @$,
                      {$9, $10, $11, opt_path_expression});
        create->set_is_or_replace($2);
        create->set_is_if_not_exists($6);
        create->set_has_access_keyword($4);
        $$ = create;
      }
    ;

with_partition_columns_clause:
    "WITH" "PARTITION" "COLUMNS" opt_table_element_list
      {
        zetasql::ASTWithPartitionColumnsClause* with_partition_columns =
            MAKE_NODE(ASTWithPartitionColumnsClause, @$, {$4});
        $$ = with_partition_columns;
      }
      ;

with_connection_clause:
    "WITH" connection_clause
      {
        $$ = MAKE_NODE(ASTWithConnectionClause, @$, {$2});
      }

// An ideal solution would be to combine the rules
// 'opt_with_partition_columns_clause opt_with_connection_clause' directly in
// create_external_table_statement. However, this leads to a shift/reduce
// confilict, as when the parser sees:
// CREATE EXTERNAL TABLE t WITH ...
// it can either apply a shift, trying to match it with a
// with_partition_columns_clause, or it can apply a reduce (reducing
// opt_with_partition_columns_clause to empty), trying to match it with a
// with_connection_clause. We workaround this by combining the rules into a
// single production rule and with one empty (Nothing) option.
opt_external_table_with_clauses:
    with_partition_columns_clause with_connection_clause {
      $$.with_partition_columns_clause =
          $1->GetAsOrDie<zetasql::ASTWithPartitionColumnsClause>();
      $$.with_connection_clause =
          $2->GetAsOrDie<zetasql::ASTWithConnectionClause>();
    }
    | with_partition_columns_clause {
      $$.with_partition_columns_clause =
          $1->GetAsOrDie<zetasql::ASTWithPartitionColumnsClause>();
      $$.with_connection_clause = nullptr;
    }
    | with_connection_clause {
      $$.with_partition_columns_clause = nullptr;
      $$.with_connection_clause =
          $1->GetAsOrDie<zetasql::ASTWithConnectionClause>();
    }
    | %empty {
      $$.with_partition_columns_clause = nullptr;
      $$.with_connection_clause = nullptr;
    }
    ;

create_external_table_statement:
    "CREATE" opt_or_replace opt_create_scope "EXTERNAL"
    "TABLE" opt_if_not_exists maybe_dashed_path_expression
    opt_table_element_list opt_like_path_expression opt_default_collate_clause
    opt_external_table_with_clauses opt_options_list
      {
        if ($12 == nullptr) {
          YYERROR_AND_ABORT_AT(
              @12,
              "Syntax error: Expected keyword OPTIONS");
        }
        auto* create =
            MAKE_NODE(ASTCreateExternalTableStatement, @$,
            {$7, $8, $9, $10, $11.with_partition_columns_clause,
             $11.with_connection_clause, $12});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($6);
        $$ = create;
      }
    ;

// This rule encounters a shift/reduce conflict with
// 'create_external_table_statement' as noted in AMBIGUOUS CASE 3 in the
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

// This rule encounters a shift/reduce conflict with 'create_index_statement'
// if "PARTITION BY" and "INTERLEAVING IN" are both present. This is because in
// "PARTITION BY", a "," can be used to separate the partition columns, while
// "INTERLEAVING IN" is leading by a ",".
// To avoid this conflict, in the create index suffix, we do not allow partition
// by and interleaving in to be present at the same time.
opt_create_index_statement_suffix:
    partition_by_clause_prefix_no_hint opt_options_list
      {
        $$ = {$partition_by_clause_prefix_no_hint, $opt_options_list,
              /*spanner_index_innerleaving_clause=*/nullptr};
      }
    | opt_options_list spanner_index_interleave_clause
      {
        $$ = {/*partition_by=*/nullptr, $opt_options_list,
              $spanner_index_interleave_clause};
      }
    | options
      {
        $$ = {/*partition_by=*/nullptr, $options,
              /*spanner_index_innerleaving_clause=*/nullptr};
      }
    | %empty
      {
        $$ = {/*partition_by=*/nullptr, /*opt_options_list=*/nullptr,
              /*spanner_index_innerleaving_clause=*/nullptr};
      }
    ;

create_index_statement:
    "CREATE" opt_or_replace opt_unique opt_spanner_null_filtered opt_index_type
    "INDEX" opt_if_not_exists path_expression on_path_expression opt_as_alias
    opt_index_unnest_expression_list index_order_by_and_options
    opt_index_storing_list opt_create_index_statement_suffix
      {
        auto* create =
          MAKE_NODE(ASTCreateIndexStatement, @$,
              {$path_expression, $on_path_expression, $opt_as_alias,
              $opt_index_unnest_expression_list, $index_order_by_and_options,
              $opt_index_storing_list,
              $opt_create_index_statement_suffix.partition_by,
              $opt_create_index_statement_suffix.options_list,
              $opt_create_index_statement_suffix.spanner_index_innerleaving_clause});
        create->set_is_or_replace($opt_or_replace);
        create->set_is_unique($opt_unique);
        create->set_is_if_not_exists($opt_if_not_exists);
        create->set_spanner_is_null_filtered($opt_spanner_null_filtered);
        if ($opt_index_type == IndexTypeKeywords::kSearch) {
          create->set_is_search(true);
        } else if ($opt_index_type == IndexTypeKeywords::kVector) {
          create->set_is_vector(true);
        }
        $$ = create;
      }
    ;

create_schema_statement:
    "CREATE" opt_or_replace "SCHEMA" opt_if_not_exists path_expression
    opt_default_collate_clause opt_options_list
      {
        auto* create = MAKE_NODE(ASTCreateSchemaStatement, @$, {$5, $6, $7});
        create->set_is_or_replace($2);
        create->set_is_if_not_exists($4);
        $$ = create;
      }
    ;

create_external_schema_statement:
    "CREATE" opt_or_replace opt_create_scope "EXTERNAL" "SCHEMA" opt_if_not_exists path_expression
    opt_with_connection_clause options
      {
        auto* create = MAKE_NODE(ASTCreateExternalSchemaStatement, @$, {$7, $8, $9});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_is_if_not_exists($6);
        $$ = create;
      }
    ;

create_connection_statement:
    "CREATE" opt_or_replace "CONNECTION" opt_if_not_exists path_expression
    opt_options_list
      {
        auto* create = MAKE_NODE(ASTCreateConnectionStatement, @$, {$path_expression, $opt_options_list});
        create->set_is_or_replace($2);
        create->set_is_if_not_exists($4);
        $$ = create;
      }
    ;

undrop_statement:
    "UNDROP" schema_object_kind opt_if_not_exists path_expression
    opt_at_system_time opt_options_list
      {
        if ($schema_object_kind != zetasql::SchemaObjectKind::kSchema) {
          YYERROR_AND_ABORT_AT(@schema_object_kind, absl::StrCat("UNDROP ", absl::AsciiStrToUpper(
            parser->GetInputText(@schema_object_kind)), " is not supported"));
        }
        auto* undrop = MAKE_NODE(ASTUndropStatement, @$, {$path_expression, $opt_at_system_time, $opt_options_list});
        undrop->set_schema_object_kind($schema_object_kind);
        undrop->set_is_if_not_exists($opt_if_not_exists);
        $$ = undrop;
      }
    ;

create_snapshot_statement:
    "CREATE" opt_or_replace "SNAPSHOT" "TABLE" opt_if_not_exists maybe_dashed_path_expression
     "CLONE" clone_data_source opt_options_list
      {
        auto* create =
            MAKE_NODE(ASTCreateSnapshotTableStatement, @$, {$6, $8, $9});
        create->set_is_if_not_exists($5);
        create->set_is_or_replace($2);
        $$ = create;
      }
    | "CREATE" opt_or_replace "SNAPSHOT" schema_object_kind opt_if_not_exists maybe_dashed_path_expression
      "CLONE" clone_data_source opt_options_list
      {
        if (!zetasql::SchemaObjectAllowedForSnapshot($schema_object_kind)) {
          YYERROR_AND_ABORT_AT(@schema_object_kind, absl::StrCat("CREATE SNAPSHOT ", absl::AsciiStrToUpper(
            parser->GetInputText(@schema_object_kind)), " is not supported"));
        }
        auto* create =
            MAKE_NODE(ASTCreateSnapshotStatement, @$, {$6, $8, $9});
        create->set_schema_object_kind($schema_object_kind);
        create->set_is_if_not_exists($5);
        create->set_is_or_replace($2);
        $$ = create;
      }
    ;

unordered_language_options:
    language opt_options_list[options]
      {
        $$.language = $language;
        $$.options = $options;
      }
    | options opt_language[language]
      {
        // This production is deprecated (with no warning YET).
        $$.language = $language;
        $$.options = $options;
      }
    | %empty
      {
        $$.language = nullptr;
        $$.options = nullptr;
      }
    ;

// This rule encounters a shift/reduce conflict with 'create_table_statement'
// as noted in AMBIGUOUS CASE 3 in the file-level comment. The syntax of this
// rule and 'create_table_statement' must be kept the same until the "TABLE"
// keyword, so that parser can choose between these two rules based on the
// "FUNCTION" keyword conflict.
create_table_function_statement:
    // The preferred style is LANGUAGE OPTIONS but OPTIONS LANGUAGE is allowed
    // for backwards compatibility (no deprecation warning YET).
    "CREATE" opt_or_replace opt_create_scope "TABLE" "FUNCTION"
        opt_if_not_exists path_expression opt_function_parameters opt_returns
        opt_sql_security_clause
        unordered_language_options[ulo]
        opt_as_query_or_string[body]
      {
        if ($opt_function_parameters == nullptr) {
            // Missing function argument list.
          YYERROR_AND_ABORT_AT(@opt_function_parameters,
                               "Syntax error: Expected (");
        }
        if ($opt_returns != nullptr  &&
            $opt_returns->node_kind() != zetasql::AST_TVF_SCHEMA) {
          YYERROR_AND_ABORT_AT(@opt_returns,
                               "Syntax error: Expected keyword TABLE");
        }
        // Build the create table function statement.
        auto* fn_decl = MAKE_NODE(ASTFunctionDeclaration, @path_expression,
                                  @opt_function_parameters,
                                  {$path_expression, $opt_function_parameters});
        auto* create = MAKE_NODE(
            ASTCreateTableFunctionStatement, @$,
            {fn_decl, $opt_returns, $ulo.options, $ulo.language, $body});
        create->set_is_or_replace($opt_or_replace);
        create->set_scope($opt_create_scope);
        create->set_is_if_not_exists($opt_if_not_exists);
        create->set_sql_security($opt_sql_security_clause);
        $$ = create;
      }
    ;

// This rule encounters a shift/reduce conflict with
// 'create_table_function_statement' as noted in AMBIGUOUS CASE 3 in the
// file-level comment. The syntax of this rule and
// 'create_table_function_statement' must be kept the same until the "TABLE"
// keyword, so that parser can choose between these two rules based on the
// "FUNCTION" keyword conflict.
create_table_statement:
    "CREATE" opt_or_replace opt_create_scope "TABLE" opt_if_not_exists
    maybe_dashed_path_expression opt_table_element_list
    opt_spanner_table_options opt_like_path_expression opt_clone_table
    opt_copy_table opt_default_collate_clause opt_partition_by_clause_no_hint
    opt_cluster_by_clause_no_hint opt_ttl_clause opt_with_connection_clause
    opt_options_list opt_as_query
      {
        zetasql::ASTCreateStatement* create =
            MAKE_NODE(ASTCreateTableStatement, @$, {
              $maybe_dashed_path_expression,
              $opt_table_element_list,
              $opt_like_path_expression,
              $opt_spanner_table_options,
              $opt_clone_table,
              $opt_copy_table,
              $opt_default_collate_clause,
              $opt_partition_by_clause_no_hint,
              $opt_cluster_by_clause_no_hint,
              $opt_ttl_clause,
              $opt_with_connection_clause,
              $opt_options_list,
              $opt_as_query,
            });
        create->set_is_or_replace($opt_or_replace);
        create->set_scope($opt_create_scope);
        create->set_is_if_not_exists($opt_if_not_exists);
        $$ = create;
      }
    ;

append_or_overwrite:
    "INTO" {  // INTO to mean append, which is consistent with INSERT INTO
      $$ = zetasql::ASTAuxLoadDataStatement::InsertionMode::APPEND;
    }
    | "OVERWRITE" {
      $$ = zetasql::ASTAuxLoadDataStatement::InsertionMode::OVERWRITE;
    }
    ;

aux_load_data_from_files_options_list:
    "FROM" "FILES" options_list
      {
        $$ = MAKE_NODE(ASTAuxLoadDataFromFilesOptionsList, @$, {$3});
      }
    ;

opt_overwrite:
    "OVERWRITE" { $$ = true; }
    | %empty { $$ = false; }
    ;

load_data_partitions_clause:
    opt_overwrite "PARTITIONS" "(" expression ")"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_V_1_4_LOAD_DATA_PARTITIONS)) {
            YYERROR_AND_ABORT_AT(
              @2,
              "LOAD DATA statement with PARTITIONS is not supported");
        }
        zetasql::ASTAuxLoadDataPartitionsClause* partitions_clause =
            MAKE_NODE(ASTAuxLoadDataPartitionsClause, @$, {$4});
        partitions_clause->set_is_overwrite($1);
        $$ = partitions_clause;
      }
    ;

opt_load_data_partitions_clause:
    load_data_partitions_clause
    | %empty { $$ = nullptr; }
    ;

maybe_dashed_path_expression_with_scope:
    "TEMP" "TABLE" maybe_dashed_path_expression
      {
        $$.maybe_dashed_path_expression =
            $3->GetAsOrDie<zetasql::ASTExpression>();
        $$.is_temp_table = true;
      }
    | "TEMPORARY" "TABLE" maybe_dashed_path_expression
      {
        $$.maybe_dashed_path_expression =
            $3->GetAsOrDie<zetasql::ASTExpression>();
        $$.is_temp_table = true;
      }
    | maybe_dashed_path_expression
      {
        $$.maybe_dashed_path_expression =
            $1->GetAsOrDie<zetasql::ASTExpression>();
        $$.is_temp_table = false;
      }
    ;

aux_load_data_statement:
    "LOAD" "DATA" append_or_overwrite
    maybe_dashed_path_expression_with_scope opt_table_element_list
    opt_load_data_partitions_clause
    opt_collate_clause
    opt_partition_by_clause_no_hint
    opt_cluster_by_clause_no_hint
    opt_options_list
    aux_load_data_from_files_options_list
    opt_external_table_with_clauses
      {
        zetasql::ASTAuxLoadDataStatement* statement =
            MAKE_NODE(
                ASTAuxLoadDataStatement, @$,
                {$4.maybe_dashed_path_expression,
                 $5, $6, $7, $8, $9, $10, $11,
                 $12.with_partition_columns_clause,
                 $12.with_connection_clause});
        statement->set_insertion_mode($3);
        if (!parser->language_options().LanguageFeatureEnabled(
            zetasql::FEATURE_V_1_4_LOAD_DATA_TEMP_TABLE)
            && $4.is_temp_table) {
            YYERROR_AND_ABORT_AT(
              @4,
              "LOAD DATA statement with TEMP TABLE is not supported");
        }
        statement->set_is_temp_table($4.is_temp_table);
        $$ = statement;
      }
    ;

generic_entity_type_unchecked:
    IDENTIFIER
      {
        // It is by design that we don't want to support backtick quoted
        // entity type. Backtick is kept as part of entity type name, and will
        // be rejected by engine later.
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    | "PROJECT"
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));;
      }
    ;

generic_entity_type:
    generic_entity_type_unchecked
      {
        std::string entity_type(parser->GetInputText(@1));
        if (!parser->language_options().
                 GenericEntityTypeSupported(entity_type)) {
          YYERROR_AND_ABORT_AT(@1, absl::StrCat(
                               entity_type, " is not a supported object type"));
        }
        $$ = $1;
      }
    ;

// This rule can't use the normal `identifier` production, because that includes
// `keyword_as_identifier`, which includes all non-reserved keywords.
// Including the non-reserved keywords causes many ambiguities with non-generic
// DDL rules - e.g. ADD COLUMN.
//
// Any non-reserved keywords that need to work as generic DDL object types need
// to be included here explicitly.
sub_entity_type_identifier:
    IDENTIFIER
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));;
      }
    | "REPLICA"
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));;
      }
    ;

generic_sub_entity_type:
  sub_entity_type_identifier
    {
      if (!parser->language_options().
                GenericSubEntityTypeSupported($1->GetAsString())) {
        YYERROR_AND_ABORT_AT(
          @1, absl::StrCat(zetasql::ToIdentifierLiteral($1->GetAsString()),
                           " is not a supported nested object type"));
      }
      $$ = $1;
    }

generic_entity_body:
    json_literal
      {
        $$ = $json_literal;
      }
    | string_literal
      {
        $$ = $string_literal;
      }
    ;

opt_generic_entity_body:
    "AS" generic_entity_body
      {
        $$ = $2;
      }
    | %empty { $$ = nullptr; }
    ;

create_entity_statement:
    "CREATE" opt_or_replace generic_entity_type opt_if_not_exists
    path_expression opt_options_list opt_generic_entity_body
      {
        auto* node = MAKE_NODE(
            ASTCreateEntityStatement,
            @$,
            {
              $generic_entity_type,
              $path_expression,
              $opt_options_list,
              $opt_generic_entity_body
            });
        node->set_is_or_replace($opt_or_replace);
        node->set_is_if_not_exists($opt_if_not_exists);
        $$ = node;
      }
    ;

create_model_statement:
    "CREATE" opt_or_replace opt_create_scope "MODEL" opt_if_not_exists
    path_expression opt_input_output_clause opt_transform_clause
    opt_remote_with_connection_clause opt_options_list
    opt_as_query_or_aliased_query_list
      {
        auto* node = MAKE_NODE(
            ASTCreateModelStatement,
            @$,
            {
              $path_expression,
              $opt_input_output_clause,
              $opt_transform_clause,
              $opt_remote_with_connection_clause.with_connection_clause,
              $opt_options_list,
              $opt_as_query_or_aliased_query_list,
            });
        node->set_is_or_replace($opt_or_replace);
        node->set_scope($opt_create_scope);
        node->set_is_if_not_exists($opt_if_not_exists);
        node->set_is_remote($opt_remote_with_connection_clause.is_remote);
        $$ = node;
      }
    ;

opt_table_element_list:
    table_element_list
    | %empty { $$ = nullptr; }
    ;

table_element_list:
    table_element_list_prefix ")"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
            YYERROR_AND_ABORT_AT(@2, "A table must define at least one "
              "column.");
        }
        $$ = MAKE_NODE(ASTTableElementList, @$, {});
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

// The table_element grammar includes 2 shift/reduce conflicts in its
// table_constraint_definition rule. See the file header comment for
// AMBIGUOUS CASE 4: CREATE TABLE CONSTRAINTS.
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
// never an IDENTIFIER. This enables the grammar to unambiguously distinguish
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
        auto* schema = parser->WithEndLocation(
            WithExtraChildren($2, {$3, $4}), @$);
        $$ = MAKE_NODE(ASTColumnDefinition, @$, {$1, schema});
      }
    ;

table_column_schema:
    column_schema_inner opt_collate_clause opt_column_info
      {
        if ($3.generated_column_info != nullptr) {
          $$ = parser->WithEndLocation(
              WithExtraChildren($1, {$2, $3.generated_column_info,
                                     /*default_expression=*/nullptr}), @$);
        } else if ($3.default_expression != nullptr) {
          $$ = parser->WithEndLocation(
              WithExtraChildren($1, {$2, /*generated_column_info=*/nullptr,
                                     $3.default_expression}), @$);
        } else {
          $$ = parser->WithEndLocation(
              WithExtraChildren($1, {$2, /*generated_column_info=*/nullptr,
                                     /*default_expression=*/nullptr}), @$);
        }
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
    // Unlike other type names, 'INTERVAL' is a reserved keyword.
    | "INTERVAL"
      {
        auto* id = parser->MakeIdentifier(@1, parser->GetInputText(@1));
        auto* path_expression = MAKE_NODE(ASTPathExpression, @$, {id});
        $$ = MAKE_NODE(ASTSimpleColumnSchema, @$, {path_expression});
      }
    ;

array_column_schema_inner:
    "ARRAY" template_type_open field_schema template_type_close
      {
        $$ = MAKE_NODE(ASTArrayColumnSchema, @$, {$3});
      }
    ;

range_column_schema_inner:
    "RANGE" template_type_open field_schema template_type_close
      {
        $$ = MAKE_NODE(ASTRangeColumnSchema, @$, {$3});
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
    column_schema_inner opt_collate_clause opt_field_attributes
      {
        auto* schema = parser->WithEndLocation(
            WithExtraChildren($1, {$2, $3}), @$);
        $$ = MAKE_NODE(ASTStructColumnField, @$, {schema});
      }
    | identifier field_schema
      {
        $$ = MAKE_NODE(ASTStructColumnField, @$, {$1, $2});
      }
    ;

struct_column_schema_prefix:
    "STRUCT" template_type_open struct_column_field
      {
        $$ = MAKE_NODE(ASTStructColumnSchema, @$, {$3});
      }
    | struct_column_schema_prefix "," struct_column_field
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

// This node does not apply parser->WithEndLocation. column_schema and
// field_schema do.
struct_column_schema_inner:
    "STRUCT" template_type_open template_type_close
      {
        $$ = MAKE_NODE(ASTStructColumnSchema, @$);
      }
    | struct_column_schema_prefix template_type_close
    ;

raw_column_schema_inner:
    simple_column_schema_inner
    | array_column_schema_inner
    | struct_column_schema_inner
    | range_column_schema_inner
    ;

column_schema_inner:
    raw_column_schema_inner opt_type_parameters
    {
      $$ = WithExtraChildren(parser->WithEndLocation($1, @2), {$2});
    };

generated_mode:
  "GENERATED" "AS"
    {
      $$ = zetasql::ASTGeneratedColumnInfo::GeneratedMode::ALWAYS;
    }
  | "GENERATED" "ALWAYS" "AS"
    {
      $$ = zetasql::ASTGeneratedColumnInfo::GeneratedMode::ALWAYS;
    }
  | "GENERATED" "BY" "DEFAULT" "AS"
    {
      $$ = zetasql::ASTGeneratedColumnInfo::GeneratedMode::BY_DEFAULT;
    }
  | "AS"
    {
      $$ = zetasql::ASTGeneratedColumnInfo::GeneratedMode::ALWAYS;
    }
  ;

stored_mode:
  "STORED" "VOLATILE"
    {
      $$ = zetasql::ASTGeneratedColumnInfo::StoredMode::STORED_VOLATILE;
    }
  | "STORED"
    {
      $$ = zetasql::ASTGeneratedColumnInfo::StoredMode::STORED;
    }
  | %empty
    {
      $$ = zetasql::ASTGeneratedColumnInfo::StoredMode::NON_STORED;
    }
  ;

signed_numerical_literal:
 integer_literal
 | numeric_literal
 | bignumeric_literal
 | floating_point_literal
 {
  $$ = $1;
 }
 | "-" integer_literal[literal]
 {
  auto* expression = MAKE_NODE(ASTUnaryExpression, @$, {$literal});
  expression->set_op(zetasql::ASTUnaryExpression::MINUS);
  $$ = expression;
 }
 | "-" floating_point_literal[literal]
 {
  auto* expression = MAKE_NODE(ASTUnaryExpression, @$, {$literal});
  expression->set_op(zetasql::ASTUnaryExpression::MINUS);
  $$ = expression;
 }
 ;

opt_start_with:
  "START" "WITH" signed_numerical_literal[literal]
  {
    $$ = MAKE_NODE(ASTIdentityColumnStartWith, @$, {$literal});
  }
  | %empty
  {
    $$ = nullptr;
  }
  ;

opt_increment_by:
  "INCREMENT" "BY" signed_numerical_literal[literal]
  {
    $$ = MAKE_NODE(ASTIdentityColumnIncrementBy, @$, {$literal});
  }
  | %empty
  {
    $$ = nullptr;
  }
  ;

opt_maxvalue:
  "MAXVALUE" signed_numerical_literal[literal]
  {
    $$ = MAKE_NODE(ASTIdentityColumnMaxValue, @$, {$literal});
  }
  | %empty
  {
    $$ = nullptr;
  }
  ;

opt_minvalue:
  "MINVALUE" signed_numerical_literal[literal]
  {
    $$ = MAKE_NODE(ASTIdentityColumnMinValue, @$, {$literal});
  }
  | %empty
  {
    $$ = nullptr;
  }
  ;

opt_cycle:
  "CYCLE"
  {
    $$ = true;
  }
  | "NO" "CYCLE"
  {
    $$ = false;
  }
  | %empty
  {
    $$ = false;
  }
  ;

identity_column_info:
  "IDENTITY" "(" opt_start_with[start] opt_increment_by[increment]
  opt_maxvalue[max] opt_minvalue[min] opt_cycle[cycle] ")"
    {
      auto* identity_column =
        MAKE_NODE(ASTIdentityColumnInfo, @$, {$start, $increment, $max, $min});
      identity_column->set_cycling_enabled($cycle);
      $$ = identity_column;
    }
  ;

generated_column_info:
  generated_mode "(" expression ")" stored_mode
    {
      auto* column = MAKE_NODE(ASTGeneratedColumnInfo, @$, {$expression});
      column->set_stored_mode($stored_mode);
      column->set_generated_mode($generated_mode);
      $$ = column;
    }
  | generated_mode identity_column_info
    {
      auto* column = MAKE_NODE(ASTGeneratedColumnInfo, @$, {$2});
      column->set_generated_mode($generated_mode);
      $$ = column;
    }
  ;

invalid_generated_column:
  generated_column_info
    {
      $$ = true;
    }
  | %empty
    {
      $$ = false;
    }
  ;

default_column_info:
  "DEFAULT" expression
    {
      if (parser->language_options().LanguageFeatureEnabled(
             zetasql::FEATURE_V_1_3_COLUMN_DEFAULT_VALUE)) {
        $$ = $2;
      } else {
        YYERROR_AND_ABORT_AT(@2, "Column DEFAULT value is not supported.");
      }
    }
  ;

invalid_default_column:
  default_column_info
    {
      $$ = true;
    }
  | %empty
    {
      $$ = false;
    }
  ;

opt_column_info:
  generated_column_info invalid_default_column
    {
      if ($2) {
        YYERROR_AND_ABORT_AT(@2, "Syntax error: \"DEFAULT\" and \"GENERATED "
            "ALWAYS AS\" clauses must not be both provided for the column");
      }
      $$.generated_column_info =
          static_cast<zetasql::ASTGeneratedColumnInfo*>($1);
      $$.default_expression = nullptr;
    }
  | default_column_info invalid_generated_column
    {
      if ($2) {
        YYERROR_AND_ABORT_AT(@2, "Syntax error: \"DEFAULT\" and \"GENERATED "
            "ALWAYS AS\" clauses must not be both provided for the column");
      }
      $$.generated_column_info = nullptr;
      $$.default_expression = static_cast<zetasql::ASTExpression*>($1);
    }
  | %empty
    {
      $$.generated_column_info = nullptr;
      $$.default_expression = nullptr;
    }
  ;

field_schema:
  column_schema_inner opt_collate_clause opt_field_attributes opt_options_list
    {
      $$ = parser->WithEndLocation(WithExtraChildren($1, {$2, $3, $4}), @$);
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
      $$ = parser->WithStartLocation(node, FirstNonEmptyLocation(@1, @2));
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
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$2}), @$);
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
        last = parser->WithEndLocation(last, @$);
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

opt_column_attributes:
    column_attributes
    | %empty { $$ = nullptr; }
    ;

opt_field_attributes:
  not_null_column_attribute
    {
      $$ = MAKE_NODE(ASTColumnAttributeList, @$, {$1});
    }
  | %empty { $$ = nullptr; }
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
    | %empty { $$ = nullptr; }
    ;

fill_using_expression:
    "FILL" "USING" expression
      {
        $$ = $3;
      }
    ;

opt_fill_using_expression:
    fill_using_expression
    | %empty { $$ = nullptr; }
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

primary_key_element:
    identifier opt_asc_or_desc opt_null_order
      {
        if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_4_ORDERED_PRIMARY_KEYS)) {
          if ($opt_asc_or_desc != zetasql::ASTOrderingExpression::UNSPECIFIED
              || $opt_null_order != nullptr) {
            YYERROR_AND_ABORT_AT(@2,
              "Ordering for primary keys is not supported");
          }
        }
        auto* node = MAKE_NODE(ASTPrimaryKeyElement, @$, {
          $identifier,
          $opt_null_order,
        });
        node->set_ordering_spec($opt_asc_or_desc);
        $$ = node;
      }
    ;

primary_key_element_list_prefix:
    "(" primary_key_element
      {
        $$ = MAKE_NODE(ASTPrimaryKeyElementList, @$, {$2});
      }
    | primary_key_element_list_prefix "," primary_key_element
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

primary_key_element_list:
    primary_key_element_list_prefix ")"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    | "(" ")" { $$ = nullptr; }
    ;

primary_key_spec:
  "PRIMARY" "KEY" primary_key_element_list opt_constraint_enforcement
  opt_options_list
    {
      zetasql::ASTPrimaryKey* node = MAKE_NODE(ASTPrimaryKey, @$, {$3, $5});
      node->set_enforced($4);
      $$ = node;
    }
  ;

primary_key_or_table_constraint_spec:
    primary_key_spec
  | table_constraint_spec
  ;

// This rule produces 2 shift/reduce conflicts and requires manual parsing of
// named constraints. See table_element for details.
table_constraint_definition:
      primary_key_spec
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
        $$ = parser->WithLocation(node, @$);
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
    | %empty { $$ = zetasql::ASTForeignKeyReference::SIMPLE; }
    ;

foreign_key_match_mode:
    "SIMPLE" { $$ = zetasql::ASTForeignKeyReference::SIMPLE; }
    | "FULL" { $$ = zetasql::ASTForeignKeyReference::FULL; }
    | "NOT_SPECIAL" "DISTINCT" {
      $$ = zetasql::ASTForeignKeyReference::NOT_DISTINCT;
    }
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
    | %empty
      {
        $$ = MAKE_NODE(ASTForeignKeyActions, @$, {});
      }
    ;

opt_foreign_key_on_update:
    foreign_key_on_update
    | %empty { $$ = zetasql::ASTForeignKeyActions::NO_ACTION; }
    ;

opt_foreign_key_on_delete:
    foreign_key_on_delete
    | %empty { $$ = zetasql::ASTForeignKeyActions::NO_ACTION; }
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
    | %empty { $$ = nullptr; }
    ;

opt_constraint_enforcement:
    constraint_enforcement
    | %empty { $$ = true; }
    ;

constraint_enforcement:
    "ENFORCED" { $$ = true; }
    | "NOT" "ENFORCED" { $$ = false; }
    ;

// Matches either "TABLE" or "TABLE FUNCTION". This encounters a shift/reduce
// conflict as noted in AMBIGUOUS CASE 3 in the file-level comment.
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
    "TABLE" template_type_open tvf_schema_column
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
    tvf_schema_prefix template_type_close
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

opt_recursive: "RECURSIVE" { $$ = true; }
  | %empty { $$ = false; }
  ;

create_view_statement:
    "CREATE" opt_or_replace opt_create_scope opt_recursive "VIEW"
    opt_if_not_exists maybe_dashed_path_expression opt_column_with_options_list
    opt_sql_security_clause
    opt_options_list as_query
      {
        auto* create =
            MAKE_NODE(ASTCreateViewStatement, @$, {$7, $8, $10, $11});
        create->set_is_or_replace($2);
        create->set_scope($3);
        create->set_recursive($4);
        create->set_is_if_not_exists($6);
        create->set_sql_security($9);
        $$ = create;
      }
    |
    "CREATE" opt_or_replace "MATERIALIZED" opt_recursive "VIEW"
    opt_if_not_exists maybe_dashed_path_expression opt_column_with_options_list
    opt_sql_security_clause opt_partition_by_clause_no_hint
    opt_cluster_by_clause_no_hint opt_options_list "AS" query_or_replica_source
      {
        auto* create = MAKE_NODE(ASTCreateMaterializedViewStatement, @$,
          {$7, $8, $10, $11, $12, $14.query, $14.replica_source});
        create->set_is_or_replace($2);
        create->set_recursive($4);
        create->set_scope(zetasql::ASTCreateStatement::DEFAULT_SCOPE);
        create->set_is_if_not_exists($6);
        create->set_sql_security($9);
        $$ = create;
      }
    |
    "CREATE" opt_or_replace "APPROX" opt_recursive "VIEW"
    opt_if_not_exists maybe_dashed_path_expression opt_column_with_options_list
    opt_sql_security_clause
    opt_options_list as_query
      {
        auto* create = MAKE_NODE(
          ASTCreateApproxViewStatement, @$, {$7, $8, $10, $11});
        create->set_is_or_replace($2);
        create->set_scope(zetasql::ASTCreateStatement::DEFAULT_SCOPE);
        create->set_recursive($4);
        create->set_is_if_not_exists($6);
        create->set_sql_security($9);
        $$ = create;
      }
    ;
query_or_replica_source:
    query[q]
    {
      $$ = {.query = $q, .replica_source = nullptr };
    }
    |
    "REPLICA" "OF" maybe_dashed_path_expression[path]
    {
      $$ = {.query = nullptr,
            .replica_source = static_cast<zetasql::ASTPathExpression*>($path)};
    }
    ;

as_query:
    "AS" query
    {
      $$ = $2;
    }
    ;

opt_as_query:
    as_query { $$ = $1; }
    | %empty { $$ = nullptr; }
    ;

opt_as_query_or_string :
    as_query { $$ = $1; }
    | "AS" string_literal { $$ = $2; }
    | %empty { $$ = nullptr; }
    ;

opt_as_query_or_aliased_query_list:
    as_query { $$ = $1; }
    | "AS" "(" aliased_query_list ")" { $$ = $3; }
    | /* Nothing */  %empty { $$ = nullptr; }
    ;

opt_if_not_exists:
    "IF" "NOT" "EXISTS" { $$ = true; }
    | %empty { $$ = false; }
    ;

describe_statement:
    describe_keyword describe_info
      {
        $$ = parser->WithStartLocation($2, @$);
      }
    ;

describe_info:
    identifier maybe_slashed_or_dashed_path_expression opt_from_path_expression
      {
        $$ = MAKE_NODE(ASTDescribeStatement, @$, {$1, $2, $3});
      }
    | maybe_slashed_or_dashed_path_expression opt_from_path_expression
      {
        $$ = MAKE_NODE(ASTDescribeStatement, @$, {nullptr, $1, $2});
      }
    ;

opt_from_path_expression:
    "FROM" maybe_slashed_or_dashed_path_expression
      {
        $$ = $2;
      }
    | %empty { $$ = nullptr; }
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

export_model_statement:
    "EXPORT" "MODEL" path_expression opt_with_connection_clause opt_options_list
      {
        $$ = MAKE_NODE(ASTExportModelStatement, @$, {$3, $4, $5});
      }
    ;

export_metadata_statement:
    "EXPORT" table_or_table_function "METADATA" "FROM"
    maybe_dashed_path_expression opt_with_connection_clause opt_options_list
      {
        if ($2 == TableOrTableFunctionKeywords::kTableAndFunctionKeywords) {
          YYERROR_AND_ABORT_AT(@2,
          "EXPORT TABLE FUNCTION METADATA is not supported");
        }
        auto* export_metadata =
        MAKE_NODE(ASTExportMetadataStatement, @$, {$5, $6, $7});
        export_metadata->set_schema_object_kind(
          zetasql::SchemaObjectKind::kTable);
        $$ = export_metadata;
      }
    ;

grant_statement:
    "GRANT" privileges "ON" identifier path_expression "TO" grantee_list
      {
        $$ = MAKE_NODE(ASTGrantStatement, @$, {$2, $4, $5, $7});
      }
    | "GRANT" privileges "ON" identifier identifier path_expression
        "TO" grantee_list
      {
        $$ = MAKE_NODE(ASTGrantStatement, @$, {$2, $4, $5, $6, $8});
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
    | "REVOKE" privileges "ON" identifier identifier path_expression
        "FROM" grantee_list
      {
        $$ = MAKE_NODE(ASTRevokeStatement, @$, {$2, $4, $5, $6, $8});
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
    "PRIVILEGES"
    | %empty
    ;

privilege_list:
    privilege
      {
        $$ = MAKE_NODE(ASTPrivileges, @$, {$1});
      }
    | privilege_list "," privilege
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

privilege:
    privilege_name opt_path_expression_list_with_parens
      {
        $$ = MAKE_NODE(ASTPrivilege, @$, {$1, $2});
      }
    ;

privilege_name:
    identifier
      {
        $$ = $1;
      }
    | "SELECT"
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


column_ordering_and_options_expr:
    expression opt_collate_clause opt_asc_or_desc opt_null_order opt_options_list
      {
        auto* ordering_expr =
            MAKE_NODE(ASTOrderingExpression, @$, {
              $expression,
              $opt_collate_clause,
              $opt_null_order,
              $opt_options_list
            });
        ordering_expr->set_ordering_spec($3);
        $$ = ordering_expr;
      }
    ;

index_order_by_and_options_prefix:
    "(" column_ordering_and_options_expr
      {
        $$ = MAKE_NODE(ASTIndexItemList, @$, {$2});
      }
    | index_order_by_and_options_prefix "," column_ordering_and_options_expr
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

all_column_column_options:
    index_order_by_and_options_prefix ")"

opt_with_column_options:
    "WITH" "COLUMN" "OPTIONS" all_column_column_options
      {
        $$ = $4;
      }
    | %empty { $$ = nullptr; }
    ;

index_all_columns:
    "(" "ALL" "COLUMNS" opt_with_column_options ")"
      {
        auto* all_columns = MAKE_NODE(ASTIndexAllColumns, @$, {$4});
        all_columns->set_image("ALL COLUMNS");
        auto* ordering_expr =
            MAKE_NODE(ASTOrderingExpression, @$,
                      {all_columns, nullptr, nullptr, nullptr});
        ordering_expr->set_ordering_spec(
                                zetasql::ASTOrderingExpression::UNSPECIFIED);
        $$ = MAKE_NODE(ASTIndexItemList, @$, {ordering_expr});
      }

index_order_by_and_options:
    index_order_by_and_options_prefix ")"
    {
      $$ = parser->WithEndLocation($1, @$);
    }
    | index_all_columns
    {
      $$ = parser->WithEndLocation($1, @$);
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
   |  %empty { $$ = nullptr; }
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

index_storing_list:
  "STORING" index_storing_expression_list {
    $$ = $2;
  }
  ;

opt_index_storing_list:
   index_storing_list
   | %empty { $$ = nullptr; }
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

opt_column_list:
    column_list
    | %empty { $$ = nullptr; }
    ;

column_with_options:
    identifier opt_options_list
      {
        $$ = MAKE_NODE(ASTColumnWithOptions, @$, {$1, $2});
      }
    ;

column_with_options_list_prefix:
    "(" column_with_options
      {
        $$ = MAKE_NODE(ASTColumnWithOptionsList, @$, {$2});
      }
    | column_with_options_list_prefix "," column_with_options
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

column_with_options_list:
    column_with_options_list_prefix ")"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

opt_column_with_options_list:
    column_with_options_list
    | /* Nothing */  %empty { $$ = nullptr; }
    ;

grantee_list:
    string_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTGranteeList, @$, {$1});
      }
    | grantee_list "," string_literal_or_parameter
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

grantee_list_with_parens_prefix:
    "(" string_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTGranteeList, @$, {$2});
      }
    | grantee_list_with_parens_prefix "," string_literal_or_parameter
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

possibly_empty_grantee_list:
    grantee_list_with_parens_prefix ")"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        $$ = MAKE_NODE(ASTGranteeList, @$, {});
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
    | %empty { $$ = nullptr; }
    ;

opt_like_path_expression:
    "LIKE" maybe_dashed_path_expression
      {
        $$ = $2;
      }
    | %empty { $$ = nullptr; }
    ;

opt_clone_table:
    "CLONE" clone_data_source
      {
        $$ = $2;
      }
    | %empty { $$ = nullptr; }
    ;

opt_copy_table:
    "COPY" copy_data_source
      {
        $$ = $2;
      }
    | %empty { $$ = nullptr; }
    ;

all_or_distinct:
    "ALL" {
      $$ = MAKE_NODE(ASTSetOperationAllOrDistinct, @$, {});
      $$->set_value(zetasql::ASTSetOperation::ALL);
    }
    | "DISTINCT" {
      $$ = MAKE_NODE(ASTSetOperationAllOrDistinct, @$, {});
      $$->set_value(zetasql::ASTSetOperation::DISTINCT);
    }
    ;

// Returns the token for a set operation as expected by
// ASTSetOperation::op_type().
query_set_operation_type:
    "UNION"
      {
        $$ = MAKE_NODE(ASTSetOperationType, @$, {});
        $$->set_value(zetasql::ASTSetOperation::UNION);
      }
    | KW_EXCEPT_IN_SET_OP
      {
        $$ = MAKE_NODE(ASTSetOperationType, @$, {});
        $$->set_value(zetasql::ASTSetOperation::EXCEPT);
      }
    | "INTERSECT"
      {
        $$ = MAKE_NODE(ASTSetOperationType, @$, {});
        $$->set_value(zetasql::ASTSetOperation::INTERSECT);
      }
    ;

query_primary_or_set_operation:
    query_primary
    | query_set_operation
    ;

parenthesized_query:
    "(" query ")"
      {
        // We do not call $query->set_parenthesized(true) because typically the
        // calling rule expects parentheses and will already insert one pair
        // when unparsing.
        $$ = $query;
      }
  ;

select_or_from_keyword:
    "SELECT" | "FROM"
  ;

// These rules are for generating errors for unexpected clauses after FROM
// queries.  The returned string constant is the name for the error message.
bad_keyword_after_from_query:
    "WHERE" { $$ = "WHERE"; }
    | "SELECT" { $$ = "SELECT"; }
    | "GROUP" { $$ = "GROUP BY"; }
  ;

// These produce a different error that says parentheses are also allowed.
bad_keyword_after_from_query_allows_parens:
    "ORDER" { $$ = "ORDER BY"; }
    | "UNION" { $$ = "UNION"; }
    | "INTERSECT" { $$ = "INTERSECT"; }
    | KW_EXCEPT_IN_SET_OP { $$ = "EXCEPT"; }
    | "LIMIT" { $$ = "LIMIT"; }
  ;

query_without_pipe_operators:
    // We don't use an opt_with_clause for the first element because it causes
    // shift/reduce conflicts.
    with_clause query_primary_or_set_operation[query_primary]
      opt_order_by_clause[order_by]
      opt_limit_offset_clause[offset]
      {
        auto* node = MAKE_NODE(ASTQuery, @$,
           {$with_clause, $query_primary, $order_by, $offset});
        $$ = node;
      }
    | with_clause_with_trailing_comma select_or_from_keyword
      {
        // TODO: Consider pointing the error location at the comma
        // instead of at the SELECT.
        YYERROR_AND_ABORT_AT(@2,
                             "Syntax error: Trailing comma after the WITH "
                             "clause before the main query is not allowed");
      }
    | with_clause "|>"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_PIPES)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected |");
        } else {
          YYERROR_AND_ABORT_AT(@2,
                               "Syntax error: A pipe operator cannot follow "
                               "the WITH clause before the main query; The "
                               "main query usually starts with SELECT or "
                               "FROM here");
        }
      }
    | query_primary_or_set_operation[query_primary]
      opt_order_by_clause[order_by]
      opt_limit_offset_clause[offset]
      {
        zetasql::ASTQuery* query = $query_primary->GetAsOrNull<
          zetasql::ASTQuery>();
        if (query && !query->parenthesized()) {
          auto* node = WithExtraChildren(query,
             {$order_by, $offset});
          $$ = node;
        } else if (query && !$order_by && !$offset
          ) {
          // This means it is a query originally and there are no other clauses.
          // So then wrapping it is semantically useless.
          $$ = query;
        } else {
          auto* node = MAKE_NODE(ASTQuery, @$,
             {$query_primary, $order_by, $offset});
          $$ = node;
        }
      }
    // Support FROM queries, which just have a standalone FROM clause and
    // no other clauses (other than pipe operators).
    // FROM queries also cannot be followed with a LIMIT, ORDER BY, or set
    // operations (which would be allowed if this was attached in
    // query_primary rather than here).
    | opt_with_clause from_clause
     {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_PIPES)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected FROM");
        }
        zetasql::ASTFromQuery* from_query = MAKE_NODE(ASTFromQuery, @2, {$2});
        $$ = MAKE_NODE(ASTQuery, @$, {$1, from_query});
     }
    | opt_with_clause from_clause bad_keyword_after_from_query
     {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_PIPES)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected FROM");
        }
        const absl::string_view keyword = $3;
        YYERROR_AND_ABORT_AT(@3, absl::StrCat(
            "Syntax error: ", keyword, " not supported after FROM query; "
            "Consider using pipe operator `|> ",
            keyword == "GROUP BY" ? "AGGREGATE" : keyword, "`"));
     }
    | opt_with_clause from_clause bad_keyword_after_from_query_allows_parens
     {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_PIPES)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected FROM");
        }
        const absl::string_view keyword = $3;
        YYERROR_AND_ABORT_AT(@3, absl::StrCat(
            "Syntax error: ", keyword, " not supported after FROM query; "
            "Consider using pipe operator `|> ", keyword,
            "` or parentheses around the FROM query"));
     }
    ;

query:
    query_without_pipe_operators
    | query "|>" pipe_operator
    {
      // Adjust the location on the operator node to include the pipe symbol.
      zetasql::ASTNode* pipe_op =
          parser->WithStartLocation($pipe_operator, @2);

      zetasql::ASTQuery* query = $1;
      if (query->parenthesized()) {
        // When we have a pipe operator following a parenthesized query, rather
        // than just adding it on, we created a nested query expression, so
        // we get a better representation of how the pipes bind.
        // We set is_nested for the Unparser, and unset parenthesized to avoid
        // printing double-parentheses.
        query->set_is_nested(true);
        query->set_parenthesized(false);
        $$ = MAKE_NODE(ASTQuery, @$, {query, pipe_op});
      } else {
        $$ = WithExtraChildren(query, {pipe_op});
      }
    }
  ;

pipe_operator:
    pipe_where_clause
    | pipe_select_clause
    | pipe_extend_clause
    | pipe_rename
    | pipe_aggregate_clause
    | pipe_group_by
    | pipe_limit_offset_clause
    | pipe_set_operation_clause
    | pipe_order_by_clause
    | pipe_join
    | pipe_call
    | pipe_window_clause
    | pipe_distinct
    | pipe_tablesample
    | pipe_as
    | pipe_static_describe
    | pipe_assert
    | pipe_drop
    | pipe_set
    | pipe_pivot
    | pipe_unpivot
  ;

pipe_where_clause:
    where_clause
    {
      $$ = MAKE_NODE(ASTPipeWhere, @$, {$1});
    }
  ;

pipe_select_clause:
    select_clause
    {
      $$ = MAKE_NODE(ASTPipeSelect, @$, {$1});
    }
  ;

pipe_limit_offset_clause:
    limit_offset_clause
    {
      $$ = MAKE_NODE(ASTPipeLimitOffset, @$, {$1});
    }
  ;

pipe_order_by_clause:
    order_by_clause_with_opt_comma
    {
      $$ = MAKE_NODE(ASTPipeOrderBy, @$, {$1});
    }
  ;

pipe_extend_clause:
    "EXTEND" pipe_selection_item_list
      {
        // Pipe EXTEND is represented as an ASTSelect inside an
        // ASTPipeExtend.  This allows more resolver code sharing.
        zetasql::ASTSelect* select =
            MAKE_NODE(ASTSelect, @$, {$pipe_selection_item_list});
        $$ = MAKE_NODE(ASTPipeExtend, @$, {select});
      }
  ;

pipe_selection_item:
    select_column_expr
    | select_column_dot_star
  ;

// This adds optional selection_item_order suffixes on the expression cases.
// Dot-star cases are also supported, without order suffixes.
pipe_selection_item_with_order:
    select_column_expr opt_selection_item_order
      {
        $$ = WithExtraChildren($select_column_expr, {$opt_selection_item_order});
      }
    | select_column_dot_star
  ;

// This is a restricted form of ASTSelectList that excludes * and
// other SELECT-list specific syntaxes.
pipe_selection_item_list_no_comma:
    pipe_selection_item
      {
        $$ = MAKE_NODE(ASTSelectList, @$, {$1});
      }
    | pipe_selection_item_list_no_comma "," pipe_selection_item
      {
        $$ = WithExtraChildren($1, {$3});
      }
  ;

pipe_selection_item_list_no_comma_with_order:
    pipe_selection_item_with_order
      {
        $$ = MAKE_NODE(ASTSelectList, @$, {$1});
      }
    | pipe_selection_item_list_no_comma_with_order ","
      pipe_selection_item_with_order
      {
        $$ = WithExtraChildren($1, {$3});
      }
  ;

// This is the selection list used for most pipe operators.
// It resolves to an ASTSelectList.
pipe_selection_item_list:
    pipe_selection_item_list_no_comma opt_comma
      {
        $$ = parser->WithEndLocation($1, @$);
      }
  ;

// This extends pipe_selection_item_list to support
// ASTGroupingItemOrder suffixes.
pipe_selection_item_list_with_order:
    pipe_selection_item_list_no_comma_with_order opt_comma
      {
        $$ = parser->WithEndLocation($1, @$);
      }
  ;

pipe_selection_item_list_with_order_or_empty:
    pipe_selection_item_list_with_order
      {
        $$ = $1;
      }
    | %empty
      {
        $$ = MAKE_NODE(ASTSelectList, @$, {});
      }
  ;

pipe_rename_item:
    identifier[old] opt_as identifier[new]
      {
        $$ = MAKE_NODE(ASTPipeRenameItem, @$, {$old, $new});
      }
    | identifier "."
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Pipe RENAME can only rename columns by name alone; "
            "Renaming columns under table aliases or fields under paths is not "
            "supported");
      }
  ;

pipe_rename_item_list:
    pipe_rename_item[item]
      {
        $$ = MAKE_NODE(ASTPipeRename, @$, {$item});
      }
    | pipe_rename_item_list[list] "," pipe_rename_item[item]
      {
        $$ = WithExtraChildren($list, {$item});
      }
  ;

pipe_rename:
    "RENAME" pipe_rename_item_list opt_comma
      {
        $$ = $pipe_rename_item_list;
      }
  ;


// Note that when using opt_comma for trailing commas, and passing through a
// node constructed in another rule (rather than construcing the node locally),
// it may be necessary to call
//      $$ = parser->WithEndLocation($1, @$);
// to ensure the node's location range includes the comma.
opt_comma:
    ","
    | %empty
  ;

pipe_aggregate_clause:
    "AGGREGATE" pipe_selection_item_list_with_order_or_empty
                opt_group_by_clause_with_opt_comma
      {
        // Pipe AGGREGATE is represented as an ASTSelect inside an
        // ASTPipeAggregate.  This allows more resolver code sharing.
        zetasql::ASTSelect* select = MAKE_NODE(ASTSelect, @$, {$2, $3});
        $$ = MAKE_NODE(ASTPipeAggregate, @$, {select});
      }
  ;

// |> GROUP BY is an error - likely because of an unwanted pipe character.
pipe_group_by:
    "GROUP"
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: GROUP BY should be part of a pipe AGGREGATE "
            "operator, without a leading pipe character");
      }
  ;

pipe_set_operation_clause:
    set_operation_metadata parenthesized_query
      {
        $2->set_parenthesized(true);
        $$ = MAKE_NODE(ASTPipeSetOperation, @$, {$1, $2});
      }
    | pipe_set_operation_clause "," parenthesized_query
      {
        $3->set_parenthesized(true);
        $$ = WithExtraChildren($1, {$3});
      }
    ;

pipe_join:
    opt_natural join_type join_hint "JOIN" opt_hint table_primary
    opt_on_or_using_clause
      {
        // Pipe JOIN has no LHS, so we use this placeholder in the ASTJoin.
        zetasql::ASTPipeJoinLhsPlaceholder* join_lhs =
            MAKE_NODE(ASTPipeJoinLhsPlaceholder, @$, {});

        // JoinRuleAction expects a list of clauses, but this grammar rule only
        // accepts one.  Wrap it into a list.
        zetasql::ASTOnOrUsingClauseList* clause_list = nullptr;
        if ($7 != nullptr) {
          clause_list = MAKE_NODE(ASTOnOrUsingClauseList, @$, {$7});
        }

        // Our main code for constructing ASTJoin is in JoinRuleAction.
        // In other places, it handles complex cases of chains of joins with
        // repeated join clauses.  Here, we always have just one JOIN,
        // so it just constructs a single ASTJoin.
        zetasql::parser::ErrorInfo error_info;
        auto *join_location =
            parser->MakeLocation(NonEmptyRangeLocation(@1, @2, @3, @4));
        zetasql::ASTNode* join = zetasql::parser::JoinRuleAction(
            @1, @$,
            join_lhs, $1, $2, $3, $5, $6, clause_list,
            join_location, parser, &error_info);
        if (join == nullptr) {
          YYERROR_AND_ABORT_AT(error_info.location, error_info.message);
        }

        $$ = MAKE_NODE(ASTPipeJoin, @$, {join});
      }
    ;

pipe_call:
    "CALL" tvf opt_as_alias
      {
        $$ = MAKE_NODE(ASTPipeCall, @$, {WithExtraChildren($2, {$3})});
      }
    ;

pipe_window_clause:
    "WINDOW" pipe_selection_item_list
      {
        // Pipe WINDOW is represented as an ASTSelect inside an
        // ASTPipeWindow.  This allows more resolver code sharing.
        zetasql::ASTSelect* select = MAKE_NODE(ASTSelect, @$, {$2});
        $$ = MAKE_NODE(ASTPipeWindow, @$, {select});
      }
  ;

pipe_distinct:
  "DISTINCT"
    {
      $$ = MAKE_NODE(ASTPipeDistinct, @$);
    }
  ;

pipe_tablesample:
  sample_clause
    {
      $$ = MAKE_NODE(ASTPipeTablesample, @$, {$1});
    }
  ;

pipe_as:
  "AS" identifier
    {
      auto* alias = MAKE_NODE(ASTAlias, @2, {$2});
      $$ = MAKE_NODE(ASTPipeAs, @$, {alias});
    }
  ;

pipe_static_describe:
  "STATIC_DESCRIBE"
    {
      $$ = MAKE_NODE(ASTPipeStaticDescribe, @$);
    }
  ;

pipe_assert_base:
  "ASSERT" expression
    {
      $$ = MAKE_NODE(ASTPipeAssert, @$, {$2});
    }
  | pipe_assert_base "," expression
    {
      $$ = WithExtraChildren($1, {$3});
    }
  ;

pipe_assert:
  pipe_assert_base opt_comma
    {
      $$ = parser->WithEndLocation($1, @$);
    }
  ;

identifier_in_pipe_drop:
    identifier
    {
      $$ = $identifier;
    }
    | identifier "."
    {
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Pipe DROP can only drop columns by name alone; "
            "Dropping columns under table aliases or fields under paths is not "
            "supported");
      }
    }
  ;

// This is the same as identifier_list, but gives a custom error if the
// items are paths rather than just identifiers.
identifier_list_in_pipe_drop:
    identifier_in_pipe_drop[item]
    {
      $$ = MAKE_NODE(ASTIdentifierList, @$, {$item});
    }
    | identifier_list_in_pipe_drop[list] "," identifier_in_pipe_drop[item]
    {
      $$ = parser->WithEndLocation(WithExtraChildren($list, {$item}), @$);
    }
  ;

pipe_drop:
  "DROP" identifier_list_in_pipe_drop[list] opt_comma
    {
      $$ = MAKE_NODE(ASTPipeDrop, @$, {$list});
    }
  ;

pipe_set_item:
    identifier "=" expression
      {
        $$ = MAKE_NODE(ASTPipeSetItem, @$, {$1, $3});
      }
    | identifier "."
    {
      {
        YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Pipe SET can only update columns by column name "
            "alone; Setting columns under table aliases or fields under "
            "paths is not supported");
      }
    }
    ;

pipe_set_item_list:
    pipe_set_item
      {
        $$ = MAKE_NODE(ASTPipeSet, @$, {$1});
      }
    | pipe_set_item_list "," pipe_set_item
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

pipe_set:
  "SET" pipe_set_item_list opt_comma
    {
      $$ = parser->WithLocation($2, @$);
    }
  ;

pipe_pivot:
  pivot_clause opt_as_alias
    {
      // The alias is parsed separately from pivot_clause but needs to be
      // added into that AST node.
      $$ = MAKE_NODE(ASTPipePivot, @$, { WithExtraChildren($1, {$2}) });
    }
  ;

pipe_unpivot:
  unpivot_clause opt_as_alias
    {
      // The alias is parsed separately from unpivot_clause but needs to be
      // added into that AST node.
      $$ = MAKE_NODE(ASTPipeUnpivot, @$, { WithExtraChildren($1, {$2}) });
    }
  ;

opt_corresponding_outer_mode:
    KW_FULL_IN_SET_OP opt_outer
      {
        $$ = MAKE_NODE(ASTSetOperationColumnPropagationMode, @$, {});
        $$->set_value(zetasql::ASTSetOperation::FULL);
      }
    | "OUTER"
      {
        $$ = MAKE_NODE(ASTSetOperationColumnPropagationMode, @$, {});
        $$->set_value(zetasql::ASTSetOperation::FULL);
      }
    | KW_LEFT_IN_SET_OP opt_outer
      {
        $$ = MAKE_NODE(ASTSetOperationColumnPropagationMode, @$, {});
        $$->set_value(zetasql::ASTSetOperation::LEFT);
      }
    | %empty
      {
        $$ = nullptr;
      }
    ;

opt_strict:
    "STRICT"
      {
        $$ = MAKE_NODE(ASTSetOperationColumnPropagationMode, @$, {});
        $$->set_value(zetasql::ASTSetOperation::STRICT);
      }
    | %empty
      {
        $$ = nullptr;
      }
    ;

opt_column_match_suffix:
    "CORRESPONDING"
      {
        auto* mode = MAKE_NODE(ASTSetOperationColumnMatchMode, @$, {});
        mode->set_value(zetasql::ASTSetOperation::CORRESPONDING);
        $$.column_match_mode = mode;
        $$.column_list = nullptr;
      }
    | "CORRESPONDING" "BY" column_list
      {
        auto* mode = MAKE_NODE(ASTSetOperationColumnMatchMode, @1, @2, {});
        mode->set_value(zetasql::ASTSetOperation::CORRESPONDING_BY);
        $$.column_match_mode = mode;
        $$.column_list = $column_list->GetAsOrDie<zetasql::ASTColumnList>();
      }
    | %empty
      {
        $$.column_match_mode = nullptr;
        $$.column_list = nullptr;
      }
    ;

// This rule allows combining multiple query_primaries with set operations
// as long as all the set operations are identical. It is written to allow
// different set operations grammatically, but it generates an error if
// the set operations in an unparenthesized sequence are different.
// We have no precedence rules for associativity between different set
// operations but parentheses are supported to disambiguate.
query_set_operation_prefix:
    query_primary[left_query] set_operation_metadata[set_op] query_primary[right_query]
      {
        auto* metadata_list =
            MAKE_NODE(ASTSetOperationMetadataList, @set_op, {$set_op});
        $$ = MAKE_NODE(ASTSetOperation, @$,
                      {metadata_list, $left_query, $right_query});
      }
    | query_set_operation_prefix[prefix] set_operation_metadata query_primary
      {
        $prefix->mutable_child(0)->AddChild($set_operation_metadata);
        $$ = WithExtraChildren($prefix, {$query_primary});
      }
    | query_primary set_operation_metadata "FROM"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_PIPES)) {
          YYERROR_AND_ABORT_AT(@3, "Syntax error: Unexpected FROM");
        }
        YYERROR_AND_ABORT_AT(@3, absl::StrCat(
            "Syntax error: Unexpected FROM; "
            "FROM queries following a set operation must be parenthesized"));
      }
    | query_set_operation_prefix set_operation_metadata "FROM"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_PIPES)) {
          YYERROR_AND_ABORT_AT(@3, "Syntax error: Unexpected FROM");
        }
        YYERROR_AND_ABORT_AT(@3, absl::StrCat(
            "Syntax error: Unexpected FROM; "
            "FROM queries following a set operation must be parenthesized"));
      }
    ;

set_operation_metadata:
    opt_corresponding_outer_mode query_set_operation_type opt_hint
    all_or_distinct opt_strict opt_column_match_suffix
      {
        if ($opt_corresponding_outer_mode != nullptr && $opt_strict != nullptr) {
          YYERROR_AND_ABORT_AT(@opt_strict,
                               "Syntax error: STRICT cannot be used with outer "
                               "mode in set operations");
        }
        zetasql::ASTSetOperationColumnPropagationMode* column_propagation_mode =
            $opt_strict == nullptr ? $opt_corresponding_outer_mode : $opt_strict;
        $$ = MAKE_NODE(ASTSetOperationMetadata, @$,
                 {$query_set_operation_type, $all_or_distinct, $opt_hint,
                  $opt_column_match_suffix.column_match_mode,
                  column_propagation_mode, $opt_column_match_suffix.column_list});
      }
    ;

query_set_operation:
   query_set_operation_prefix
     {
       $$ = parser->WithEndLocation($1, @$);
     }
   ;

query_primary:
    select
    | parenthesized_query[query] opt_as_alias_with_required_as[alias]
     {
        if ($alias != nullptr) {
          if (!parser->language_options().LanguageFeatureEnabled(
                  zetasql::FEATURE_PIPES)) {
            YYERROR_AND_ABORT_AT(
                @alias, "Syntax error: Alias not allowed on parenthesized "
                        "outer query");
          }
          $$ = MAKE_NODE(ASTAliasedQueryExpression, @$, {$query, $alias});
       } else {
         $query->set_parenthesized(true);
         $$ = $query;
       }
     }
    ;

// This makes an ASTSelect with none of the clauses after SELECT filled in.
select_clause:
    "SELECT" opt_hint
    opt_select_with
    opt_all_or_distinct
    opt_select_as_clause select_list
      {
        auto* select =
            MAKE_NODE(ASTSelect, @$, {$2, $3, $5, $6});
        select->set_distinct($4 == AllOrDistinctKeyword::kDistinct);
        $$ = select;
      }
    | "SELECT" opt_hint
      opt_select_with
      opt_all_or_distinct
      opt_select_as_clause "FROM"
      {
        YYERROR_AND_ABORT_AT(
            @6,
            "Syntax error: SELECT list must not be empty");
      }
    ;

select:
    select_clause opt_from_clause opt_clauses_following_from
    {
      zetasql::ASTSelect* select = static_cast<zetasql::ASTSelect*>($1);
      $$ = WithExtraChildren(select, {$2, $3.where, $3.group_by,
                                      $3.having, $3.qualify, $3.window});
    }

pre_select_with:
  %empty
  { OVERRIDE_NEXT_TOKEN_LOOKBACK(KW_WITH, LB_WITH_IN_SELECT_WITH_OPTIONS); }

opt_select_with:
    pre_select_with "WITH"[with] identifier
      {
        $$ = MAKE_NODE(ASTSelectWith, @$, {$identifier});
      }
    | pre_select_with "WITH"[with] identifier KW_OPTIONS_IN_SELECT_WITH_OPTIONS options_list
      {
        $$ = MAKE_NODE(ASTSelectWith, @$, {$identifier, $options_list});
      }
    | pre_select_with { $$ = nullptr; }
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
    | %empty { $$ = nullptr; }
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
    OPEN_INTEGER_PREFIX_HINT integer_literal KW_OPEN_HINT "{" hint_entry[entry]
      {
        $$ = MAKE_NODE(ASTHint, @$, {$2, $entry});
      }
    | KW_OPEN_HINT "{" hint_entry[entry]
      {
        $$ = MAKE_NODE(ASTHint, @$, {$entry});
      }
    | hint_with_body_prefix "," hint_entry
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

hint_with_body:
    hint_with_body_prefix "}"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

// We can have "@<int>", "@<int> @{hint_body}", or "@{hint_body}". The case
// where both @<int> and @{hint_body} are present is covered by
// hint_with_body_prefix.
hint:
    KW_OPEN_INTEGER_HINT integer_literal
      {
        ABORT_CHECK(@$, PARSER_LA_IS_EMPTY(),
                    "Expected parser lookahead to be empty following hint.");
        $$ = MAKE_NODE(ASTHint, @$, {$integer_literal});
      }
    | hint_with_body
      {
        ABORT_CHECK(@$,PARSER_LA_IS_EMPTY(),
                    "Expected parser lookahead to be empty following hint.");
        $$ = $hint_with_body;
      }
    ;

// This returns an AllOrDistinctKeyword to indicate what was present.
opt_all_or_distinct:
    "ALL" { $$ = AllOrDistinctKeyword::kAll; }
    | "DISTINCT" { $$ = AllOrDistinctKeyword::kDistinct; }
    | %empty { $$ = AllOrDistinctKeyword::kNone; }
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
        $$ = parser->WithEndLocation($1, @$);
      }
    |
    select_list_prefix ","
      {
        $$ = parser->WithEndLocation($1, @$);
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
        $$ = parser->WithEndLocation($1, @$);
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

select_column:
    select_column_expr
    | select_column_dot_star
    | select_column_star
    ;

// These are the ASTSelectColumn cases for `expression [[AS] alias]`.
select_column_expr:
    expression
      {
        $$ = MAKE_NODE(ASTSelectColumn, @$, {$1});
      }
    | select_column_expr_with_as_alias
    | expression[e] identifier[alias]
      {
        auto* alias = MAKE_NODE(ASTAlias, @alias, {$alias});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {$e, alias});
      }
    ;

select_list_prefix_with_as_aliases:
    select_column_expr_with_as_alias[c]
      {
        $$ = MAKE_NODE(ASTSelectList, @$, {$c});
      }
    | select_list_prefix_with_as_aliases[list] "," select_column_expr_with_as_alias[c]
      {
        $$ = WithExtraChildren($list, {$c});
      }
    ;

select_column_expr_with_as_alias:
    expression[expr] "AS"[as] identifier[alias]
      {
        auto* alias = MAKE_NODE(ASTAlias, @as, @alias, {$alias});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {$expr, alias});
      }
    ;

// These are the ASTSelectColumn cases for `expression.*`, plus optional
// EXCEPT/REPLACE modifiers.
select_column_dot_star:
    // Bison uses the precedence of the last terminal in the rule, but this is a
    // post-fix expression, and it really should have the precedence of ".", not
    // "*".
    expression_higher_prec_than_and[expr] "." "*" %prec "."
      {
        auto* dot_star = MAKE_NODE(ASTDotStar, @$, {$expr});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {dot_star});
      }
    // Bison uses the precedence of the last terminal in the rule, but this is a
    // post-fix expression, and it really should have the precedence of ".", not
    // "*".
    | expression_higher_prec_than_and "." "*" star_modifiers[modifiers] %prec "."
      {
        auto* dot_star_with_modifiers =
            MAKE_NODE(ASTDotStarWithModifiers, @$, {$1, $modifiers});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {dot_star_with_modifiers});
      }
    ;

// These are the ASTSelectColumn cases for `*`, plus optional
// EXCEPT/REPLACE modifiers.
select_column_star:
    "*"
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
        $$ = MAKE_NODE(ASTAlias, FirstNonEmptyLocation(@1, @2), @2, {$2});
      }
    | %empty { $$ = nullptr; }
    ;

opt_as_alias_with_required_as:
    "AS" identifier
      {
        $$ = MAKE_NODE(ASTAlias, @$, {$2});
      }
    | %empty { $$ = nullptr; }
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
    | %empty { $$ = nullptr; }
    ;

opt_as:
    "AS"
    | %empty
    ;

// Returns true for "NATURAL", false for not-natural.
opt_natural:
    "NATURAL" { $$ = true; }
    | %empty { $$ = false; }
    ;

opt_outer: "OUTER" | %empty ;

opt_int_literal_or_parameter:
    int_literal_or_parameter
    | %empty { $$ = nullptr; }

int_literal_or_parameter:
    integer_literal
    | parameter_expression
    | system_variable_expression;

cast_int_literal_or_parameter:
    "CAST" "(" int_literal_or_parameter "AS" type opt_format ")"
      {
        $$ = MAKE_NODE(ASTCastExpression, @$, {$3, $5, $6});
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
    | %empty { $$ = nullptr; }
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
    | %empty { $$ = nullptr; }
    ;

sample_clause:
    "TABLESAMPLE" identifier "(" sample_size ")" opt_sample_clause_suffix
      {
        $$ = MAKE_NODE(ASTSampleClause, @$, {$2, $4, $6});
      }
    ;

opt_match_recognize_and_sample_clauses:
  %empty
  {
    $$.match_recognize_clause = nullptr;
    $$.sample_clause = nullptr;
  }
  | match_recognize_clause[match_recognize] opt_sample_clause[sample]
    {
      $$.match_recognize_clause = $match_recognize;
      $$.sample_clause = $sample;
    }
  | sample_clause[sample] opt_match_recognize_clause[match_recognize]
    {
      $$.match_recognize_clause = $match_recognize;
      $$.sample_clause = $sample;
    }
  ;

opt_sample_clause:
    sample_clause
    | %empty { $$ = nullptr; }
    ;

pivot_expression:
  expression opt_as_alias {
    $$ = MAKE_NODE(ASTPivotExpression, @$, {$1, $2});
  }
  ;

pivot_expression_list:
  pivot_expression {
    $$ = MAKE_NODE(ASTPivotExpressionList, @$, {$1});
  }
  | pivot_expression_list "," pivot_expression {
    $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
  }
;

pivot_value:
  expression opt_as_alias {
    $$ = MAKE_NODE(ASTPivotValue, @$, {$1, $2});
  };

pivot_value_list:
  pivot_value {
    $$ = MAKE_NODE(ASTPivotValueList, @$, {$1});
  }
  | pivot_value_list "," pivot_value {
    $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
  };

pivot_clause:
    "PIVOT" "(" pivot_expression_list
    "FOR" expression_higher_prec_than_and "IN" "(" pivot_value_list ")" ")"
    {
      if ($3 == nullptr) {
        YYERROR_AND_ABORT_AT(@3,
        "PIVOT clause requires at least one pivot expression");
      }
      $$ = MAKE_NODE(ASTPivotClause, @$, {$3, $5, $8});
  };

opt_as_string_or_integer:
  opt_as string_literal{
    $$ = MAKE_NODE(ASTUnpivotInItemLabel, @$, {$2});
  }
  | opt_as integer_literal{
    $$ = MAKE_NODE(ASTUnpivotInItemLabel, @$, {$2})
  }
  | %empty { $$ = nullptr; };

path_expression_list:
    path_expression
    {
      $$ = MAKE_NODE(ASTPathExpressionList, @$, {$1});
    }
    | path_expression_list "," path_expression
    {
      $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
    };

path_expression_list_with_opt_parens:
 "(" path_expression_list ")" {
   $$ = $2;
 }
 |
 path_expression {
   $$ = MAKE_NODE(ASTPathExpressionList, @$, {$1});
 };

path_expression_list_prefix:
    "(" path_expression
      {
        $$ = MAKE_NODE(ASTPathExpressionList, @$, {$2});
      }
    | path_expression_list_prefix "," path_expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

path_expression_list_with_parens:
    path_expression_list_prefix ")"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

opt_path_expression_list_with_parens:
    path_expression_list_with_parens
    | %empty { $$ = nullptr; }
    ;

unpivot_in_item:
  path_expression_list_with_opt_parens opt_as_string_or_integer {
    $$ = MAKE_NODE(ASTUnpivotInItem, @$, {$1, $2});
  };

unpivot_in_item_list_prefix:
  "(" unpivot_in_item {
    $$ = MAKE_NODE(ASTUnpivotInItemList, @$, {$2});
  }
  | unpivot_in_item_list_prefix "," unpivot_in_item {
    $$ = WithExtraChildren($1, {$3});
  };

unpivot_in_item_list:
  unpivot_in_item_list_prefix ")" {
    $$ = parser->WithEndLocation($1, @$);
  } ;

opt_unpivot_nulls_filter:
    "EXCLUDE" "NULLS" { $$ = zetasql::ASTUnpivotClause::kExclude; }
    | "INCLUDE" "NULLS" { $$ = zetasql::ASTUnpivotClause::kInclude; }
    | %empty { $$ = zetasql::ASTUnpivotClause::kUnspecified; }
    ;

unpivot_clause:
   "UNPIVOT" opt_unpivot_nulls_filter "("
   path_expression_list_with_opt_parens
   "FOR" path_expression "IN" unpivot_in_item_list ")" {
    auto* unpivot_clause = MAKE_NODE(ASTUnpivotClause, @$, {$4, $6, $8});
    unpivot_clause->set_null_filter($2);
    $$ = unpivot_clause;
   } ;

// Ideally, we would have an 'opt_pivot_or_unpivot_clause' rule that covers
// just PIVOT and UNPIVOT and use 'op_as_alias' to cover the alias.
//
// Unfortunately, that doesn't work because it would cause ambiguities in the
// grammar. The ambiguities arise because bison only supports a single token
// lookahead, so when it sees:
//   SELECT * FROM t PIVOT ...
// it can't tell whether the PIVOT token means the start of a PIVOT clause
// or an alias for table t named "PIVOT". We work around this by combining PIVOT
// and table aliases into one grammar rule and list out all the possible
// combinations explicitly.
//
opt_pivot_or_unpivot_clause_and_alias:
  "AS" identifier {
    $$.alias = MAKE_NODE(ASTAlias, @$, {$2});
    $$.pivot_clause = nullptr;
    $$.unpivot_clause = nullptr;
  }
  | identifier {
    $$.alias = MAKE_NODE(ASTAlias, @$, {$1});
    $$.pivot_clause = nullptr;
    $$.unpivot_clause = nullptr;
  }
  | "AS" identifier pivot_clause opt_as_alias {
    $$.alias = MAKE_NODE(ASTAlias, @1, {$2});
    $$.alias = parser->WithEndLocation($$.alias, @2);
    $$.pivot_clause = WithExtraChildren($3, {$4});
    $$.unpivot_clause = nullptr;
  }
  | "AS" identifier unpivot_clause opt_as_alias {
    $$.alias = MAKE_NODE(ASTAlias, @1, {$2});
    $$.alias = parser->WithEndLocation($$.alias, @2);
    $$.unpivot_clause = WithExtraChildren($3, {$4});
    $$.pivot_clause = nullptr;
  }
  | "AS" identifier qualify_clause_nonreserved {
    YYERROR_AND_ABORT_AT(
        @3,
        "QUALIFY clause must be used in conjunction with WHERE or GROUP BY "
        "or HAVING clause");
  }
  | identifier pivot_clause opt_as_alias {
    $$.alias = MAKE_NODE(ASTAlias, @1, {$1});
    $$.pivot_clause = WithExtraChildren($2, {$3});
    $$.unpivot_clause = nullptr;
  }
  | identifier unpivot_clause opt_as_alias {
    $$.alias = MAKE_NODE(ASTAlias, @1, {$1});
    $$.unpivot_clause = WithExtraChildren($2, {$3});
    $$.pivot_clause = nullptr;
  }
  | identifier qualify_clause_nonreserved {
    YYERROR_AND_ABORT_AT(
        @2,
        "QUALIFY clause must be used in conjunction with WHERE or GROUP BY "
        "or HAVING clause");
  }
  | pivot_clause opt_as_alias {
    $$.alias = nullptr;
    $$.pivot_clause = WithExtraChildren($1, {$2});
    $$.unpivot_clause = nullptr;
  }
  | unpivot_clause opt_as_alias {
    $$.alias = nullptr;
    $$.unpivot_clause = WithExtraChildren($1, {$2});
    $$.pivot_clause = nullptr;
  }
  | qualify_clause_nonreserved {
    YYERROR_AND_ABORT_AT(
        @1,
        "QUALIFY clause must be used in conjunction with WHERE or GROUP BY "
        "or HAVING clause");
  }
  | %empty {
    $$.alias = nullptr;
    $$.pivot_clause = nullptr;
    $$.unpivot_clause = nullptr;
  }
  ;

opt_match_recognize_clause:
  match_recognize_clause
  | %empty { $$ = nullptr; }
  ;

match_recognize_clause:
  KW_MATCH_RECOGNIZE_RESERVED "("
    opt_partition_by_clause[partition_by]
    order_by_clause[order_by]
    "MEASURES" select_list_prefix_with_as_aliases[measures]
    "PATTERN" "(" row_pattern_expr ")"
    "DEFINE" with_expression_variable_prefix[definitions]
    ")" opt_as_alias[alias]
      {
        $$ = MAKE_NODE(ASTMatchRecognizeClause, @$,
                       {$partition_by, $order_by, $measures, $row_pattern_expr,
                        $definitions, $alias});
      }
  ;

row_pattern_expr:
  row_pattern_concatenation
| row_pattern_expr[alt] "|" row_pattern_concatenation[e]
    {
      $$ = MakeOrCombineRowPatternOperation(
          zetasql::ASTRowPatternOperation::ALTERNATE, parser, @$, $alt, $e);
    }
;

row_pattern_concatenation:
  row_pattern_factor
  | row_pattern_concatenation[sequence] row_pattern_factor[e]
    {
      $$ = MakeOrCombineRowPatternOperation(
          zetasql::ASTRowPatternOperation::CONCAT, parser, @$, $sequence, $e);
    }
  ;

row_pattern_factor:
  identifier
    {
      $$ = MAKE_NODE(ASTRowPatternVariable, @$, {$1});
    }
  | "(" row_pattern_expr[e] ")"
    {
      $e->set_parenthesized(true);
      // Don't include the location in the parentheses. Semantic error
      // messages about this expression should point at the start of the
      // expression, not at the opening parentheses.
      $$ = $e;
    }
  ;

table_subquery:
  parenthesized_query[query]
  opt_pivot_or_unpivot_clause_and_alias[pivot_and_alias]
  opt_match_recognize_and_sample_clauses[postfix_ops]
      {
        zetasql::ASTQuery* query = $query;
        if ($pivot_and_alias.pivot_clause != nullptr) {
          query->set_is_pivot_input(true);
        }
        query->set_is_nested(true);
        // As we set is_nested true, if parenthesized is also true, then
        // we print two sets of brackets in very disorderly way.
        // So set parenthesized to false.
        query->set_parenthesized(false);
        $$ = MAKE_NODE(ASTTableSubquery, @$, {
            $query, $pivot_and_alias.alias, $pivot_and_alias.pivot_clause,
            $pivot_and_alias.unpivot_clause,
            $postfix_ops.match_recognize_clause, $postfix_ops.sample_clause});
      }
    ;

table_clause:
    "TABLE" tvf_with_suffixes
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
    "CONNECTION" path_expression_or_default
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
    tvf_prefix_no_args ")" opt_hint
      {
        $$ = WithExtraChildren(parser->WithEndLocation($1, @$), {$3});
      }
    | tvf_prefix ")" opt_hint
      {
        $$ = WithExtraChildren(parser->WithEndLocation($1, @$), {$3});
      }
    ;

tvf_with_suffixes:
    // Using the `tvf` production inside these rules causes a reduce conflict.
    tvf_prefix_no_args[prefix] ")"
    opt_hint[hint]
    opt_pivot_or_unpivot_clause_and_alias[pivot_and_alias]
    opt_match_recognize_and_sample_clauses[postfix_ops]
      {
        $$ = WithExtraChildren(parser->WithEndLocation($prefix, @$), {
            $hint, $pivot_and_alias.alias, $pivot_and_alias.pivot_clause,
            $pivot_and_alias.unpivot_clause,
            $postfix_ops.match_recognize_clause, $postfix_ops.sample_clause});
      }
    | tvf_prefix[prefix] ")"
      opt_hint[hint]
      opt_pivot_or_unpivot_clause_and_alias[pivot_and_alias]
      opt_match_recognize_and_sample_clauses[postfix_ops]
      {
        $$ = WithExtraChildren(parser->WithEndLocation($prefix, @$), {
            $hint, $pivot_and_alias.alias, $pivot_and_alias.pivot_clause,
            $pivot_and_alias.unpivot_clause,
            $postfix_ops.match_recognize_clause, $postfix_ops.sample_clause});
      }
    ;

table_path_expression_base:
    unnest_expression
    | maybe_slashed_or_dashed_path_expression { $$ = $1; }
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
    table_path_expression_base[path]
    opt_hint[hint]
    opt_pivot_or_unpivot_clause_and_alias[pivot_and_alias]
    opt_with_offset_and_alias[offset]
    opt_at_system_time[time]
    opt_match_recognize_and_sample_clauses[postfix_ops]
      {
        if ( $offset != nullptr) {
          // We do not support combining PIVOT or UNPIVOT with WITH OFFSET.
          // If we did, we would want the WITH OFFSET clause to appear in the
          // grammar before PIVOT so that it operates on the pivot input.
          // However, putting it there results in reduce/reduce conflicts and,
          // even if there were a way to avoid such conflicts, the resultant
          // tree would be thrown out in the resolver later anyway, since we
          // don't support value-tables as PIVOT input.
          //
          // So, the simplest solution to avoid dealing with the above is to
          // put opt_with_offset_and_alias after PIVOT (so the right action
          // happens if we have a WITH OFFSET without PIVOT) and give an explicit
          // error if both clauses are present.
          if ($pivot_and_alias.pivot_clause != nullptr) {
            YYERROR_AND_ABORT_AT(@4,
              "PIVOT and WITH OFFSET cannot be combined");
          }
          if ($pivot_and_alias.unpivot_clause != nullptr) {
            YYERROR_AND_ABORT_AT(@4,
              "UNPIVOT and WITH OFFSET cannot be combined");
          }
        }

        if ($time != nullptr) {
          if ($pivot_and_alias.pivot_clause != nullptr) {
            YYERROR_AND_ABORT_AT(
                @5,
                "Syntax error: PIVOT and FOR SYSTEM TIME AS OF "
                "may not be combined");
          }
          if ($pivot_and_alias.unpivot_clause != nullptr) {
            YYERROR_AND_ABORT_AT(
                @5,
                "Syntax error: UNPIVOT and FOR SYSTEM TIME AS OF "
                "may not be combined");
          }
        }
        $$ = MAKE_NODE(ASTTablePathExpression, @$, {$path, $hint,
            $pivot_and_alias.alias, $pivot_and_alias.pivot_clause,
            $pivot_and_alias.unpivot_clause, $offset, $time,
            $postfix_ops.match_recognize_clause, $postfix_ops.sample_clause});
      };

table_primary:
    // TODO: sample, pivot, match_recognize, etc should be grouped
    //                both in the grammar and in the AST, to apply to all
    //                table primary types, and would help dedup the unparser.
    // Test coverage to add: pipes, parenthesized join.
    tvf_with_suffixes
    | table_path_expression
    | "(" join ")"
      opt_match_recognize_and_sample_clauses[postfix_ops]
      {
        zetasql::parser::ErrorInfo error_info;
        auto node = zetasql::parser::TransformJoinExpression(
          $join, parser, &error_info);
        if (node == nullptr) {
          YYERROR_AND_ABORT_AT(error_info.location, error_info.message);
        }

        $$ = MAKE_NODE(ASTParenthesizedJoin, @$,
                       {node, $postfix_ops.match_recognize_clause,
                        $postfix_ops.sample_clause});
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

    | %empty { $$ = nullptr; }
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

opt_on_or_using_clause_list:
    on_or_using_clause_list
    | %empty
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
        if (parser->language_options().LanguageFeatureEnabled(
               zetasql::FEATURE_V_1_3_ALLOW_CONSECUTIVE_ON)) {
          $$ = parser->WithEndLocation(WithExtraChildren($1, {$2}), @$);
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
  ;

opt_on_or_using_clause:
    on_or_using_clause
    | %empty { $$ = nullptr; }
  ;

// Returns the join type id. Returns 0 to indicate "just a join".
join_type:
    "CROSS" { $$ = zetasql::ASTJoin::CROSS; }
    | "FULL" opt_outer { $$ = zetasql::ASTJoin::FULL; }
    | "INNER" { $$ = zetasql::ASTJoin::INNER; }
    | "LEFT" opt_outer { $$ = zetasql::ASTJoin::LEFT; }
    | "RIGHT" opt_outer { $$ = zetasql::ASTJoin::RIGHT; }
    | %empty  { $$ = zetasql::ASTJoin::DEFAULT_JOIN_TYPE; }
    ;

// Return the join hint token as expected by ASTJoin::set_join_hint().
join_hint:
    "HASH" { $$ = zetasql::ASTJoin::HASH; }
    | "LOOKUP" { $$ = zetasql::ASTJoin::LOOKUP; }
    | %empty { $$ = zetasql::ASTJoin::NO_JOIN_HINT; }
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
        auto *join_location =
            parser->MakeLocation(NonEmptyRangeLocation(@2, @3, @4, @5));
        auto node = zetasql::parser::JoinRuleAction(
            @1, @$,
            $1, $2, $3, $4, $6, $7, $8, join_location, parser, &error_info);
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
        auto* comma_location = parser->MakeLocation(@2);
        auto node = zetasql::parser::CommaJoinRuleAction(
            @1, @3, $1, $3, comma_location, parser, &error_info);
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
        auto* join_location = parser->MakeLocation(
            NonEmptyRangeLocation(@2, @3, @4, @5));
        auto node = zetasql::parser::JoinRuleAction(
            @1, @$,
            $1, $2, $3, $4, $6, $7, $8,
            join_location,
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
    | "@@"
      {
        YYERROR_AND_ABORT_AT(
            @1, "System variables cannot be used in place of table names");
      }
    ;

opt_from_clause:
    from_clause
    | %empty { $$ = nullptr; }
;

from_clause:
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
    ;

// The rules opt_clauses_following_from, opt_clauses_following_where and
// opt_clauses_following_group_by exist to constrain QUALIFY clauses to require
// a WHERE, GROUP BY, or HAVING clause when the QUALIFY keyword is nonreserved.
//
// This restriction exists to ensure that there is a clause that starts with a
// reserved keyword (WHERE, GROUP BY or HAVING) between them.
//
// When QUALIFY is enabled as a reserved keyword in the LanguageOptions, the
// requirement for the QUALIFY clause to have WHERE, GROUP BY, or HAVING
// preceding it goes away.
opt_clauses_following_from:
    where_clause opt_group_by_clause opt_having_clause
    opt_qualify_clause opt_window_clause
      {
        $$ = {$1, $2, $3, $4, $5};
      }
    | opt_clauses_following_where
      {
        $$ = {/*where=*/nullptr, $1.group_by, $1.having, $1.qualify, $1.window};
      };

opt_clauses_following_where:
    group_by_clause opt_having_clause opt_qualify_clause opt_window_clause
      {
        $$ = {/*where=*/nullptr, $1, $2, $3, $4};
      }
    | opt_clauses_following_group_by
      {
        $$ = {/*where=*/nullptr, /*group_by=*/nullptr, $1.having, $1.qualify,
              $1.window};
      };

opt_clauses_following_group_by:
    having_clause opt_qualify_clause opt_window_clause
      {
        $$ = {/*where=*/nullptr, /*group_by=*/nullptr, $1, $2, $3};
      }
    | opt_qualify_clause_reserved opt_window_clause
      {
        $$ = {/*where=*/nullptr, /*group_by=*/nullptr, /*having=*/nullptr,
              $1, $2};
      };

where_clause:
    "WHERE" expression { $$ = MAKE_NODE(ASTWhereClause, @$, {$2}); };

opt_where_clause:
    where_clause
    | %empty { $$ = nullptr; }
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

cube_list:
    "CUBE" "(" expression
      {
        $$ = MAKE_NODE(ASTCube, @$, {$3});
      }
    | cube_list "," expression
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

grouping_set:
    "(" ")"
      {
        auto* grouping_set = MAKE_NODE(ASTGroupingSet, @$, {});
        $$ = parser->WithEndLocation(grouping_set, @$);
      }
    | expression
      {
        $$ = MAKE_NODE(ASTGroupingSet, @$, {parser->WithEndLocation($1, @$)});
      }
    | rollup_list ")"
      {
        $$ = MAKE_NODE(ASTGroupingSet, @$, {parser->WithEndLocation($1, @$)});
      }
    | cube_list ")"
      {
        $$ = MAKE_NODE(ASTGroupingSet, @$, {parser->WithEndLocation($1, @$)});
      }
    ;

grouping_set_list:
    "GROUPING" "SETS" "(" grouping_set
      {
        $$ = MAKE_NODE(ASTGroupingSetList, @$, {$4});
      }
    | grouping_set_list "," grouping_set
      {
        $$ = WithExtraChildren($1, {$3});
      }
      ;

// In selection items, NULLS FIRST/LAST is not allowed without ASC/DESC first.
opt_selection_item_order:
    asc_or_desc opt_null_order
      {
        auto* node =
            MAKE_NODE(ASTGroupingItemOrder, @$, {$opt_null_order});
        node->set_ordering_spec($1);
        $$ = node;
      }
    | %empty { $$ = nullptr; }
  ;

// In grouping items, NULLS FIRST/LAST is allowed without ASC/DESC first.
opt_grouping_item_order:
    opt_selection_item_order
    | null_order
      {
        auto* node =
            MAKE_NODE(ASTGroupingItemOrder, @$, {$null_order});
        $$ = node;
      }
    ;

grouping_item:
    "(" ")"
      {
        auto* grouping_item = MAKE_NODE(ASTGroupingItem, @$, {});
        $$ = parser->WithEndLocation(grouping_item, @$);
      }
    // Making AS optional currently causes a conflict because
    // KW_QUALIFY_NONRESERVED can follow GROUP BY.
    | expression opt_as_alias_with_required_as
          opt_grouping_item_order
      {
        if ($2 != nullptr
            && !parser->language_options().LanguageFeatureEnabled(
                  zetasql::FEATURE_PIPES)
           ) {
          YYERROR_AND_ABORT_AT(
              @2, "Syntax error: GROUP BY does not support aliases");
        }
        if ($3 != nullptr
            && !parser->language_options().LanguageFeatureEnabled(
                  zetasql::FEATURE_PIPES)
           ) {
          YYERROR_AND_ABORT_AT(
              @3, absl::StrCat("Syntax error: Unexpected ",
                               parser->GetFirstTokenOfNode(@3)));
        }
        $$ = MAKE_NODE(ASTGroupingItem, @$, {$1, $2, $3});
      }
    | rollup_list ")"
      {
        $$ = MAKE_NODE(ASTGroupingItem, @$, {parser->WithEndLocation($1, @$)});
      }
    | cube_list ")"
      {
        $$ = MAKE_NODE(ASTGroupingItem, @$, {parser->WithEndLocation($1, @$)});
      }
    | grouping_set_list ")"
      {
        $$ = MAKE_NODE(ASTGroupingItem, @$, {parser->WithEndLocation($1, @$)});
      }
    ;

opt_and_order:
    "AND" "ORDER" {
        if (!parser->language_options().LanguageFeatureEnabled(
                  zetasql::FEATURE_PIPES)) {
          YYERROR_AND_ABORT_AT(
              @1, "Syntax error: Unexpected AND");
        }
        $$ = true;
      }
    |
    %empty { $$ = false; }
  ;

group_by_preamble:
    "GROUP" opt_hint
        opt_and_order
        "BY"
      {
        $$.hint = $opt_hint;
        $$.and_order_by = $opt_and_order;
      }
    ;

group_by_clause_prefix:
    group_by_preamble[preamble] grouping_item[item]
      {
        auto* node = MAKE_NODE(ASTGroupBy, @$, {$preamble.hint, $item});
        node->set_and_order_by($preamble.and_order_by);
        $$ = node;
      }
    | group_by_clause_prefix[prefix] "," grouping_item[item]
      {
        $$ = WithExtraChildren($prefix, {$item});
      }
    ;

group_by_all:
    group_by_preamble[preamble] "ALL"[all]
      {
        auto* group_by_all = MAKE_NODE(ASTGroupByAll, @all, {});
        auto* node = MAKE_NODE(ASTGroupBy, @$, {$preamble.hint, group_by_all});
        node->set_and_order_by($preamble.and_order_by);
        $$ = node;
      }
    ;

group_by_clause:
    group_by_all
    | group_by_clause_prefix
    ;

opt_group_by_clause:
    group_by_clause
    | %empty { $$ = nullptr; }
    ;

// Note: This version does not support GROUP BY ALL.
// Using `group_by_clause` instead of `group_by_clause_prefix` causes
// a shift/reduce conflict.
opt_group_by_clause_with_opt_comma:
    group_by_clause_prefix opt_comma
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    | %empty { $$ = nullptr; }
    ;

having_clause:
    "HAVING" expression
      {
        $$ = MAKE_NODE(ASTHaving, @$, {$2});
      };

opt_having_clause:
    having_clause
    | %empty { $$ = nullptr; }
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | %empty { $$ = nullptr; }
    ;

opt_qualify_clause:
      qualify_clause_reserved { $$ = $1; }
    | qualify_clause_nonreserved { $$ = $1; }
    | %empty { $$ = nullptr; }
    ;

qualify_clause_reserved:
    KW_QUALIFY_RESERVED expression
      {
       if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_QUALIFY)) {
          YYERROR_AND_ABORT_AT(@1, "QUALIFY is not supported");
        }
        $$ = MAKE_NODE(ASTQualify, @$, {$2});
      }
    ;

opt_qualify_clause_reserved:
   qualify_clause_reserved { $$ = $1; }
   | %empty { $$ = nullptr; }

qualify_clause_nonreserved:
    KW_QUALIFY_NONRESERVED expression
      {
       if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_QUALIFY)) {
          YYERROR_AND_ABORT_AT(@1, "QUALIFY is not supported");
        }
        $$ = MAKE_NODE(ASTQualify, @$, {$2});
      }
    ;

limit_offset_clause:
    "LIMIT" expression
    "OFFSET" expression
      {
        $$ = MAKE_NODE(ASTLimitOffset, @$, {$2, $4});
      }
    | "LIMIT" expression
      {
        $$ = MAKE_NODE(ASTLimitOffset, @$, {$2});
      }
    ;

opt_limit_offset_clause:
    limit_offset_clause { $$ = $1; }
    | %empty { $$ = nullptr; }
    ;

// The GROUP BY modifier cannot be used with the HAVING modifier.
opt_having_or_group_by_modifier:
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
    | group_by_clause_prefix
      {
        $$ = $1;
      }
    | %empty { $$ = nullptr; }
    ;

opt_clamped_between_modifier:
    "CLAMPED" "BETWEEN" expression_higher_prec_than_and "AND" expression %prec "BETWEEN"
      {
        $$ = MAKE_NODE(ASTClampedBetweenModifier, @$, {$3, $5})
      }
    | %empty { $$ = nullptr; }
    ;

opt_with_report_modifier:
    "WITH" "REPORT" opt_with_report_format
      {
        $$ = MAKE_NODE(ASTWithReportModifier, @$, {$3});
      }
    | %empty { $$ = nullptr; }
    ;

opt_with_report_format:
    options_list { $$ = $1; }
    | %empty { $$ = nullptr; }
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
    | %empty
      {
        $$ = zetasql::ASTFunctionCall::DEFAULT_NULL_HANDLING;
      }
    ;

possibly_unbounded_int_literal_or_parameter:
    int_literal_or_parameter { $$ = MAKE_NODE(ASTIntOrUnbounded, @$, {$1}); }
    | "UNBOUNDED" { $$ = MAKE_NODE(ASTIntOrUnbounded, @$, {}); }
    ;

recursion_depth_modifier:
    "WITH" "DEPTH" opt_as_alias_with_required_as[alias]
      {
        auto empty_location = LocationFromOffset(@alias.end());

        // By default, they're unbounded when unspecified.
        auto* lower_bound = MAKE_NODE(ASTIntOrUnbounded, empty_location, {});
        auto* upper_bound = MAKE_NODE(ASTIntOrUnbounded, empty_location, {});
        $$ = MAKE_NODE(ASTRecursionDepthModifier, @$,
                       {$alias, lower_bound, upper_bound});
      }
      // Bison uses the prec of the last terminal (which is "AND" in this case),
      // so we need to explicitly set to %prec "BETWEEN"
      // TODO: Clean up BETWEEN ... AND ... syntax once
      // we move to TextMapper.
    | "WITH" "DEPTH" opt_as_alias_with_required_as[alias]
      "BETWEEN" possibly_unbounded_int_literal_or_parameter[lower_bound]
      "AND" possibly_unbounded_int_literal_or_parameter[upper_bound]
      %prec "BETWEEN"
      {
        $$ = MAKE_NODE(ASTRecursionDepthModifier, @$,
                       {$alias, $lower_bound, $upper_bound});
      }
    | "WITH" "DEPTH" opt_as_alias_with_required_as[alias]
      "MAX" possibly_unbounded_int_literal_or_parameter[upper_bound]
      {
        auto empty_location = LocationFromOffset(@alias.end());

        // Lower bound is unspecified in this case.
        auto* lower_bound = MAKE_NODE(ASTIntOrUnbounded, empty_location, {});
        $$ = MAKE_NODE(ASTRecursionDepthModifier, @$,
                       {$alias, lower_bound, $upper_bound});
      }
    ;

aliased_query_modifiers:
    recursion_depth_modifier
      {
        $$ = MAKE_NODE(ASTAliasedQueryModifiers, @$,
                       {$recursion_depth_modifier});
      }
    | %empty { $$ = nullptr; }
    ;

aliased_query:
    identifier "AS" parenthesized_query[query]
                    aliased_query_modifiers[modifiers]
      {
        $$ = MAKE_NODE(ASTAliasedQuery, @$, {$1, $query, $modifiers});
      }
    ;

aliased_query_list:
    aliased_query { $$ = MAKE_NODE(ASTAliasedQueryList, @$, {$1}); }
    | aliased_query_list "," aliased_query
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

with_clause:
    "WITH" aliased_query
      {
        $$ = MAKE_NODE(ASTWithClause, @$, {$2});
        $$ = parser->WithEndLocation($$, @$);
      }
    | "WITH" "RECURSIVE" aliased_query
      {
        zetasql::ASTWithClause* with_clause =
            MAKE_NODE(ASTWithClause, @$, {$3})
        with_clause = parser->WithEndLocation(with_clause, @$);
        with_clause->set_recursive(true);
        $$ = with_clause;
      }
    | with_clause "," aliased_query
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

opt_with_clause:
    with_clause
    | %empty { $$ = nullptr; }
  ;

opt_with_connection_clause:
    with_connection_clause
    | %empty { $$ = nullptr; }
    ;

with_clause_with_trailing_comma:
    with_clause ","
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

asc_or_desc:
    "ASC" { $$ = zetasql::ASTOrderingExpression::ASC; }
    | "DESC" { $$ = zetasql::ASTOrderingExpression::DESC; }
    ;

opt_asc_or_desc:
    asc_or_desc { $$ = $1; }
    | %empty { $$ = zetasql::ASTOrderingExpression::UNSPECIFIED; }
    ;

null_order:
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
    ;

opt_null_order:
    null_order
    | %empty { $$ = nullptr; }
    ;

string_literal_or_parameter:
    string_literal { $$ = $string_literal; }
    | parameter_expression
    | system_variable_expression;

collate_clause:
    "COLLATE" string_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTCollate, @$, {$2});
      }

opt_collate_clause:
    collate_clause
    | %empty { $$ = nullptr; }
    ;

opt_default_collate_clause:
    "DEFAULT" collate_clause
      {
        $$ = $2;
      }
    | %empty { $$ = nullptr; }
    ;

ordering_expression:
    expression opt_collate_clause opt_asc_or_desc opt_null_order
      {
        auto* ordering_expr =
            MAKE_NODE(ASTOrderingExpression, @$, {$1, $2, $4, nullptr});
        ordering_expr->set_ordering_spec($3);
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

order_by_clause:
    order_by_clause_prefix
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

order_by_clause_with_opt_comma:
    order_by_clause_prefix opt_comma
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

opt_order_by_clause:
    order_by_clause { $$ = $1; }
    | %empty { $$ = nullptr; }
    ;

parenthesized_in_rhs:
    parenthesized_query[query]
      {
        $$ = $query;
      }
    | "(" expression_maybe_parenthesized_not_a_query[e] ")"
      {
        // `expression_maybe_parenthesized_not_a_query` is NOT a query.
        $$ = MAKE_NODE(ASTInList, @e, {$e});
      }
    | in_list_two_or_more_prefix ")"
      {
        // Don't include the ")" in the location, to match the JavaCC parser.
        // TODO: Fix that.
        $$ = parser->WithEndLocation($1, @1);
      }
    ;

parenthesized_anysomeall_list_in_rhs:
    // This block of the rule will cover following types of queries:
    // (1) LIKE ANY|SOME|ALL (query)
    // (2) LIKE ANY|SOME|ALL ((query))
    // (3) LIKE ANY|SOME|ALL ('a', (query))
    // (1) falls under V_1_4_LIKE_ANY_SOME_ALL_SUBQUERY feature since it is
    // not treated as a scalar query. (2) and (3) are treated a scalar queries.
    parenthesized_query[query]
      {
        if (!$query->parenthesized() &&
          !parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_V_1_4_LIKE_ANY_SOME_ALL_SUBQUERY)) {
          YYERROR_AND_ABORT_AT(@1, "The LIKE ANY|SOME|ALL operator does "
            "not support subquery expression as patterns. "
            "Patterns must be string or bytes; "
            "did you mean LIKE ANY|SOME|ALL (pattern1, pattern2, ...)?");
        }
        $query->set_parenthesized(false);
        auto* sub_query = MAKE_NODE(ASTExpressionSubquery, @query, {$query});
        $$ = MAKE_NODE(ASTInList, @$, {sub_query});
      }
    | "(" expression_maybe_parenthesized_not_a_query[e] ")"
      {
        // `expression_maybe_parenthesized_not_a_query` is NOT a query.
        $$ = MAKE_NODE(ASTInList, @e, {$e});
      }
    | in_list_two_or_more_prefix ")"
      {
        // Don't include the ")" in the location, to match the JavaCC parser.
        // TODO: Fix that.
        $$ = parser->WithEndLocation($1, @1);
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

expression_with_opt_alias:
    expression opt_as_alias_with_required_as[opt_alias]
      {
        $$ = MAKE_NODE(ASTExpressionWithOptAlias, @$, {$expression, $opt_alias});
      }

unnest_expression_prefix:
    "UNNEST" "(" expression_with_opt_alias[expression]
      {
        $$ = MAKE_NODE(ASTUnnestExpression, @$, {$expression});
      }
    | unnest_expression_prefix[prefix] "," expression_with_opt_alias[expression]
      {
        $$ = WithExtraChildren($prefix, {$expression});
      }

opt_array_zip_mode:
    "," named_argument { $$ = $named_argument; }
    | %empty { $$ = nullptr; }

unnest_expression:
    unnest_expression_prefix[prefix] opt_array_zip_mode ")"
      {
        $$ = parser->WithEndLocation(
          WithExtraChildren($prefix, {$opt_array_zip_mode}), @$);
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

// This returns an AnySomeAllOp to indicate what keyword was present.
any_some_all:
    "ANY"
      {
       if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_LIKE_ANY_SOME_ALL)) {
          YYERROR_AND_ABORT_AT(@1, "LIKE ANY is not supported");
        }
        auto* op =
            MAKE_NODE(ASTAnySomeAllOp, @$, {});
        op->set_op(zetasql::ASTAnySomeAllOp::kAny);
        $$ = op;
      }
    | "SOME"
      {
       if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_LIKE_ANY_SOME_ALL)) {
          YYERROR_AND_ABORT_AT(@1, "LIKE SOME is not supported");
        }
        auto* op =
            MAKE_NODE(ASTAnySomeAllOp, @$, {});
        op->set_op(zetasql::ASTAnySomeAllOp::kSome);
        $$ = op;
      }
    | "ALL"
      {
       if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_LIKE_ANY_SOME_ALL)) {
          YYERROR_AND_ABORT_AT(@1, "LIKE ALL is not supported");
        }
        auto* op =
            MAKE_NODE(ASTAnySomeAllOp, @$, {});
        op->set_op(zetasql::ASTAnySomeAllOp::kAll);
        $$ = op;
      }
    ;

// Returns NotKeywordPresence to indicate whether NOT was present.
like_operator:
    "LIKE" { $$ = NotKeywordPresence::kAbsent; } %prec "LIKE"
    | "NOT_SPECIAL" "LIKE"
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
    | "NOT_SPECIAL" "BETWEEN"
      {
        @$ = @2;  // Error messages should point at the "BETWEEN".
        $$ = NotKeywordPresence::kPresent;
      } %prec "BETWEEN"
    ;

distinct_operator:
    "IS" "DISTINCT" "FROM"
      {
        $$ = NotKeywordPresence::kAbsent;
      } %prec "DISTINCT"
    | "IS" "NOT_SPECIAL" "DISTINCT" "FROM"
      {
        @$ = @3;  // Error messages should point at the "DISTINCT".
        $$ = NotKeywordPresence::kPresent;
      } %prec "DISTINCT"
    ;

// Returns NotKeywordPresence to indicate whether NOT was present.
in_operator:
    "IN" { $$ = NotKeywordPresence::kAbsent; } %prec "IN"
    | "NOT_SPECIAL" "IN"
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

with_expression_variable:
  identifier "AS" expression
      {
        auto* alias = MAKE_NODE(ASTAlias, @1, @2, {$1});
        $$ = MAKE_NODE(ASTSelectColumn, @$, {$3, alias});
      }

with_expression_variable_prefix:
    with_expression_variable
      {
        $$ = MAKE_NODE(ASTSelectList, @$, {$1});
      }
    |
    with_expression_variable_prefix "," with_expression_variable
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

with_expression:
  KW_WITH_STARTING_WITH_EXPRESSION "(" with_expression_variable_prefix "," expression ")"
    {
      $$ = MAKE_NODE(ASTWithExpression, @$, {$3, $5});
    }
  ;

// This top level rule is designed to make it possible to use one token of
// lookahead to decide whether to turn a parenthesized query query into an
// expression.
// The lookahead is used to disambiguate between
// - the expression case like ((SELECT 1)) + 1
// - and the set operation query case like ((SELECT 1)) UNION ALL (SELECT 2).
expression:
  expression_higher_prec_than_and
  | and_expression %prec "AND"
  | or_expression %prec "OR"
  ;

or_expression:
    expression[lhs] "OR" expression[rhs] %prec "OR"
      {
        if ($lhs->node_kind() == zetasql::AST_OR_EXPR &&
            !$lhs->parenthesized()) {
          // Embrace and extend $lhs's ASTNode.
          $$ = WithExtraChildren(parser->WithEndLocation($lhs, @3), {$rhs});
        } else {
          $$ = MAKE_NODE(ASTOrExpr, @$, {$lhs, $rhs});
        }
      }
    ;

and_expression:
    and_expression[lhs] "AND" expression_higher_prec_than_and[rhs] %prec "AND"
      {
        // Embrace and extend $lhs's ASTNode to flatten a series of ANDs.
        $$ = WithExtraChildren(parser->WithEndLocation($lhs, @3), {$rhs});
      }
    | expression_higher_prec_than_and[lhs] "AND" expression_higher_prec_than_and[rhs] %prec "AND"
       {
          $$ = MAKE_NODE(ASTAndExpr, @$, {$lhs, $rhs});
       }
    ;

// Any expression that has a higher precedence than AND. Anything here can
// directly go as a lower bound for a BETWEEN expression (i.e., without needing
// parens), or a parenthesized expression of any form. Anything matching this
// rule can thus serve as a lower bound for BETWEEN.
expression_higher_prec_than_and:
  unparenthesized_expression_higher_prec_than_and
  | parenthesized_expression_not_a_query
  | parenthesized_query[query]
    {
      // As the query ASTExpressionSubquery already has parentheses, set this
      // flag to false to avoid a double nesting like SELECT ((SELECT 1)).
      $query->set_parenthesized(false);
      $$ = MAKE_NODE(ASTExpressionSubquery, @query, {$query});
    }
  ;

expression_maybe_parenthesized_not_a_query:
  parenthesized_expression_not_a_query
  | unparenthesized_expression_higher_prec_than_and
  | and_expression
  | or_expression
  ;

// Don't include the location in the parentheses. Semantic error messages about
// this expression should point at the start of the expression, not at the
// opening parentheses.
parenthesized_expression_not_a_query:
  "(" expression_maybe_parenthesized_not_a_query[e] ")"
      {
        $e->set_parenthesized(true);
        $$ = $e;
      }
    ;


// The most restrictive rule: expression that is not parenthesized, and is not
// `e1 AND e2`. Anything in this rule can directly be a lower bound for a
// BETWEEN expression, without any parens.
unparenthesized_expression_higher_prec_than_and:
    null_literal
    | boolean_literal
    | string_literal { $$ = $string_literal; }
    | bytes_literal { $$ = $bytes_literal; }
    | integer_literal
    | numeric_literal
    | bignumeric_literal
    | json_literal
    | floating_point_literal
    | date_or_time_literal
    | range_literal
    | parameter_expression
    | system_variable_expression
    | array_constructor
    | new_constructor
    | braced_constructor[ctor] { $$ = $ctor; }
    | braced_new_constructor
    | struct_braced_constructor
    | case_expression
    | cast_expression
    | extract_expression
    | with_expression
    | replace_fields_expression
    | function_call_expression_with_clauses
    | interval_expression
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
    | struct_constructor
    | expression_subquery_with_keyword
      {
        $$ = $1;
      }
    | expression_higher_prec_than_and "[" expression "]" %prec PRIMARY_PRECEDENCE
      {
        auto* bracket_loc = parser->MakeLocation(@2);
        $$ = MAKE_NODE(ASTArrayElement, @1, @4, {$1, bracket_loc, $3});
      }
    | expression_higher_prec_than_and "." "(" path_expression ")"  %prec PRIMARY_PRECEDENCE
      {
        $$ = MAKE_NODE(ASTDotGeneralizedField, @1, @5, {$1, $4});
      }
    | expression_higher_prec_than_and "." identifier %prec PRIMARY_PRECEDENCE
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
          $$ = WithExtraChildren(parser->WithEndLocation($1, @3), {$3});
        } else {
          $$ = MAKE_NODE(ASTDotIdentifier, @1, @3, {$1, $3});
        }
      }
    | "NOT" expression_higher_prec_than_and %prec UNARY_NOT_PRECEDENCE
      {
        auto* not_expr = MAKE_NODE(ASTUnaryExpression, @$, {$2});
        not_expr->set_op(zetasql::ASTUnaryExpression::NOT);
        $$ = not_expr;
      }
    | expression_higher_prec_than_and like_operator any_some_all opt_hint unnest_expression %prec "LIKE"
        {
          if ($4) {
            YYERROR_AND_ABORT_AT(@4,
                                 "Syntax error: HINTs cannot be specified on "
                                 "LIKE clause with UNNEST");
          }
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                 "Syntax error: Expression to the left of LIKE "
                                 "must be parenthesized");
          }
          auto* like_location = parser->MakeLocation(@2);
          auto* like_expression = MAKE_NODE(ASTLikeExpression, @1, @5,
                                            {$1, like_location, $3, $5});
          like_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = like_expression;
        }
    | expression_higher_prec_than_and like_operator any_some_all opt_hint parenthesized_anysomeall_list_in_rhs %prec "LIKE"
        {
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                "Syntax error: Expression to the left of LIKE "
                                "must be parenthesized");
          }
          auto* like_location = parser->MakeLocation(@2);
          zetasql::ASTLikeExpression* like_expression = nullptr;
          if ($5->node_kind() == zetasql::AST_QUERY) {
            like_expression = MAKE_NODE(ASTLikeExpression, @1, @5,
                                        {$1, like_location, $3, $4, $5});
          } else {
            if($4) {
              YYERROR_AND_ABORT_AT(@4,
                                  "Syntax error: HINTs cannot be specified on "
                                  "LIKE clause with value list");
            }
            like_expression = MAKE_NODE(ASTLikeExpression, @1, @5,
                                        {$1, like_location, $3, $5});
          }
          like_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = like_expression;
        }
    | expression_higher_prec_than_and like_operator expression_higher_prec_than_and %prec "LIKE"
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
    | expression_higher_prec_than_and distinct_operator expression_higher_prec_than_and %prec "DISTINCT"
        {
          if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_3_IS_DISTINCT)) {
            YYERROR_AND_ABORT_AT(
                @2,
                "IS DISTINCT FROM is not supported");
          }
          auto binary_expression =
              MAKE_NODE(ASTBinaryExpression, @1, @3, {$1, $3});
              binary_expression->set_is_not($2 == NotKeywordPresence::kPresent);
              binary_expression->set_op(
                  zetasql::ASTBinaryExpression::DISTINCT);
          $$ = binary_expression;
        }
    | expression_higher_prec_than_and in_operator opt_hint unnest_expression %prec "IN"
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
          zetasql::ASTLocation* in_location = parser->MakeLocation(@2);
          auto* in_expression =
              MAKE_NODE(ASTInExpression, @1, @4, {$1, in_location, $4});
          in_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = in_expression;
        }
    | expression_higher_prec_than_and in_operator opt_hint parenthesized_in_rhs %prec "IN"
        {
          // Bison allows some cases like IN on the left hand side because it's
          // not ambiguous. The language doesn't allow this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                "Syntax error: Expression to the left of IN "
                                "must be parenthesized");
          }
          zetasql::ASTInExpression* in_expression = nullptr;
          zetasql::ASTLocation* in_location = parser->MakeLocation(@2);
          if ($4->node_kind() == zetasql::AST_QUERY) {
            in_expression =
                MAKE_NODE(ASTInExpression, @1, @4, {$1, in_location, $3, $4});
          } else {
            if($3) {
              YYERROR_AND_ABORT_AT(@3,
                                  "Syntax error: HINTs cannot be specified on "
                                  "IN clause with value list");
            }
            in_expression =
                MAKE_NODE(ASTInExpression, @1, @4, {$1, in_location, $4});
          }
          in_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = in_expression;
        }
    // Bison uses the prec of the last terminal (which is "AND" in this case),
    // so we need to explicitly set to %prec "BETWEEN"
    | expression_higher_prec_than_and between_operator
      expression_higher_prec_than_and "AND"
      expression_higher_prec_than_and %prec "BETWEEN"
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
          auto* between_loc = parser->MakeLocation(@2);
          auto* between_expression =
              MAKE_NODE(ASTBetweenExpression, @1, @5, {$1, between_loc, $3, $5});
          between_expression->set_is_not($2 == NotKeywordPresence::kPresent);
          $$ = between_expression;
        }
    | expression_higher_prec_than_and between_operator
      expression_higher_prec_than_and[lower] "OR" %prec "BETWEEN"
        {
          // The "OR" operator has lower precedence than "AND". The rule
          // immediately above tries to catch all expressions with lower or
          // equal precedence to "AND" and block them from being the middle
          // argument of "BETWEEN". See the comment on the generic rule above
          // for justification of the error. We need this special rule to catch
          // "OR", and only "OR", because "OR" is the only operator with lower
          // precendence to "AND" that is factored out of
          // `expression_higher_prec_than_and` into its own rule.
          YYERROR_AND_ABORT_AT(@lower,
                               "Syntax error: Expression in BETWEEN must be "
                               "parenthesized");
        }
    | expression_higher_prec_than_and is_operator "UNKNOWN" %prec "IS"
        {
          // The Bison parser allows comparison expressions in the LHS, even
          // though these operators are at the same precedence level and are not
          // associative. Explicitly forbid this.
          if (!$1->IsAllowedInComparison()) {
            YYERROR_AND_ABORT_AT(@2,
                                 "Syntax error: Expression to the left of IS "
                                 "must be parenthesized");
          }
          auto* unary_expression = MAKE_NODE(ASTUnaryExpression, @$, {$1});
          if ($2 == NotKeywordPresence::kPresent) {
            unary_expression->set_op(
              zetasql::ASTUnaryExpression::IS_NOT_UNKNOWN);
          }
          else {
            unary_expression->set_op(
              zetasql::ASTUnaryExpression::IS_UNKNOWN);
          }
          $$ = unary_expression;
        }
    | expression_higher_prec_than_and is_operator null_literal %prec "IS"
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
    | expression_higher_prec_than_and is_operator boolean_literal %prec "IS"
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
    | expression_higher_prec_than_and comparative_operator expression_higher_prec_than_and %prec "="
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
    | expression_higher_prec_than_and "|" expression_higher_prec_than_and
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
    | expression_higher_prec_than_and "^" expression_higher_prec_than_and
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
    | expression_higher_prec_than_and "&" expression_higher_prec_than_and
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
    | expression_higher_prec_than_and "||" expression_higher_prec_than_and
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
    | expression_higher_prec_than_and shift_operator expression_higher_prec_than_and %prec "<<"
      {
        // NOT has lower precedence but can be parsed unparenthesized in the
        // rhs because it is not ambiguous. This is not allowed. Other
        // expressions with lower precedence wouldn't be parsed as children, so
        // we don't have to check for those.
        if (IsUnparenthesizedNotExpression($3)) {
          YYERROR_UNEXPECTED_AND_ABORT_AT(@3);
        }
        auto* operator_location = parser->MakeLocation(@2);
        auto* binary_expression =
            MAKE_NODE(ASTBitwiseShiftExpression, @1, @3, {$1, operator_location, $3});
        binary_expression->set_is_left_shift($2 == ShiftOperator::kLeft);
        $$ = binary_expression;
      }
    | expression_higher_prec_than_and additive_operator expression_higher_prec_than_and %prec "+"
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
    | expression_higher_prec_than_and multiplicative_operator expression_higher_prec_than_and %prec "*"
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
    | unary_operator expression_higher_prec_than_and %prec UNARY_PRECEDENCE
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
        $$ = WithExtraChildren(parser->WithEndLocation($1, @3), {$3});
      }
    ;

dashed_identifier:
    identifier "-" identifier
      {
        // a - b
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        absl::string_view id1 = parser->GetInputText(@1);
        absl::string_view id2 = parser->GetInputText(@3);
        if (id1[0] == '`' || id2[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts({{id1, "-", id2}});
        $$ = out;
      }
    | dashed_identifier "-" identifier
      {
        // a-b - c
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        SeparatedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        absl::string_view id2 = parser->GetInputText(@3);
        if (id2[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        // Add an extra sub-part to the ending dashed identifier.
        prev.back().push_back("-");
        prev.back().push_back(id2);
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }
    | identifier "-" INTEGER_LITERAL
      {
        // a - 5
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        absl::string_view id1 = parser->GetInputText(@1);
        absl::string_view id2 = parser->GetInputText(@3);
        if (id1[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts({{id1, "-", id2}});
        $$ = out;
      }
    | dashed_identifier "-" INTEGER_LITERAL
      {
        // a-b - 5
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        SeparatedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        absl::string_view id2 = parser->GetInputText(@3);
        prev.back().push_back("-");
        prev.back().push_back(id2);
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }
    | identifier "-" FLOATING_POINT_LITERAL identifier
      {
        // a - 1. b
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        absl::string_view id1 = parser->GetInputText(@1);
        absl::string_view id2 = parser->GetInputText(@3);
        absl::string_view id3 = parser->GetInputText(@4);
        if (id1[0] == '`') {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        // Here (and below) we need to handle the case where dot is lex'ed as
        // part of floating number as opposed to path delimiter. To parse it
        // correctly, we push the components separately (as string_view).
        // {{"a", "1"}, "b"}
        out->set_path_parts({{id1, "-", id2}, {id3}});
        $$ = out;
      }
    | dashed_identifier "-" FLOATING_POINT_LITERAL identifier
      {
        // a-b - 1. c
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected \"-\"");
        }
        SeparatedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        absl::string_view id1 = parser->GetInputText(@3);
        absl::string_view id2 = parser->GetInputText(@4);
        // This case is a continuation of an existing dashed_identifier `prev`,
        // followed by what the lexer believes is a floating point literal.
        // here: /*prev=*/={{"a", "b"}}
        // we append "1" to complete the dashed components, followed
        // by the identifier ("c") as {{"c"}}.
        // Thus, we end up with {{"a", "b", "1"}, {"c"}}
        prev.back().push_back("-");
        prev.back().push_back(id1);
        prev.push_back({id2});
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }

dashed_path_expression:
    dashed_identifier
      {
        absl::StatusOr<std::vector<zetasql::ASTNode*>> path_parts =
          SeparatedIdentifierTmpNode::BuildPathParts(@1,
            std::move($1->release_path_parts()), parser);
        if (!path_parts.ok()) {
          YYERROR_AND_ABORT_AT(@1, std::string(path_parts.status().message()));
        }
        $$ = MAKE_NODE(ASTPathExpression, @$, std::move(path_parts).value());
      }
    | dashed_path_expression "." identifier
      {
        $$ = WithExtraChildren(parser->WithEndLocation($1, @3), {$3});
      }
    ;

maybe_dashed_path_expression:
    path_expression { $$ = $1; }
    | dashed_path_expression
      {
        if (parser->language_options().LanguageFeatureEnabled(
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

maybe_slashed_or_dashed_path_expression:
    maybe_dashed_path_expression { $$ = $1; }
    | slashed_path_expression
      {
        if (parser->language_options().LanguageFeatureEnabled(
               zetasql::FEATURE_V_1_3_ALLOW_SLASH_PATHS)) {
          $$ = $1;
        } else {
          YYERROR_AND_ABORT_AT(
              @1,
              absl::StrCat(
                "Syntax error: Table name contains '/' character. "
                "It needs to be quoted: ",
                zetasql::ToIdentifierLiteral(
                  parser->GetInputText(@1), false)));
        }
      }
    ;

slashed_identifier_separator: "-" | "/" | ":"

// Identifier or integer. SCRIPT_LABEL is also included so that a ":" in a path
// followed by begin/while/loop/repeat/for doesn't trigger the
// script label grammar.
identifier_or_integer: identifier | INTEGER_LITERAL | SCRIPT_LABEL

// An identifier that starts with a "/" and can contain non-adjacent /:-
// separators.
slashed_identifier:
    "/" identifier_or_integer
      {
        // Return an error if there is embedded whitespace.
        if (parser->HasWhitespace(@1, @2)) {
          YYERROR_AND_ABORT_AT(@1, "Syntax error: Unexpected \"/\"");
        }
        absl::string_view id = parser->GetInputText(@2);
        // Return an error if the identifier/literal is quoted.
        if (id[0] == '`') {
          YYERROR_AND_ABORT_AT(@1, "Syntax error: Unexpected \"/\"");
        }
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts({{"/", id}});
        $$ = out;
      }
    | slashed_identifier slashed_identifier_separator
      identifier_or_integer
      {
        absl::string_view separator = parser->GetInputText(@2);
        absl::string_view id = parser->GetInputText(@3);
        // Return an error if there is embedded whitespace.
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2,
            absl::StrFormat("Syntax error: Unexpected \"%s\"", separator));
        }
        // Return an error if the identifier/literal is quoted.
        if (id[0] == '`') {
          YYERROR_AND_ABORT_AT(@2,
            absl::StrFormat("Syntax error: Unexpected \"%s\"", separator));
        }
        SeparatedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        // Add the separator and extra sub-part to the end of the current
        // identifier: {"a", "-", "b"} -> {"a", "-", "b", ":", "c"}
        prev.back().push_back(separator);
        prev.back().push_back(id);
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }
    | slashed_identifier slashed_identifier_separator FLOATING_POINT_LITERAL
      slashed_identifier_separator identifier_or_integer
      {
        // This rule handles floating point literals between separator
        // characters (/:-) before the first dot.  The floating point literal
        // can be {1., .1, 1.1, 1e2, 1.e2, .1e2, 1.1e2}.  The only valid form is
        // "1e2".  All forms containing a dot are invalid because the separator
        // characters are not allowed in identifiers after the dot.
        absl::string_view separator1 = parser->GetInputText(@2);
        absl::string_view float_literal = parser->GetInputText(@3);
        absl::string_view separator2 = parser->GetInputText(@4);
        absl::string_view id = parser->GetInputText(@5);
        // Return an error if there is embedded whitespace.
        if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
          YYERROR_AND_ABORT_AT(@2,
            absl::StrFormat("Syntax error: Unexpected \"%s\"", separator1));
        }
        // Return an error if there is embedded whitespace.
        if (parser->HasWhitespace(@3, @4) || parser->HasWhitespace(@4, @5)) {
          YYERROR_AND_ABORT_AT(@2,
            absl::StrFormat("Syntax error: Unexpected \"%s\"", separator2));
        }
        // Return an error if the trailing identifier is quoted.
        if (id[0] == '`') {
          YYERROR_AND_ABORT_AT(@2,
            absl::StrFormat("Syntax error: Unexpected \"%s\"", separator2));
        }
        // Return an error if the floating point literal contains a dot. Only
        // scientific notation is allowed in this rule.
        if (absl::StrContains(float_literal, '.')) {
          YYERROR_AND_ABORT_AT(@3,
            "Syntax error: Unexpected floating point literal");
        }
        // We are parsing a floating point literal that uses scientific notation
        // in the middle of a slashed path, so just append the text to the
        // existing path. For text: "/a/1e10-b", {"/", "a"} becomes
        // {"/", "a", "/", "1e10". "-", "b"} after matching this rule.
        SeparatedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
        prev.back().push_back(separator1);
        prev.back().push_back(float_literal);
        prev.back().push_back(separator2);
        prev.back().push_back(id);
        auto out = parser->CreateASTNode<SeparatedIdentifierTmpNode>(@1);
        out->set_path_parts(std::move(prev));
        $$ = out;
      }
    ;


// A path where the first identifier starts with "/" and can contain
// non-adjacent /:- separators.  Identifiers after the first dot are regular
// identifiers, except they can also start with a digit.
slashed_path_expression:
  slashed_identifier
     {
       // Build the path.
       absl::StatusOr<std::vector<zetasql::ASTNode*>> path_parts =
          SeparatedIdentifierTmpNode::BuildPathParts(@1,
            std::move($1->release_path_parts()), parser);
       if (!path_parts.ok()) {
         YYERROR_AND_ABORT_AT(@1, std::string(path_parts.status().message()));
       }
       $$ = MAKE_NODE(ASTPathExpression, @$, std::move(path_parts).value());
     }
  | slashed_identifier slashed_identifier_separator FLOATING_POINT_LITERAL
    identifier
    {
      // This rule handles floating point literals that are preceded by a
      // separator character (/:-). The floating point literal can be
      // {1., .1, 1.1, 1e2, 1.e2, .1e2, 1.1e2}, but the only valid form is a
      // floating point that ends with a dot. The dot is interpreted as the path
      // component separator, and we only allow a regular identifier following
      // the dot. A floating point that starts with a dot is not valid becuase
      // this implies that a dot and separator are adjacent: "-.1". A floating
      // point that has a dot in the middle is not supported because this format
      // is rejected by the tokenizer: "1.5table". A floating point literal that
      // does not contain a dot is not valid because this implies scientific
      // notation was lexed when adjacent to an identifier:
      // "/path/1e10  table". In this case it is not possible to determine if
      // the next token is an alias or part of the next statement.
      absl::string_view separator = parser->GetInputText(@2);
      absl::string_view float_literal = parser->GetInputText(@3);
      absl::string_view id = $4->GetAsStringView();
      // Return an error if there is embedded whitespace.
      if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
        YYERROR_AND_ABORT_AT(@2,
          absl::StrFormat("Syntax error: Unexpected \"%s\"", separator));
      }
      // Assert that the raw text of the floating literal ends in a dot since
      // we expect this rule to match at the boundary of a new path component.
      if (!absl::EndsWith(float_literal, ".")) {
        YYERROR_AND_ABORT_AT(@2,absl::StrFormat(
          "Syntax error: Unexpected floating point literal \"%s\" after \"%s\"",
          float_literal, separator));
      }
      SeparatedIdentifierTmpNode::PathParts prev =
        $1->release_path_parts();
      // This case is a continuation of an existing slashed_identifier
      // `prev`, followed by what the lexer believes is a floating point
      // literal.
      // here: /*prev=*/={{"a", "-", "b"}}
      // we append "1" to complete the identifier components, followed
      // by the identifier ("c") as {{"c"}}.
      // Thus, we end up with {{"a", "-", "b", "/", "1"}, {"c"}}
      prev.back().push_back(separator);
      prev.back().push_back(float_literal);
      prev.push_back({id});

      // Build the path.
      absl::StatusOr<std::vector<zetasql::ASTNode*>> path_parts =
        SeparatedIdentifierTmpNode::BuildPathParts(@$,
          std::move(prev), parser);
      if (!path_parts.ok()) {
        YYERROR_AND_ABORT_AT(@1, std::string(path_parts.status().message()));
      }
      $$ = MAKE_NODE(ASTPathExpression, @$, std::move(path_parts).value());
    }
  | slashed_identifier slashed_identifier_separator FLOATING_POINT_LITERAL "."
    identifier
    {
      // This rule matches a slashed_identifier that terminates in a floating
      // point literal and is followed by the next path component, which must be
      // a regular identifier. The floating point literal can be
      // {1., .1, 1.1, 1e2, 1.e2, .1e2, 1.1e2}, but the only valid form is
      // "1e2".  All forms containing a dot are invalid because this implies
      // that either there are two dots in a row "1.." or the next path
      // component is a number itself, which we do not support (like "1.5.table"
      // and "1.1e10.table"). Note: paths like "/span/global.5.table" are
      // supported because once the lexer sees the first dot it enters
      // DOT_IDENTIFIER mode and lexs the "5" as an identifier rather than
      // producing a ".5" floating point literal token.
      absl::string_view separator = parser->GetInputText(@2);
      absl::string_view float_literal = parser->GetInputText(@3);
      // Return an error if there is embedded whitespace.
      if (parser->HasWhitespace(@1, @2) || parser->HasWhitespace(@2, @3)) {
        YYERROR_AND_ABORT_AT(@2,
          absl::StrFormat("Syntax error: Unexpected \"%s\"", separator));
      }
      // Reject any floating point literal that contains a dot.
      if (absl::StrContains(float_literal, '.')) {
        YYERROR_AND_ABORT_AT(@3,
          "Syntax error: Unexpected floating point literal");
      }
      // We are parsing a floating point literal that uses scientific notation
      // "1e10" that is followed by a dot and then an identifier. Append the
      // separator and floating point literal to the existing path and then
      // form an ASTPathExpression from the slash path and the trailing
      // identifier.
      SeparatedIdentifierTmpNode::PathParts prev = $1->release_path_parts();
      prev.back().push_back(separator);
      prev.back().push_back(float_literal);

      // Build the slash path.
      absl::StatusOr<std::vector<zetasql::ASTNode*>> path_parts =
        SeparatedIdentifierTmpNode::BuildPathParts(@$,
          std::move(prev), parser);
      if (!path_parts.ok()) {
        YYERROR_AND_ABORT_AT(@1, std::string(path_parts.status().message()));
      }
      // Add the trailing identifier to the path.
      path_parts.value().push_back($5);
      $$ = MAKE_NODE(ASTPathExpression, @$, std::move(path_parts).value());
    }
  | slashed_path_expression "." identifier
    {
      $$ = WithExtraChildren(parser->WithEndLocation($1, @3), {$3});
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | array_constructor_prefix "]"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

range_literal:
    range_type string_literal
      {
        $$ = MAKE_NODE(ASTRangeLiteral, @$, {$1, $2});
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

interval_expression:
    "INTERVAL" expression identifier
      {
        $$ = MAKE_NODE(ASTIntervalExpr, @$, {$2, $3});
      }
    | "INTERVAL" expression identifier "TO" identifier
      {
        $$ = MAKE_NODE(ASTIntervalExpr, @$, {$2, $3, $5});
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
    "@"[at] identifier
      {
        if (parser->HasWhitespace(@at, @identifier)) {
          // TODO: Add a deprecation warning in this case.
        }
        $$ = MAKE_NODE(ASTParameterExpr, @$, {$2});
      }
    ;

type_name:
    path_expression
      {
        $$ = MAKE_NODE(ASTSimpleType, @$, {$1});
      }
    // Unlike other type names, 'INTERVAL' is a reserved keyword.
    | "INTERVAL"
      {
        auto* id = parser->MakeIdentifier(@1, parser->GetInputText(@1));
        auto* path_expression = MAKE_NODE(ASTPathExpression, @$, {id});
        $$ = MAKE_NODE(ASTSimpleType, @$, {path_expression});
      }
    ;

template_type_open:
    { OVERRIDE_NEXT_TOKEN_CHAR_LOOKBACK('<', LB_OPEN_TYPE_TEMPLATE); }
    "<"[open]
    { @$ = @open; }
    ;

template_type_close:
    { OVERRIDE_NEXT_TOKEN_CHAR_LOOKBACK('>', LB_CLOSE_TYPE_TEMPLATE); }
    ">"[close]
    { @$ = @close; }
    ;

array_type:
    "ARRAY" template_type_open type template_type_close
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
    "STRUCT" template_type_open struct_field
      {
        $$ = MAKE_NODE(ASTStructType, @$, {$3});
      }
    | struct_type_prefix "," struct_field
      {
        $$ = WithExtraChildren($1, {$3});
      }
    ;

struct_type:
    "STRUCT" template_type_open template_type_close
      {
        $$ = MAKE_NODE(ASTStructType, @$);
      }
    | struct_type_prefix template_type_close
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

range_type:
    "RANGE" template_type_open type template_type_close
      {
        $$ = MAKE_NODE(ASTRangeType, @$, {$3});
      }
    ;

function_type_prefix:
    "FUNCTION" template_type_open "(" type
      {
        $$ = MAKE_NODE(ASTFunctionTypeArgList, @$, {$type});
      }
    | function_type_prefix[prev] "," type
      {
        $$ = WithExtraChildren($prev, {$type});
      }
    ;

function_type:
    "FUNCTION" template_type_open "("[open_paren] ")"[close_paren] "->" type[return_type] template_type_close
      {
        auto empty_arg_list =
            MAKE_NODE(ASTFunctionTypeArgList, @open_paren, @close_paren, {});
        $$ = MAKE_NODE(ASTFunctionType, @$, {empty_arg_list, $return_type});
      }
    | "FUNCTION" template_type_open type[arg_type] "->" type[return_type] template_type_close
      {
        auto arg_list =
            MAKE_NODE(ASTFunctionTypeArgList, @arg_type, {$arg_type});
        $$ = MAKE_NODE(ASTFunctionType, @$, {arg_list, $return_type});
      }
    | function_type_prefix[arg_list] ")" "->" type[return_type] template_type_close
      {
        $$ = MAKE_NODE(ASTFunctionType, @$, {$arg_list, $return_type});
      }
    ;

map_type:
    "MAP" template_type_open type[key_type] "," type[value_type] template_type_close
      {
        $$ = MAKE_NODE(ASTMapType, @$, {$key_type, $value_type});
      }
    ;


raw_type:
    array_type | struct_type | type_name | range_type | function_type | map_type;

type_parameter:
      integer_literal
    | boolean_literal
    | string_literal { $$ = $string_literal; }
    | bytes_literal { $$ = $bytes_literal; }
    | floating_point_literal { $$ = $floating_point_literal; }
    | "MAX"
      {
        $$ = MAKE_NODE(ASTMaxLiteral, @1, {});
      }
    ;

type_parameters_prefix:
    "(" type_parameter
      {
        $$ = MAKE_NODE(ASTTypeParameterList, @$, {$2});
      }
    | type_parameters_prefix "," type_parameter
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

opt_type_parameters:
    type_parameters_prefix ")" { $$ = $1; }
    | type_parameters_prefix "," ")"
      {
        YYERROR_AND_ABORT_AT(@2,
                             "Syntax error: Trailing comma in type parameter "
                             "list is not allowed.");
      }
    | %empty { $$ = nullptr; }
    ;

type: raw_type opt_type_parameters opt_collate_clause
    {
      $$ = parser->WithEndLocation(WithExtraChildren($1, {$2, $3}), @$);
    };

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
        const absl::string_view templated_type_string = $1->GetAsStringView();
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
        $$ = parser->WithEndLocation($1, @2);
      }
    | new_constructor_prefix_no_arg ")"
      {
        $$ = parser->WithEndLocation($1, @2);
      }
    ;

braced_constructor_field_value:
    ":" expression
      {
        $$ = MAKE_NODE(ASTBracedConstructorFieldValue, @$, {$2});
        $$->set_colon_prefixed(true);
      }
    | braced_constructor
      {
        $$ = MAKE_NODE(ASTBracedConstructorFieldValue, @$, {$1});
      }
    ;

braced_constructor_extension:
    "(" path_expression ")" braced_constructor_field_value
      {
        $$ = MAKE_NODE(ASTBracedConstructorField, @$, {$2, $4});
      }
    ;

braced_constructor_field:
    identifier braced_constructor_field_value
      {
        $$ = MAKE_NODE(ASTBracedConstructorField, @$, {$1, $2});
      }
    ;

braced_constructor_start:
    "{"
    {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_BRACED_PROTO_CONSTRUCTORS)) {
          YYERROR_AND_ABORT_AT(@1, "Braced constructors are not supported");
        }
        $$ = MAKE_NODE(ASTBracedConstructor, @$);
    }
    ;

braced_constructor_prefix:
    braced_constructor_start braced_constructor_field
      {
        $$ = WithExtraChildren($1, {$2});
      }
    | braced_constructor_start braced_constructor_extension
      {
        $$ = WithExtraChildren($1, {$2});
      }
    | braced_constructor_prefix "," braced_constructor_field
      {
        $$ = WithExtraChildren($1, {$3});
        $3->set_comma_separated(true);
      }
    | braced_constructor_prefix braced_constructor_field
      {
        $$ = WithExtraChildren($1, {$2});
      }
    // If we do not require a comma before a path_expression for extensions
    // then it leads to a shift-reduce conflict. An example is:
    //
    // foo: column_name
    // (bar): 3
    //
    // (bar) can be interpreted as part of the previous expression as a
    // function 'column_name(bar)' or independently as a path expression.
    //
    // Fixing this is not possible without arbitrary lookahead.
    | braced_constructor_prefix "," braced_constructor_extension
      {
        $$ = WithExtraChildren($1, {$3});
        $3->set_comma_separated(true);
      }
    ;

braced_constructor:
    braced_constructor_start "}"
      {
        $$ = parser->WithEndLocation($1, @2);
      }
    | braced_constructor_prefix "}"
      {
        $$ = parser->WithEndLocation($1, @2);
      }
    // Allow trailing comma
    | braced_constructor_prefix "," "}"
      {
        $$ = parser->WithEndLocation($1, @3);
      }
    ;

braced_new_constructor:
    "NEW" type_name braced_constructor
      {
        $$ = MAKE_NODE(ASTBracedNewConstructor, @$, {$2, $3});
      }
    ;

struct_braced_constructor:
    struct_type[type] braced_constructor[ctor]
      {
        $$ = MAKE_NODE(ASTStructBracedConstructor, @$, {$type, $ctor});
      }
    | "STRUCT" braced_constructor[ctor]
      {
        $$ = MAKE_NODE(ASTStructBracedConstructor, @$, {$ctor});
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | case_expression_prefix "ELSE" expression "END"
      {
        $$ = WithExtraChildren(parser->WithEndLocation($1, @$), {$3});
      }
    ;

opt_at_time_zone:
    "AT" "TIME" "ZONE" expression
      {
        $$ = $4;
      }
    | %empty { $$ = nullptr; }
    ;


opt_format:
    "FORMAT" expression opt_at_time_zone
       {
         $$ = MAKE_NODE(ASTFormatClause, @$, {$2, $3});
       }
    | %empty { $$ = nullptr; }
    ;

cast_expression:
      "CAST" "(" expression "AS" type opt_format ")"
      {
        auto* cast = MAKE_NODE(ASTCastExpression, @$, {$3, $5, $6});
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
    | "SAFE_CAST" "(" expression "AS" type opt_format ")"
      {
        auto* cast = MAKE_NODE(ASTCastExpression, @$, {$3, $5, $6});
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | extract_expression_base "AT" "TIME" "ZONE" expression ")"
      {
        $$ = WithExtraChildren(parser->WithEndLocation($1, @$), {$5});
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
        $$ = parser->WithEndLocation($1, @$);
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
    | "COLLATE"
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    | "RANGE"
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
    expression_higher_prec_than_and "(" "DISTINCT" %prec PRIMARY_PRECEDENCE
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
    | expression_higher_prec_than_and "(" %prec PRIMARY_PRECEDENCE
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
    expression opt_as_alias_with_required_as
      {
        // When "AS alias" shows up in a function call argument, we wrap a new
        // node ASTExpressionWithAlias with required alias field to indicate
        // the existence of alias. This approach is taken mainly to avoid
        // backward compatibility break to existing widespread usage of
        // ASTFunctionCall.
        if ($2 != nullptr) {
          $$ = MAKE_NODE(ASTExpressionWithAlias, @$, {$1, $2});
        } else {
          $$ = $1;
        }
      }
    | named_argument
    | lambda_argument
    | sequence_arg
    | "SELECT"
      {
        YYERROR_AND_ABORT_AT(
        @1,
        "Each function argument is an expression, not a query; to use a "
        "query as an expression, the query must be wrapped with additional "
        "parentheses to make it a scalar subquery expression");
      }
    ;

sequence_arg:
    "SEQUENCE" path_expression
      {
        $$ = MAKE_NODE(ASTSequenceArg, @$, {$2});
      }
    ;

named_argument:
    identifier "=>" expression
      {
        $$ = MAKE_NODE(ASTNamedArgument, @$, {$1, $3});
      }
    | identifier "=>" lambda_argument
      {
        $$ = MAKE_NODE(ASTNamedArgument, @$, {$identifier, $lambda_argument});
      }
    ;

lambda_argument:
    lambda_argument_list "->" expression
      {
        $$ = MAKE_NODE(ASTLambda, @$, {$1, $3});
      }
    ;

// Lambda argument list could be:
//  * one argument without parenthesis, e.g. e.
//  * one argument with parenthesis, e.g. (e).
//  * multiple argument with parenthesis, e.g. (e, i).
// All of the above could be parsed as expression. (e, i) is parsed as struct
// constructor with parenthesis. We use expression rule to cover them all and to
// avoid conflict.
//
// We cannot use an identifier_list rule as that results in conflict with
// expression function argument. For ''(a, b) -> a + b', bison parser was not
// able to decide what to do with the following working stack: ['(', ID('a')]
// and seeing ID('b'), as bison parser won't look ahead to the '->' token.
lambda_argument_list:
    expression
      {
        auto expr_kind = $1->node_kind();
        if (expr_kind != zetasql::AST_STRUCT_CONSTRUCTOR_WITH_PARENS &&
            expr_kind != zetasql::AST_PATH_EXPRESSION) {
          YYERROR_AND_ABORT_AT(
            @1,
            "Syntax error: Expecting lambda argument list");
        }
        $$ = $1;
      }
    | "(" ")"
    {
      $$ = MAKE_NODE(ASTStructConstructorWithParens, @$);
    }
    ;

function_call_expression_with_args_prefix:
    function_call_expression_base function_call_argument
      {
        $$ = WithExtraChildren($1, {$2});
      }
    // The first argument may be a "*" instead of an expression. This is valid
    // for COUNT(*), which has no other arguments
    // and ANON_COUNT(*), which has multiple other arguments.
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
    function_call_expression_base opt_having_or_group_by_modifier opt_order_by_clause
      opt_limit_offset_clause ")"
      {
        $$ = WithExtraChildren(parser->WithEndLocation($1, @$), {$2, $3, $4});
      }
    // Non-empty argument list.
    // opt_clamped_between_modifier and
    // opt_null_handling_modifier only appear here as they require at least
    // one argument.
    | function_call_expression_with_args_prefix opt_null_handling_modifier
      opt_having_or_group_by_modifier
      opt_clamped_between_modifier
      opt_with_report_modifier
      opt_order_by_clause
      opt_limit_offset_clause ")"
      {
        $1->set_null_handling_modifier($2);
        $$ = WithExtraChildren(parser->WithEndLocation($1, @$), {
            $3,
            $4,
            $5,
            $6,
            $7});
      }
    ;

opt_identifier:
    identifier
    | %empty { $$ = nullptr; }
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
    partition_by_clause_prefix { $$ = parser->WithEndLocation($1, @$); }
    | %empty { $$ = nullptr; }
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
    partition_by_clause_prefix_no_hint { $$ = parser->WithEndLocation($1, @$); }
    | %empty { $$ = nullptr; }
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
    cluster_by_clause_prefix_no_hint { $$ = parser->WithEndLocation($1, @$); }
    | %empty { $$ = nullptr; }
    ;

opt_ttl_clause:
  "ROW" "DELETION" "POLICY" "(" expression ")"
  {
    if (!parser->language_options().LanguageFeatureEnabled(
        zetasql::FEATURE_V_1_4_TTL)) {
      YYERROR_AND_ABORT_AT(@1, "ROW DELETION POLICY clause is not supported.");
    }
    $$ = MAKE_NODE(ASTTtlClause, @$, {$5});
  }
  | %empty { $$ = nullptr; }
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
    frame_unit "BETWEEN" window_frame_bound "AND" window_frame_bound %prec "BETWEEN"
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
    | %empty { $$ = nullptr; }

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

function_call_expression_with_clauses:
    function_call_expression opt_hint opt_with_group_rows opt_over_clause
      {
        zetasql::ASTExpression* current_expression = $1;
        if ($2 != nullptr) {
          current_expression->AddChild($2);
        }
        if ($3 != nullptr) {
          if (!parser->language_options().LanguageFeatureEnabled(
                  zetasql::FEATURE_V_1_3_WITH_GROUP_ROWS)) {
            YYERROR_AND_ABORT_AT(@3, "WITH GROUP ROWS is not supported");
          }
          auto* with_group_rows = MAKE_NODE(ASTWithGroupRows, @$, {$3});
          current_expression->AddChild(with_group_rows);
        }
        if ($4 != nullptr) {
          current_expression = MAKE_NODE(ASTAnalyticFunctionCall, @$,
              {current_expression, $4});
        }
        $$ = current_expression;
      }

opt_with_group_rows:
    KW_WITH_STARTING_WITH_GROUP_ROWS "GROUP" "ROWS" parenthesized_query[query]
      {
        $$ = $query;
      }
    | %empty { $$ = nullptr; }
    ;

opt_over_clause:
    "OVER" window_specification
      {
        $$ = $2;
      }
    | %empty { $$ = nullptr; }
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | struct_constructor_prefix_with_keyword_no_arg ")"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    | struct_constructor_prefix_without_keyword ")"
      {
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

expression_subquery_with_keyword:
    "ARRAY" parenthesized_query[query]
      {
        auto* subquery = MAKE_NODE(ASTExpressionSubquery, @$, {$query});
        subquery->set_modifier(zetasql::ASTExpressionSubquery::ARRAY);
        $$ = subquery;
      }
    | "EXISTS" opt_hint parenthesized_query[query]
      {
        auto* subquery = MAKE_NODE(ASTExpressionSubquery, @$, {$opt_hint, $query});
        subquery->set_modifier(zetasql::ASTExpressionSubquery::EXISTS);
        $$ = subquery;
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

string_literal_component:
    STRING_LITERAL
      {
        const absl::string_view input_text = parser->GetInputText(@1);
        std::string str;
        std::string error_string;
        int error_offset;
        const absl::Status parse_status = zetasql::ParseStringLiteral(
            input_text, &str, &error_string, &error_offset);
        if (!parse_status.ok()) {
          auto location = @1;
          location.mutable_start().IncrementByteOffset(error_offset);
          if (!error_string.empty()) {
            YYERROR_AND_ABORT_AT(location,
                                 absl::StrCat("Syntax error: ", error_string));
          }
          ABSL_DLOG(FATAL) << "ParseStringLiteral did not return an error string";
          YYERROR_AND_ABORT_AT(location,
                               absl::StrCat("Syntax error: ",
                                            parse_status.message()));
        }

        auto* literal = MAKE_NODE(ASTStringLiteralComponent, @1);
        literal->set_string_value(std::move(str));
        // TODO: Migrate to absl::string_view or avoid having to
        // set this at all if the client isn't interested.
        literal->set_image(std::string(input_text));
        $$ = literal;
      }
    ;

// Can be a concatenation of multiple string literals
string_literal:
    string_literal_component[component]
      {
        $$ = MAKE_NODE(ASTStringLiteral, @$, {$component});
        $$->set_string_value($component->string_value());
      }
    | string_literal[list] string_literal_component[component]
      {
        if (@component.start().GetByteOffset() == @list.end().GetByteOffset()) {
          YYERROR_AND_ABORT_AT(@2, "Syntax error: concatenated string literals must be separated by whitespace or comments");
        }

        $$ = WithExtraChildren($list, {$component});
        // TODO: append the value in place, instead of StrCat()
        // then set().
        $$->set_string_value(
              absl::StrCat($$->string_value(), $component->string_value()));
      }
    | string_literal bytes_literal_component[component]
      {
        // Capture this case to provide a better error message
        YYERROR_AND_ABORT_AT(
          @component,
          "Syntax error: string and bytes literals cannot be concatenated.");
      }
    ;

bytes_literal_component:
    BYTES_LITERAL
      {
        const absl::string_view input_text = parser->GetInputText(@1);
        std::string bytes;
        std::string error_string;
        int error_offset;
        const absl::Status parse_status = zetasql::ParseBytesLiteral(
            input_text, &bytes, &error_string, &error_offset);
        if (!parse_status.ok()) {
          auto location = @1;
          location.mutable_start().IncrementByteOffset(error_offset);
          if (!error_string.empty()) {
            YYERROR_AND_ABORT_AT(location,
                                 absl::StrCat("Syntax error: ", error_string));
          }
          ABSL_DLOG(FATAL) << "ParseBytesLiteral did not return an error string";
          YYERROR_AND_ABORT_AT(location,
                               absl::StrCat("Syntax error: ",
                                            parse_status.message()));
        }

        // The identifier is parsed *again* in the resolver. The output of the
        // parser maintains the original image.
        // TODO: Fix this wasted work when the JavaCC parser is gone.
        auto* literal = MAKE_NODE(ASTBytesLiteralComponent, @1);
        literal->set_bytes_value(std::move(bytes));
        // TODO: Migrate to absl::string_view or avoid having to
        // set this at all if the client isn't interested.
        literal->set_image(std::string(input_text));
        $$ = literal;
      }
    ;


// Can be a concatenation of multiple string literals
bytes_literal:
    bytes_literal_component[component]
      {
        $$ = MAKE_NODE(ASTBytesLiteral, @$, {$component});
        $$->set_bytes_value($component->bytes_value());
      }
    | bytes_literal[list] bytes_literal_component[component]
      {
        if (@component.start().GetByteOffset() == @list.end().GetByteOffset()) {
          YYERROR_AND_ABORT_AT(
            @2,
            "Syntax error: concatenated bytes literals must be separated by whitespace or comments");
        }

        $$ = WithExtraChildren($list, {$component});
        // TODO: append the value in place, instead of StrCat()
        // then set().
        $$->set_bytes_value(
              absl::StrCat($$->bytes_value(), $2->bytes_value()));
      }
    | bytes_literal string_literal_component[component]
      {
        // Capture this case to provide a better error message
        YYERROR_AND_ABORT_AT(@component, "Syntax error: string and bytes literals cannot be concatenated.");
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

numeric_literal_prefix:
    "NUMERIC"
    | "DECIMAL"
    ;

numeric_literal:
    numeric_literal_prefix string_literal
      {
        $$ = MAKE_NODE(ASTNumericLiteral, @$, {$2});
      }
    ;

bignumeric_literal_prefix:
    "BIGNUMERIC"
    | "BIGDECIMAL"
    ;

bignumeric_literal:
    bignumeric_literal_prefix string_literal
      {
        $$ = MAKE_NODE(ASTBigNumericLiteral, @$, {$2});
      }
    ;

json_literal:
    "JSON" string_literal
      {
        $$ = MAKE_NODE(ASTJSONLiteral, @$, {$2});
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

token_identifier:
    IDENTIFIER
      {
        const absl::string_view identifier_text($1.str, $1.len);
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
            auto location = @1;
            location.mutable_start().IncrementByteOffset(error_offset);
            if (!error_string.empty()) {
              YYERROR_AND_ABORT_AT(location,
                                   absl::StrCat("Syntax error: ",
                                                error_string));
            }
            ABSL_DLOG(FATAL) << "ParseIdentifier did not return an error string";
            YYERROR_AND_ABORT_AT(location,
                                 absl::StrCat("Syntax error: ",
                                              parse_status.message()));
          }
          $$ = parser->MakeIdentifier(@1, str);
        } else {
          $$ = parser->MakeIdentifier(@1, identifier_text);
        }
      }

identifier:
    token_identifier
    | keyword_as_identifier
      {
        $$ = parser->MakeIdentifier(@1, parser->GetInputText(@1));
      }
    ;

label:
  SCRIPT_LABEL
    {
      const absl::string_view label_text = parser->GetInputText(@1);
      // The tokenizer rule already validates that the identifier is valid and
      // non-empty, except for backquoted identifiers.
      if (label_text[0] == '`') {
        std::string str;
        std::string error_string;
        int error_offset;
        const absl::Status parse_status =
            zetasql::ParseGeneralizedIdentifier(
                label_text, &str, &error_string, &error_offset);
        if (!parse_status.ok()) {
          auto location = @1;
          location.mutable_start().IncrementByteOffset(error_offset);
          if (!error_string.empty()) {
            YYERROR_AND_ABORT_AT(location,
                                 absl::StrCat("Syntax error: ",
                                              error_string));
          }
          ABSL_DLOG(FATAL) << "ParseIdentifier did not return an error string";
          YYERROR_AND_ABORT_AT(location,
                               absl::StrCat("Syntax error: ",
                                            parse_status.message()));
        }
        $$ = parser->MakeIdentifier(@1, str);
      } else {
        $$ = parser->MakeIdentifier(@1, label_text);
      }
    }
;

system_variable_expression:
    "@@" path_expression %prec DOUBLE_AT_PRECEDENCE
    {
      if (parser->HasWhitespace(@1, @2)) {
        // TODO: Add a deprecation warning in this case.
      }
      $$ = MAKE_NODE(ASTSystemVariableExpr, @$, {$2});
    }
    ;

// This includes non-reserved keywords that can also be used as identifiers.
// This production returns nothing -- the enclosing rule uses only the location
// of the keyword to retrieve the token image from the parser input.
common_keyword_as_identifier:
    // WARNING: If you add something here, add it in the non-reserved token list
    // at the top.
    // BEGIN_KEYWORD_AS_IDENTIFIER -- Do not remove this!
    "ABORT"
    | "ACCESS"
    | "ACTION"
    | "AGGREGATE"
    | "ADD"
    | "ALTER"
    | "ALWAYS"
    | "ANALYZE"
    | "APPROX"
    | "ARE"
    | "ASSERT"
    | "BATCH"
    | "BEGIN"
    | "BIGDECIMAL"
    | "BIGNUMERIC"
    | "BREAK"
    | "CALL"
    | "CASCADE"
    | "CHECK"
    | "CLAMPED"
    | "CLONE"
    | "COPY"
    | "CLUSTER"
    | "COLUMN"
    | "COLUMNS"
    | "COMMIT"
    | "CONNECTION"
    | "CONSTANT"
    | "CONSTRAINT"
    | "CONTINUE"
    | "CORRESPONDING"
    | "CYCLE"
    | "DATA"
    | "DATABASE"
    | "DATE"
    | "DATETIME"
    | "DECIMAL"
    | "DECLARE"
    | "DEFINER"
    | "DELETE"
    | "DELETION"
    | "DEPTH"
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
    | "EXTEND"
    | "EXTERNAL"
    | "FILES"
    | "FILTER"
    | "FILL"
    | "FIRST"
    | "FOREIGN"
    | "FORMAT"
    | "FUNCTION"
    | "GENERATED"
    | "GRANT"
    | "GROUP_ROWS"
    | "HIDDEN"
    | "IDENTITY"
    | "IMMEDIATE"
    | "IMMUTABLE"
    | "IMPORT"
    | "INCLUDE"
    | "INCREMENT"
    | "INDEX"
    | "INOUT"
    | "INPUT"
    | "INSERT"
    | "INVOKER"
    | "ISOLATION"
    | "ITERATE"
    | "JSON"
    | "KEY"
    | "LANGUAGE"
    | "LAST"
    | "LEAVE"
    | "LEVEL"
    | "LOAD"
    | "LOOP"
    | "MACRO"
    | "MAP"
    | "MATCH"
    | KW_MATCH_RECOGNIZE_NONRESERVED
    | "MATCHED"
    | "MATERIALIZED"
    | "MAX"
    | "MAXVALUE"
    | "MEASURES"
    | "MESSAGE"
    | "METADATA"
    | "MIN"
    | "MINVALUE"
    | "MODEL"
    | "MODULE"
    | "NUMERIC"
    | "OFFSET"
    | "ONLY"
    | "OPTIONS"
    | "OUT"
    | "OUTPUT"
    | "OVERWRITE"
    | "PARTITIONS"
    | "PATTERN"
    | "PERCENT"
    | "PIVOT"
    | "POLICIES"
    | "POLICY"
    | "PRIMARY"
    | "PRIVATE"
    | "PRIVILEGE"
    | "PRIVILEGES"
    | "PROCEDURE"
    | "PROJECT"
    | "PUBLIC"
    | KW_QUALIFY_NONRESERVED
      {
          // TODO: this warning should point to documentation once
          // we have the engine-specific root URI to use.
          parser->AddWarning(parser->GenerateWarningForFutureKeywordReservation(
                                        zetasql::parser::kQualify,
                                        (@1).start().GetByteOffset()));
      }
    | "RAISE"
    | "READ"
    | "REFERENCES"
    | "REMOTE"
    | "REMOVE"
    | "RENAME"
    | "REPEAT"
    | "REPEATABLE"
    | "REPLACE"
    | "REPLACE_FIELDS"
    | "REPLICA"
    | "REPORT"
    | "RESTRICT"
    | "RESTRICTION"
    | "RETURNS"
    | "RETURN"
    | "REVOKE"
    | "ROLLBACK"
    | "ROW"
    | "RUN"
    | "SAFE_CAST"
    | "SCHEMA"
    | "SEARCH"
    | "SECURITY"
    | "SEQUENCE"
    | "SETS"
    | "SHOW"
    | "SNAPSHOT"
    | "SOURCE"
    | "SQL"
    | "STABLE"
    | "START"
    | "STATIC_DESCRIBE"
    | "STORED"
    | "STORING"
    | "STRICT"
    | "SYSTEM"
    | "SYSTEM_TIME"
    | "TABLE"
    | "TABLES"
    | "TARGET"
    | "TEMP"
    | "TEMPORARY"
    | "TIME"
    | "TIMESTAMP"
    | "TRANSACTION"
    | "TRANSFORM"
    | "TRUNCATE"
    | "TYPE"
    | "UNDROP"
    | "UNIQUE"
    | "UNKNOWN"
    | "UNPIVOT"
    | "UNTIL"
    | "UPDATE"
    | "VALUE"
    | "VALUES"
    | "VECTOR"
    | "VIEW"
    | "VIEWS"
    | "VOLATILE"
    | "WEIGHT"
    | "WHILE"
    | "WRITE"
    | "ZONE"
    | "DESCRIPTOR"

    // Spanner-specific keywords
    | "INTERLEAVE"
    | "NULL_FILTERED"
    | "PARENT"
    ;

keyword_as_identifier:
    common_keyword_as_identifier
    | "SIMPLE"
    // END_KEYWORD_AS_IDENTIFIER -- Do not remove this!
  ;

opt_or_replace: "OR" "REPLACE" { $$ = true; } | %empty { $$ = false; } ;

opt_create_scope:
    "TEMP" { $$ = zetasql::ASTCreateStatement::TEMPORARY; }
    | "TEMPORARY" { $$ = zetasql::ASTCreateStatement::TEMPORARY; }
    | "PUBLIC" { $$ = zetasql::ASTCreateStatement::PUBLIC; }
    | "PRIVATE" { $$ = zetasql::ASTCreateStatement::PRIVATE; }
    | %empty { $$ = zetasql::ASTCreateStatement::DEFAULT_SCOPE; }
    ;

opt_unique: "UNIQUE" { $$ = true; } | %empty { $$ = false; } ;

describe_keyword: "DESCRIBE" | "DESC" ;

opt_hint:
    hint
    | %empty { $$ = nullptr; }
    ;

options_entry:
    identifier_in_hints options_assignment_operator expression_or_proto
      {
        auto* options_entry =
            MAKE_NODE(ASTOptionsEntry, @1, @3, {$1, $3});
        options_entry->set_assignment_op($2);
        $$ = options_entry;
      }
    ;

options_assignment_operator:
    "=" { $$ = zetasql::ASTOptionsEntry::ASSIGN; }
    | "+=" { $$ = zetasql::ASTOptionsEntry::ADD_ASSIGN; }
    | "-=" { $$ = zetasql::ASTOptionsEntry::SUB_ASSIGN; }
    ;

expression_or_proto:
    "PROTO"
      {
        zetasql::ASTIdentifier* proto_identifier =
            parser->MakeIdentifier(@1, "PROTO");
        $$ = MAKE_NODE(ASTPathExpression, @$, {proto_identifier});
      }
    | expression
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | "(" ")"
      {
        $$ = MAKE_NODE(ASTOptionsList, @$);
      }
    ;

options:
    "OPTIONS" options_list { $$ = $2; }
    ;

opt_options_list:
    options { $$ = $1; }
    | %empty { $$ = nullptr; }
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

opt_from_keyword: "FROM" | %empty ;

opt_where_expression:
    "WHERE" expression
      {
        $$ = $2;
      }
    | %empty
      {
        $$ = nullptr;
      }
    ;

opt_assert_rows_modified:
    "ASSERT_ROWS_MODIFIED" possibly_cast_int_literal_or_parameter
      {
        $$ = MAKE_NODE(ASTAssertRowsModified, @$, {$2});
      }
    | %empty
      {
        $$ = nullptr;
      }
    ;

opt_returning_clause:
    "THEN" "RETURN" select_list
      {
        $$ = MAKE_NODE(ASTReturningClause, @$, {$3});
      }
    | "THEN" "RETURN" "WITH" "ACTION" select_list
      {
        zetasql::ASTIdentifier* default_identifier =
          parser->MakeIdentifier(@4, "ACTION");
        auto* action_alias = MAKE_NODE(ASTAlias, @$, {default_identifier});
        $$ = MAKE_NODE(ASTReturningClause, @$, {$5, action_alias});
      }
    | "THEN" "RETURN" "WITH" "ACTION" "AS" identifier select_list
      {
        auto* action_alias = MAKE_NODE(ASTAlias, @$, {$6});
        $$ = MAKE_NODE(ASTReturningClause, @$, {$7, action_alias});
      }
    | %empty { $$ = nullptr; }
    ;

opt_or_ignore_replace_update:
    "OR" "IGNORE" { $$ = zetasql::ASTInsertStatement::IGNORE; }
    | "IGNORE" { $$ = zetasql::ASTInsertStatement::IGNORE; }
    | "OR" "REPLACE" { $$ = zetasql::ASTInsertStatement::REPLACE; }
    | KW_REPLACE_AFTER_INSERT { $$ = zetasql::ASTInsertStatement::REPLACE; }
    | "OR" "UPDATE" { $$ = zetasql::ASTInsertStatement::UPDATE; }
    | KW_UPDATE_AFTER_INSERT { $$ = zetasql::ASTInsertStatement::UPDATE; }
    | %empty { $$ = zetasql::ASTInsertStatement::DEFAULT_MODE; }
    ;

insert_statement_prefix:
    "INSERT" opt_or_ignore_replace_update[insert_mode] opt_into maybe_dashed_generalized_path_expression[insert_target] opt_hint
      {
        $$ = MAKE_NODE(ASTInsertStatement, @$, {$insert_target, $opt_hint});
        $$->set_insert_mode($insert_mode);
      }
    ;

insert_statement:
    insert_statement_prefix column_list insert_values_or_query opt_assert_rows_modified opt_returning_clause
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$2, $3, $4, $5}), @$);
      }
    | insert_statement_prefix insert_values_or_query opt_assert_rows_modified opt_returning_clause
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$2, $3, $4}), @$);
      }
    ;

copy_data_source:
    maybe_dashed_path_expression opt_at_system_time opt_where_clause
      {
        $$ = MAKE_NODE(ASTCopyDataSource, @$, {$1, $2, $3});
      }
    ;

clone_data_source:
    maybe_dashed_path_expression opt_at_system_time opt_where_clause
      {
        $$ = MAKE_NODE(ASTCloneDataSource, @$, {$1, $2, $3});
      }
    ;

clone_data_source_list:
    clone_data_source
      {
        $$ = MAKE_NODE(ASTCloneDataSourceList, @$, {$1});
      }
    | clone_data_source_list "UNION" "ALL" clone_data_source
      {
        $$ = WithExtraChildren($1, {$4});
      }
    ;

clone_data_statement:
    "CLONE" "DATA" "INTO" maybe_dashed_path_expression
    "FROM" clone_data_source_list
      {
        $$ = MAKE_NODE(ASTCloneDataStatement, @$, {$4, $6});
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

insert_values_or_query:
  insert_values_list { $$ = $insert_values_list; }
  | query { $$ = $query; }
  ;

insert_values_list:
    "VALUES" insert_values_row
      {
        $$ = MAKE_NODE(ASTInsertValuesRowList, @$, {$2});
      }
    | insert_values_list "," insert_values_row
      {
        $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
      }
    ;

delete_statement:
    "DELETE" opt_from_keyword maybe_dashed_generalized_path_expression opt_hint
    opt_as_alias opt_with_offset_and_alias opt_where_expression
    opt_assert_rows_modified opt_returning_clause
      {
        $$ = MAKE_NODE(ASTDeleteStatement, @$, {$3, $4, $5, $6, $7, $8, $9});
      }
    ;

opt_with_offset_and_alias:
    "WITH" "OFFSET" opt_as_alias
      {
        $$ = MAKE_NODE(ASTWithOffset, @$, {$3});
      }
    | %empty { $$ = nullptr; }
    ;

update_statement:
   "UPDATE" maybe_dashed_generalized_path_expression opt_hint opt_as_alias
    opt_with_offset_and_alias "SET" update_item_list opt_from_clause
    opt_where_expression opt_assert_rows_modified opt_returning_clause
      {
        $$ = MAKE_NODE(ASTUpdateStatement, @$, {$2, $3, $4, $5, $7, $8, $9, $10, $11});
      }
    ;

truncate_statement:
    "TRUNCATE" "TABLE" maybe_dashed_path_expression opt_where_expression
      {
        $$ = MAKE_NODE(ASTTruncateStatement, @$, {$3, $4});
      }
    ;

nested_dml_statement:
    { OVERRIDE_NEXT_TOKEN_CHAR_LOOKBACK('(', LB_OPEN_NESTED_DML); }
    "("[open] dml_statement ")"
      {
        $$ = $dml_statement;
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
        $$ = MAKE_NODE(ASTDotGeneralizedField, @1, @3, {$1, $3});
      }
    | generalized_path_expression "." identifier
      {
        if ($1->node_kind() == zetasql::AST_PATH_EXPRESSION) {
          $$ = WithExtraChildren(parser->WithEndLocation($1, @3), {$3});
        } else {
          $$ = MAKE_NODE(ASTDotIdentifier, @1, @3, {$1, $3});
        }
      }
    | generalized_path_expression "[" expression "]"
      {
        auto* bracket_loc = parser->MakeLocation(@2);
        $$ = MAKE_NODE(ASTArrayElement, @1, @4, {$1, bracket_loc, $3});
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
      if (parser->language_options().LanguageFeatureEnabled(
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
        $$ = MAKE_NODE(ASTDotGeneralizedField, @1, @5, {$1, $4});
      }
    | generalized_extension_path "." identifier
      {
        $$ = MAKE_NODE(ASTDotIdentifier, @1, @3, {$1, $3});
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
       $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
     }
   ;

opt_into:
    "INTO"
    | %empty
    ;

opt_by_target:
    "BY" "TARGET"
    | %empty
    ;

opt_and_expression:
    "AND" expression
      {
        $$ = $2;
      }
    | %empty
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
      $$ = parser->WithEndLocation(WithExtraChildren($1, {$2}), @$);
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
      parser->WithEndLocation(WithExtraChildren($1, {$2}), @$);
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
        $$ = parser->WithEndLocation($1, @$);
      }
    | "CALL" path_expression "(" ")"
      {
        $$ = MAKE_NODE(ASTCallStatement, @$, {$2});
      }
    ;

opt_function_parameters:
    function_parameters
    | %empty
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
    | %empty
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
    | %empty
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

opt_on_path_expression:
    "ON" path_expression
      {
        $$ = $2;
      }
    | %empty
      {
        $$ = nullptr;
      }
    ;

opt_drop_mode:
    "RESTRICT" { $$ = zetasql::ASTDropStatement::DropMode::RESTRICT; }
    | "CASCADE" { $$ = zetasql::ASTDropStatement::DropMode::CASCADE; }
    | %empty
    { $$ = zetasql::ASTDropStatement::DropMode::DROP_MODE_UNSPECIFIED; }
    ;

drop_statement:
    "DROP" "PRIVILEGE" "RESTRICTION" opt_if_exists
    "ON" privilege_list "ON" identifier path_expression
      {
        auto* node = MAKE_NODE(ASTDropPrivilegeRestrictionStatement, @$,
                               {$6, $8, $9});
        node->set_is_if_exists($4);
        $$ = node;
      }
    | "DROP" "ROW" "ACCESS" "POLICY" opt_if_exists identifier
    on_path_expression
      {
        zetasql::ASTPathExpression* path_expression =
            $7 == nullptr ? nullptr : MAKE_NODE(ASTPathExpression, @6, {$6});
        // This is a DROP ROW ACCESS POLICY statement.
        auto* drop_row_access_policy = MAKE_NODE(
            ASTDropRowAccessPolicyStatement, @$, {path_expression, $7});
        drop_row_access_policy->set_is_if_exists($5);
        $$ = drop_row_access_policy;
      }
    | "DROP" index_type "INDEX" opt_if_exists path_expression
      opt_on_path_expression
      {
        if ($2 == IndexTypeKeywords::kSearch) {
          auto* drop_search_index = MAKE_NODE(
             ASTDropSearchIndexStatement, @$, {$5, $6});
          drop_search_index->set_is_if_exists($4);
          $$ = drop_search_index;
        }
        if ($2 == IndexTypeKeywords::kVector) {
          auto* drop_vector_index = MAKE_NODE(
            ASTDropVectorIndexStatement, @$, {$5, $6});
          drop_vector_index->set_is_if_exists($4);
          $$ = drop_vector_index;
        }
      }
    | "DROP" table_or_table_function opt_if_exists maybe_dashed_path_expression
      opt_function_parameters
      {
        if ($2 == TableOrTableFunctionKeywords::kTableAndFunctionKeywords) {
          // Table functions don't support overloading so this statement doesn't
          // accept any function parameters.
          // (broken link)
          if ($5 != nullptr) {
            YYERROR_AND_ABORT_AT(@5,
                                 "Syntax error: Parameters are not supported "
                                 "for DROP TABLE FUNCTION because table "
                                 "functions don't support "
                                 "overloading");
          }
          auto* drop = MAKE_NODE(ASTDropTableFunctionStatement, @$, {$4});
          drop->set_is_if_exists($3);
          $$ = drop;
        } else {
          // This is a DROP TABLE statement. Table function parameters should
          // not be populated.
          if ($5 != nullptr) {
            YYERROR_AND_ABORT_AT(@5,
                                 "Syntax error: Unexpected \"(\"");
          }
          auto* drop = MAKE_NODE(ASTDropStatement, @$, {$4});
          drop->set_schema_object_kind(zetasql::SchemaObjectKind::kTable);
          drop->set_is_if_exists($3);
          $$ = drop;
        }
      }
    | "DROP" "SNAPSHOT" "TABLE" opt_if_exists maybe_dashed_path_expression
      {
        auto* drop = MAKE_NODE(ASTDropSnapshotTableStatement, @$, {$5});
        drop->set_is_if_exists($4);
        $$ = drop;
      }
    | "DROP" generic_entity_type opt_if_exists path_expression
      {
        auto* drop = MAKE_NODE(ASTDropEntityStatement, @$, {$2, $4});
        drop->set_is_if_exists($3);
        $$ = drop;
      }
    | "DROP" schema_object_kind opt_if_exists path_expression
      opt_function_parameters opt_drop_mode
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
        if ($2 != zetasql::SchemaObjectKind::kSchema) {
          if ($6 != zetasql::ASTDropStatement::DropMode::DROP_MODE_UNSPECIFIED) {
            YYERROR_AND_ABORT_AT(
              @6, absl::StrCat(
              "Syntax error: '",
              zetasql::ASTDropStatement::GetSQLForDropMode($6),
              "' is not supported for DROP ",
              zetasql::SchemaObjectKindToName($2)));
            }
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
            auto* drop = MAKE_NODE(ASTDropStatement, @$, {$4});
            drop->set_schema_object_kind($2);
            drop->set_is_if_exists($3);
            drop->set_drop_mode($6);
            $$ = drop;
          }
        }
      }
    ;

index_type:
    "SEARCH"
      { $$ = IndexTypeKeywords::kSearch; }
    | "VECTOR"
      { $$ = IndexTypeKeywords::kVector; };

opt_index_type:
    index_type
    | %empty { $$ = IndexTypeKeywords::kNone; };

unterminated_non_empty_statement_list:
    unterminated_statement[stmt]
      {
        if ($stmt->Is<zetasql::ASTDefineMacroStatement>()) {
          YYERROR_AND_ABORT_AT(
            @stmt,
            "DEFINE MACRO statements cannot be nested under other statements "
            "or blocks.");
        }
        $$ = MAKE_NODE(ASTStatementList, @$, {$stmt});
      }
    | unterminated_non_empty_statement_list[old_list] ";"
      unterminated_statement[new_stmt]
      {
        if ($new_stmt->Is<zetasql::ASTDefineMacroStatement>()) {
          YYERROR_AND_ABORT_AT(
            @new_stmt,
            "DEFINE MACRO statements cannot be nested under other statements "
            "or blocks.");
        }
        $$ = parser->WithEndLocation(WithExtraChildren($old_list, {$new_stmt}), @$);
      }
    ;

unterminated_non_empty_top_level_statement_list:
    unterminated_statement[stmt]
      {
        $$ = MAKE_NODE(ASTStatementList, @$, {$stmt});
      }
    | unterminated_non_empty_top_level_statement_list[old_list] ";"
      unterminated_statement[new_stmt]
      {
        $$ = parser->WithEndLocation(WithExtraChildren($old_list, {$new_stmt}), @$);
      }
    ;

opt_execute_into_clause:
  "INTO" identifier_list
    {
      $$ = MAKE_NODE(ASTExecuteIntoClause, @$, {$2});
    }
  | %empty
    {
      $$ = nullptr;
    }
  ;

execute_using_argument:
  expression "AS" identifier
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
      $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
    }
  ;

opt_execute_using_clause:
  "USING" execute_using_argument_list
    {
      $$ = $2;
    }
  | %empty
    {
      $$ = nullptr;
    }
  ;

execute_immediate:
  "EXECUTE" "IMMEDIATE" expression opt_execute_into_clause
  opt_execute_using_clause
    {
      $$ = MAKE_NODE(ASTExecuteImmediateStatement, @$, {$3, $4, $5});
    }
  ;

script:
  unterminated_non_empty_top_level_statement_list
  {
    $1->set_variable_declarations_allowed(true);
    $$ = MAKE_NODE(ASTScript, @$, {$1});
  }
  | unterminated_non_empty_top_level_statement_list ";"
  {
    $1->set_variable_declarations_allowed(true);
    $$ = MAKE_NODE(ASTScript, @$, {parser->WithEndLocation($1, @$)});
  }
  | %empty
    {
      // Resolve to an empty script.
      zetasql::ASTStatementList* empty_stmt_list = MAKE_NODE(
          ASTStatementList, @$, {});
      $$ = MAKE_NODE(ASTScript, @$, {empty_stmt_list});
    }
  ;

statement_list:
  unterminated_non_empty_statement_list ";"
    {
      $$ = parser->WithEndLocation($1, @$);
    }
  | %empty
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
    | %empty
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
    $$ = parser->WithEndLocation(WithExtraChildren(
        $1, {parser->WithEndLocation(elseif_clause, @$)}), @$);
  };

opt_elseif_clauses:
  elseif_clauses
    {
      $$ = $1;
    }
  | %empty
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
        $$ = parser->WithEndLocation($1, @$);
      }
    ;

when_then_clauses:
    "WHEN" expression "THEN" statement_list
    {
      zetasql::ASTWhenThenClause* when_then_clause = MAKE_NODE(
          ASTWhenThenClause, @$, {$2, $4});
      $$ = MAKE_NODE(ASTWhenThenClauseList, @$, {when_then_clause});
    }
    | when_then_clauses "WHEN" expression "THEN" statement_list
    {
      zetasql::ASTWhenThenClause* when_then_clause = MAKE_NODE(
          ASTWhenThenClause, @2, {$3, $5});
      $$ = parser->WithEndLocation(WithExtraChildren(
          $1, {parser->WithEndLocation(when_then_clause, @$)}), @$);
    };

opt_expression:
    expression
    {
      $$ = $1;
    }
    | %empty
    {
      $$ = nullptr;
    }
    ;

case_statement:
    "CASE" opt_expression when_then_clauses opt_else "END" "CASE"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
                zetasql::FEATURE_V_1_3_CASE_STMT)) {
          YYERROR_AND_ABORT_AT(@1, "Statement CASE...WHEN is not supported");
        }
        $$ = MAKE_NODE(ASTCaseStatement, @$, {$2, $3, $4});
      }
    ;

begin_end_block:
    "BEGIN" statement_list[stmts] opt_exception_handler[handlers] "END"
    {
      $stmts->set_variable_declarations_allowed(true);
      $$ = MAKE_NODE(ASTBeginEndBlock, @$, {$stmts, $handlers});
    }
    ;

opt_exception_handler:
    "EXCEPTION" "WHEN" "ERROR" "THEN" statement_list {
      zetasql::ASTExceptionHandler* handler = MAKE_NODE(
          ASTExceptionHandler, @2, {$5});
      $$ = MAKE_NODE(ASTExceptionHandlerList, @1, {handler});
    }
    | %empty
    {
      $$ = nullptr;
    }
    ;

opt_default_expression:
    "DEFAULT" expression
    {
      $$ = $2;
    }
    | %empty
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
      $$ = parser->WithEndLocation(WithExtraChildren($1, {$3}), @$);
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
    { OVERRIDE_NEXT_TOKEN_LOOKBACK(KW_LOOP, LB_OPEN_STATEMENT_BLOCK); }
    "LOOP"[loop] statement_list "END" "LOOP"
    {
      $$ = MAKE_NODE(ASTWhileStatement, @$, {$statement_list});
    }
    ;

while_statement:
    "WHILE" expression { OVERRIDE_NEXT_TOKEN_LOOKBACK(KW_DO, LB_OPEN_STATEMENT_BLOCK); } "DO" statement_list "END" "WHILE"
    {
      $$ = MAKE_NODE(ASTWhileStatement, @$, {$expression, $statement_list});
    }
    ;

until_clause:
    "UNTIL" expression
    {
      $$ = MAKE_NODE(ASTUntilClause, @$, {$2});
    }
    ;

repeat_statement:
    { OVERRIDE_NEXT_TOKEN_LOOKBACK(KW_REPEAT, LB_OPEN_STATEMENT_BLOCK); }
    "REPEAT"[repeat] statement_list until_clause "END" "REPEAT"
    {
     if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_3_REPEAT)) {
        YYERROR_AND_ABORT_AT(@repeat, "REPEAT is not supported");
      }
      $$ = MAKE_NODE(ASTRepeatStatement, @$, {$statement_list, $until_clause});
    }
    ;

for_in_statement:
    "FOR" identifier "IN" parenthesized_query[query]
    { OVERRIDE_NEXT_TOKEN_LOOKBACK(KW_DO, LB_OPEN_STATEMENT_BLOCK); } "DO" statement_list "END" "FOR"
    {
     if (!parser->language_options().LanguageFeatureEnabled(
              zetasql::FEATURE_V_1_3_FOR_IN)) {
        YYERROR_AND_ABORT_AT(@1, "FOR...IN is not supported");
      }
      $$ = MAKE_NODE(ASTForInStatement, @$,
        {$identifier, $query, $statement_list});
    }
    ;

break_statement:
    "BREAK" opt_identifier
    {
      CHECK_LABEL_SUPPORT($2, @2);
      zetasql::ASTBreakStatement* stmt;
      if ($2 == nullptr) {
        stmt = MAKE_NODE(ASTBreakStatement, @$, {});
      } else {
        auto label = MAKE_NODE(ASTLabel, @2, {$2});
        stmt = MAKE_NODE(ASTBreakStatement, @$, {label});
      }
      stmt->set_keyword(zetasql::ASTBreakContinueStatement::BREAK);
      $$ = stmt;
    }
    | "LEAVE" opt_identifier
    {
      CHECK_LABEL_SUPPORT($2, @2);
      zetasql::ASTBreakStatement* stmt;
      if ($2 == nullptr) {
        stmt = MAKE_NODE(ASTBreakStatement, @$, {});
      } else {
        auto label = MAKE_NODE(ASTLabel, @2, {$2});
        stmt = MAKE_NODE(ASTBreakStatement, @$, {label});
      }
      stmt->set_keyword(zetasql::ASTBreakContinueStatement::LEAVE);
      $$ = stmt;
    }
    ;

continue_statement:
    "CONTINUE" opt_identifier
    {
      CHECK_LABEL_SUPPORT($2, @2);
      zetasql::ASTContinueStatement* stmt;
      if ($2 == nullptr) {
        stmt = MAKE_NODE(ASTContinueStatement, @$, {});
      } else {
        auto label = MAKE_NODE(ASTLabel, @2, {$2});
        stmt = MAKE_NODE(ASTContinueStatement, @$, {label});
      }
      stmt->set_keyword(zetasql::ASTBreakContinueStatement::CONTINUE);
      $$ = stmt;
    }
    | "ITERATE" opt_identifier
    {
      CHECK_LABEL_SUPPORT($2, @2);
      zetasql::ASTContinueStatement* stmt;
      if ($2 == nullptr) {
        stmt = MAKE_NODE(ASTContinueStatement, @$, {});
      } else {
        auto label = MAKE_NODE(ASTLabel, @2, {$2});
        stmt = MAKE_NODE(ASTContinueStatement, @$, {label});
      }
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
    next_statement_kind_without_hint[kind]
      {
        *ast_node_result = nullptr;
        // The parser will complain about the remainder of the input if we let
        // the tokenizer continue to produce tokens, because we don't have any
        // grammar for the rest of the input.
        SetForceTerminate(tokenizer, /*end_byte_offset=*/nullptr);
        $$ = $kind;
      }
    | hint { OVERRIDE_CURRENT_TOKEN_LOOKBACK(@hint, LB_END_OF_STATEMENT_LEVEL_HINT); }
      next_statement_kind_without_hint[kind]
      {
        if ($kind == zetasql::ASTDefineMacroStatement::kConcreteNodeKind) {
          YYERROR_AND_ABORT_AT(
            @hint, "Hints are not allowed on DEFINE MACRO statements.");
        }
        *ast_node_result = $hint;
        // The parser will complain about the remainder of the input if we let
        // the tokenizer continue to produce tokens, because we don't have any
        // grammar for the rest of the input.
        SetForceTerminate(tokenizer, /*end_byte_offset=*/nullptr);
        $$ = $kind;
      }
    ;

next_statement_kind_parenthesized_select:
    "(" next_statement_kind_parenthesized_select { $$ = $2; }
    | "SELECT" { $$ = zetasql::ASTQueryStatement::kConcreteNodeKind; }
    | "WITH" { $$ = zetasql::ASTQueryStatement::kConcreteNodeKind; }
    // FROM is always treated as indicating the statement is a query, even
    // if the syntax is not enabled.  This is okay because a statement
    // starting with FROM couldn't be anything else, and it'll be reasonable
    // to give errors a statement starting with FROM being an invalid query.
    | "FROM" { $$ = zetasql::ASTQueryStatement::kConcreteNodeKind; }
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
    | %empty
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
    | KW_DEFINE_FOR_MACROS "MACRO"
      { $$ = zetasql::ASTDefineMacroStatement::kConcreteNodeKind; }
    | "EXECUTE" "IMMEDIATE"
      { $$ = zetasql::ASTExecuteImmediateStatement::kConcreteNodeKind; }
    | "EXPORT" "DATA"
      { $$ = zetasql::ASTExportDataStatement::kConcreteNodeKind; }
    | "EXPORT" "MODEL"
      { $$ = zetasql::ASTExportModelStatement::kConcreteNodeKind; }
    | "EXPORT" table_or_table_function "METADATA"
      { $$ = zetasql::ASTExportMetadataStatement::kConcreteNodeKind; }
    | "INSERT" { $$ = zetasql::ASTInsertStatement::kConcreteNodeKind; }
    | "UPDATE" { $$ = zetasql::ASTUpdateStatement::kConcreteNodeKind; }
    | "DELETE" { $$ = zetasql::ASTDeleteStatement::kConcreteNodeKind; }
    | "MERGE" { $$ = zetasql::ASTMergeStatement::kConcreteNodeKind; }
    | "CLONE" "DATA"
      { $$ = zetasql::ASTCloneDataStatement::kConcreteNodeKind; }
    | "LOAD" "DATA"
      { $$ = zetasql::ASTAuxLoadDataStatement::kConcreteNodeKind; }
    | describe_keyword
      { $$ = zetasql::ASTDescribeStatement::kConcreteNodeKind; }
    | "SHOW" { $$ = zetasql::ASTShowStatement::kConcreteNodeKind; }
    | "DROP" "PRIVILEGE"
      {
        $$ = zetasql::ASTDropPrivilegeRestrictionStatement::kConcreteNodeKind;
      }
    | "DROP" "ALL" "ROW" opt_access "POLICIES"
      {
        $$ = zetasql::ASTDropAllRowAccessPoliciesStatement::kConcreteNodeKind;
      }
    | "DROP" "ROW" "ACCESS" "POLICY"
      { $$ = zetasql::ASTDropRowAccessPolicyStatement::kConcreteNodeKind; }
    | "DROP" "SEARCH" "INDEX"
      { $$ = zetasql::ASTDropSearchIndexStatement::kConcreteNodeKind; }
    | "DROP" "VECTOR" "INDEX"
      { $$ = zetasql::ASTDropVectorIndexStatement::kConcreteNodeKind; }
    | "DROP" table_or_table_function
      {
        if ($2 == TableOrTableFunctionKeywords::kTableAndFunctionKeywords) {
          $$ = zetasql::ASTDropTableFunctionStatement::kConcreteNodeKind;
        } else {
          $$ = zetasql::ASTDropStatement::kConcreteNodeKind;
        }
      }
    | "DROP" "SNAPSHOT" "TABLE"
      { $$ = zetasql::ASTDropSnapshotTableStatement::kConcreteNodeKind; }
    | "DROP" generic_entity_type
      { $$ = zetasql::ASTDropEntityStatement::kConcreteNodeKind; }
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
    | "GRAPH" { $$ = zetasql::ASTQueryStatement::kConcreteNodeKind; }
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
    | "ALTER" "APPROX" "VIEW"
      { $$ = zetasql::ASTAlterApproxViewStatement::kConcreteNodeKind; }
    | "ALTER" "CONNECTION"
      { $$ = zetasql::ASTAlterConnectionStatement::kConcreteNodeKind; }
    | "ALTER" "DATABASE"
      { $$ = zetasql::ASTAlterDatabaseStatement::kConcreteNodeKind; }
    | "ALTER" "SCHEMA"
      { $$ = zetasql::ASTAlterSchemaStatement::kConcreteNodeKind; }
    | "ALTER" "EXTERNAL" "SCHEMA"
      { $$ = zetasql::ASTAlterExternalSchemaStatement::kConcreteNodeKind; }
    | "ALTER" "TABLE"
      { $$ = zetasql::ASTAlterTableStatement::kConcreteNodeKind; }
    | "ALTER" "PRIVILEGE"
      {
        $$ = zetasql::ASTAlterPrivilegeRestrictionStatement::kConcreteNodeKind;
      }
    | "ALTER" "ROW"
      { $$ = zetasql::ASTAlterRowAccessPolicyStatement::kConcreteNodeKind; }
    | "ALTER" "ALL" "ROW" "ACCESS" "POLICIES"
      { $$ =
          zetasql::ASTAlterAllRowAccessPoliciesStatement::kConcreteNodeKind; }
    | "ALTER" "VIEW"
      { $$ = zetasql::ASTAlterViewStatement::kConcreteNodeKind; }
    | "ALTER" "MATERIALIZED" "VIEW"
      { $$ = zetasql::ASTAlterMaterializedViewStatement::kConcreteNodeKind; }
    | "ALTER" generic_entity_type
      { $$ = zetasql::ASTAlterEntityStatement::kConcreteNodeKind; }
    | "ALTER" "MODEL"
      { $$ = zetasql::ASTAlterModelStatement::kConcreteNodeKind; }
    | "CREATE" "DATABASE"
      { $$ = zetasql::ASTCreateDatabaseStatement::kConcreteNodeKind; }
    | "CREATE" next_statement_kind_create_modifiers "CONNECTION"
      { $$ = zetasql::ASTCreateConnectionStatement::kConcreteNodeKind; }
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
    | "CREATE" opt_or_replace opt_unique opt_spanner_null_filtered opt_index_type
      "INDEX"
      { $$ = zetasql::ASTCreateIndexStatement::kConcreteNodeKind; }
    | "CREATE" opt_or_replace "SCHEMA"
      { $$ = zetasql::ASTCreateSchemaStatement::kConcreteNodeKind; }
    | "CREATE" opt_or_replace generic_entity_type
      { $$ = zetasql::ASTCreateEntityStatement::kConcreteNodeKind; }
    | "CREATE" next_statement_kind_create_modifiers
      next_statement_kind_table opt_if_not_exists
      maybe_dashed_path_expression opt_table_element_list
      opt_like_path_expression opt_clone_table opt_copy_table
      opt_default_collate_clause
      opt_partition_by_clause_no_hint
      opt_cluster_by_clause_no_hint opt_with_connection_clause opt_options_list
      next_statement_kind_create_table_opt_as_or_semicolon
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
    | "CREATE" next_statement_kind_create_modifiers "EXTERNAL" "TABLE"
      {
        $$ = zetasql::ASTCreateExternalTableStatement::kConcreteNodeKind;
      }
    | "CREATE" next_statement_kind_create_modifiers "EXTERNAL" "SCHEMA"
      {
        $$ = zetasql::ASTCreateExternalSchemaStatement::kConcreteNodeKind;
      }
    | "CREATE" opt_or_replace "PRIVILEGE"
      {
        $$ = zetasql::ASTCreatePrivilegeRestrictionStatement::kConcreteNodeKind;
      }
    | "CREATE" opt_or_replace "ROW" opt_access "POLICY"
      { $$ = zetasql::ASTCreateRowAccessPolicyStatement::kConcreteNodeKind; }
    | "CREATE" next_statement_kind_create_modifiers opt_recursive "VIEW"
      {
        $$ = zetasql::ASTCreateViewStatement::kConcreteNodeKind;
      }
    | "CREATE" opt_or_replace "APPROX" opt_recursive "VIEW"
      { $$ = zetasql::ASTCreateApproxViewStatement::kConcreteNodeKind; }
    | "CREATE" opt_or_replace "MATERIALIZED" opt_recursive "VIEW"
      { $$ = zetasql::ASTCreateMaterializedViewStatement::kConcreteNodeKind; }
    | "CREATE" opt_or_replace "SNAPSHOT" "SCHEMA"
      { $$ = zetasql::ASTCreateSnapshotStatement::kConcreteNodeKind; }
    | "CREATE" opt_or_replace "SNAPSHOT" "TABLE"
      { $$ = zetasql::ASTCreateSnapshotTableStatement::kConcreteNodeKind; }
    | "CALL"
      { $$ = zetasql::ASTCallStatement::kConcreteNodeKind; }
    | "RETURN"
      { $$ = zetasql::ASTReturnStatement::kConcreteNodeKind; }
    | "IMPORT"
      { $$ = zetasql::ASTImportStatement::kConcreteNodeKind; }
    | "MODULE"
      { $$ = zetasql::ASTModuleStatement::kConcreteNodeKind; }
    | "ANALYZE"
      { $$ = zetasql::ASTAnalyzeStatement::kConcreteNodeKind; }
    | "ASSERT"
      { $$ = zetasql::ASTAssertStatement::kConcreteNodeKind; }
    | "TRUNCATE"
      { $$ = zetasql::ASTTruncateStatement::kConcreteNodeKind; }
    | "IF"
      { $$ = zetasql::ASTIfStatement::kConcreteNodeKind; }
    | "WHILE"
      { $$ = zetasql::ASTWhileStatement::kConcreteNodeKind; }
    | "LOOP"
      { $$ = zetasql::ASTWhileStatement::kConcreteNodeKind; }
    | "DECLARE"
      { $$ = zetasql::ASTVariableDeclaration::kConcreteNodeKind; }
    | "BREAK"
      { $$ = zetasql::ASTBreakStatement::kConcreteNodeKind; }
    | "LEAVE"
      { $$ = zetasql::ASTBreakStatement::kConcreteNodeKind; }
    | "CONTINUE"
      { $$ = zetasql::ASTContinueStatement::kConcreteNodeKind; }
    | "ITERATE"
      { $$ = zetasql::ASTContinueStatement::kConcreteNodeKind; }
    | "RAISE"
      { $$ = zetasql::ASTRaiseStatement::kConcreteNodeKind; }
    | "FOR"
      { $$ = zetasql::ASTForInStatement::kConcreteNodeKind; }
    | "REPEAT"
      { $$ = zetasql::ASTRepeatStatement::kConcreteNodeKind; }
    | label ":" "BEGIN"
      { $$ = zetasql::ASTBeginStatement::kConcreteNodeKind; }
    | label ":" "LOOP"
      { $$ = zetasql::ASTWhileStatement::kConcreteNodeKind; }
    | label ":" "WHILE"
      { $$ = zetasql::ASTWhileStatement::kConcreteNodeKind; }
    | label ":" "FOR"
      { $$ = zetasql::ASTForInStatement::kConcreteNodeKind; }
    | label ":" "REPEAT"
      { $$ = zetasql::ASTRepeatStatement::kConcreteNodeKind; }
    | "UNDROP" schema_object_kind
      { $$ = zetasql::ASTUndropStatement::kConcreteNodeKind; }
    ;

// Spanner-specific non-terminal definitions
spanner_primary_key:
    "PRIMARY" "KEY" primary_key_element_list
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
              YYERROR_AND_ABORT_AT(@1, "PRIMARY KEY must be defined in the "
                "table element list as column attribute or constraint.");
        }
        $$ = MAKE_NODE(ASTPrimaryKey, @$, {$3});
      }
    ;

spanner_index_interleave_clause:
    "," "INTERLEAVE" "IN" maybe_dashed_path_expression
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
            YYERROR_AND_ABORT_AT(@1, "Syntax error: Expected end of input but "
              "got \",\"");
        }
        auto* clause = MAKE_NODE(ASTSpannerInterleaveClause, @$, {$4});
        clause->set_type(zetasql::ASTSpannerInterleaveClause::IN);
        $$ = clause;
      }

opt_spanner_interleave_in_parent_clause:
    "," "INTERLEAVE" "IN" "PARENT" maybe_dashed_path_expression
    opt_foreign_key_on_delete
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
            YYERROR_AND_ABORT_AT(@1, "Syntax error: Expected end of input but "
              "got \",\"");
        }

        auto* clause = MAKE_NODE(ASTSpannerInterleaveClause, @$, {$5});
        clause->set_action($6);
        clause->set_type(zetasql::ASTSpannerInterleaveClause::IN_PARENT);
        $$ = clause;
      }
    | %empty { $$ = nullptr; }
    ;

opt_spanner_table_options:
    spanner_primary_key opt_spanner_interleave_in_parent_clause
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
            YYERROR_AND_ABORT_AT(@1, "PRIMARY KEY must be defined in the "
                "table element list as column attribute or constraint.");
        }

        $$ = MAKE_NODE(ASTSpannerTableOptions, @$, {$1, $2});
      }
    | %empty { $$ = nullptr; }
    ;

opt_spanner_null_filtered:
    "NULL_FILTERED"
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
            YYERROR_AND_ABORT_AT(
              @1, "null_filtered is not a supported object type");
        }
        $$ = true;
      }
    | %empty { $$ = false; }
    ;

// Feature-checking in this rule would make parser reduce and error out
// too early, so we rely on the check in spanner_alter_column_action.
spanner_generated_or_default:
    "AS" "(" expression ")" "STORED"
      {
        auto* node = MAKE_NODE(ASTGeneratedColumnInfo, @$, {$3});
        node->set_stored_mode(zetasql::ASTGeneratedColumnInfo::STORED);
        $$ = node;
      }
    | default_column_info
    ;

opt_spanner_generated_or_default:
    spanner_generated_or_default
    | %empty { $$ = nullptr; }
    ;

opt_spanner_not_null_attribute:
    not_null_column_attribute
      {
        // Feature-checking here would make parser reduce and error out
        // too early, so we rely on the check in spanner_alter_column_action.
        $$ = MAKE_NODE(ASTColumnAttributeList, @$, {$1});
      }
    | %empty { $$ = nullptr; }
    ;

spanner_alter_column_action:
    "ALTER" "COLUMN" opt_if_exists identifier column_schema_inner
    opt_spanner_not_null_attribute opt_spanner_generated_or_default
    opt_options_list
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
            YYERROR_AND_ABORT_AT(@column_schema_inner,
              "Expected keyword DROP or keyword SET but got identifier");
        }
        if ($opt_if_exists) {
          YYERROR_AND_ABORT_AT(@opt_if_exists,
            "Syntax error: IF EXISTS is not supported");
        }
        auto* schema = parser->WithEndLocation(
            WithExtraChildren($column_schema_inner, {
              $opt_spanner_generated_or_default,
              $opt_spanner_not_null_attribute,
              $opt_options_list
            }), @$);
        auto* column = MAKE_NODE(ASTColumnDefinition, @$,
          {$identifier, schema});
        $$ = MAKE_NODE(ASTSpannerAlterColumnAction, @$,
          {parser->WithStartLocation(column, @identifier)});
      }
    ;

spanner_set_on_delete_action:
    "SET" "ON" "DELETE" foreign_key_action
      {
        if (!parser->language_options().LanguageFeatureEnabled(
          zetasql::FEATURE_SPANNER_LEGACY_DDL)) {
            YYERROR_AND_ABORT_AT(@2, "Syntax error: Unexpected keyword ON");
        }
        auto* node = MAKE_NODE(ASTSpannerSetOnDeleteAction, @$, {});
        node->set_action($foreign_key_action);
        $$ = node;
      }

%%

void zetasql_bison_parser::BisonParserImpl::error(
    const zetasql::ParseLocationRange& loc,
    const std::string& msg) {
  *error_message = msg;
  *error_location = loc.start();
}
