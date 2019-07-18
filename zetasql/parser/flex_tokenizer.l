/*
 * Copyright 2019 ZetaSQL Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/* Tokenize the SQL input stream into a series of tokens to feed into the
   parser.
*/

/* Flex will generate ZetaSqlFlexTokenizer::GetNextTokenFlexImpl(). The
   remainder of the class definition is controlled by us, in
   flex_tokenizer.h. */
%option prefix="ZetaSql"
%option yyclass="FlexTokenizer"

/* After "." we allow more things, including all keywords and all
   integers, to be returned as identifiers. This state is initiated when we
   recognize an identifier followed by a ".". It is also initiated after a
   closing parenthesis, square bracket, or "?" (positional parameter) followed
   by a ".", to handle cases like foo[3].array. See the "." rule and the
   <DOT_IDENTIFIER>{generalized_identifier}.

   The %x means that in this state, the only rules that are active are the ones
   that are explicitly marked with <DOT_IDENTIFIER> or <*>. It is essentially a
   small separate tokenizer. The tokenizer only has rules to recognize
   {generalized_identifier}, and a catchall rule that falls back to the regular
   tokenizer state. This fallback ensures that things like ".(" are parsed using
   the normal rules. */
%x DOT_IDENTIFIER

/* This inclusive state is for in ARRAY<...> and STRUCT<...>. It turns off the
   parsing of <<, >>, and <>, but leaves everything else the same. Doing this in
   the tokenizer avoids complicated rules and duplication at the parser level.

   This state is marked %s, which means that all rules are active unless they
   provide an explicit list of states that do not include
   IN_ARRAY_OR_STRUCT_TYPE.
*/
%s IN_ARRAY_OR_STRUCT_TYPE

/* This inclusive state is for BETWEEN...AND. In this state, everything works
   as normal, but the "AND" keyword is returned as KW_AND_FOR_BETWEEN instead of
   KW_AND. This resolves what would otherwise be ambiguous in the bison grammar.

   For comparison, in F1 this was also a problem, and it was resolved by
   duplicating the entire expression hierarchy exclusively for one of the
   arguments of BETWEEN, so that the hierarchy would never include AND unless it
   was parenthesized. This is very involved and error prone, and a tokenizer
   state is a much cleaner solution.

   Note that this state is automatically turned off within parentheses and
   square brackets, using yy_push_state() and yy_pop_state(). That ensures that
   something like BETWEEN (a AND b) AND c still parses.

   This state is marked %s, which means that all rules are active unless they
   provide an explicit list of states that do not include IN_BETWEEN. No rules
   are actually disabled or added for IN_BETWEEN -- we only check the state at
   the top of the stack in the "and" keyword rule, but that doesn't require a
   separate rule.
*/
%s IN_BETWEEN

/* This state is pushed onto the bottom of the stack at initialization time.
   This ensures that the yy_pop_state() called in the rules for ), ] and } can
   recognize when they have reached the bottom of the stack and when they should
   not pop another state. The state is marked as %x, which means that only rules
   marked <*> or <STACK_BOTTOM> will be active in this mode. In practice this
   means that only the whitespace and catchall error rules will be active.
*/
%x STACK_BOTTOM

%{
#include <algorithm>
#include <string>
#include <string.h>

#include "zetasql/parser/bison_parser.bison.h"
#include "zetasql/parser/flex_tokenizer.h"
#include "zetasql/parser/keywords.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/strings.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"

using zetasql_bison_parser::BisonParserImpl;

#undef YY_DECL
#define YY_DECL \
    int zetasql::parser::ZetaSqlFlexTokenizer::GetNextTokenFlexImpl( \
        zetasql_bison_parser::location* yylloc)

// This action is executed for every token that is matched, before the defined
// actions are executed. We use this to:
// - Keep location up to date. We abuse the column value and treat it as a byte
//   offset.
// - Support forcible termination. This is used by the parser to terminate
//   the parsing process when it has read enough of a prefix, e.g. for
//   multi-statement parsing or for determining the next statement kind.
#define YY_USER_ACTION \
  if (force_terminate_) yyterminate(); \
  /* Note that we store byte offsets in the 'column' field. */ \
  yylloc->begin.column = yylloc->end.column; \
  yylloc->end.column += yyleng;

// Call this in an action to return only a prefix of the match of
// 'prefix_length' bytes.
#define SET_RETURN_PREFIX_LENGTH(prefix_length) \
  do { \
    const int prefix_length_result = (prefix_length); \
    yyless(prefix_length_result); \
    yylloc->end.column = yylloc->begin.column + prefix_length_result; \
  } while (0)

constexpr char ::zetasql::parser::ZetaSqlFlexTokenizer::kEofSentinelInput[];

%}

%option c++ 8bit noyywrap nounput case-insensitive never-interactive
%option nodefault warn stack ecs

/* These are some basic regex definitions that are used in the lexer rules
   below.
*/

bit_shift_op                <<|>>
comparison_op               =|<=|>=|<|>|<>|!=
decimal_digit               [0-9]
decimal_digits              {decimal_digit}+
opt_decimal_digits          {decimal_digit}*
hex_digit                   [0-9a-f]
hex_integer                 (0x{hex_digit}+)

dot                         "."
exp_nosign                  e{decimal_digits}
exp_sign                    e[+-]{decimal_digits}
exp                         ({exp_nosign}|{exp_sign})
opt_exp                     {exp}?

/* Floating point formats are identified by the presence of a dot and/or an
   exponent. If there's a dot, there has to be at least one digit either before
   or after the dot. This is covered by the first two regexes. The third regex
   covers digits with an exponent but without a dot. */
decimal_dot                 {decimal_digits}{dot}{opt_decimal_digits}{opt_exp}
dot_decimal                 {dot}{decimal_digits}{opt_exp}
decimal_exp                 {decimal_digits}{exp}
floating_point_literal      {decimal_dot}|{dot_decimal}|{decimal_exp}

/* Whitespace, including Unicode whitespace characters encoded as UTF-8, as well
   as all comments.
   https://www.cs.tut.fi/~jkorpela/chars/spaces.html

   OGHAM SPACE MARK (U+1680) is omitted because it looks like "-".
   MONGOLIAN VOWEL SEPARATOR (U+180E) is omitted because it has no width.
   ZERO WIDTH SPACE (U+200B) is omitted because it has no width.
   ZERO WIDTH NO-BREAK SPACE (U+FEFF) is omitted because it has no width.

   The whitespace rule has a "*" so that we match all consecutive whitespace
   without running YY_USER_ACTION.
*/
utf8_no_break_space            "\xC2\xA0"
utf8_en_quad                   "\xE2\x80\x80"
utf8_em_quad                   "\xE2\x80\x81"
utf8_en_space                  "\xE2\x80\x82"
utf8_em_space                  "\xE2\x80\x83"
utf8_three_per_em_space        "\xE2\x80\x84"
utf8_four_per_em_space         "\xE2\x80\x85"
utf8_six_per_em_space          "\xE2\x80\x86"
utf8_figure_space              "\xE2\x80\x87"
utf8_punctuation_space         "\xE2\x80\x88"
utf8_thin_space                "\xE2\x80\x89"
utf8_hair_space                "\xE2\x80\x8A"
utf8_narrow_no_break_space     "\xE2\x80\xAF"
utf8_medium_mathematical_space "\xE2\x81\x9F"
utf8_ideographic_space         "\xE3\x80\x80"
whitespace_character           ([ \n\r\t\b\f\v]|{utf8_no_break_space}|{utf8_en_quad}|{utf8_em_quad}|{utf8_en_space}|{utf8_em_space}|{utf8_three_per_em_space}|{utf8_four_per_em_space}|{utf8_six_per_em_space}|{utf8_figure_space}|{utf8_punctuation_space}|{utf8_thin_space}|{utf8_hair_space}|{utf8_narrow_no_break_space}|{utf8_medium_mathematical_space}|{utf8_ideographic_space})
opt_whitespace                 ({whitespace_character}|{comment})*
whitespace                     ({whitespace_character}|{comment})+
opt_whitespace_no_comments     {whitespace_character}*
whitespace_no_comments         {whitespace_character}+

/* String/bytes literals and identifiers.

   The abbreviations here:
     sq = single quote(d)
     dq = double quote(d)
     bq = back quote(d)
     3 = triple quoted
     r = raw
     _0 = unterminated versions. They are used to return better error
          messages for unterminated strings.

   For instance, rsq3 means "raw triple single-quoted", or r'''...'''.

   The regexes accept arbitrary escapes instead of trying to narrow it down to
   just the valid set. This is safe because in valid strings the character after
   the escape is *always* eaten, even in raw strings. The actual validation of
   the escapes, and of things like UTF-8 structure, is done in the parser.
   This also allows us to use the same regex for raw strings that we use for any
   other std::string. Raw strings interpret the escapes differently (they allow all
   escapes and pass them through verbatim), but the termination condition is
   the same: escaped quotes don't count.

   In single quoted strings/bytes we don't accept \n so that a single-line
   unterminated std::string literal is recognized as an unterminated std::string literal
   at that point, instead of being bogusly matched up with another quote on a
   subsequent line. However, we do accept escaped newlines. These get a separate
   and nicer error message pointing directly at the escaped newline.
*/
any_escape                (\\(.|\n|\r|\r\n))
sq                        \'
sq3                       {sq}{sq}{sq}
dq                        \"
dq3                       {dq}{dq}{dq}
bq                        \`
backslash                 \\
no_backslash_sq_newline   [^\'\\\n\r]
no_backslash_dq_newline   [^\"\\\n\r]
no_backslash_sq           [^\'\\]
no_backslash_dq           [^\"\\]

/* Strings and bytes: */
sqtext_0           {sq}({no_backslash_sq_newline}|{any_escape})*
sqtext             {sqtext_0}{sq}
dqtext_0           {dq}({no_backslash_dq_newline}|{any_escape})*
dqtext             {dqtext_0}{dq}
sq3text_0          {sq3}(({sq}|{sq}{sq})?({no_backslash_sq}|{any_escape}))*
sq3text            {sq3text_0}{sq3}
dq3text_0          {dq3}(({dq}|{dq}{dq})?({no_backslash_dq}|{any_escape}))*
dq3text            {dq3text_0}{dq3}
string_literal                  r?({sqtext}|{dqtext}|{sq3text}|{dq3text})
bytes_literal                   (b|rb|br)({sqtext}|{dqtext}|{sq3text}|{dq3text})
unterminated_string_literal     ({sqtext_0}|{dqtext_0})
unterminated_triple_quoted_string_literal ({sq3text_0}|{dq3text_0})
unterminated_raw_string_literal r({sqtext_0}|{dqtext_0})
unterminated_triple_quoted_raw_string_literal r({sq3text_0}|{dq3text_0})
unterminated_bytes_literal      b({sqtext_0}|{dqtext_0})
unterminated_triple_quoted_bytes_literal b({sq3text_0}|{dq3text_0})
unterminated_raw_bytes_literal  (rb|br)({sqtext_0}|{dqtext_0})
unterminated_triple_quoted_raw_bytes_literal  (rb|br)({sq3text_0}|{dq3text_0})

/* Identifiers: */
unquoted_identifier             [A-Z_][A-Z_0-9]*
unquoted_generalized_identifier [A-Z_0-9]+
bqtext_0                        {bq}([^\\\`\r\n]|({any_escape}))*
bqtext                          {bqtext_0}{bq}
identifier                      {unquoted_identifier}|{bqtext}
generalized_identifier          {unquoted_generalized_identifier}|{bqtext}
unterminated_escaped_identifier {bqtext_0}

/* C-style comments using slash+star.
   cs_ prefix is for "c-style comment", shortened to avoid long lines.
   For more information about how this works, see
   "Using one, even more complicated, pattern" from
   http://www.cs.man.ac.uk/~pjj/cs212/ex2_str_comm.html
*/
cs_start              "/*"
cs_not_star           [^*]
cs_star               "*"
cs_not_star_or_slash  [^/*]
cs_slash              "/"
/* Contents of a C-style comment that may embed a * (or a sequence of stars)
   followed by not-a-slash. */
cs_embed_star         ({cs_not_star}*({cs_star}+{cs_not_star_or_slash})*)*
/* Matches the beginning of a comment, to detect unterminated comments. */
cs_comment_begin      {cs_start}{cs_embed_star}{cs_star}*
cs_comment            {cs_start}{cs_embed_star}{cs_star}+{cs_slash}

/* Dash comments using -- */
dash_comment          \-\-[^\r\n]*(\r|\n|\r\n)?

/* # comment ignores anything from # to the end of the line. */
pound_comment         #[^\r\n]*(\r|\n|\r\n)?

comment               ({cs_comment}|{dash_comment}|{pound_comment})

%%
 /* RULES SECTION

    This is a list of lexer rules, where the left side is the token and the
    right side is a return that yields the enum for the token.
    bison_parser.y refers to the token enums.
 */

%{
    // Has to be first in 'rules' section, do not move it.
    // We use this code to support multiple start-symbols in bison.
    // For more information see section 11.5 of bison manual.
    // gnu.org/software/bison/manual/html_node/Multiple-start_002dsymbols.html
    if (is_first_token_) {
      // TODO: See if we can do this with YY_USER_INIT instead.
      is_first_token_ = false;
      // Because (), [] and {} push INITIAL, a rogue ), ] or } could pop beyond
      // the bottom of the stack, which would normally crash. Instead, put an
      // extra mode at the bottom of the stack that catches this case and
      // that generates an error instead. (In practice the parser will reject
      // the unbalanced ) or ] so this should never happen unless there is a
      // bug in the grammar.)
      yy_push_state(STACK_BOTTOM);
      yy_push_state(INITIAL);
      // Note that we store byte offsets in the 'column' field.
      yylloc->begin.column = yylloc->end.column = start_offset_;
      switch (mode_) {
        case BisonParserMode::kStatement:
          return BisonParserImpl::token::MODE_STATEMENT;
        case BisonParserMode::kScript:
          return BisonParserImpl::token::MODE_SCRIPT;
        case BisonParserMode::kNextStatement:
          return BisonParserImpl::token::MODE_NEXT_STATEMENT;
        case BisonParserMode::kNextScriptStatement:
          return BisonParserImpl::token::MODE_NEXT_SCRIPT_STATEMENT;
        case BisonParserMode::kNextStatementKind:
          return BisonParserImpl::token::MODE_NEXT_STATEMENT_KIND;
        case BisonParserMode::kExpression:
          return BisonParserImpl::token::MODE_EXPRESSION;
        case BisonParserMode::kType:
          return BisonParserImpl::token::MODE_TYPE;
        case BisonParserMode::kTokenizer:
        case BisonParserMode::kTokenizerPreserveComments:
          // Don't generate a mode token when we are doing raw tokenization.
          // With or without comments.
          break;
      }
    }
    yylloc->begin = yylloc->end;
%}

 /* All keywords (used, reserved, and usable-as-identifiers). These are
    ambiguous with the identifier rule, but they win because they are
    specified before the identifier rule.

    IMPORTANT:
    All these keywords MUST be listed in GetAllKeywords() in keywords.cc as
    well. We have tried removing the keyword rules and instead using the
    keyword functions in the {identifier} production, but that was significantly
    slower.
 */
 /* BEGIN_KEYWORDS -- Do not remove! */
abort { return BisonParserImpl::token::KW_ABORT; }
access { return BisonParserImpl::token::KW_ACCESS; }
action { return BisonParserImpl::token::KW_ACTION; }
add { return BisonParserImpl::token::KW_ADD; }
aggregate { return BisonParserImpl::token::KW_AGGREGATE; }
all { return BisonParserImpl::token::KW_ALL; }
alter { return BisonParserImpl::token::KW_ALTER; }
and {
  if (YY_START == IN_BETWEEN) {
    // See IN_BETWEEN tokenizer mode description.
    yy_pop_state();
    if (mode_ == BisonParserMode::kTokenizer) {
      return BisonParserImpl::token::KW_AND;
    }
    return BisonParserImpl::token::KW_AND_FOR_BETWEEN;
  }
  return BisonParserImpl::token::KW_AND;
}
any { return BisonParserImpl::token::KW_ANY; }
array { return BisonParserImpl::token::KW_ARRAY; }
as { return BisonParserImpl::token::KW_AS; }
asc { return BisonParserImpl::token::KW_ASC; }
assert { return BisonParserImpl::token::KW_ASSERT; }
assert_rows_modified {
  return BisonParserImpl::token::KW_ASSERT_ROWS_MODIFIED;
}
at { return BisonParserImpl::token::KW_AT; }
batch { return BisonParserImpl::token::KW_BATCH; }
begin { return BisonParserImpl::token::KW_BEGIN; }
between {
  // See IN_BETWEEN tokenizer mode description.
  yy_push_state(IN_BETWEEN);
  return BisonParserImpl::token::KW_BETWEEN;
}
break { return BisonParserImpl::token::KW_BREAK; }
by { return BisonParserImpl::token::KW_BY; }
call { return BisonParserImpl::token::KW_CALL; }
cascade { return BisonParserImpl::token::KW_CASCADE; }
case { return BisonParserImpl::token::KW_CASE; }
cast { return BisonParserImpl::token::KW_CAST; }
check { return BisonParserImpl::token::KW_CHECK; }
cluster { return BisonParserImpl::token::KW_CLUSTER; }
collate { return BisonParserImpl::token::KW_COLLATE; }
column { return BisonParserImpl::token::KW_COLUMN; }
commit { return BisonParserImpl::token::KW_COMMIT; }
constant { return BisonParserImpl::token::KW_CONSTANT; }
constraint { return BisonParserImpl::token::KW_CONSTRAINT; }
continue { return BisonParserImpl::token::KW_CONTINUE; }
contains { return BisonParserImpl::token::KW_CONTAINS; }
create { return BisonParserImpl::token::KW_CREATE; }
cross { return BisonParserImpl::token::KW_CROSS; }
cube { return BisonParserImpl::token::KW_CUBE; }
current { return BisonParserImpl::token::KW_CURRENT; }
data { return BisonParserImpl::token::KW_DATA; }
database { return BisonParserImpl::token::KW_DATABASE; }
date { return BisonParserImpl::token::KW_DATE; }
datetime { return BisonParserImpl::token::KW_DATETIME; }
declare { return BisonParserImpl::token::KW_DECLARE; }
default { return BisonParserImpl::token::KW_DEFAULT; }
define { return BisonParserImpl::token::KW_DEFINE; }
definer { return BisonParserImpl::token::KW_DEFINER; }
delete { return BisonParserImpl::token::KW_DELETE; }
desc { return BisonParserImpl::token::KW_DESC; }
describe { return BisonParserImpl::token::KW_DESCRIBE; }
distinct { return BisonParserImpl::token::KW_DISTINCT; }
do { return BisonParserImpl::token::KW_DO; }
drop { return BisonParserImpl::token::KW_DROP; }
else { return BisonParserImpl::token::KW_ELSE; }
end { return BisonParserImpl::token::KW_END; }
enforced { return BisonParserImpl::token::KW_ENFORCED; }
enum { return BisonParserImpl::token::KW_ENUM; }
escape { return BisonParserImpl::token::KW_ESCAPE; }
except/{opt_whitespace}"(" { return BisonParserImpl::token::KW_EXCEPT; }
except/{whitespace}(all|distinct|"@") {
  if (mode_ == BisonParserMode::kTokenizer) {
    return BisonParserImpl::token::KW_EXCEPT;
  }
  return BisonParserImpl::token::KW_EXCEPT_IN_SET_OP;
}
except {
  if (mode_ == BisonParserMode::kTokenizer ||
      mode_ == BisonParserMode::kTokenizerPreserveComments) {
    // In raw tokenization mode, this may be legal.
    return BisonParserImpl::token::KW_EXCEPT;
  }
  SetOverrideError(*yylloc,
                   "EXCEPT must be followed by ALL, DISTINCT, or \"(\"");
  yyterminate();
}
exclude { return BisonParserImpl::token::KW_EXCLUDE; }
exists { return BisonParserImpl::token::KW_EXISTS; }
explain { return BisonParserImpl::token::KW_EXPLAIN; }
export { return BisonParserImpl::token::KW_EXPORT; }
external { return BisonParserImpl::token::KW_EXTERNAL; }
extract { return BisonParserImpl::token::KW_EXTRACT; }
false { return BisonParserImpl::token::KW_FALSE; }
fetch { return BisonParserImpl::token::KW_FETCH; }
fill { return BisonParserImpl::token::KW_FILL; }
filter { return BisonParserImpl::token::KW_FILTER; }
following { return BisonParserImpl::token::KW_FOLLOWING; }
for { return BisonParserImpl::token::KW_FOR; }
foreign { return BisonParserImpl::token::KW_FOREIGN; }
from { return BisonParserImpl::token::KW_FROM; }
full { return BisonParserImpl::token::KW_FULL; }
function { return BisonParserImpl::token::KW_FUNCTION; }
grant { return BisonParserImpl::token::KW_GRANT; }
group { return BisonParserImpl::token::KW_GROUP; }
grouping { return BisonParserImpl::token::KW_GROUPING; }
groups { return BisonParserImpl::token::KW_GROUPS; }
hash { return BisonParserImpl::token::KW_HASH; }
having { return BisonParserImpl::token::KW_HAVING; }
hidden { return BisonParserImpl::token::KW_HIDDEN; }
if { return BisonParserImpl::token::KW_IF; }
ignore { return BisonParserImpl::token::KW_IGNORE; }
import { return BisonParserImpl::token::KW_IMPORT; }
in { return BisonParserImpl::token::KW_IN; }
inout { return BisonParserImpl::token::KW_INOUT; }
index { return BisonParserImpl::token::KW_INDEX; }
inner { return BisonParserImpl::token::KW_INNER; }
insert { return BisonParserImpl::token::KW_INSERT; }
intersect { return BisonParserImpl::token::KW_INTERSECT; }
interval { return BisonParserImpl::token::KW_INTERVAL; }
iterate { return BisonParserImpl::token::KW_ITERATE; }
into { return BisonParserImpl::token::KW_INTO; }
invoker { return BisonParserImpl::token::KW_INVOKER; }
is { return BisonParserImpl::token::KW_IS; }
isolation { return BisonParserImpl::token::KW_ISOLATION; }
join { return BisonParserImpl::token::KW_JOIN; }
key { return BisonParserImpl::token::KW_KEY; }
language { return BisonParserImpl::token::KW_LANGUAGE; }
lateral { return BisonParserImpl::token::KW_LATERAL; }
leave { return BisonParserImpl::token::KW_LEAVE; }
left { return BisonParserImpl::token::KW_LEFT; }
level { return BisonParserImpl::token::KW_LEVEL; }
like { return BisonParserImpl::token::KW_LIKE; }
limit { return BisonParserImpl::token::KW_LIMIT; }
lookup { return BisonParserImpl::token::KW_LOOKUP; }
loop { return BisonParserImpl::token::KW_LOOP; }
match { return BisonParserImpl::token::KW_MATCH; }
matched { return BisonParserImpl::token::KW_MATCHED; }
materialized  { return BisonParserImpl::token::KW_MATERIALIZED; }
max { return BisonParserImpl::token::KW_MAX; }
merge { return BisonParserImpl::token::KW_MERGE; }
min { return BisonParserImpl::token::KW_MIN; }
model { return BisonParserImpl::token::KW_MODEL; }
module { return BisonParserImpl::token::KW_MODULE; }
natural { return BisonParserImpl::token::KW_NATURAL; }
new { return BisonParserImpl::token::KW_NEW; }
no { return BisonParserImpl::token::KW_NO; }
not { return BisonParserImpl::token::KW_NOT; }
 /* This returns a different token because returning KW_NOT would confuse the
    operator precedence parsing. Boolean NOT has a different precedence than
    NOT BETWEEN/IN/LIKE. The final character at the end is intended to avoid
    cases like "NOT Info.foo" being interpreted as having a NOT for IN. This
    unfortunately doesn't match at EOF, so "NOT IN" at the very end of the file
    will cause bad error messages. There is no situation where that is valid
    syntax, so there will never be any rejections as a result.
 */
not{whitespace}(between|in|like)[^A-Z_0-9] {
  SET_RETURN_PREFIX_LENGTH(3);
  if (mode_ == BisonParserMode::kTokenizer) {
    return BisonParserImpl::token::KW_NOT;
  }
  return BisonParserImpl::token::KW_NOT_FOR_BETWEEN_IN_LIKE;
}
null { return BisonParserImpl::token::KW_NULL; }
nulls { return BisonParserImpl::token::KW_NULLS; }
numeric { return BisonParserImpl::token::KW_NUMERIC; }
of { return BisonParserImpl::token::KW_OF; }
offset { return BisonParserImpl::token::KW_OFFSET; }
on { return BisonParserImpl::token::KW_ON; }
only { return BisonParserImpl::token::KW_ONLY; }
options { return BisonParserImpl::token::KW_OPTIONS; }
or { return BisonParserImpl::token::KW_OR; }
order { return BisonParserImpl::token::KW_ORDER; }
out { return BisonParserImpl::token::KW_OUT; }
outer { return BisonParserImpl::token::KW_OUTER; }
over { return BisonParserImpl::token::KW_OVER; }
partition { return BisonParserImpl::token::KW_PARTITION; }
percent { return BisonParserImpl::token::KW_PERCENT; }
policies { return BisonParserImpl::token::KW_POLICIES; }
policy { return BisonParserImpl::token::KW_POLICY; }
preceding { return BisonParserImpl::token::KW_PRECEDING; }
procedure { return BisonParserImpl::token::KW_PROCEDURE; }
primary { return BisonParserImpl::token::KW_PRIMARY; }
private { return BisonParserImpl::token::KW_PRIVATE; }
privileges { return BisonParserImpl::token::KW_PRIVILEGES; }
proto { return BisonParserImpl::token::KW_PROTO; }
public { return BisonParserImpl::token::KW_PUBLIC; }
range { return BisonParserImpl::token::KW_RANGE; }
read { return BisonParserImpl::token::KW_READ; }
recursive { return BisonParserImpl::token::KW_RECURSIVE; }
references { return BisonParserImpl::token::KW_REFERENCES; }
rename { return BisonParserImpl::token::KW_RENAME; }
repeatable { return BisonParserImpl::token::KW_REPEATABLE; }
replace { return BisonParserImpl::token::KW_REPLACE; }
replace_fields { return BisonParserImpl::token::KW_REPLACE_FIELDS; }
respect { return BisonParserImpl::token::KW_RESPECT; }
restrict { return BisonParserImpl::token::KW_RESTRICT; }
return { return BisonParserImpl::token::KW_RETURN; }
returns { return BisonParserImpl::token::KW_RETURNS; }
revoke { return BisonParserImpl::token::KW_REVOKE; }
right { return BisonParserImpl::token::KW_RIGHT; }
rollback { return BisonParserImpl::token::KW_ROLLBACK; }
rollup { return BisonParserImpl::token::KW_ROLLUP; }
row { return BisonParserImpl::token::KW_ROW; }
rows { return BisonParserImpl::token::KW_ROWS; }
run { return BisonParserImpl::token::KW_RUN; }
safe_cast { return BisonParserImpl::token::KW_SAFE_CAST; }
security { return BisonParserImpl::token::KW_SECURITY; }
select { return BisonParserImpl::token::KW_SELECT; }
set { return BisonParserImpl::token::KW_SET; }
show { return BisonParserImpl::token::KW_SHOW; }
simple { return BisonParserImpl::token::KW_SIMPLE; }
some { return BisonParserImpl::token::KW_SOME; }
source { return BisonParserImpl::token::KW_SOURCE; }
sql { return BisonParserImpl::token::KW_SQL; }
start { return BisonParserImpl::token::KW_START; }
stored { return BisonParserImpl::token::KW_STORED; }
storing { return BisonParserImpl::token::KW_STORING; }
struct { return BisonParserImpl::token::KW_STRUCT; }
system { return BisonParserImpl::token::KW_SYSTEM; }
system_time { return BisonParserImpl::token::KW_SYSTEM_TIME; }
table { return BisonParserImpl::token::KW_TABLE; }
tablesample { return BisonParserImpl::token::KW_TABLESAMPLE; }
target { return BisonParserImpl::token::KW_TARGET; }
temp { return BisonParserImpl::token::KW_TEMP; }
temporary { return BisonParserImpl::token::KW_TEMPORARY; }
then { return BisonParserImpl::token::KW_THEN; }
time { return BisonParserImpl::token::KW_TIME; }
timestamp { return BisonParserImpl::token::KW_TIMESTAMP; }
to { return BisonParserImpl::token::KW_TO; }
transaction { return BisonParserImpl::token::KW_TRANSACTION; }
transform { return BisonParserImpl::token::KW_TRANSFORM; }
treat { return BisonParserImpl::token::KW_TREAT; }
true { return BisonParserImpl::token::KW_TRUE; }
truncate { return BisonParserImpl::token::KW_TRUNCATE; }
type { return BisonParserImpl::token::KW_TYPE; }
unbounded { return BisonParserImpl::token::KW_UNBOUNDED; }
union { return BisonParserImpl::token::KW_UNION; }
unnest { return BisonParserImpl::token::KW_UNNEST; }
unique { return BisonParserImpl::token::KW_UNIQUE; }
update { return BisonParserImpl::token::KW_UPDATE; }
using { return BisonParserImpl::token::KW_USING; }
value { return BisonParserImpl::token::KW_VALUE; }
values { return BisonParserImpl::token::KW_VALUES; }
view { return BisonParserImpl::token::KW_VIEW; }
weight { return BisonParserImpl::token::KW_WEIGHT; }
when { return BisonParserImpl::token::KW_WHEN; }
where { return BisonParserImpl::token::KW_WHERE; }
while { return BisonParserImpl::token::KW_WHILE; }
window { return BisonParserImpl::token::KW_WINDOW; }
with { return BisonParserImpl::token::KW_WITH; }
within { return BisonParserImpl::token::KW_WITHIN; }
write { return BisonParserImpl::token::KW_WRITE; }
zone { return BisonParserImpl::token::KW_ZONE; }
 /* END_KEYWORDS -- Do not remove! */

 /* All unescaping and error checking is done in the parser. This allows us */
 /* to give better error messages. */
{string_literal} {
  return BisonParserImpl::token::STRING_LITERAL;
}
{unterminated_string_literal} {
  SetOverrideError(*yylloc, "Syntax error: Unclosed std::string literal");
  yyterminate();
}
{unterminated_triple_quoted_string_literal} {
  SetOverrideError(*yylloc,
                   "Syntax error: Unclosed triple-quoted std::string literal");
  yyterminate();
}
{unterminated_raw_string_literal} {
  SetOverrideError(*yylloc, "Syntax error: Unclosed raw std::string literal");
  yyterminate();
}
{unterminated_triple_quoted_raw_string_literal} {
  SetOverrideError(*yylloc,
                   "Syntax error: Unclosed triple-quoted raw std::string literal");
  yyterminate();
}

{bytes_literal} { return BisonParserImpl::token::BYTES_LITERAL; }
{unterminated_bytes_literal} {
  SetOverrideError(*yylloc, "Syntax error: Unclosed bytes literal");
  yyterminate();
}
{unterminated_triple_quoted_bytes_literal} {
  SetOverrideError(*yylloc,
                   "Syntax error: Unclosed triple-quoted bytes literal");
  yyterminate();
}
{unterminated_raw_bytes_literal} {
  SetOverrideError(*yylloc, "Syntax error: Unclosed raw bytes literal");
  yyterminate();
}
{unterminated_triple_quoted_raw_bytes_literal} {
  SetOverrideError(*yylloc,
                   "Syntax error: Unclosed triple-quoted raw bytes literal");
  yyterminate();
}

{identifier} { return BisonParserImpl::token::IDENTIFIER; }

{unterminated_escaped_identifier} {
  SetOverrideError(*yylloc, "Syntax error: Unclosed identifier literal");
  yyterminate();
}

 /* Error rules for a number followed by an identifier without white space in
    between. We don't want to parse the identifier as accidental alias. For
    instance, 123abc should be error, and we don't want it to be parsed as
    123 [AS] abc. */
{decimal_digits}[A-Z_] {
  yylloc->begin.column += YYLeng() - 1;
  SetOverrideError(
      *yylloc, "Syntax error: Missing whitespace between literal and alias");
  yyterminate();
}
{hex_integer}[G-Z_] {
  yylloc->begin.column += YYLeng() - 1;
  SetOverrideError(
      *yylloc, "Syntax error: Missing whitespace between literal and alias");
  yyterminate();
}
{floating_point_literal}/[A-Z_] {
  // If the floating point literal starts with a ".", and the preceding token
  // is an identifier or unreserved keyword, then we should tokenize this
  // as "identifier.".
  if (YYText()[0] == '.' &&
      IsDotGeneralizedIdentifierPrefixToken(prev_token_)) {
    yy_push_state(DOT_IDENTIFIER);
    SET_RETURN_PREFIX_LENGTH(1);
    return '.';
  }
  // Trigger the missing-whitespace error, but only if the floating point
  // literal ends in a digit, e.g. "123.456abc". (Note that this rule only
  // matches the floating point literal itself, so the last character in
  // YYText() is the last character in {floating_point_literal}. We don't
  // trigger the missing whitespace error for cases that don't end in a digit,
  // e.g. "123.abc". It's a case that is less likely to be an error, and the
  // JavaCC parser doesn't trigger the missing whitespace warning in this case
  // either.
  // TODO: Consider making this an error too. It's not that likely to
  // be correct either.
  if (isdigit(YYText()[YYLeng() - 1])) {
    yylloc->begin.column += YYLeng();
    SetOverrideError(
        *yylloc, "Syntax error: Missing whitespace between literal and alias");
    yyterminate();
  }
  return BisonParserImpl::token::FLOATING_POINT_LITERAL;
}

{decimal_digits} { return BisonParserImpl::token::INTEGER_LITERAL; }
{hex_integer} { return BisonParserImpl::token::INTEGER_LITERAL; }

{floating_point_literal} {
  // If the floating point literal starts with a ".", and the preceding token
  // is an identifier or unreserved keyword, then we should tokenize this
  // as "identifier.".
  if (YYText()[0] == '.' &&
      IsDotGeneralizedIdentifierPrefixToken(prev_token_)) {
    yy_push_state(DOT_IDENTIFIER);
    SET_RETURN_PREFIX_LENGTH(1);
    return '.';
  }
  return BisonParserImpl::token::FLOATING_POINT_LITERAL;
}

{cs_comment_begin}        {
  // Unfortunately this doesn't catch all cases. We also accept whitespace
  // inside some tokens, and this rule only catches whitespace between
  // tokens. The whitespace-within-token rules are generally cases where nobody
  // would ever actually write comments, so this is not a big loss.
  SetOverrideError(*yylloc, "Syntax error: Unclosed comment");
  yyterminate();
}

 /* Open parentheses/brackets. The { rule is for parity with the JavaCC
    tokenizer and for use by clients that use the ZetaSQL tokenizer. We don't
    use it in the grammar. */
"("|"["|"{"              {
   // These need to suspend special modes such as IN_BETWEEN. This is popped
   // again in the close rule below.
   yy_push_state(INITIAL);
   return YYText()[0];
}
 /* TODO: If a ) or a ] is followed by a dot, switch to
    DOT_IDENTIFIER mode. Do this after the JavaCC parser is retired. */
")"|"]"|"}"                       {
  // The corresponding open rule always pushes INITIAL. Even if we entered extra
  // modes in the mean time (such as IN_BETWEEN), we should leave those modes
  // now and pop the INITIAL mode that was pushed by the opening rule. While
  // doing this, we make sure to never pop STACK_BOTTOM.
  while (YY_START != INITIAL && YY_START != STACK_BOTTOM) {
    yy_pop_state();
  }
  // If this triggers, then we pushed a non-INITIAL state on top of
  // STACK_BOTTOM. That should be impossible, because the only rules that are
  // active in state STACK_BOTTOM are the catch-all error rules.
  DCHECK(YY_START != STACK_BOTTOM);
  if (YY_START != STACK_BOTTOM) {
    yy_pop_state();
  }
  if (YY_START == STACK_BOTTOM) {
    // Unbalanced parentheses/brackets. The grammar will catch this. We allow
    // the tokenizer to continue by pushing INITIAL back onto the stack.
    yy_push_state(INITIAL);
  }
  return YYText()[0];
}

"."{opt_whitespace}"*"    {
  if (mode_ == BisonParserMode::kTokenizerPreserveComments) {
    SET_RETURN_PREFIX_LENGTH(1);
    return '.';
  }
  return BisonParserImpl::token::KW_DOT_STAR;
}
"*"                       { return '*'; }
","                       { return ','; }

"="                       { return '='; }
"!="                      {
  return BisonParserImpl::token::KW_NOT_EQUALS_C_STYLE;
}

"<="                      { return BisonParserImpl::token::KW_LESS_EQUALS; }
 /* Don't recognize these in ARRAY<> or STRUCT<> context. */
"<>"                      {
  if (prev_token_ == BisonParserImpl::token::KW_ARRAY ||
      prev_token_ == BisonParserImpl::token::KW_STRUCT) {
    // Match only the '<', and move to the same state that that production would
    // have moved to.
    yy_push_state(IN_ARRAY_OR_STRUCT_TYPE);
    SET_RETURN_PREFIX_LENGTH(1);
    return '<';
  }
  return BisonParserImpl::token::KW_NOT_EQUALS_SQL_STYLE;
}
<INITIAL,IN_BETWEEN>">>"  { return BisonParserImpl::token::KW_SHIFT_RIGHT; }
"<<"                      { return BisonParserImpl::token::KW_SHIFT_LEFT; }
"=>"                      {
  return BisonParserImpl::token::KW_NAMED_ARGUMENT_ASSIGNMENT;
}
"<"                       {
  if (prev_token_ == BisonParserImpl::token::KW_ARRAY ||
      prev_token_ == BisonParserImpl::token::KW_STRUCT) {
    // Switch to a mode that does not recognize >>. This only works as long as
    // there are no legal "independent" < and > inside array or struct types
    // (i.e., without ARRAY or STRUCT preceding) in the grammar. If there are,
    // then the state pushes and pops would become unbalanced, because ">" pops
    // this state.
    yy_push_state(IN_ARRAY_OR_STRUCT_TYPE);
  }
  return '<';
}
">"                       {
  if (YY_START == IN_ARRAY_OR_STRUCT_TYPE) yy_pop_state();
  return '>';
}
">="                      { return BisonParserImpl::token::KW_GREATER_EQUALS; }
"|"                       { return '|'; }
"^"                       { return '^'; }
"&"                       { return '&'; }
"+"                       { return '+'; }
"-"                       { return '-'; }
"/"                       { return '/'; }
"~"                       { return '~'; }
"?"                       { return '?'; }
"@"{opt_whitespace}"{"    {
  // "{" needs to suspend special modes such as IN_BETWEEN. This is popped
  // again in the "}" rule.
  if (mode_ == BisonParserMode::kTokenizerPreserveComments) {
    SET_RETURN_PREFIX_LENGTH(1);
    return '@';
  }
  yy_push_state(INITIAL);
  return BisonParserImpl::token::KW_OPEN_HINT;
}
"@"/{opt_whitespace}({decimal_digits}|{hex_integer}) {
  return BisonParserImpl::token::KW_OPEN_INTEGER_HINT;
}
"@"                       { return '@'; }
"." {
  if (IsDotGeneralizedIdentifierPrefixToken(prev_token_) ) {
    // When an identifier or unreserved keyword is followed by a dot, always
    // move to DOT_IDENTIFIER mode. This can recognize keywords as an
    // identifier.
    yy_push_state(DOT_IDENTIFIER);
  }
  return '.';
}

 /* The JavaCC Tokenizer recognized this as a token, even though the language
    does not use it anywhere. This is here to maintain compatibility. */
":"                       { return ':'; }

";"{opt_whitespace}       {
  if (mode_ == BisonParserMode::kTokenizerPreserveComments) {
    SET_RETURN_PREFIX_LENGTH(1);
    return ';';
  } else if (yylloc->end.column == input_size_ + 1) {
    // Don't return the final \n. It is handled by the whitespace rule and will
    // trigger EOF.
    SET_RETURN_PREFIX_LENGTH(YYLeng() - 1);
  } else if (mode_ == BisonParserMode::kNextStatement ||
             mode_ == BisonParserMode::kNextStatementKind ||
             mode_ == BisonParserMode::kNextScriptStatement) {
    // Don't return anything more if we're just looking at a single statement.
    // Only return the semicolon, not the whitespace.
    SET_RETURN_PREFIX_LENGTH(1);
  }
  return ';';
}

 /* Whitespace and EOF rule.

    We append a \n to every input. This ensures that we will encounter a
    match for the whitespace rule at every EOF, or for a rule that eats trailing
    whitespace!

    This rule eats leading whitespace but not comments. This makes the EOF
    location reported to the parser skip the trailing whitespace, which results
    in better errors for unexpected end of input. But it doesn't skip trailing
    comments.

    See also the comment for kEofSentinelInput in flex_tokenizer.h.
 */
<*>{whitespace_no_comments}   {
  if (yylloc->end.column == input_size_ + 1) {
    // The whitespace is adjacent to the end of the input, and includes the
    // \n that we add to the end of the input. Return EOF at the start of the
    // whitespace, with zero length. This produces better errors, because the
    // "unexpected EOF" errors will be adjacent to the last token.
    yylloc->end.column = yylloc->begin.column;
    yyterminate();
  }
  // The whitespace is not at the end of input. Just skip it.
}

<*>{comment}              {
  if (mode_ == BisonParserMode::kTokenizerPreserveComments) {
    if (yylloc->end.column == input_size_ + 1) {
      // Don't return the final \n. It is handled by the whitespace rule and
      // will trigger EOF. If we didn't do this, the <<EOF>> rule would trigger
      // instead and return an error.
      SET_RETURN_PREFIX_LENGTH(YYLeng() - 1);
    }
    return BisonParserImpl::token::COMMENT;
  }
  if (yylloc->end.column == input_size_ + 1) {
    // The comment is adjacent to the end of the input, and includes the
    // \n that we add to the end of the input. Return EOF at the end of the
    // comment, excluding the extra \n, with zero length. This puts the
    // "unexpected EOF" errors at the line after end-of-line comments.
    yylloc->begin.column = yylloc->end.column = yylloc->end.column - 1;
    yyterminate();
  }
  // The comment is not at the end of input and we are not preserving comments.
  // Just skip it.
}

<<EOF>>                   {
  // This shouldn't happen. Instead, the {whitespace} condition should trigger
  // at the end of the input, because we always ensure that the query ends in
  // \n.
  SetOverrideError(*yylloc, "Internal error: Encountered real EOF");
  yyterminate();
}

 /* Rules for the DOT_IDENTIFIER mini-tokenizer. This tokenizer state is
    triggered when we see "identifier.". It enables a generalized_identifier
    rule that allows things like "42" and arbitrary keywords to be treated like
    identifiers. Note that the rules marked with <*> are also active! Those
    rules take care of eating whitespace and comments, and handling EOF. */
<DOT_IDENTIFIER>{generalized_identifier} {
  yy_pop_state();
  return BisonParserImpl::token::IDENTIFIER;
}
 /* Catchall rule for the DOT_IDENTIFIER mini-tokenizer. This catches anything
    that isn't matched above, and instead of consuming it, switches back to the
    regular tokenizer mode and tries again.
    TODO: Add a tokenizer test for this catchall rule. */
<DOT_IDENTIFIER>.         {
  yy_pop_state();
  SET_RETURN_PREFIX_LENGTH(0);
}

 /* Catchall rule. This rule should also work in exclusive tokenizer states such
    as STACK_BOTTOM. */
<*>.                      {
  SetOverrideError(
      *yylloc,
      absl::StrCat("Syntax error: Illegal input character \"",
                   absl::CEscape(absl::string_view(YYText(), 1)), "\""));
  yyterminate();
}
