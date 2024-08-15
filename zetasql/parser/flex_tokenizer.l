/*
 * Copyright 2019 Google LLC
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

/* Tokenize the SQL input stream into a series of tokens. */

/* Flex will generate ZetaSqlFlexTokenizer::GetNextTokenFlexImpl(). The
   remainder of the class definition is controlled by us, in
   flex_tokenizer.h. */
%option prefix="ZetaSql"
%option yyclass="FlexTokenizer"

%{
#include "zetasql/parser/flex_tokenizer.cc.inc"
%}

%option c++ 8bit noyywrap nounput case-insensitive never-interactive
%option nodefault warn stack full

/* These are some basic regex definitions that are used in the lexer rules
   below.
*/

decimal_digit               [0-9]
decimal_digits              {decimal_digit}+
hex_digit                   [0-9a-f]
hex_integer                 (0x{hex_digit}+)


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
   other string. Raw strings interpret the escapes differently (they allow all
   escapes and pass them through verbatim), but the termination condition is
   the same: escaped quotes don't count.

   In single quoted strings/bytes we don't accept \n so that a single-line
   unterminated string literal is recognized as an unterminated string literal
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
unclosed_string_literal     ({sqtext_0}|{dqtext_0})
unclosed_triple_quoted_string_literal ({sq3text_0}|{dq3text_0})
unclosed_raw_string_literal r({sqtext_0}|{dqtext_0})
unclosed_triple_quoted_raw_string_literal r({sq3text_0}|{dq3text_0})
unclosed_bytes_literal      b({sqtext_0}|{dqtext_0})
unclosed_triple_quoted_bytes_literal b({sq3text_0}|{dq3text_0})
unclosed_raw_bytes_literal  (rb|br)({sqtext_0}|{dqtext_0})
unclosed_triple_quoted_raw_bytes_literal  (rb|br)({sq3text_0}|{dq3text_0})

/* Identifiers: */
exponent_without_sign           E[0-9]+
unquoted_identifier             [A-Z_][A-Z_0-9]*
bqtext_0                        {bq}([^\\\`\r\n]|({any_escape}))*
bqtext                          {bqtext_0}{bq}
identifier                      {unquoted_identifier}|{bqtext}
unclosed_escaped_identifier     {bqtext_0}

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

/* Requiring a newline at the end of dash_coment and pound_comment does not
   cause an error even if the comment comes in the last line of a query,
   thanks to the newline sentinel input (See:
   https://github.com/google/zetasql/blob/master/zetasql/parser/flex_tokenizer.h?l=128).
*/
/* Dash comments using -- */
dash_comment          \-\-[^\r\n]*(\r|\n|\r\n)?

/* # comment ignores anything from # to the end of the line. */
pound_comment         #[^\r\n]*(\r|\n|\r\n)?

comment               ({cs_comment}|{dash_comment}|{pound_comment})

%%
 /* RULES SECTION

    This is a list of lexer rules, where the left side is the token and the
    right side is a return that yields the TokenKind enum for the token.
    Downstream components operate on the TokenKind enum values.

    All keywords (used, reserved, and usable-as-identifiers). These are
    ambiguous with the identifier rule, but they win because they are
    specified before the identifier rule.

    IMPORTANT:
    All these keywords MUST be listed in GetAllKeywords() in keywords.cc as
    well. We have tried removing the keyword rules and instead using the
    keyword functions in the {identifier} production, but that was significantly
    slower.
 */
 /* BEGIN_KEYWORDS -- Do not remove! */

 /* Spanner-specific keywords */
interleave      return Tokens::KW_INTERLEAVE;
null_filtered   return Tokens::KW_NULL_FILTERED;
parent          return Tokens::KW_PARENT;
 /* End of Spanner-specific keywords */

 /* (broken link) start */
abort           return Tokens::KW_ABORT;
access          return Tokens::KW_ACCESS;
action          return Tokens::KW_ACTION;
add             return Tokens::KW_ADD;
aggregate       return Tokens::KW_AGGREGATE;
all             return Tokens::KW_ALL;
alter           return Tokens::KW_ALTER;
always          return Tokens::KW_ALWAYS;
analyze         return Tokens::KW_ANALYZE;
and             return Tokens::KW_AND;
any             return Tokens::KW_ANY;
approx          return Tokens::KW_APPROX;
are             return Tokens::KW_ARE;
array           return Tokens::KW_ARRAY;
as              return Tokens::KW_AS;
asc             return Tokens::KW_ASC;
assert          return Tokens::KW_ASSERT;
assert_rows_modified  return Tokens::KW_ASSERT_ROWS_MODIFIED;
at              return Tokens::KW_AT;
batch           return Tokens::KW_BATCH;
begin           return Tokens::KW_BEGIN;
between         return Tokens::KW_BETWEEN;
bigdecimal      return Tokens::KW_BIGDECIMAL;
bignumeric      return Tokens::KW_BIGNUMERIC;
break           return Tokens::KW_BREAK;
by              return Tokens::KW_BY;
call            return Tokens::KW_CALL;
cascade         return Tokens::KW_CASCADE;
case            return Tokens::KW_CASE;
cast            return Tokens::KW_CAST;
check           return Tokens::KW_CHECK;
clamped         return Tokens::KW_CLAMPED;
clone           return Tokens::KW_CLONE;
cluster         return Tokens::KW_CLUSTER;
collate         return Tokens::KW_COLLATE;
column          return Tokens::KW_COLUMN;
columns         return Tokens::KW_COLUMNS;
commit          return Tokens::KW_COMMIT;
connection      return Tokens::KW_CONNECTION;
constant        return Tokens::KW_CONSTANT;
constraint      return Tokens::KW_CONSTRAINT;
contains        return Tokens::KW_CONTAINS;
continue        return Tokens::KW_CONTINUE;
copy            return Tokens::KW_COPY;
corresponding   return Tokens::KW_CORRESPONDING;
create          return Tokens::KW_CREATE;
cross           return Tokens::KW_CROSS;
cube            return Tokens::KW_CUBE;
current         return Tokens::KW_CURRENT;
cycle           return Tokens::KW_CYCLE;
data            return Tokens::KW_DATA;
database        return Tokens::KW_DATABASE;
date            return Tokens::KW_DATE;
datetime        return Tokens::KW_DATETIME;
decimal         return Tokens::KW_DECIMAL;
declare         return Tokens::KW_DECLARE;
default         return Tokens::KW_DEFAULT;
define          return Tokens::KW_DEFINE;
definer         return Tokens::KW_DEFINER;
delete          return Tokens::KW_DELETE;
deletion        return Tokens::KW_DELETION;
depth           return Tokens::KW_DEPTH;
desc            return Tokens::KW_DESC;
describe        return Tokens::KW_DESCRIBE;
descriptor      return Tokens::KW_DESCRIPTOR;
deterministic   return Tokens::KW_DETERMINISTIC;
distinct        return Tokens::KW_DISTINCT;
do              return Tokens::KW_DO;
drop            return Tokens::KW_DROP;
else            return Tokens::KW_ELSE;
elseif          return Tokens::KW_ELSEIF;
end             return Tokens::KW_END;
enforced        return Tokens::KW_ENFORCED;
enum            return Tokens::KW_ENUM;
error           return Tokens::KW_ERROR;
escape          return Tokens::KW_ESCAPE;
except          return Tokens::KW_EXCEPT;
exception       return Tokens::KW_EXCEPTION;
exclude         return Tokens::KW_EXCLUDE;
execute         return Tokens::KW_EXECUTE;
exists          return Tokens::KW_EXISTS;
explain         return Tokens::KW_EXPLAIN;
export          return Tokens::KW_EXPORT;
extend          return Tokens::KW_EXTEND;
external        return Tokens::KW_EXTERNAL;
extract         return Tokens::KW_EXTRACT;
false           return Tokens::KW_FALSE;
fetch           return Tokens::KW_FETCH;
files           return Tokens::KW_FILES;
fill            return Tokens::KW_FILL;
filter          return Tokens::KW_FILTER;
first           return Tokens::KW_FIRST;
following       return Tokens::KW_FOLLOWING;
for             return Tokens::KW_FOR;
foreign         return Tokens::KW_FOREIGN;
format          return Tokens::KW_FORMAT;
from            return Tokens::KW_FROM;
full            return Tokens::KW_FULL;
function        return Tokens::KW_FUNCTION;
generated       return Tokens::KW_GENERATED;
grant           return Tokens::KW_GRANT;
group           return Tokens::KW_GROUP;
group_rows      return Tokens::KW_GROUP_ROWS;
grouping        return Tokens::KW_GROUPING;
groups          return Tokens::KW_GROUPS;
hash            return Tokens::KW_HASH;
having          return Tokens::KW_HAVING;
hidden          return Tokens::KW_HIDDEN;
identity        return Tokens::KW_IDENTITY;
if              return Tokens::KW_IF;
ignore          return Tokens::KW_IGNORE;
immediate       return Tokens::KW_IMMEDIATE;
immutable       return Tokens::KW_IMMUTABLE;
import          return Tokens::KW_IMPORT;
in              return Tokens::KW_IN;
include         return Tokens::KW_INCLUDE;
increment       return Tokens::KW_INCREMENT;
index           return Tokens::KW_INDEX;
inner           return Tokens::KW_INNER;
inout           return Tokens::KW_INOUT;
input           return Tokens::KW_INPUT;
insert          return Tokens::KW_INSERT;
intersect       return Tokens::KW_INTERSECT;
interval        return Tokens::KW_INTERVAL;
into            return Tokens::KW_INTO;
invoker         return Tokens::KW_INVOKER;
is              return Tokens::KW_IS;
isolation       return Tokens::KW_ISOLATION;
iterate         return Tokens::KW_ITERATE;
join            return Tokens::KW_JOIN;
json            return Tokens::KW_JSON;
key             return Tokens::KW_KEY;
language        return Tokens::KW_LANGUAGE;
last            return Tokens::KW_LAST;
lateral         return Tokens::KW_LATERAL;
leave           return Tokens::KW_LEAVE;
left            return Tokens::KW_LEFT;
level           return Tokens::KW_LEVEL;
like            return Tokens::KW_LIKE;
limit           return Tokens::KW_LIMIT;
load            return Tokens::KW_LOAD;
lookup          return Tokens::KW_LOOKUP;
loop            return Tokens::KW_LOOP;
macro           return Tokens::KW_MACRO;
map             return Tokens::KW_MAP;
match           return Tokens::KW_MATCH;
match_recognize return Tokens::KW_MATCH_RECOGNIZE_NONRESERVED;
matched         return Tokens::KW_MATCHED;
materialized    return Tokens::KW_MATERIALIZED;
max             return Tokens::KW_MAX;
maxvalue        return Tokens::KW_MAXVALUE;
measures        return Tokens::KW_MEASURES;
merge           return Tokens::KW_MERGE;
message         return Tokens::KW_MESSAGE;
metadata        return Tokens::KW_METADATA;
min             return Tokens::KW_MIN;
minvalue        return Tokens::KW_MINVALUE;
model           return Tokens::KW_MODEL;
module          return Tokens::KW_MODULE;
natural         return Tokens::KW_NATURAL;
new             return Tokens::KW_NEW;
no              return Tokens::KW_NO;
not             return Tokens::KW_NOT;
null            return Tokens::KW_NULL;
nulls           return Tokens::KW_NULLS;
numeric         return Tokens::KW_NUMERIC;
of              return Tokens::KW_OF;
offset          return Tokens::KW_OFFSET;
on              return Tokens::KW_ON;
only            return Tokens::KW_ONLY;
options         return Tokens::KW_OPTIONS;
or              return Tokens::KW_OR;
order           return Tokens::KW_ORDER;
out             return Tokens::KW_OUT;
outer           return Tokens::KW_OUTER;
output          return Tokens::KW_OUTPUT;
over            return Tokens::KW_OVER;
overwrite       return Tokens::KW_OVERWRITE;
partition       return Tokens::KW_PARTITION;
partitions      return Tokens::KW_PARTITIONS;
pattern         return Tokens::KW_PATTERN;
percent         return Tokens::KW_PERCENT;
pivot           return Tokens::KW_PIVOT;
policies        return Tokens::KW_POLICIES;
policy          return Tokens::KW_POLICY;
preceding       return Tokens::KW_PRECEDING;
primary         return Tokens::KW_PRIMARY;
private         return Tokens::KW_PRIVATE;
privilege       return Tokens::KW_PRIVILEGE;
privileges      return Tokens::KW_PRIVILEGES;
procedure       return Tokens::KW_PROCEDURE;
project         return Tokens::KW_PROJECT;
proto           return Tokens::KW_PROTO;
public          return Tokens::KW_PUBLIC;
qualify         return Tokens::KW_QUALIFY_NONRESERVED;
raise           return Tokens::KW_RAISE;
range           return Tokens::KW_RANGE;
read            return Tokens::KW_READ;
recursive       return Tokens::KW_RECURSIVE;
references      return Tokens::KW_REFERENCES;
remote          return Tokens::KW_REMOTE;
remove          return Tokens::KW_REMOVE;
rename          return Tokens::KW_RENAME;
repeat          return Tokens::KW_REPEAT;
repeatable      return Tokens::KW_REPEATABLE;
replace         return Tokens::KW_REPLACE;
replace_fields  return Tokens::KW_REPLACE_FIELDS;
replica         return Tokens::KW_REPLICA;
report          return Tokens::KW_REPORT;
respect         return Tokens::KW_RESPECT;
restrict        return Tokens::KW_RESTRICT;
restriction     return Tokens::KW_RESTRICTION;
return          return Tokens::KW_RETURN;
returns         return Tokens::KW_RETURNS;
revoke          return Tokens::KW_REVOKE;
right           return Tokens::KW_RIGHT;
rollback        return Tokens::KW_ROLLBACK;
rollup          return Tokens::KW_ROLLUP;
row             return Tokens::KW_ROW;
rows            return Tokens::KW_ROWS;
run             return Tokens::KW_RUN;
safe_cast       return Tokens::KW_SAFE_CAST;
schema          return Tokens::KW_SCHEMA;
search          return Tokens::KW_SEARCH;
security        return Tokens::KW_SECURITY;
select          return Tokens::KW_SELECT;
sequence        return Tokens::KW_SEQUENCE;
set             return Tokens::KW_SET;
sets            return Tokens::KW_SETS;
show            return Tokens::KW_SHOW;
simple          return Tokens::KW_SIMPLE;
snapshot        return Tokens::KW_SNAPSHOT;
some            return Tokens::KW_SOME;
source          return Tokens::KW_SOURCE;
sql             return Tokens::KW_SQL;
stable          return Tokens::KW_STABLE;
start           return Tokens::KW_START;
static_describe  return Tokens::KW_STATIC_DESCRIBE;
stored          return Tokens::KW_STORED;
storing         return Tokens::KW_STORING;
strict          return Tokens::KW_STRICT;
struct          return Tokens::KW_STRUCT;
system          return Tokens::KW_SYSTEM;
system_time     return Tokens::KW_SYSTEM_TIME;
table           return Tokens::KW_TABLE;
tables          return Tokens::KW_TABLES;
tablesample     return Tokens::KW_TABLESAMPLE;
target          return Tokens::KW_TARGET;
temp            return Tokens::KW_TEMP;
temporary       return Tokens::KW_TEMPORARY;
then            return Tokens::KW_THEN;
time            return Tokens::KW_TIME;
timestamp       return Tokens::KW_TIMESTAMP;
to              return Tokens::KW_TO;
transaction     return Tokens::KW_TRANSACTION;
transform       return Tokens::KW_TRANSFORM;
treat           return Tokens::KW_TREAT;
true            return Tokens::KW_TRUE;
truncate        return Tokens::KW_TRUNCATE;
type            return Tokens::KW_TYPE;
unbounded       return Tokens::KW_UNBOUNDED;
undrop          return Tokens::KW_UNDROP;
union           return Tokens::KW_UNION;
unique          return Tokens::KW_UNIQUE;
unknown         return Tokens::KW_UNKNOWN;
unnest          return Tokens::KW_UNNEST;
unpivot         return Tokens::KW_UNPIVOT;
until           return Tokens::KW_UNTIL;
update          return Tokens::KW_UPDATE;
using           return Tokens::KW_USING;
value           return Tokens::KW_VALUE;
values          return Tokens::KW_VALUES;
vector          return Tokens::KW_VECTOR;
view            return Tokens::KW_VIEW;
views           return Tokens::KW_VIEWS;
volatile        return Tokens::KW_VOLATILE;
weight          return Tokens::KW_WEIGHT;
when            return Tokens::KW_WHEN;
where           return Tokens::KW_WHERE;
while           return Tokens::KW_WHILE;
window          return Tokens::KW_WINDOW;
with            return Tokens::KW_WITH;
within          return Tokens::KW_WITHIN;
write           return Tokens::KW_WRITE;
zone            return Tokens::KW_ZONE;
 /* (broken link) end */
 /* END_KEYWORDS -- Do not remove! */

 /* All unescaping and error checking is done in the parser. This allows us */
 /* to give better error messages. */
{string_literal}                            return Tokens::STRING_LITERAL;
{unclosed_string_literal}                   UNCLOSED("string literal");
{unclosed_triple_quoted_string_literal}     UNCLOSED_3("string literal");
{unclosed_raw_string_literal}               UNCLOSED("raw string literal");
{unclosed_triple_quoted_raw_string_literal} UNCLOSED_3("raw string literal");

{bytes_literal}                             return Tokens::BYTES_LITERAL;
{unclosed_bytes_literal}                    UNCLOSED("bytes literal");
{unclosed_triple_quoted_bytes_literal}      UNCLOSED_3("bytes literal");
{unclosed_raw_bytes_literal}                UNCLOSED("raw bytes literal");
{unclosed_triple_quoted_raw_bytes_literal}  UNCLOSED_3("raw bytes literal");

 /* "E" and `exponent_without_sign` represent the exponent part of a floating
    point literal, for example "1.2E-10" and "1e10". They can also be used as
    identifiers. */
"e"   return Tokens::STANDALONE_EXPONENT_SIGN;
{exponent_without_sign} return Tokens::EXP_IN_FLOAT_NO_SIGN;

{identifier}       return Tokens::IDENTIFIER;
{unclosed_escaped_identifier} UNCLOSED("identifier literal");

{decimal_digits}   return Tokens::DECIMAL_INTEGER_LITERAL;
{hex_integer}      return Tokens::HEX_INTEGER_LITERAL;

{cs_comment_begin} UNCLOSED("comment");

"("             return '(';
"["             return '[';
"{"             return '{';
")"             return ')';
"]"             return ']';
"}"             return '}';
"*"             return '*';
","             return ',';
"="             return '=';
"+="            return Tokens::KW_ADD_ASSIGN;
"-="            return Tokens::KW_SUB_ASSIGN;
"!="            return Tokens::KW_NOT_EQUALS_C_STYLE;
"<="            return Tokens::KW_LESS_EQUALS;
"<<"            return Tokens::KW_SHIFT_LEFT;
"=>"            return Tokens::KW_NAMED_ARGUMENT_ASSIGNMENT;
"->"            return Tokens::KW_LAMBDA_ARROW;
"<"             return '<';
">"             return '>';
">="            return Tokens::KW_GREATER_EQUALS;
"||"            return Tokens::KW_CONCAT_OP;
"|"             return '|';
"^"             return '^';
"&"             return '&';
"+"             return '+';
"-"             return '-';
"/"             return '/';
"~"             return '~';
"?"             return '?';
"!"             return '!';
"%"             return '%';
"|>"            return Tokens::KW_PIPE;
"@"             return '@';
"@@"            return Tokens::KW_DOUBLE_AT;
"."             return '.';
":"             return ':';
"\\"            return Tokens::BACKSLASH;
";"             return ';';
"$"{unquoted_identifier}    return Tokens::MACRO_INVOCATION;
"$"{decimal_digits}         return Tokens::MACRO_ARGUMENT_REFERENCE;
"$"             return Tokens::DOLLAR_SIGN;

 /* Whitespace and EOF rule.

    This rule eats leading whitespace but not comments. This makes the EOF
    location reported to the parser skip the trailing whitespace, which results
    in better errors for unexpected end of input. But it doesn't skip trailing
    comments.
 */
{whitespace_no_comments}   {}

{comment}       return Tokens::COMMENT;

<<EOF>>                   {
  /* The location of YYEOF is always [N, N), where N is the length of the input.
  */
  location->mutable_start().SetByteOffset(
      location->mutable_end().GetByteOffset());
  yyterminate();
}

 /* Catchall rule. */
<*>. ILLEGAL_INPUT_CHAR;
