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

#ifndef ZETASQL_PARSER_JOIN_PROCCESSOR_H_
#define ZETASQL_PARSER_JOIN_PROCCESSOR_H_

#include "zetasql/parser/bison_parser.h"

namespace zetasql {
namespace parser {

// Processing of JOIN expressions
// ==============================
//
// For the simple case that the expression does not contain consecutive ON/USING
// clauses, there are two cases:
// - There is no ON/USING clause at all. That is,
//
//      t1 JOIN t2;
//
//   In this case, an ASTJoin with no clause is returned by the rule actions.
//
// - There is exactly one ON/USING cluase. That is
//
//     t1 JOIN t2 ON cond1
//
//   In this case, an ASTJoin with one ON/USING clause is returned by the rule
//   actions.
//
// If the expression contains consecutive ON/USING clauses, it is processed in
// two steps:
//
// - In the first step, consecutive ON/USING clauses are collected and saved as
//   clause_list_ in ASTJoin nodes.
//
//   For example, for the follwing statement:
//
//     SELECT * FROM t1 JOIN t2 JOIN t3 ON cond1 ON cond2
//        JOIN t4 JOIN t5 ON cond3 ON cond4;
//
//   After the parser parses "t1 JOIN t2 JOIN t3 ON cond1 ON cond2", the
//   generated AST is:
//
//   ASTJoin(
//     lhs = ASTJoin(lhs = t1, rhs = t2),
//     rhs = ASTJoin(t3),
//     clause_list = [ASTOnClause(cond1), ASTOnClause(cond2)])
//
//   The parsing continues, and when the parser reaches the secomicolon, the
//   generated AST is:
//
//   ASTJoin(
//     lhs = ASTJoin(
//             lhs = ASTJoin(
//                     lhs = ASTJoin(lhs = t1, rhs = t2),
//                     rhs = ASTJoin(t3),
//                     clause_list = [ASTOnClause(cond1), ASTOnClause(cond2)]),
//             rhs = t4),
//     rhs = t5,
//     clause_list = [ASTOnClause(cond3), ASTOnClause(cond4)])
//
//   Since the parse reaches the semicolon, it knows that the whole expression
//   is parsed, so this step is finished.
//
// - After the whole expression is parsed into an ASTNode, this ASTNode is
//   transformed (described below) into a new ASTNode which does not
//   contain any ASTJoin with clause list, and returns the ASTNode to the
//   parser.
//
//   For example, the AST generated from the previous step is transformed into:
//
//   ASTJoin(
//     lhs = ASTJoin(
//             lhs = ASTJoin(
//                      lhs = ASTJoin(
//                              lhs = t1,
//                              rhs = t2,
//                              on_clause = ASTOnClause(cond1)),
//                      rhs = t3,
//                      on_clause = ASTOnClause(cond2)),
//             rhs = t4,
//             on_clause = ASTOnClause(cond3)),
//     rhs = t5,
//     on_clause = ASTOnClause(cond4))
//
//   And this is returned as the final result.
//
// The transformation algorithm
// ============================
//
// The transformation takes two steps. In the first step, the ASTNode that
// represents a table expression is flattened into a list. In the second step,
// the list is processed in a way that is very similar to how reverse polish
// notations are processed to generate the result: we iterate through the list,
// and push/pop items into a stack depending on the current item from the
// list. See the following example for the details.
//
// For example, in the first step, the node:
//
//     ASTJoin(
//       lhs = ASTJoin(lhs=t1, rhs=t2),
//       rhs = t3,
//       clause_list = [ASTOnClause(cond1), ASTOnClause(cond2)] )
//
// is flattened into this list:
//   [t1, ASTJoin, t2, ASTJoin, t3, ASTOnClause(cond1), ASTOnClause(cond2)]
//
// Flattening requires us to traverse down the left path (lhs) of the tree if
// there are multiple levels. E.g. from the expression
//
//    t1 JOIN t2 JOIN t3 JOIN t4 ON cond1 ON cond2 ON cond3
//
// we'll generate this node:
//
//    ASTJoin(
//      lhs = ASTJoin(
//           lhs = ASTJoin(lhs=t1, rhs=t2),
//           rhs = t3),
//      rhs = t4,
//      clause_list =
//           [ASTOnClause(cond1), ASTOnClause(cond2), ASTOnClause(cond3)])
//
// it will be flattend into this list:
//
//   [t1, JOIN, t2, JOIN, t3, JOIN, t4,
//    ASTOnClause(cond1), ASTOnClause(cond2), ASTOnClause(cond3)]
//
// Note that the rhs path will not be traversed. That is because JOINs are left
// associative, thus all JOINs will lie on the lhs path of the tree.
//
// In the second step, in a while loop, we remove an item from the front of the
// list, then check the item:
//
// - if the item is an ON/USING clause, then the top of the stack is expected to
//   be:
//                        stack top
//                           |
//                           V
//   ----+------+---------+-------+
//   ... | exp1 | ASTJoin |  exp2 |
//   ----+------+---------+-------+
//
//   In this case, we pop 3 times, create a new ASTJoin:
//
//          ASTJoin(
//              rhs = exp1
//              lhs = exp2
//              on_clause = item)
//
//  and push it into the stack. So the stack becomes:
//
//                        stack top
//                           |
//                           V
//   ----+-----------------------------------------------+
//   ... | ASTJoin(rhs=exp1, rhs=exp2, on_clause = item) |
//   ----+-----------------------------------------------+
//
// - if the item is a CROSS/COMMA/NATURAL JOIN, then the expected state is:
//
//       stack top      item          list front
//          |                           |
//          V                           V
//   ----+------+                    +------+-----
//   ... | exp1 |   ASTJoin(CROSS)   | exp2 | ...
//   ----+------+                    +------+-----
//
//   In this case, we pop exp1 from the stack, remove exp2 from the list, and
//   create a new ASTJoin:
//
//       ASTJoin(
//         join_type = CROSS,
//         lhs = exp1,
//         rhs = exp2)
//
//  and push it into the stack. So the stack becomes:
//
//                        stack top
//                           |
//                           V
//   ----+------------------------------------+
//   ... | ASTJoin(CROSS, rhs=exp1, rhs=exp2) |
//   ----+------------------------------------+
//
// - otherwise, just push the item into the stack.
//
// After all items in the list are processed, the stack will contain exactly one
// item for valid input, and that item is the final result of the
// transformation.
//
// An example to show the transformation process. Given the following
// expression:
//
//   t1 JOIN t2 CROSS JOIN t3 JOIN t4 ON cond1 ON cond2
//
// It is flattened into:
//
//  [t1, JOIN, t2, CROSS JOIN, t3, JOIN, t4, ON cond1, ON cond2]
//
// After the start of the second step, we have:
//
//   stack=[],
//   list=[t1, JOIN, t2, CROSS JOIN, t3, JOIN, t4, ON cond1, ON cond2]
//
// Here are the state after every iteration:
//
// iteration 1:
//   stack=[t1],
//   list=[JOIN, t2, CROSS JOIN, t3, JOIN, t4, ON cond1, ON cond2]
//
// iteration 2:
//   stack=[t1, JOIN],
//   list=[t2, CROSS JOIN, t3, JOIN, t4, ON cond1, ON cond2]
//
// iteration 3:
//   stack=[t1, JOIN, t2],
//   list=[CROSS JOIN, t3, JOIN, t4, ON cond1, ON cond2]
//
// iteration 4:
//   stack=[t1, JOIN, (t2 CROSS JOIN t3)],
//   list=[JOIN, t4, ON cond1, ON cond2]
//
// iteration 5:
//   stack=[t1, JOIN, (t2 CROSS JOIN t3), JOIN],
//   list=[t4, ON cond1, ON cond2]
//
// iteration 6:
//   stack=[t1, JOIN, (t2 CROSS JOIN t3), JOIN, t4],
//   list=[ON cond1, ON cond2]
//
// iteration 7:
//   stack=[t1, JOIN, ((t2 CROSS JOIN t3) JOIN t4 ON cond1)],
//   list=[ON cond2]
//
// iteration 8:
//   stack=[(t1 JOIN ((t2 CROSS JOIN t3) JOIN t4 ON cond1) ON cond2)],
//   list=[]
//
// Now the correct parse tree
//
//  (t1 JOIN ((t2 CROSS JOIN t3) JOIN t4 ON cond1) ON cond2)
//
// is generated.
//
//
// Number of unmatched join
// =========================
//
// The number of unmatched joins of an expression is the number of qualified
// joins that do not have a matching ON/USING clause.  A qualified join is
// either an inner join, or an outer join. Cross, comma or natural joins are not
// qualified joins.
//
// Examples:
// - for this expression:
//
//     t1 JOIN t2 JOIN t3
//
//   the value is 2, which is 2 (number of qualified joins) - 0 (number of
//   ON/USING clauses).
//
// - for this this expression:
//
//     t1 JOIN t2 JOIN t3 ON cond1
//
//   then value is 1, which is 2 (number of qualified joins) - 1 (number of
//   ON/USING clauses).
//
// - If the node represents this expression:
//
//     t1 JOIN t2 CROSS JOIN t3 ON cond1
//
//   then value is 0, which is 1 (number of qualified joins) - 1
//   (number of ON/USING clauses).
//
// Only the qualified joins at the top level, i.e. not inside parentheses, are
// counted. For example, for this expression
//
//    (t1 JOIN t2) JOIN t3
//
// The number of unmatched join is 1, not 2.
//
// This is used to detect errors as quickly as possible. For example, for this
// invalid query:
//
//   SELECT * FROM t1 JOIN t2 ON cond1 ON cond2 JOIN t3 JOIN t4 JOIN t5;
//
// The error is detected when the parser reaches "ON cond1 ON cond2": the number
// of unmatched join is 1, and the number of ON clauses is 2, so we know there
// is an error.
//
// Without keeping track of the number of unmatched join, the whole query has to
// be parsed first. Then in the middle of the transformation time, the error is
// detected.
//
// Mixing consecutive ON/USING clauses with COMMA JOINs
// ===================================================
//
// After discussion, we made the decision that mixing consecutive ON/USING
// clauses with COMMA JOINs is not supported. For example, this query will
// generate an error:
//
//   SELECT * FROM t1, t2 JOIN t3 JOIN t4 ON cond1 ON cond2;
//
// Note that mixing COMMA JOINs with single ON/USING clause, e.g.
//
//   SELECT * FROM t1, t2 JOIN t3 ON cond1;
//
// continues to work.

struct ErrorInfo {
  zetasql_bison_parser::location location;
  std::string message;
};

// The action to run when the join/from_clause_contents rule is matched.
// On success, returns the ASTNode that should be assigned to $$. Returns
// nullptr on failure, and 'error_info' will contain the error information.
ASTNode* JoinRuleAction(
    const zetasql_bison_parser::location& start_location,
    const zetasql_bison_parser::location& end_location, ASTNode* lhs,
    bool opt_natural, ASTJoin::JoinType join_type, ASTJoin::JoinHint join_hint,
    ASTNode* opt_hint, ASTNode* table_primary,
    ASTNode* opt_on_or_using_clause_list,
    BisonParser* parser, ErrorInfo* error_info);

// The action to run when the grammar rule
//   from_clause_contents: from_clause_contents "," table_primary
// is matched.
// Returns the ASTNode that should be assigned to $$ on success. Returns
// nullptr on failure, and 'error_info' will contain the error information.
ASTNode* CommaJoinRuleAction(
    const zetasql_bison_parser::location& start_location,
    const zetasql_bison_parser::location& end_location, ASTNode* lhs,
    ASTNode* table_primary,
    BisonParser* parser, ErrorInfo* error_info);

// Performs the transformation algorithm on the expression 'node'.
// On success, returns the created ASTNode. Returns nullptr on failure,
// and 'error_info' will contain the error information.
ASTNode* TransformJoinExpression(ASTNode* node,
                                 BisonParser* parser, ErrorInfo* error_info);

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_JOIN_PROCCESSOR_H_
