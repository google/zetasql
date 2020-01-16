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

#include "zetasql/scripting/control_flow_graph.h"

#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/scripting/script_segment.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

namespace {

std::string DebugLocationText(const ASTNode* node, const ParsedScript* script) {
  std::string node_text;
  ParseLocationPoint pos = node->GetParseLocationRange().start();
  ParseLocationTranslator translator(script->script_text());
  zetasql_base::StatusOr<std::pair<int, int>> line_and_column =
      translator.GetLineAndColumnAfterTabExpansion(pos);
  if (line_and_column.ok()) {
    absl::StrAppend(&node_text, " [at ", line_and_column.ValueOrDie().first,
                    ":", line_and_column.ValueOrDie().second, "]");
  }
  return node_text;
}

// Returns a single line consisting of the text used to form <node>
// (truncated if necessary), along with the line and column number.  This is
// intended to be used for debug output that is simply concerned with
// identifying the node, as opposed to the node's AST structure that would be
// visible with node->DebugString().
std::string DebugNodeIdentifier(const ASTNode* node,
                                const ParsedScript* script) {
  std::string node_text = std::string(
      ScriptSegment::FromASTNode(script->script_text(), node).GetSegmentText());
  absl::StripAsciiWhitespace(&node_text);
  size_t newline_idx = node_text.find('\n');
  if (newline_idx != node_text.npos) {
    node_text = absl::StrCat(node_text.substr(0, newline_idx), "...");
  }
  absl::StrAppend(&node_text, DebugLocationText(node, script));
  return node_text;
}

std::string ControlFlowEdgeKindString(ControlFlowEdge::Kind kind) {
  switch (kind) {
    case ControlFlowEdge::Kind::kNormal:
      return "kNormal";
    case ControlFlowEdge::Kind::kFalseCondition:
      return "kFalseCondition";
    case ControlFlowEdge::Kind::kTrueCondition:
      return "kTrueCondition";
    case ControlFlowEdge::Kind::kException:
      return "kException";
    default:
      return absl::StrCat("Unknown ControlFlowEdge::Kind value: ", kind);
  }
}

// Represents the incomplete making of an edge, where the predecessor and edge
// kind is known, but the successor is not known yet.  The incomplete edge will
// be converted to a complete edge, once the successor is known.
struct IncompleteEdge {
  ControlFlowNode* predecessor;
  ControlFlowEdge::Kind kind;
  std::vector<const ASTBeginEndBlock*> blocks_exited;
};

// Information about a statement needed while constructing the control flow
// graph, and discarded when the graph is complete.
//
// Unlike ControlFlowNodes, NodeData objects are created for statement lists,
// and everything that can go inside of a statement list (including compound
// statements such as IF and BEGIN).
//
// (We do not create NodeData objects for condition expressions or ELSEIF
//  clauses, as they are dealt with entirely within the visitors for their
//  enclosing statements).
struct NodeData {
  // The AST node
  const ASTNode* ast_node = nullptr;

  // The control-flow node that is entered when this AST begins; nullptr if
  // this AST node begins and enters without executing any code (e.g.
  // empty BEGIN block).
  ControlFlowNode* start = nullptr;

  // List of edges between somewhere within <ast_node> and whatever comes after
  // it, assuming no exceptions.
  //
  // Once the successor node to <ast_node> is known, each entry in <end_nodes>
  // will be converted to an edge as follows:
  //  - predecessor: entry.first
  //  - successor: Whichever node follows <ast_node>
  //  - kind: entry.second
  std::vector<IncompleteEdge> end_edges;

  // Returns true if the node does not execute any code.
  bool empty() const { return start == nullptr; }

  // Return true if this node refers to a statement require special
  // control-flow, rather than simply advancing to the next statement in the
  // enclosing statement list.
  bool IsSpecialControlFlowStatement() const {
    switch (ast_node->node_kind()) {
      case AST_BREAK_STATEMENT:
      case AST_CONTINUE_STATEMENT:
      case AST_RETURN_STATEMENT:
        return true;
      default:
        return false;
    }
  }

  // Adds an end edge to this NodeData.
  void AddOpenEndEdge(
      ControlFlowNode* cfg_node, ControlFlowEdge::Kind kind,
      const std::vector<const ASTBeginEndBlock*>& blocks_exited = {}) {
    end_edges.emplace_back(IncompleteEdge{cfg_node, kind, blocks_exited});
  }

  // Copies all end edges from <node_data>.
  void AddAllEndEdges(const NodeData* node_data) {
    end_edges.insert(end_edges.end(), node_data->end_edges.begin(),
                     node_data->end_edges.end());
  }

  std::string DebugString(const ParsedScript* script) const {
    std::string debug_string = DebugNodeIdentifier(ast_node, script);
    absl::StrAppend(&debug_string, " (");
    if (empty()) {
      absl::StrAppend(&debug_string, "<empty>");
    } else {
      absl::StrAppend(
          &debug_string, "start: ", start->DebugString(), ", end edges: ",
          absl::StrJoin(end_edges, ", ",
                        [](std::string* out, const IncompleteEdge& edge) {
                          absl::StrAppend(
                              out, edge.predecessor->DebugString(), "( ",
                              ControlFlowEdgeKindString(edge.kind), ")");
                        }));
    }
    absl::StrAppend(&debug_string, ")");
    return debug_string;
  }
};

// Contains information about a pending loop statement while we are processing
// its body.
struct LoopData {
  // List of control flow nodes that will break out of the current loop.
  std::vector<ControlFlowNode*> break_nodes;

  // List of control flow nodes that will continue the current loop.
  std::vector<ControlFlowNode*> continue_nodes;
};

}  // namespace

// Parse tree visitor to set up a ControlFlowGraph.
class ControlFlowGraphBuilder : public NonRecursiveParseTreeVisitor {
 public:
  explicit ControlFlowGraphBuilder(ControlFlowGraph* graph) : graph_(graph) {}

  zetasql_base::StatusOr<VisitResult> visitASTBreakStatement(
      const ASTBreakStatement* node) override {
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data, AddNodeDataAndGraphNode(node));
    ZETASQL_RET_CHECK(!loop_data_.empty()) << "BREAK statement without enclosing loop; "
                                      "ParsedScript should have failed earlier";
    loop_data_.back().break_nodes.push_back(node_data->start);
    return VisitResult::Empty();
  }

  zetasql_base::StatusOr<VisitResult> visitASTContinueStatement(
      const ASTContinueStatement* node) override {
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data, AddNodeDataAndGraphNode(node));
    ZETASQL_RET_CHECK(!loop_data_.empty())
        << "CONTINUE statement without enclosing loop; ParsedScript should "
           "have failed earlier";
    loop_data_.back().continue_nodes.push_back(node_data->start);
    return VisitResult::Empty();
  }

  zetasql_base::StatusOr<VisitResult> visitASTReturnStatement(
      const ASTReturnStatement* node) override {
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data, AddNodeDataAndGraphNode(node));

    // Need to exit all blocks when executing a RETURN statement.
    std::vector<const ASTBeginEndBlock*> blocks_to_exit;
    for (const ASTNode* parent = node->parent(); parent != nullptr;
         parent = parent->parent()) {
      if (parent->node_kind() == AST_BEGIN_END_BLOCK) {
        blocks_to_exit.push_back(parent->GetAsOrDie<ASTBeginEndBlock>());
      }
    }
    ZETASQL_RETURN_IF_ERROR(LinkNodes(node_data->start, graph_->end_node_.get(),
                              ControlFlowEdge::Kind::kNormal, blocks_to_exit));
    return VisitResult::Empty();
  }

  zetasql_base::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    if (node->IsStatement()) {
      const ASTStatement* stmt = node->GetAsOrDie<ASTStatement>();
      if (DoesASTNodeHaveCFGNode(stmt)) {
        ZETASQL_RETURN_IF_ERROR(
            AddNodeDataAndGraphNode(node->GetAsOrDie<ASTStatement>()).status());
      } else {
        // Unsupported scripting statement
        return zetasql_base::UnimplementedErrorBuilder()
               << "ControlFlowGraphBuilder: (" << DebugNodeIdentifier(node)
               << "): "
               << "Script statement kind " << node->SingleNodeDebugString()
               << " not implemented";
      }
      return VisitResult::Empty();
    }
    return VisitResult::VisitChildren(node);
  }

  zetasql_base::StatusOr<VisitResult> visitASTStatementList(
      const ASTStatementList* node) override {
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      // Link together each of the statements in the list
      const NodeData* first_nonempty_child_data = nullptr;
      const NodeData* prev_nonempty_child_data = nullptr;
      const NodeData* curr_child_data = nullptr;

      for (int i = 0; i < node->num_children(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(curr_child_data, GetNodeData(node->child(i)));
        if (curr_child_data->empty()) {
          continue;
        }
        if (prev_nonempty_child_data != nullptr &&
            !prev_nonempty_child_data->IsSpecialControlFlowStatement()) {
          ZETASQL_RETURN_IF_ERROR(
              LinkNodeData(prev_nonempty_child_data, curr_child_data));
        }

        if (first_nonempty_child_data == nullptr) {
          first_nonempty_child_data = curr_child_data;
        }
        prev_nonempty_child_data = curr_child_data;
      }

      ZETASQL_ASSIGN_OR_RETURN(NodeData * stmt_list_node_data, CreateNodeData(node));
      if (first_nonempty_child_data != nullptr) {
        stmt_list_node_data->start = first_nonempty_child_data->start;
        if (!prev_nonempty_child_data->IsSpecialControlFlowStatement()) {
          stmt_list_node_data->end_edges = prev_nonempty_child_data->end_edges;
        }
      }

      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTScript(const ASTScript* node) override {
    graph_->end_node_ = absl::WrapUnique(new ControlFlowNode(nullptr, graph_));
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ZETASQL_ASSIGN_OR_RETURN(const NodeData* stmt_list_data,
                       GetNodeData(node->statement_list_node()));
      if (stmt_list_data->empty()) {
        // The entire script does not execute any code.
        graph_->start_node_ = graph_->end_node();
      } else {
        graph_->start_node_ = stmt_list_data->start;
        ZETASQL_RETURN_IF_ERROR(LinkEndNodes(stmt_list_data, graph_->end_node_.get()));
      }
      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTIfStatement(
      const ASTIfStatement* node) override {
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ZETASQL_ASSIGN_OR_RETURN(NodeData * if_stmt_node_data, CreateNodeData(node));
      ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * condition,
                       AddGraphNode(node->condition()));
      ZETASQL_ASSIGN_OR_RETURN(const NodeData* then_list_data,
                       GetNodeData(node->then_list()));

      if_stmt_node_data->start = condition;
      if (then_list_data->empty()) {
        if_stmt_node_data->AddOpenEndEdge(condition,
                                      ControlFlowEdge::Kind::kTrueCondition);
      } else {
        ZETASQL_RETURN_IF_ERROR(LinkNodes(condition, then_list_data->start,
                                  ControlFlowEdge::Kind::kTrueCondition));
        if_stmt_node_data->AddAllEndEdges(then_list_data);
      }

      ControlFlowNode* prev_condition = condition;
      if (node->elseif_clauses() != nullptr) {
        for (const ASTElseifClause* elseif_clause :
             node->elseif_clauses()->elseif_clauses()) {
          ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * curr_condition,
                           AddGraphNode(elseif_clause->condition()));
          ZETASQL_ASSIGN_OR_RETURN(const NodeData* body_data,
                           GetNodeData(elseif_clause->body()));

          ZETASQL_RETURN_IF_ERROR(LinkNodes(prev_condition, curr_condition,
                                    ControlFlowEdge::Kind::kFalseCondition));
          if (body_data->empty()) {
            if_stmt_node_data->AddOpenEndEdge(
                curr_condition, ControlFlowEdge::Kind::kTrueCondition);
          } else {
            ZETASQL_RETURN_IF_ERROR(LinkNodes(curr_condition, body_data->start,
                                      ControlFlowEdge::Kind::kTrueCondition));
            if_stmt_node_data->AddAllEndEdges(body_data);
          }
          prev_condition = curr_condition;
        }
      }

      bool has_nonempty_else = false;
      if (node->else_list() != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(const NodeData* else_data,
                         GetNodeData(node->else_list()));
        if (!else_data->empty()) {
          has_nonempty_else = true;
          ZETASQL_RETURN_IF_ERROR(LinkNodes(prev_condition, else_data->start,
                                    ControlFlowEdge::Kind::kFalseCondition));
          if_stmt_node_data->AddAllEndEdges(else_data);
        }
      }
      if (!has_nonempty_else) {
        if_stmt_node_data->AddOpenEndEdge(prev_condition,
                                      ControlFlowEdge::Kind::kFalseCondition);
      }
      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTWhileStatement(
      const ASTWhileStatement* node) override {
    loop_data_.emplace_back();
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ControlFlowNode* condition_cfg_node = nullptr;
      ZETASQL_ASSIGN_OR_RETURN(NodeData * while_stmt_node_data, CreateNodeData(node));
      ZETASQL_ASSIGN_OR_RETURN(const NodeData* body_data, GetNodeData(node->body()));
      if (node->condition() != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(condition_cfg_node, AddGraphNode(node->condition()));
        while_stmt_node_data->start = condition_cfg_node;
        ZETASQL_RETURN_IF_ERROR(LinkNodes(
            condition_cfg_node,
            body_data->empty() ? condition_cfg_node : body_data->start,
            ControlFlowEdge::Kind::kTrueCondition));
        ZETASQL_RETURN_IF_ERROR(LinkEndNodes(body_data, condition_cfg_node));
        while_stmt_node_data->AddOpenEndEdge(
            condition_cfg_node, ControlFlowEdge::Kind::kFalseCondition);
      } else {
        if (body_data->empty()) {
          // Create a dummy control-flow node based on the body's empty
          // statement list with an edge onto itself.  Normally, we don't
          // generate control-flow nodes for empty statement lists, but we have
          // to make an exception in this case; otherwise, there would be no way
          // to represent an infinite loop without code in it.
          ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * body_cfg_node,
                           AddGraphNode(node->body()));
          while_stmt_node_data->start = body_cfg_node;
          ZETASQL_RETURN_IF_ERROR(LinkNodes(body_cfg_node, body_cfg_node,
                                    ControlFlowEdge::Kind::kNormal));
        } else {
          while_stmt_node_data->start = body_data->start;
          ZETASQL_RETURN_IF_ERROR(LinkEndNodes(body_data, body_data->start));
        }
      }

      // Handle BREAK/CONTINUE statements inside the loop.
      const LoopData& loop_data = loop_data_.back();
      for (ControlFlowNode* break_node : loop_data.break_nodes) {
        const std::vector<const ASTBeginEndBlock*>& blocks_to_exit =
            graph_->script()
                ->break_continue_map()
                .at(break_node->ast_node()->GetAsOrDie<ASTBreakStatement>())
                .blocks_to_exit();
        while_stmt_node_data->AddOpenEndEdge(
            break_node, ControlFlowEdge::Kind::kNormal, blocks_to_exit);
      }
      for (ControlFlowNode* continue_node : loop_data.continue_nodes) {
        const std::vector<const ASTBeginEndBlock*>& blocks_to_exit =
            graph_->script()
                ->break_continue_map()
                .at(continue_node->ast_node()
                        ->GetAsOrDie<ASTContinueStatement>())
                .blocks_to_exit();
        if (condition_cfg_node != nullptr) {
          ZETASQL_RETURN_IF_ERROR(LinkNodes(continue_node, condition_cfg_node,
                                    ControlFlowEdge::Kind::kNormal,
                                    blocks_to_exit));
        } else {
          ZETASQL_RET_CHECK(!body_data->empty())
              << "Empty loop body detected, but loop body contains a CONTINUE "
                 "statement; should not be possible";
          ZETASQL_RETURN_IF_ERROR(LinkNodes(continue_node, body_data->start,
                                    ControlFlowEdge::Kind::kNormal,
                                    blocks_to_exit));
        }
      }
      loop_data_.pop_back();
      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTBeginEndBlock(
      const ASTBeginEndBlock* node) override {
    if (node->has_exception_handler()) {
      return zetasql_base::InternalErrorBuilder()
             << "ControlGraphBuilder: Blocks with exception handlers not yet "
                "implemented";
    }
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ZETASQL_ASSIGN_OR_RETURN(const NodeData* stmt_list_data,
                       GetNodeData(node->statement_list_node()));
      ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data, CreateNodeData(node));
      *node_data = *stmt_list_data;
      for (IncompleteEdge& edge : node_data->end_edges) {
        edge.blocks_exited.push_back(node);
      }
      return zetasql_base::OkStatus();
    });
  }

 private:
  zetasql_base::StatusOr<const NodeData*> GetNodeData(const ASTNode* node) {
    auto it = node_data_.find(node);
    if (it == node_data_.end()) {
      ZETASQL_RET_CHECK_FAIL() << "Unable to locate node data for "
                       << DebugNodeIdentifier(node);
    }
    return it->second.get();
  }

  zetasql_base::StatusOr<NodeData*> CreateNodeData(const ASTNode* node) {
    auto pair = node_data_.emplace(node, absl::make_unique<NodeData>());
    if (!pair.second) {
      return zetasql_base::InternalErrorBuilder()
             << "Node data for " << DebugNodeIdentifier(node)
             << " already exists";
    }
    pair.first->second->ast_node = node;
    return pair.first->second.get();
  }

  bool DoesASTNodeHaveCFGNode(const ASTStatement* node) {
    switch (node->node_kind()) {
      case AST_VARIABLE_DECLARATION:
      case AST_SYSTEM_VARIABLE_ASSIGNMENT:
      case AST_SINGLE_ASSIGNMENT:
      case AST_ASSIGNMENT_FROM_STRUCT:
      case AST_BREAK_STATEMENT:
      case AST_CONTINUE_STATEMENT:
        return true;
      default:
        // TODO: Also consider RETURN statements and IF/WHILE
        // condition nodes.
        return node->IsSqlStatement();
    }
  }
  std::string DebugNodeIdentifier(const ASTNode* node) {
    CHECK(node != nullptr);
    return zetasql::DebugNodeIdentifier(node, graph_->script());
  }

  zetasql_base::StatusOr<ControlFlowNode*> AddGraphNode(const ASTNode* ast_node) {
    auto emplace_result = graph_->node_map_.emplace(
        ast_node, absl::WrapUnique(new ControlFlowNode(ast_node, graph_)));
    if (!emplace_result.second) {
      return zetasql_base::InternalErrorBuilder()
             << "Graph node already exists for AST node "
             << DebugNodeIdentifier(ast_node);
    }
    return emplace_result.first->second.get();
  }

  zetasql_base::StatusOr<NodeData*> AddNodeDataAndGraphNode(
      const ASTStatement* ast_stmt) {
    ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * cfg_node, AddGraphNode(ast_stmt));
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data, CreateNodeData(ast_stmt));
    node_data->start = cfg_node;
    node_data->AddOpenEndEdge(cfg_node, ControlFlowEdge::Kind::kNormal);
    return node_data;
  }

  zetasql_base::StatusOr<ControlFlowNode*> LookupNode(const ASTNode* ast_node) {
    auto it = graph_->node_map_.find(ast_node);
    if (it == graph_->node_map_.end()) {
      ZETASQL_RET_CHECK_FAIL() << zetasql_base::InternalErrorBuilder()
                       << "Unable to locate node in graph: "
                       << DebugNodeIdentifier(ast_node);
    }
    return it->second.get();
  }

  zetasql_base::Status LinkEndNodes(const NodeData* pred, ControlFlowNode* succ) {
    for (auto edge : pred->end_edges) {
      ZETASQL_RETURN_IF_ERROR(
          LinkNodes(edge.predecessor, succ, edge.kind, edge.blocks_exited));
    }
    return zetasql_base::OkStatus();
  }

  zetasql_base::Status LinkNodeData(const NodeData* pred, const NodeData* succ) {
    if (pred->empty()) {
      return zetasql_base::InternalErrorBuilder()
             << "LinkNodeData: predecessor is empty: "
             << DebugNodeIdentifier(pred->ast_node);
    }
    if (succ->empty()) {
      return zetasql_base::InternalErrorBuilder()
             << "LinkNodeData: successor is empty: "
             << DebugNodeIdentifier(succ->ast_node);
    }
    ZETASQL_RETURN_IF_ERROR(LinkEndNodes(pred, succ->start));
    return zetasql_base::OkStatus();
  }

  zetasql_base::Status LinkNodes(
      ControlFlowNode* cfg_pred, ControlFlowNode* cfg_succ,
      ControlFlowEdge::Kind kind,
      const std::vector<const ASTBeginEndBlock*>& exited_blocks = {}) {
    if (cfg_pred->ast_node()->IsStatement() ||
        cfg_pred->ast_node()->node_kind() == AST_STATEMENT_LIST) {
      ZETASQL_RET_CHECK(kind == ControlFlowEdge::Kind::kNormal)
          << "Unconditional statement must use normal edge"
          << cfg_pred->DebugString();
    } else if (cfg_pred->ast_node()->IsExpression()) {
      ZETASQL_RET_CHECK(kind == ControlFlowEdge::Kind::kTrueCondition ||
                kind == ControlFlowEdge::Kind::kFalseCondition)
          << "conditional statement must use true/false condition"
          << cfg_pred->DebugString();
    } else {
      ZETASQL_RET_CHECK_FAIL() << "unexpected ast node";
    }
    graph_->edges_.emplace_back(absl::WrapUnique(
        new ControlFlowEdge(cfg_pred, cfg_succ, kind, graph_, exited_blocks)));
    const ControlFlowEdge* edge = graph_->edges_.back().get();

    // Mark edge as successor of predecessor
    if (!cfg_pred->successors_.emplace(kind, edge).second) {
      return zetasql_base::InternalErrorBuilder()
             << "Node " << cfg_pred->DebugString()
             << " already contains a successor with kind "
             << ControlFlowEdgeKindString(kind) << ": "
             << cfg_pred->successors_.at(kind)->successor()->DebugString();
    }

    // Mark edge as predecessor of successor
    cfg_succ->predecessors_.emplace_back(edge);

    return zetasql_base::OkStatus();
  }

  // Keeps track of temporary information about each AST node needed while
  // building the graph.
  absl::flat_hash_map<const ASTNode*, std::unique_ptr<NodeData>> node_data_;

  // Stack keeping track of additional pending information on loops that we are
  // currently visiting the body of.  One entry per loop statement, with the
  // innermost loop at the back of the list.
  std::vector<LoopData> loop_data_;

  // The ControlFlowGraph being built.
  ControlFlowGraph* graph_;
};

zetasql_base::StatusOr<std::unique_ptr<const ControlFlowGraph>>
ControlFlowGraph::Create(const ParsedScript* script) {
  auto graph = absl::WrapUnique(new ControlFlowGraph(script));
  ControlFlowGraphBuilder builder(graph.get());
  ZETASQL_RETURN_IF_ERROR(script->script()->TraverseNonRecursive(&builder));
  return std::move(graph);
}

ControlFlowGraph::ControlFlowGraph(const ParsedScript* script)
    : start_node_(nullptr), script_(script) {}

namespace {
void AddDestroyedVariables(const ASTStatementList* stmt_list,
                           std::set<std::string>* destroyed_variables) {
  for (const ASTStatement* stmt : stmt_list->statement_list()) {
    if (stmt->node_kind() != AST_VARIABLE_DECLARATION) {
      // Variable declarations are allowed only at the start of a block, so
      // no more variable declarations in this block can exist.
      break;
    }
    for (const ASTIdentifier* id : stmt->GetAs<ASTVariableDeclaration>()
                                       ->variable_list()
                                       ->identifier_list()) {
      destroyed_variables->emplace(id->GetAsString());
    }
  }
}
}  // namespace

std::set<std::string> ControlFlowEdge::GetDestroyedVariables() const {
  std::set<std::string> destroyed_variables;
  for (const ASTBeginEndBlock* block : blocks_to_exit_) {
    AddDestroyedVariables(block->statement_list_node(), &destroyed_variables);
  }

  // If exiting the entire script, top-level variables are destroyed as well
  if (successor_ == graph_->end_node()) {
    AddDestroyedVariables(graph_->script()->script()->statement_list_node(),
                          &destroyed_variables);
  }

  return destroyed_variables;
}

namespace {
void AddDestroyedVariablesToDebugString(const ControlFlowEdge& edge,
                                        std::string* debug_string) {
  std::set<std::string> destroyed_variables = edge.GetDestroyedVariables();
  if (!destroyed_variables.empty()) {
    std::vector<std::string> destroyed_variables_vector(
        destroyed_variables.begin(), destroyed_variables.end());
    std::sort(destroyed_variables_vector.begin(),
              destroyed_variables_vector.end());
    absl::StrAppend(debug_string, " [destroying ",
                    absl::StrJoin(destroyed_variables_vector, ", "), "]");
  }
}
}  // namespace

std::string ControlFlowEdge::DebugString() const {
  std::string debug_string;
  absl::StrAppend(&debug_string, predecessor_->DebugString(), " => (",
                  ControlFlowEdgeKindString(kind_), ") ",
                  successor_->DebugString());
  AddDestroyedVariablesToDebugString(*this, &debug_string);

  return debug_string;
}

namespace {
bool CompareControlFlowNodesByScriptLocation(const ControlFlowNode* node1,
                                             const ControlFlowNode* node2) {
  if (node1->ast_node() != nullptr && node2->ast_node() == nullptr) {
    return node1;
  }
  if (node2->ast_node() != nullptr && node1->ast_node() == nullptr) {
    return node2;
  }

  int node1_offset =
      node1->ast_node()->GetParseLocationRange().start().GetByteOffset();
  int node2_offset =
      node2->ast_node()->GetParseLocationRange().start().GetByteOffset();

  return node1_offset < node2_offset;
}
}  // namespace

std::string ControlFlowNode::DebugString() const {
  if (ast_node_ != nullptr) {
    if (ast_node_->node_kind() == AST_STATEMENT_LIST) {
      // We normally don't generate control-flow nodes for statement lists, but
      // empty loop bodies (without a condition) are an exception, since it's
      // the only way to represent an infinite loop that doesn't execute any
      // code.
      return absl::StrCat("<empty loop body>",
                          DebugLocationText(ast_node_, graph_->script()));
    }
    return DebugNodeIdentifier(ast_node_, graph_->script());
  } else {
    return "<end>";
  }
}

std::string ControlFlowNode::SuccessorsDebugString(
    absl::string_view indent) const {
  std::vector<std::string> lines;
  for (ControlFlowEdge::Kind kind :
       {ControlFlowEdge::Kind::kNormal, ControlFlowEdge::Kind::kTrueCondition,
        ControlFlowEdge::Kind::kFalseCondition,
        ControlFlowEdge::Kind::kException}) {
    auto it = successors_.find(kind);
    if (it != successors_.end()) {
      lines.push_back(absl::StrCat(indent, "(", ControlFlowEdgeKindString(kind),
                                   ") => ",
                                   it->second->successor()->DebugString()));
      AddDestroyedVariablesToDebugString(*it->second, &lines.back());
    }
  }
  return absl::StrJoin(lines, "\n");
}

std::vector<const ControlFlowNode*> ControlFlowGraph::GetAllNodes() const {
  std::vector<const ControlFlowNode*> nodes;
  nodes.reserve(node_map_.size());
  for (const auto& entry : node_map_) {
    nodes.push_back(entry.second.get());
  }
  return nodes;
}

std::string ControlFlowGraph::DebugString() const {
  std::string debug_string;
  absl::StrAppend(&debug_string, "start: ", start_node_->DebugString(),
                  "\nedges:");

  // Sort nodes by script location so that the debug string is stable enough to
  // be used in test output.
  std::vector<const ControlFlowNode*> nodes = GetAllNodes();
  std::sort(nodes.begin(), nodes.end(),
            CompareControlFlowNodesByScriptLocation);

  for (const ControlFlowNode* node : nodes) {
    absl::StrAppend(&debug_string, "\n  ", node->DebugString(), "\n",
                    node->SuccessorsDebugString("    "));
  }
  return debug_string;
}

}  // namespace zetasql
