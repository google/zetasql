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

std::string DebugLocationText(const ASTNode* node,
                              const absl::string_view script_text) {
  std::string node_text;
  ParseLocationPoint pos = node->GetParseLocationRange().start();
  ParseLocationTranslator translator(script_text);
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
                                const absl::string_view script_text) {
  std::string node_text = std::string(
      ScriptSegment::FromASTNode(script_text, node).GetSegmentText());
  absl::StripAsciiWhitespace(&node_text);
  size_t newline_idx = node_text.find('\n');
  if (newline_idx != node_text.npos) {
    node_text = absl::StrCat(node_text.substr(0, newline_idx), "...");
  }
  absl::StrAppend(&node_text, DebugLocationText(node, script_text));
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

// Indicates whether a particular statement type can/must throw an exception.
enum class ThrowSemantics {
  kNoThrow,
  kCanThrow,
  kMustThrow,
};

// Represents the incomplete making of an edge, where the predecessor and edge
// kind is known, but the successor is not known yet.  The incomplete edge will
// be converted to a complete edge, once the successor is known.
struct IncompleteEdge {
  IncompleteEdge(const IncompleteEdge& edge) = delete;
  IncompleteEdge(IncompleteEdge&& edge) = default;
  IncompleteEdge& operator=(const IncompleteEdge& edge) = delete;
  IncompleteEdge& operator=(IncompleteEdge&& edge) = default;

  ControlFlowNode* predecessor;
  ControlFlowEdge::Kind kind;
  std::vector<const ASTBeginEndBlock*> blocks_exited;
  int num_exception_handlers_exited = 0;
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
  std::list<IncompleteEdge> end_edges;

  // Returns true if this node is skipped entirely in the control-flow graph.
  // This arises when execution of the node is completely empty, causing any
  // edge leading into the node to instead go to whatever follows it.
  // Examples include an empty statement list or BEGIN/END block.
  //
  // Note: Skipped nodes may still contain end-edges, representing unreachable
  // statements within them, for example:
  //
  // BEGIN
  // EXCEPTION WHEN ERROR THEN
  //   SELECT 1;
  // END;
  // SELECT 2;
  //
  // In this script, execution of the BEGIN node is a nop because the exception
  // handler clause can never be reached.  However, "SELECT 1" still exists as
  // an end-edge, so that the "SELECT 1" => "SELECT 2" edge can be constructed
  // later.  The end result is that "SELECT 1" is unreachable (no predecessors),
  // but, if we were to somehow reach it, "SELECT 2" would come next.
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
  void AddOpenEndEdge(ControlFlowNode* cfg_node, ControlFlowEdge::Kind kind) {
    return AddOpenEndEdge(cfg_node, kind, {}, 0);
  }

  void AddOpenEndEdge(ControlFlowNode* cfg_node, ControlFlowEdge::Kind kind,
                      const std::vector<const ASTBeginEndBlock*>& blocks_exited,
                      int num_exception_handlers_exited) {
    end_edges.emplace_front(IncompleteEdge{cfg_node, kind, blocks_exited,
                                           num_exception_handlers_exited});
  }

  // Moves all end edges from <node_data> into here.
  void TakeEndEdgesFrom(NodeData* node_data) {
    end_edges.splice(end_edges.begin(), std::move(node_data->end_edges));
  }

  std::string DebugString(absl::string_view script_text) const {
    std::string debug_string = DebugNodeIdentifier(ast_node, script_text);
    absl::StrAppend(&debug_string, " (");
    absl::StrAppend(
        &debug_string,
        "start: ", (start == nullptr ? "<empty>" : start->DebugString()),
        ", end edges: ",
        absl::StrJoin(
            end_edges, ", ", [](std::string* out, const IncompleteEdge& edge) {
              absl::StrAppend(out, edge.predecessor->DebugString(), "( ",
                              ControlFlowEdgeKindString(edge.kind), ")");
            }));
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

// Contains information about a pending block.
struct BlockWithExceptionHandlerData {
  // Lists control-flow nodes which might throw an exception handled by the
  // current block.
  std::vector<ControlFlowNode*> handled_nodes;
};

}  // namespace

// Parse tree visitor to set up a ControlFlowGraph.
class ControlFlowGraphBuilder : public NonRecursiveParseTreeVisitor {
 public:
  explicit ControlFlowGraphBuilder(ControlFlowGraph* graph) : graph_(graph) {}

  zetasql_base::StatusOr<VisitResult> visitASTBreakStatement(
      const ASTBreakStatement* node) override {
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data,
                     AddNodeDataAndGraphNode(node, ThrowSemantics::kNoThrow));
    ZETASQL_RET_CHECK(!loop_data_.empty()) << "BREAK statement without enclosing loop; "
                                      "ParsedScript should have failed earlier";
    loop_data_.back().break_nodes.push_back(node_data->start);
    return VisitResult::Empty();
  }

  zetasql_base::StatusOr<VisitResult> visitASTContinueStatement(
      const ASTContinueStatement* node) override {
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data,
                     AddNodeDataAndGraphNode(node, ThrowSemantics::kNoThrow));
    ZETASQL_RET_CHECK(!loop_data_.empty())
        << "CONTINUE statement without enclosing loop; ParsedScript should "
           "have failed earlier";
    loop_data_.back().continue_nodes.push_back(node_data->start);
    return VisitResult::Empty();
  }

  // Gathers a list of all block/exception handler exits when constructing an
  // edge from <start_node> to some other node directly underneath <stop_node>,
  // which must be an ancestor of <start_node>.
  void GatherBlocksAndExceptionHandlersExited(
      const ASTNode* start_node, const ASTNode* stop_node,
      std::vector<const ASTBeginEndBlock*>* blocks_to_exit,
      int* num_exception_handlers_exited) {
    blocks_to_exit->clear();
    *num_exception_handlers_exited = 0;
    for (const ASTNode* node = start_node; node != stop_node && node != nullptr;
         node = node->parent()) {
      switch (node->node_kind()) {
        case AST_BEGIN_END_BLOCK:
          blocks_to_exit->push_back(node->GetAsOrDie<ASTBeginEndBlock>());
          break;
        case AST_EXCEPTION_HANDLER:
          // Exiting the exception handler doesn't destroy variables in the
          // enclosing BEGIN/END block; these variables have already been
          // destroyed, so skip it.
          ++*num_exception_handlers_exited;
          node = node->parent()
                     ->GetAsOrDie<ASTExceptionHandlerList>()
                     ->parent()
                     ->GetAsOrDie<ASTBeginEndBlock>();
          break;
        default:
          break;
      }
    }
  }

  zetasql_base::StatusOr<VisitResult> visitASTRaiseStatement(
      const ASTRaiseStatement* node) override {
    ZETASQL_RETURN_IF_ERROR(
        AddNodeDataAndGraphNode(node, ThrowSemantics::kMustThrow).status());
    return VisitResult::Empty();
  }

  zetasql_base::StatusOr<VisitResult> visitASTReturnStatement(
      const ASTReturnStatement* node) override {
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data,
                     AddNodeDataAndGraphNode(node, ThrowSemantics::kNoThrow));

    // Need to exit all blocks and exception handlers when executing a RETURN
    // statement.
    std::vector<const ASTBeginEndBlock*> blocks_to_exit;
    int num_exception_handlers_exited = 0;
    GatherBlocksAndExceptionHandlersExited(node, /*stop_node=*/nullptr,
                                           &blocks_to_exit,
                                           &num_exception_handlers_exited);
    ZETASQL_RETURN_IF_ERROR(LinkNodes(node_data->start, graph_->end_node_.get(),
                              ControlFlowEdge::Kind::kNormal, blocks_to_exit,
                              num_exception_handlers_exited));
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

  zetasql_base::StatusOr<VisitResult> visitASTExceptionHandlerList(
      const ASTExceptionHandlerList* node) override {
    return VisitResult::VisitChildren(node);
  }

  zetasql_base::StatusOr<VisitResult> visitASTExceptionHandler(
      const ASTExceptionHandler* node) override {
    return VisitResult::VisitChildren(node);
  }

  zetasql_base::StatusOr<VisitResult> visitASTStatementList(
      const ASTStatementList* node) override {
    // Check if this block adds any new exception handlers.
    bool try_block = false;
    if (node->parent()->node_kind() == AST_BEGIN_END_BLOCK &&
        node->parent()
            ->GetAsOrDie<ASTBeginEndBlock>()
            ->has_exception_handler()) {
      ZETASQL_RET_CHECK(node->parent()
                    ->GetAsOrDie<ASTBeginEndBlock>()
                    ->statement_list_node() == node)
          << "Node should be the statement list of its enclosing block";
      exception_handler_data_stack_.push_back(
          exception_handler_block_data_map_
              .at(node->parent()->GetAsOrDie<ASTBeginEndBlock>())
              .get());
      try_block = true;
    }

    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      if (try_block) {
        ZETASQL_RET_CHECK(node->parent()
                      ->GetAsOrDie<ASTBeginEndBlock>()
                      ->statement_list_node() == node)
            << "Node should be the statement list of its enclosing block";
        exception_handler_data_stack_.pop_back();
      }

      ZETASQL_ASSIGN_OR_RETURN(NodeData * stmt_list_node_data, CreateNodeData(node));
      // Link together each of the statements in the list
      std::unique_ptr<NodeData> prev_nonempty_child_data;
      std::unique_ptr<NodeData> curr_child_data;

      std::list<IncompleteEdge> pending_unreachable_incomplete_edges;

      for (int i = 0; i < node->num_children(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(curr_child_data, TakeNodeData(node->child(i)));
        if (curr_child_data->empty()) {
          // Even if the current statement is empty, it could still have edges
          // going out of it if we have a block if nothing in it but an
          // (unreachable) exception handler.  The exception handler is
          // unreachable, but the edges going out of it still need to link to
          // the next statement.
          pending_unreachable_incomplete_edges.splice(
              pending_unreachable_incomplete_edges.begin(),
              std::move(curr_child_data->end_edges));
          continue;
        }

        for (const IncompleteEdge& edge :
             pending_unreachable_incomplete_edges) {
          ZETASQL_RETURN_IF_ERROR(LinkNodes(edge.predecessor, curr_child_data->start,
                                    edge.kind, edge.blocks_exited,
                                    edge.num_exception_handlers_exited));
        }
        pending_unreachable_incomplete_edges.clear();

        if (prev_nonempty_child_data != nullptr &&
            !prev_nonempty_child_data->IsSpecialControlFlowStatement()) {
          ZETASQL_RETURN_IF_ERROR(LinkNodeData(prev_nonempty_child_data.get(),
                                       curr_child_data.get()));
        }

        if (stmt_list_node_data->start == nullptr) {
          stmt_list_node_data->start = curr_child_data->start;
        }
        prev_nonempty_child_data = std::move(curr_child_data);
      }

      if (!stmt_list_node_data->empty() &&
          !prev_nonempty_child_data->IsSpecialControlFlowStatement()) {
        stmt_list_node_data->end_edges =
            std::move(prev_nonempty_child_data->end_edges);
      }

      stmt_list_node_data->end_edges.splice(
          stmt_list_node_data->end_edges.begin(),
          std::move(pending_unreachable_incomplete_edges));

      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTScript(const ASTScript* node) override {
    graph_->end_node_ = absl::WrapUnique(new ControlFlowNode(nullptr, graph_));
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const NodeData> stmt_list_data,
                       TakeNodeData(node->statement_list_node()));
      if (stmt_list_data->empty()) {
        // The entire script does not execute any code.
        graph_->start_node_ = graph_->end_node();
      } else {
        graph_->start_node_ = stmt_list_data->start;
      }
      ZETASQL_RETURN_IF_ERROR(
          LinkEndNodes(stmt_list_data.get(), graph_->end_node_.get()));
      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTIfStatement(
      const ASTIfStatement* node) override {
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ZETASQL_ASSIGN_OR_RETURN(NodeData * if_stmt_node_data, CreateNodeData(node));
      ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * condition, AddGraphNode(node));
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NodeData> then_list_data,
                       TakeNodeData(node->then_list()));

      if_stmt_node_data->start = condition;
      if (then_list_data->empty()) {
        if_stmt_node_data->AddOpenEndEdge(
            condition, ControlFlowEdge::Kind::kTrueCondition);
      } else {
        ZETASQL_RETURN_IF_ERROR(LinkNodes(condition, then_list_data->start,
                                  ControlFlowEdge::Kind::kTrueCondition));
      }
      if_stmt_node_data->TakeEndEdgesFrom(then_list_data.get());

      ControlFlowNode* prev_condition = condition;
      if (node->elseif_clauses() != nullptr) {
        for (const ASTElseifClause* elseif_clause :
             node->elseif_clauses()->elseif_clauses()) {
          ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * curr_condition,
                           AddGraphNode(elseif_clause));
          ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NodeData> body_data,
                           TakeNodeData(elseif_clause->body()));

          ZETASQL_RETURN_IF_ERROR(LinkNodes(prev_condition, curr_condition,
                                    ControlFlowEdge::Kind::kFalseCondition));
          if (!body_data->empty()) {
            ZETASQL_RETURN_IF_ERROR(LinkNodes(curr_condition, body_data->start,
                                      ControlFlowEdge::Kind::kTrueCondition));
          } else {
            if_stmt_node_data->AddOpenEndEdge(
                curr_condition, ControlFlowEdge::Kind::kTrueCondition);
          }
          if_stmt_node_data->TakeEndEdgesFrom(body_data.get());
          prev_condition = curr_condition;
        }
      }

      bool has_nonempty_else = false;
      if (node->else_list() != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NodeData> else_data,
                         TakeNodeData(node->else_list()));
        if (!else_data->empty()) {
          has_nonempty_else = true;
          ZETASQL_RETURN_IF_ERROR(LinkNodes(prev_condition, else_data->start,
                                    ControlFlowEdge::Kind::kFalseCondition));
        }
        if_stmt_node_data->TakeEndEdgesFrom(else_data.get());
      }
      if (!has_nonempty_else) {
        if_stmt_node_data->AddOpenEndEdge(
            prev_condition, ControlFlowEdge::Kind::kFalseCondition);
      }
      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTWhileStatement(
      const ASTWhileStatement* node) override {
    loop_data_.emplace_back();
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ControlFlowNode* loop_cfg_node = nullptr;
      ZETASQL_ASSIGN_OR_RETURN(loop_cfg_node, AddGraphNode(node));
      ZETASQL_ASSIGN_OR_RETURN(NodeData * while_stmt_node_data, CreateNodeData(node));
      while_stmt_node_data->start = loop_cfg_node;
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const NodeData> body_data,
                       TakeNodeData(node->body()));
      if (node->condition() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(
            LinkNodes(loop_cfg_node,
                      body_data->empty() ? loop_cfg_node : body_data->start,
                      ControlFlowEdge::Kind::kTrueCondition));
        ZETASQL_RETURN_IF_ERROR(LinkEndNodes(body_data.get(), loop_cfg_node));
        while_stmt_node_data->AddOpenEndEdge(
            loop_cfg_node, ControlFlowEdge::Kind::kFalseCondition);
      } else {
        if (!body_data->empty()) {
          ZETASQL_RETURN_IF_ERROR(LinkNodes(loop_cfg_node, body_data->start,
                                    ControlFlowEdge::Kind::kNormal));
        } else {
          ZETASQL_RETURN_IF_ERROR(LinkNodes(loop_cfg_node, loop_cfg_node,
                                    ControlFlowEdge::Kind::kNormal));
        }
        ZETASQL_RETURN_IF_ERROR(LinkEndNodes(body_data.get(), loop_cfg_node));
      }

      // Handle BREAK/CONTINUE statements inside the loop.
      const LoopData& loop_data = loop_data_.back();
      for (ControlFlowNode* break_node : loop_data.break_nodes) {
        std::vector<const ASTBeginEndBlock*> blocks_to_exit;
        int num_exception_handlers_exited;
        GatherBlocksAndExceptionHandlersExited(break_node->ast_node(), node,
                                               &blocks_to_exit,
                                               &num_exception_handlers_exited);
        while_stmt_node_data->AddOpenEndEdge(
            break_node, ControlFlowEdge::Kind::kNormal, blocks_to_exit,
            num_exception_handlers_exited);
      }
      for (ControlFlowNode* continue_node : loop_data.continue_nodes) {
        std::vector<const ASTBeginEndBlock*> blocks_to_exit;
        int num_exception_handlers_exited;
        GatherBlocksAndExceptionHandlersExited(continue_node->ast_node(), node,
                                               &blocks_to_exit,
                                               &num_exception_handlers_exited);
        ZETASQL_RETURN_IF_ERROR(LinkNodes(
            continue_node, loop_cfg_node, ControlFlowEdge::Kind::kNormal,
            blocks_to_exit, num_exception_handlers_exited));
      }
      loop_data_.pop_back();
      return zetasql_base::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTBeginEndBlock(
      const ASTBeginEndBlock* node) override {
    if (node->has_exception_handler()) {
      exception_handler_block_data_map_[node] =
          absl::make_unique<BlockWithExceptionHandlerData>();
    }
    return VisitResult::VisitChildren(node, [=]() -> zetasql_base::Status {
      ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data, CreateNodeData(node));

      // Create a node for entering the block.
      ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * begin_entry_node,
                       AddGraphNode(node, ThrowSemantics::kNoThrow));
      node_data->start = begin_entry_node;

      // Handle the regular statement list
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NodeData> stmt_list_data,
                       TakeNodeData(node->statement_list_node()));
      if (stmt_list_data->empty()) {
        node_data->AddOpenEndEdge(begin_entry_node,
                                  ControlFlowEdge::Kind::kNormal);
      } else {
        ZETASQL_RETURN_IF_ERROR(LinkNodes(begin_entry_node, stmt_list_data->start,
                                  ControlFlowEdge::Kind::kNormal));
      }

      node_data->TakeEndEdgesFrom(stmt_list_data.get());

      // Add the current block to the "exit list" of all end edges of the
      // primary statement list.
      for (IncompleteEdge& edge : node_data->end_edges) {
        edge.blocks_exited.push_back(node);
      }

      if (node->has_exception_handler()) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NodeData> handler_stmt_list_data,
                         TakeNodeData(node->handler_list()
                                          ->exception_handler_list()
                                          .front()
                                          ->statement_list()));

        // As end edges from the exception handler terminate the block,
        // propagate them to the block.  Don't mark the current block as
        // "exited" because the variable scope has already exited when the
        // primary statement list terminated, earlier.  We do, however, need
        // to increment the exited-exception-handler count, since the handler
        // is being exited.
        for (const auto& edge : handler_stmt_list_data->end_edges) {
          node_data->AddOpenEndEdge(edge.predecessor, edge.kind,
                                    edge.blocks_exited,
                                    edge.num_exception_handlers_exited + 1);
        }

        // Add edges from all statements in the block (which can throw) to the
        // start of the handler.
        BlockWithExceptionHandlerData* block_data =
            exception_handler_block_data_map_.at(node).get();
        for (ControlFlowNode* cfg_node : block_data->handled_nodes) {
          // Build up a list of blocks to be exited when <cfg_node> throws an
          // exception, while transitioning to the exception handler.
          std::vector<const ASTBeginEndBlock*> blocks_exited;
          int num_exception_handlers_exited;

          // Stop nodes are exclusive, so use node->parent() to ensure that the
          // current block, itself, is considered "exited".
          GatherBlocksAndExceptionHandlersExited(
              cfg_node->ast_node(), node->parent(), &blocks_exited,
              &num_exception_handlers_exited);

          // Add the edge from <cfg_node> to the exception handler.
          if (!handler_stmt_list_data->empty()) {
            ZETASQL_RETURN_IF_ERROR(LinkNodes(cfg_node, handler_stmt_list_data->start,
                                      ControlFlowEdge::Kind::kException,
                                      blocks_exited,
                                      num_exception_handlers_exited));
          } else {
            node_data->AddOpenEndEdge(
                cfg_node, ControlFlowEdge::Kind::kException, blocks_exited,
                num_exception_handlers_exited);
          }
        }
        exception_handler_block_data_map_.erase(node);
      }
      return zetasql_base::OkStatus();
    });
  }

 private:
  // Looks up, returns, and removes from the map, the NodeData object associated
  // with <node> from the map. TakeNodeData() may be called only once per AST
  // node.
  zetasql_base::StatusOr<std::unique_ptr<NodeData>> TakeNodeData(const ASTNode* node) {
    auto it = node_data_.find(node);
    if (it == node_data_.end()) {
      ZETASQL_RET_CHECK_FAIL() << "Unable to locate node data for "
                       << DebugNodeIdentifier(node);
    }
    std::unique_ptr<NodeData> result = std::move(it->second);
    node_data_.erase(it);
    return result;
  }

  // Creates an empty NodeData object for the given AST node and inserts it into
  // the NodeData map.
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
    return zetasql::DebugNodeIdentifier(node, graph_->script_text());
  }

  zetasql_base::StatusOr<ControlFlowNode*> AddGraphNode(
      const ASTNode* ast_node,
      ThrowSemantics throw_semantics = ThrowSemantics::kCanThrow) {
    auto emplace_result = graph_->node_map_.emplace(
        ast_node, absl::WrapUnique(new ControlFlowNode(ast_node, graph_)));
    if (!emplace_result.second) {
      return zetasql_base::InternalErrorBuilder()
             << "Graph node already exists for AST node "
             << DebugNodeIdentifier(ast_node);
    }
    ControlFlowNode* cfg_node = emplace_result.first->second.get();
    if (throw_semantics != ThrowSemantics::kNoThrow &&
        !exception_handler_data_stack_.empty()) {
      exception_handler_data_stack_.back()->handled_nodes.push_back(cfg_node);
    }
    return cfg_node;
  }

  zetasql_base::StatusOr<NodeData*> AddNodeDataAndGraphNode(
      const ASTStatement* ast_stmt,
      ThrowSemantics throw_semantics = ThrowSemantics::kCanThrow) {
    ZETASQL_ASSIGN_OR_RETURN(ControlFlowNode * cfg_node,
                     AddGraphNode(ast_stmt, throw_semantics));
    ZETASQL_ASSIGN_OR_RETURN(NodeData * node_data, CreateNodeData(ast_stmt));
    node_data->start = cfg_node;
    if (throw_semantics != ThrowSemantics::kMustThrow) {
      node_data->AddOpenEndEdge(cfg_node, ControlFlowEdge::Kind::kNormal);
    }
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
    CHECK(succ != nullptr);
    for (const auto& edge : pred->end_edges) {
      ZETASQL_RETURN_IF_ERROR(LinkNodes(edge.predecessor, succ, edge.kind,
                                edge.blocks_exited,
                                edge.num_exception_handlers_exited));
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

  zetasql_base::Status LinkNodes(ControlFlowNode* cfg_pred, ControlFlowNode* cfg_succ,
                         ControlFlowEdge::Kind kind) {
    return LinkNodes(cfg_pred, cfg_succ, kind, {}, 0);
  }

  zetasql_base::Status LinkNodes(
      ControlFlowNode* cfg_pred, ControlFlowNode* cfg_succ,
      ControlFlowEdge::Kind kind,
      const std::vector<const ASTBeginEndBlock*>& exited_blocks,
      int num_exception_handlers_exited) {
    CHECK(cfg_pred != nullptr);
    CHECK(cfg_succ != nullptr);
    if (kind == ControlFlowEdge::Kind::kException) {
      // Everything can throw except for break, continue, return,
      // and empty statement list.
      ZETASQL_RET_CHECK(cfg_pred->ast_node()->node_kind() != AST_BREAK_STATEMENT &&
                cfg_pred->ast_node()->node_kind() != AST_CONTINUE_STATEMENT &&
                cfg_pred->ast_node()->node_kind() != AST_RETURN_STATEMENT &&
                cfg_pred->ast_node()->node_kind() != AST_STATEMENT_LIST)
          << "Unexpected node kind throwing exception: "
          << cfg_pred->ast_node()->SingleNodeDebugString();
    } else if (cfg_pred->ast_node()->node_kind() == AST_IF_STATEMENT ||
               cfg_pred->ast_node()->node_kind() == AST_ELSEIF_CLAUSE ||
               (cfg_pred->ast_node()->node_kind() == AST_WHILE_STATEMENT &&
                cfg_pred->ast_node()
                        ->GetAsOrDie<ASTWhileStatement>()
                        ->condition() != nullptr)) {
      ZETASQL_RET_CHECK(kind == ControlFlowEdge::Kind::kTrueCondition ||
                kind == ControlFlowEdge::Kind::kFalseCondition)
          << "conditional statement must use true/false condition"
          << cfg_pred->DebugString();
    } else if (cfg_pred->ast_node()->IsStatement() ||
               cfg_pred->ast_node()->node_kind() == AST_STATEMENT_LIST ||
               (cfg_pred->ast_node()->node_kind() == AST_WHILE_STATEMENT &&
                cfg_pred->ast_node()
                        ->GetAsOrDie<ASTWhileStatement>()
                        ->condition() == nullptr)) {
      ZETASQL_RET_CHECK(kind == ControlFlowEdge::Kind::kNormal)
          << "Unconditional statement must use normal edge"
          << cfg_pred->DebugString();
    } else {
      ZETASQL_RET_CHECK_FAIL() << "unexpected ast node: "
                       << cfg_pred->ast_node()->GetNodeKindString();
    }
    graph_->edges_.emplace_back(absl::WrapUnique(
        new ControlFlowEdge(cfg_pred, cfg_succ, kind, graph_, exited_blocks,
                            num_exception_handlers_exited)));
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

  // Maps each ASTBeginEndBlock with an exception handler to its associated
  // BlockWithExceptionHandlerData.  The map is built up as we traverse the
  // block; when a block is finished being visted, it is used to add edges from
  // each statement in the block to the start of the exception handler.
  absl::flat_hash_map<const ASTBeginEndBlock*,
                      std::unique_ptr<BlockWithExceptionHandlerData>>
      exception_handler_block_data_map_;

  // Stack keeping track of the statements that might throw in each block with
  // an exception handler.  One entry per BEGIN block with an exception handler,
  // with the innermost block at the back of the list.
  // Each entry is owned by <exception_handler_block_data_map_>.
  std::vector<BlockWithExceptionHandlerData*> exception_handler_data_stack_;

  // The ControlFlowGraph being built.
  ControlFlowGraph* graph_;
};

zetasql_base::StatusOr<std::unique_ptr<const ControlFlowGraph>>
ControlFlowGraph::Create(const ASTScript* ast_script,
                         absl::string_view script_text) {
  auto graph = absl::WrapUnique(new ControlFlowGraph(ast_script, script_text));
  ControlFlowGraphBuilder builder(graph.get());
  ZETASQL_RETURN_IF_ERROR(ast_script->TraverseNonRecursive(&builder));
  return std::move(graph);
}

ControlFlowGraph::ControlFlowGraph(const ASTScript* ast_script,
                                   absl::string_view script_text)
    : start_node_(nullptr),
      ast_script_(ast_script),
      script_text_(script_text) {}

namespace {
void AddDestroyedVariables(const ASTStatementList* stmt_list,
                           const ASTVariableDeclaration* current_stmt,
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

    if (stmt == current_stmt) {
      // Ignore variable declarations in the current block, after the current
      // statement, since they haven't executed yet.  Note that we can't skip
      // variables in the current statement itself, since it is possible for
      // a DECLARE statement, itself, to fail after some variables have been
      // added, but not others.
      break;
    }
  }
}
}  // namespace

std::set<std::string> ControlFlowEdge::GetDestroyedVariables() const {
  std::set<std::string> destroyed_variables;
  const ASTVariableDeclaration* current_decl_stmt =
      predecessor_->ast_node()->GetAsOrNull<ASTVariableDeclaration>();
  for (const ASTBeginEndBlock* block : blocks_to_exit_) {
    AddDestroyedVariables(block->statement_list_node(), current_decl_stmt,
                          &destroyed_variables);
  }

  // If exiting the entire script, top-level variables are destroyed as well
  if (successor_ == graph_->end_node()) {
    AddDestroyedVariables(graph_->ast_script()->statement_list_node(),
                          current_decl_stmt, &destroyed_variables);
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

void AddNumExceptionHandlersExitedToDebugString(const ControlFlowEdge& edge,
                                                std::string* debug_string) {
  if (edge.num_exception_handlers_exited() != 0) {
    absl::StrAppend(debug_string, " [exiting ",
                    edge.num_exception_handlers_exited(),
                    " exception handler(s)]");
  }
}

}  // namespace

std::string ControlFlowEdge::DebugString() const {
  std::string debug_string;
  absl::StrAppend(&debug_string, predecessor_->DebugString(), " => (",
                  ControlFlowEdgeKindString(kind_), ") ",
                  successor_->DebugString());
  AddDestroyedVariablesToDebugString(*this, &debug_string);
  AddNumExceptionHandlersExitedToDebugString(*this, &debug_string);

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
                          DebugLocationText(ast_node_, graph_->script_text()));
    }
    return DebugNodeIdentifier(ast_node_, graph_->script_text());
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
      AddNumExceptionHandlersExitedToDebugString(*it->second, &lines.back());
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
  CHECK(start_node_ != nullptr);
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

const ControlFlowNode* ControlFlowGraph::GetControlFlowNode(
    const ASTNode* ast_node) const {
  auto it = node_map_.find(ast_node);
  if (it != node_map_.end()) {
    return it->second.get();
  }
  return nullptr;
}

}  // namespace zetasql
