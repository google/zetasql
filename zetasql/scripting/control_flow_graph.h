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

#ifndef ZETASQL_SCRIPTING_CONTROL_FLOW_GRAPH_H_
#define ZETASQL_SCRIPTING_CONTROL_FLOW_GRAPH_H_

#include <memory>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/scripting/parsed_script.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

class ControlFlowNode;
class ControlFlowGraph;

// Represents a potential transition from one node to another.
class ControlFlowEdge {
 public:
  enum class Kind {
    // The previous statement completed normally, without an exception.
    kNormal,

    // An IF, ELSEIF, or WHILE condition was just evaluated, with TRUE result.
    kTrueCondition,

    // An IF, ELSEIF, or WHILE condition was just evaluated, with FALSE result.
    kFalseCondition,

    // The previous statement (or expression) threw an exception.
    kException,
  };

  const ControlFlowGraph* graph() const { return graph_; }
  const ControlFlowNode* predecessor() const { return predecessor_; }
  const ControlFlowNode* successor() const { return successor_; }
  Kind kind() const { return kind_; }

  // Returns a single-line debug string of this edge.  Does not include the
  // details of the successor/predecessor nodes.
  std::string DebugString() const;

 private:
  friend class ControlFlowGraphBuilder;
  ControlFlowEdge(const ControlFlowNode* predecessor,
                  const ControlFlowNode* successor, Kind kind,
                  ControlFlowGraph* graph)
      : predecessor_(predecessor),
        successor_(successor),
        kind_(kind),
        graph_(graph) {}

  // Node which executes prior to this transition (owned by graph_).
  const ControlFlowNode* predecessor_;

  // Node which will execute next, after this transition (owned by graph_).
  const ControlFlowNode* successor_;

  // Specifies when this transition happens
  Kind kind_;

  // The underlying ControlFlowGraph.
  ControlFlowGraph* graph_;
};

// A node in a control flow graph representing the execution of one AST node.
//
// Control-flow nodes are only generated for leaf-level statements and
// IF/ELSEIF/WHILE conditions.  Node objects do not exist for compound
// statements or expressions (other than IF/ELSEIF/WHILE conditions).
//
// Each script ends with a sentinel node with a NULL ast node, which is reached
// whenever a script terminates without an exception.  Edges will transition
// into this node from the last statement in the script, and from each RETURN
// statement.
//
// If the script contains a LOOP statement with an empty body, a node will be
// with an edge to itself, representing the empty loop body.  This node will
// have a NULL AST-node, since there is no AST node in the loop body to
// associate with it.
class ControlFlowNode {
 public:
  using EdgeMap =
      absl::flat_hash_map<ControlFlowEdge::Kind, const ControlFlowEdge*>;

  const ASTNode* ast_node() const { return ast_node_; }
  const ControlFlowGraph* graph() const { return graph_; }
  const EdgeMap& successors() const { return successors_; }
  const std::vector<const ControlFlowEdge*> predecessors() const {
    return predecessors_;
  }

  // Returns a simple string identifiying the node in the context of the script.
  // It does not contain a dump of the successors or predecessors.
  std::string DebugString() const;

  // Returns a string describing the node's successors.
  // The return value is a multiline string, with each line indented with
  // <indent>.
  std::string SuccessorsDebugString(absl::string_view indent = "") const;

 private:
  friend class ControlFlowGraphBuilder;
  ControlFlowNode(const ASTNode* ast_node, ControlFlowGraph* graph)
      : ast_node_(ast_node), graph_(graph) {}

  // The AST node to execute. NULL for the sentinel node indicating the end of
  // the script.
  const ASTNode* ast_node_;

  // The control flow graph containing this node.  The lifetimes of all AST
  // nodes, control-flow nodes, and control-flow edges are owned here.
  ControlFlowGraph* graph_;

  // List of edges that can potentially execute after execution of <ast_node_>
  // has fully completed.
  EdgeMap successors_;

  // List of edges can can potentially execute immediately prior to this node.
  std::vector<const ControlFlowEdge*> predecessors_;
};

class ControlFlowGraph {
 public:
  static zetasql_base::StatusOr<std::unique_ptr<const ControlFlowGraph>> Create(
      const ParsedScript* script);

  // Node to execute at the start of the script, or <end_node()> if the script
  // terminates without executing any statements.  Never null.
  const ControlFlowNode* start_node() const { return start_node_; }

  // Sentinel node to mark the end of the script.  This is reached whenever the
  // script terminates normally, and has a null ast node.
  const ControlFlowNode* end_node() const { return end_node_.get(); }

  // The control-flow node associated with the given AST node, or null if
  // <ast_node> has no control-flow node associated with it.  Control-flow nodes
  // are generated for leaf statements and IF/ELSEIF/WHILE conditions only.
  const ControlFlowNode* GetControlFlowNode(const ASTNode* ast_node) const;

  // All nodes in the script, in an arbitrary order.
  std::vector<const ControlFlowNode*> GetAllNodes() const;

  // The script used to generate this graph.
  const ParsedScript* script() const { return script_; }

  // Returns a debug string for this graph.  The returned string is multi-line
  // and describes the start node, along with all edges in the graph.
  std::string DebugString() const;

 private:
  friend class ControlFlowGraphBuilder;
  explicit ControlFlowGraph(const ParsedScript* script);

  // Node to execute at the start of the script.  This is normally set to the
  // first statement in the script.  If the script is empty, <start_node_> will
  // be set to <end_node_>.  Owned by either <node_map_> or <end_node_>.
  const ControlFlowNode* start_node_;

  // Sentinel control-flow node representing a successful termination of the
  // script. This node will have a NULL AST node, and will typically contain a
  // predecessor for the last statement, plus any return statements, and no
  // successor.  (If the script is guarnateed to either loop forever or throw
  // an unhandled exception, <end_node_> will be unreachable).
  std::unique_ptr<ControlFlowNode> end_node_;

  // Maps each AST node to its corresponding ControlFlowNode, and owns the
  // lifetime of all ControlFlowNode's.
  absl::flat_hash_map<const ASTNode*, std::unique_ptr<ControlFlowNode>>
      node_map_;

  // Owns the lifetime of all ControlFlowEdge objects.  (The order of elements
  // in this vector is arbitrary).
  std::vector<std::unique_ptr<ControlFlowEdge>> edges_;

  // Owns the script text and all AST nodes.
  const ParsedScript* script_;
};

}  // namespace zetasql

#endif  // ZETASQL_SCRIPTING_CONTROL_FLOW_GRAPH_H_
