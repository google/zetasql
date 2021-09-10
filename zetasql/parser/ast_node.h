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

#ifndef ZETASQL_PARSER_AST_NODE_H_
#define ZETASQL_PARSER_AST_NODE_H_

#include <stddef.h>

#include "zetasql/base/arena_allocator.h"
#include "zetasql/parser/ast_enums.pb.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/base/status.h"

// This header file has the definition of ASTNode, the superclass of all
// AST classes. It should not be included directly. Include parse_tree.h.
//
// During the AST construction process, AddChild / AddChildren() add to the
// children_ vector.  In InitFields(), we store pointers to children into named
// member fields with more specific types.  This allows users to navigate to
// specific child objects directly.
//
// The AST can be used in two forms -
//   * using standard accessors like child() and parent() and
//     the visitor interface,
//   * using specialized getters that get specific children by name.

namespace zetasql {

class ParseTreeVisitor;
class NonRecursiveParseTreeVisitor;
class VisitResult;

namespace parser {

class BisonParser;

}  // namespace parser

std::ostream& operator<<(std::ostream& out, SchemaObjectKind kind);

// Converts a SchemaObjectKind to the SQL name of that kind.
absl::string_view SchemaObjectKindToName(SchemaObjectKind schema_object_kind);

// Base class for all AST nodes.
class ASTNode : public zetasql_base::ArenaOnlyGladiator {
 public:
  explicit ASTNode(ASTNodeKind node_kind) : node_kind_(node_kind) {}
  ASTNode(const ASTNode&) = delete;
  ASTNode& operator=(const ASTNode&) = delete;

  virtual ~ASTNode();

  // Returns this node's kind. DEPRECATED.
  ASTNodeKind getId() const { return node_kind_; }

  ASTNodeKind node_kind() const { return node_kind_; }

  // Returns a one-line description of this node, including modifiers but
  // without child nodes. Use DebugString() to get a multiline description that
  // includes child nodes.
  virtual std::string SingleNodeDebugString() const;

  void set_parent(ASTNode* parent) { parent_ = parent; }
  ASTNode* parent() const { return parent_; }

  // Adds all nodes in 'children' to the child list. Elements in 'children' are
  // allowed to be NULL, in which case they are ignored.
  void AddChildren(absl::Span<ASTNode* const> children);

  // Adds 'child' to the list of children. 'child' must be non-NULL.
  void AddChild(ASTNode* child);

  // Adds 'child' to front of the list of children. 'child' must be non-NULL.
  void AddChildFront(ASTNode* child);

  // This must be called after adding all children, to initialize the fields
  // based on the added children. This should be overridden in each subclass to
  // initialize the fields by using FieldLoader.
  virtual void InitFields() = 0;

  // Access to child nodes with generic types.
  int num_children() const {
    return children_.size();
  }
  const ASTNode* child(int i) const { return children_[i]; }
  ASTNode* mutable_child(int i) { return children_[i]; }

  // Returns the index of the first child of a node kind or -1 if not found.
  int find_child_index(ASTNodeKind kind) const {
    for (int i = 0; i < children_.size(); i++) {
      if (children_[i]->node_kind_ == kind) {
        return i;
      }
    }
    return -1;
  }

  // Returns whether or not this node is a specific node type.
  template <typename NodeType>
  bool Is() const {
    return node_kind_ == NodeType::kConcreteNodeKind;
  }

  // Return this node cast as a NodeType.
  // Use only when this node is known to be that type, otherwise, behavior
  // is undefined.
  template <typename NodeType>
  ABSL_DEPRECATED("Use GetAsOrDie or GetAsOrNull")
  const NodeType* GetAs() const {
    return static_cast<const NodeType*>(this);
  }

  // Return this node cast as a NodeType, or null if this is not possible.
  template <typename NodeType>
  const NodeType* GetAsOrNull() const;

  // Return this node cast as a NodeType, or null if this is not possible.
  template <typename NodeType>
  NodeType* GetAsOrNull();

  // Return this node cast as a NodeType.
  // Use only when this node is known to be that type, otherwise it will crash.
  template <typename NodeType>
  const NodeType* GetAsOrDie() const {
    const NodeType* as_node_type = GetAsOrNull<NodeType>();
    ZETASQL_CHECK(as_node_type != nullptr) << "Could not cast " << GetNodeKindString()
                                   << " to the specified NodeType";
    return as_node_type;
  }

  // Return this node cast as a NodeType.
  // Use only when this node is known to be that type, otherwise it will crash.
  template <typename NodeType>
  NodeType* GetAsOrDie() {
    NodeType* as_node_type = GetAsOrNull<NodeType>();
    ZETASQL_CHECK(as_node_type != nullptr) << "Could not cast " << GetNodeKindString()
                                   << " to the specified NodeType";
    return as_node_type;
  }

  // Get all descendants of this node (inclusive) that have a type in
  // <node_kinds>.  Returns the matching nodes in <*found_nodes>.
  // Order of the output vector is not defined.
  //
  // When a node is found, the traversal does not continue below that node to
  // its children, so no returned node will ever be an ancestor of another.
  //
  // BE VERY CAREFUL using this to extract specific node types.  Think carefully
  // about whether you want the traversal to continue into nodes like
  // expression subqueries.  If not, including those kinds in <node_kinds> will
  // prevent traversing underneath those nodes.
  void GetDescendantSubtreesWithKinds(const std::set<int>& node_kinds,
                              std::vector<const ASTNode*>* found_nodes) const {
    GetDescendantsWithKindsImpl(node_kinds, found_nodes,
                                false /* continue_traversal */);
  }

  // Similar to above. It continues traversal below the found node.
  void GetDescendantsWithKinds(const std::set<int>& node_kinds,
                               std::vector<const ASTNode*>* found_nodes) const {
    GetDescendantsWithKindsImpl(node_kinds, found_nodes,
                                true /* continue_traversal */);
  }

  // Traverses the tree depth-first, using a non-recursive visitor.  Each
  // visit() method, instead of visiting the children directly, returns a
  // VisitResult object describing which children to visit and which action to
  // perform after the children are visited.
  //
  // TraverseNonRecursive() may be used as an alternative to traditional
  // visitors when stack overflow caused by a deep parse tree is a concern.
  //
  // Returns OK if all visit() methods return an OK status.  If a visit method
  // returns an error status, the traversal is aborted immediately, and the
  // failed status from the visit() method is returned here.
  absl::Status TraverseNonRecursive(
      NonRecursiveParseTreeVisitor* visitor) const;

  // Accept the visitor.
  virtual void Accept(ParseTreeVisitor* visitor, void* data) const = 0;

  // Visit children in order.
  void ChildrenAccept(ParseTreeVisitor* visitor, void* data) const;

  // Returns a multiline tree dump. Parse locations are represented as integer
  // ranges.
  std::string DebugString(int max_depth = 512) const;

  // Returns a multiline tree dump similar to debug string, but represents parse
  // locations as fragments from the original text, supplied in <sql>, rather
  // than raw integer values.
  std::string DebugString(absl::string_view sql, int max_depth = 512) const;

  // Moves the start location forward by 'bytes' byte positions.
  void MoveStartLocation(int bytes) {
    parse_location_range_.set_start(ParseLocationPoint::FromByteOffset(
        parse_location_range_.start().filename(),
        parse_location_range_.start().GetByteOffset() + bytes));
  }

  // Moves the start location back by 'bytes' byte positions.
  void MoveStartLocationBack(int bytes) {
    parse_location_range_.set_start(ParseLocationPoint::FromByteOffset(
        parse_location_range_.start().filename(),
        parse_location_range_.start().GetByteOffset() - bytes));
  }

    // Sets the start location to the end location.
  void SetStartLocationToEndLocation() {
    parse_location_range_.set_start(parse_location_range_.end());
  }

  // Moves the end location back by 'bytes' byte positions.
  void MoveEndLocationBack(int bytes) {
    parse_location_range_.set_end(ParseLocationPoint::FromByteOffset(
        parse_location_range_.end().filename(),
        parse_location_range_.end().GetByteOffset() - bytes));
  }

  void set_start_location(const ParseLocationPoint& point) {
    parse_location_range_.set_start(point);
  }
  void set_end_location(const ParseLocationPoint& point) {
    parse_location_range_.set_end(point);
  }

  virtual bool IsTableExpression() const { return false; }
  virtual bool IsQueryExpression() const { return false; }
  virtual bool IsExpression() const { return false; }
  virtual bool IsType() const { return false; }
  virtual bool IsLeaf() const { return false; }
  virtual bool IsStatement() const { return false; }
  virtual bool IsScriptStatement() const { return false; }
  virtual bool IsLoopStatement() const { return false; }
  virtual bool IsSqlStatement() const { return false; }
  virtual bool IsDdlStatement() const { return false; }
  virtual bool IsCreateStatement() const { return false; }
  virtual bool IsAlterStatement() const { return false; }

  std::string GetNodeKindString() const;

  const ParseLocationRange& GetParseLocationRange() const {
    return parse_location_range_;
  }

  // If both the start and end positions have the same filename (this is
  // normally expected), then gets the position span of this node in the form:
  //   [filename:]byte_offset-byte_offset
  // Otherwise returns the position span in the form:
  //   filename:byte_offset-filename:byte_offset
  std::string GetLocationString() const;

  static std::string NodeKindToString(ASTNodeKind node_kind);

 protected:
  // Dispatches to non-recursive visitor implementation.
  // Used by TraverseNonRecursive().
  ABSL_MUST_USE_RESULT virtual absl::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const = 0;

  // Similar to GetDescendantsWithKinds. If 'continue_traversal' is true,
  // continues traversal below the found node.
  void GetDescendantsWithKindsImpl(const std::set<int>& node_kinds,
                                   std::vector<const ASTNode*>* found_nodes,
                                   bool continue_traversal) const;

  // FieldLoader is used to implement InitFields in a subclass. The usage
  // is to create a FieldLoader and then call the AddXYZ function to extract
  // pointers from children_ into other member fields.
  //
  // The calls to FieldLoader.AddXYZ are made in the order they are expected in
  // the parser.
  // Example:
  //   void InitFields() final {
  //     FieldLoader fl(this);
  //     fl.AddRequired(&field1_);
  //     fl.AddRequired(&field2_);
  //     fl.AddOptional(&field3_, AST_PATH_EXPRESSION);
  //     fl.AddRequired(&field4_);
  //     fl.AddOptionalExpression(&field5_);
  //     fl.AddRestAsRepeated(&field6_);
  //   }
  //
  // - AddRequired crashes if there is no remaining child to be assigned.
  // - AddOptional skips assignment if there is no remaining child, or if it
  //   is not of the correct type.
  // - AddOptionalExpression and AddOptionalType act as AddOptional, but
  //   call IsExpression() and IsType(), respectively, instead of checking a
  //   type value.
  // - AddRestAsRepeated appends all remaining children to the value, which
  //   should be a vector or InlinedVector.
  // - ~FieldLoader crashes if not all children were assigned.
  class FieldLoader {
   public:
    explicit FieldLoader(ASTNode* node)
        : node_(node), index_(0), end_(node_->num_children()) {
      if (ZETASQL_DEBUG_MODE) {
        for (int i = 0; i < end_; ++i) {
          ZETASQL_DCHECK(node_->child(i) != nullptr);
        }
      }
    }

    FieldLoader(const FieldLoader&) = delete;
    FieldLoader& operator=(const FieldLoader&) = delete;

    ~FieldLoader() {
      ZETASQL_CHECK_EQ(index_, end_)
          << "Did not consume last " << (end_ - index_) << " children. "
          << "Next child is a "
          << node_->child(index_)->GetNodeKindString();
    }

    // Gets the next child element into *v. Crashes if not available.
    template <typename T>
    void AddRequired(const T** v) {
      ZETASQL_CHECK_LT(index_, end_);
      *v = static_cast<const T*>(node_->child(index_++));
    }

    // Gets the next child element into *v, if it's node_kind is
    // <expected_node_kind>.
    template <typename T>
    void AddOptional(const T** v, int expected_node_kind) {
      if (index_ < end_ &&
          node_->child(index_)->node_kind() == expected_node_kind) {
        *v = static_cast<const T*>(node_->child(index_++));
      }
    }

    // Appends all remaining child elements to <v>.
    template <typename T>
    void AddRestAsRepeated(absl::Span<const T* const>* v) {
      static_assert(std::is_base_of<ASTNode, T>::value,
                    "Must be a subclass of ASTNode");
      if (end_ != index_) {
        *v = absl::MakeSpan(
            reinterpret_cast<T**>(&node_->children_[index_]),
            end_ - index_);
        index_ = end_;
      }  // else, it remains an empty Span.
    }

    // Gets the next child element into *v, if IsExpression() is true for it.
    template <typename T>
    void AddOptionalExpression(const T** v) {
      if (index_ < end_ && node_->child(index_)->IsExpression()) {
        *v = static_cast<const T*>(node_->child(index_++));
      }
    }

    // Gets the next child element into *v, if IsType() is true for it.
    template <typename T>
    void AddOptionalType(const T** v) {
      if (index_ < end_ && node_->child(index_)->IsType()) {
        *v = static_cast<const T*>(node_->child(index_++));
      }
    }

    // Appends remaining child elements to <v>, stopping when the next
    // child is !IsExpression().
    template <typename T>
    void AddRepeatedWhileIsExpression(absl::Span<const T* const>* v) {
      static_assert(std::is_base_of<ASTNode, T>::value,
                    "Must be a subclass of ASTNode");
      int start = index_;
      while (index_ < end_ && node_->child(index_)->IsExpression()) {
        index_++;
      }
      if (start != index_) {
        *v = absl::MakeSpan(
            reinterpret_cast<T**>(&node_->children_[start]),
            index_ - start);
      }  // else, it remains an empty Span.
    }

    // Appends remaining child elements to <v>, stopping when the node kind of
    // the next child is not 'node_kind'.
    template <typename T>
    void AddRepeatedWhileIsNodeKind(absl::Span<const T* const>* v,
                                    int node_kind) {
      static_assert(std::is_base_of<ASTNode, T>::value,
                    "Must be a subclass of ASTNode");
      int start = index_;
      while (index_ < end_ && node_->child(index_)->node_kind() == node_kind) {
        index_++;
      }
      if (start != index_) {
        *v = absl::MakeSpan(
            reinterpret_cast<T**>(&node_->children_[start]),
            index_ - start);
      }  // else, it remains an empty Span.
    }

   private:
    ASTNode* const node_;
    int index_;
    int end_;
  };

 private:
  friend class ::zetasql::parser::BisonParser;

  // Helper class for DebugString().
  class Dumper {
   public:
    Dumper(const ASTNode* node, absl::string_view separator, int max_depth,
           absl::optional<absl::string_view> sql, std::string* out)
        : node_(node),
          separator_(separator),
          max_depth_(max_depth),
          current_depth_(0),
          sql_(sql),
          out_(out) {}
    Dumper(const Dumper&) = delete;
    Dumper& operator=(const Dumper&) = delete;
    void Dump();

   private:
    bool DumpNode();
    const ASTNode* node_;
    const absl::string_view separator_;
    const int max_depth_;
    int current_depth_;
    absl::optional<absl::string_view> sql_;
    std::string* out_;
  };

  // Helper function for TraverseNonRecursive, invoked immediately after each
  // call to the visitor.
  //   - <result> specifies the value returned by the visitor.
  //   - <stack> specifies a list of pending visit operations that are needed to
  //         complete the traversal; they will be executed in reverse order.
  //         TraverseNonRecursiveHelper() adds or removes items to the stack as
  //         necessary to implement the result of the visitor.
  static absl::Status TraverseNonRecursiveHelper(
      const VisitResult& result, NonRecursiveParseTreeVisitor* visitor,
      std::vector<std::function<absl::Status()>>* stack);

  ASTNodeKind node_kind_;

  ASTNode* parent_ = nullptr;

  ParseLocationRange parse_location_range_;

  // Many nodes have one to two children, so InlinedVector saves allocations.
  absl::InlinedVector<ASTNode*, 4> children_;
};

}  // namespace zetasql

#endif  // ZETASQL_PARSER_AST_NODE_H_
