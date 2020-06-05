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

#ifndef ZETASQL_PARSER_PARSE_TREE_H_
#define ZETASQL_PARSER_PARSE_TREE_H_

#include <stddef.h>

#include <ostream>
#include <set>
#include <stack>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/arena_allocator.h"
#include "zetasql/base/logging.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree_decls.h"  
#include "zetasql/parser/visit_result.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/type.pb.h"
#include "absl/base/attributes.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

// This header file has definitions for the AST classes.
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

// Defines schema object kinds used by a parser rule to indicate the parsed
// object kind for ALTER and DROP statements.  These enum values are also
// used in the generic ASTDropStatement to identify what type of object is
// being dropped.
//
// Note that this list excludes ROW POLICY for practical reasons - it is not
// needed in the parser rules and it is not needed for the ASTDropStatement
// (DROP ROW POLICY has its own ASTDropRowPolicyStatement).
enum class SchemaObjectKind {
  kInvalidSchemaObjectKind,
  kAggregateFunction,
  kConstant,
  kDatabase,
  kExternalTable,
  kFunction,
  kIndex,
  kMaterializedView,
  kModel,
  kProcedure,
  kTable,
  kTableFunction,
  kView,
  __SchemaObjectKind__switch_must_have_a_default__ = -1,
};

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
    CHECK(as_node_type != nullptr) << "Could not cast " << GetNodeKindString()
                                   << " to the specified NodeType";
    return as_node_type;
  }

  // Return this node cast as a NodeType.
  // Use only when this node is known to be that type, otherwise it will crash.
  template <typename NodeType>
  NodeType* GetAsOrDie() {
    NodeType* as_node_type = GetAsOrNull<NodeType>();
    CHECK(as_node_type != nullptr) << "Could not cast " << GetNodeKindString()
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

  std::string DebugString(int max_depth = 512) const;

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
  ABSL_MUST_USE_RESULT virtual zetasql_base::StatusOr<VisitResult> Accept(
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
          DCHECK(node_->child(i) != nullptr);
        }
      }
    }

    FieldLoader(const FieldLoader&) = delete;
    FieldLoader& operator=(const FieldLoader&) = delete;

    ~FieldLoader() {
      CHECK_EQ(index_, end_)
          << "Did not consume last " << (end_ - index_) << " children. "
          << "Next child is a "
          << node_->child(index_)->GetNodeKindString();
    }

    // Gets the next child element into *v. Crashes if not available.
    template <typename T>
    void AddRequired(const T** v) {
      CHECK_LT(index_, end_);
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
           std::string* out)
        : node_(node),
          separator_(separator),
          max_depth_(max_depth),
          current_depth_(0),
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

// This is a fake ASTNode implementation that exists only for tests,
// which may need to pass an ASTNode* to some methods.
class FakeASTNode final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FAKE;

  FakeASTNode() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override {
    LOG(FATAL) << "FakeASTNode does not support Accept";
  }
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override {
    LOG(FATAL) << "FakeASTNode does not support Accept";
  }

  void InitFields() final {
    {
      FieldLoader fl(this);  // Triggers check that there were no children.
    }
    set_start_location(ParseLocationPoint::FromByteOffset("fake_filename", 7));
    set_end_location(ParseLocationPoint::FromByteOffset("fake_filename", 10));
  }
};

// Superclass of all statements.
class ASTStatement : public ASTNode {
 public:
  explicit ASTStatement(ASTNodeKind kind) : ASTNode(kind) {}

  bool IsStatement() const final { return true; }
  bool IsSqlStatement() const override { return true; }
};

class ASTScriptStatement : public ASTStatement {
 public:
  explicit ASTScriptStatement(ASTNodeKind kind) : ASTStatement(kind) {}

  bool IsScriptStatement() const final { return true; }
  bool IsSqlStatement() const override { return false; }
};

// This wraps any other statement to add statement-level hints.
class ASTHintedStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HINTED_STATEMENT;

  ASTHintedStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTHint* hint() const { return hint_; }
  const ASTStatement* statement() const { return statement_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&hint_);
    fl.AddRequired(&statement_);
  }

  const ASTHint* hint_ = nullptr;
  const ASTStatement* statement_ = nullptr;
};

// Represents an EXPLAIN statement.
class ASTExplainStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXPLAIN_STATEMENT;

  ASTExplainStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTStatement* statement() const { return statement_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&statement_);
  }

  const ASTStatement* statement_ = nullptr;
};

// Represents a DESCRIBE statement.
class ASTDescribeStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DESCRIBE_STATEMENT;

  ASTDescribeStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTIdentifier* optional_identifier() const {
    return optional_identifier_;
  }
  const ASTPathExpression* optional_from_name() const {
    return optional_from_name_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&optional_identifier_, AST_IDENTIFIER);
    fl.AddRequired(&name_);
    fl.AddOptional(&optional_from_name_, AST_PATH_EXPRESSION);
  }
  const ASTPathExpression* name_ = nullptr;
  const ASTIdentifier* optional_identifier_ = nullptr;
  const ASTPathExpression* optional_from_name_ = nullptr;
};

class ASTDescriptorColumn final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DESCRIPTOR_COLUMN;

  ASTDescriptorColumn() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  ABSL_MUST_USE_RESULT zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields
  const ASTIdentifier* name() const { return name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
  }
  const ASTIdentifier* name_ = nullptr;
};

class ASTDescriptorColumnList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DESCRIPTOR_COLUMN_LIST;

  ASTDescriptorColumnList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  ABSL_MUST_USE_RESULT zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Guaranteed by the parser to never be empty.
  absl::Span<const ASTDescriptorColumn* const> descriptor_column_list() const {
    return descriptor_column_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&descriptor_column_list_);
  }
  absl::Span<const ASTDescriptorColumn* const> descriptor_column_list_;
};

// Represents a SHOW statement.
class ASTShowStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SHOW_STATEMENT;

  ASTShowStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  const ASTIdentifier* identifier() const { return identifier_; }
  const ASTPathExpression* optional_name() const { return optional_name_; }
  const ASTStringLiteral* optional_like_string() const {
    return optional_like_string_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&identifier_);
    fl.AddOptional(&optional_name_, AST_PATH_EXPRESSION);
    fl.AddOptional(&optional_like_string_, AST_STRING_LITERAL);
  }
  const ASTIdentifier* identifier_ = nullptr;
  const ASTPathExpression* optional_name_ = nullptr;
  const ASTStringLiteral* optional_like_string_ = nullptr;
};

// Base class transaction modifier elements.
class ASTTransactionMode : public ASTNode {
 public:
  explicit ASTTransactionMode(ASTNodeKind kind) : ASTNode(kind) {}
};

class ASTTransactionIsolationLevel final : public ASTTransactionMode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_TRANSACTION_ISOLATION_LEVEL;

  ASTTransactionIsolationLevel() : ASTTransactionMode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* identifier1() const { return identifier1_; }
  // Second identifier can be non-null only if first identifier is non-null.
  const ASTIdentifier* identifier2() const { return identifier2_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&identifier1_, AST_IDENTIFIER);
    fl.AddOptional(&identifier2_, AST_IDENTIFIER);
  }

  const ASTIdentifier* identifier1_ = nullptr;
  const ASTIdentifier* identifier2_ = nullptr;
};

class ASTTransactionReadWriteMode final : public ASTTransactionMode {
 public:
  enum Mode {
    INVALID,
    READ_ONLY,
    READ_WRITE,
  };
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_TRANSACTION_READ_WRITE_MODE;

  ASTTransactionReadWriteMode() : ASTTransactionMode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  Mode mode() const { return mode_; }
  void set_mode(Mode mode) { mode_ = mode; }

 private:
  void InitFields() final {}

  Mode mode_ = INVALID;
};

class ASTTransactionModeList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TRANSACTION_MODE_LIST;

  ASTTransactionModeList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  absl::Span<const ASTTransactionMode* const> elements() const {
    return elements_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&elements_);
  }

  absl::Span<const ASTTransactionMode* const> elements_;
};

// Represents a BEGIN or START TRANSACTION statement.
class ASTBeginStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BEGIN_STATEMENT;

  ASTBeginStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTTransactionModeList* mode_list() const { return mode_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&mode_list_, AST_TRANSACTION_MODE_LIST);
  }
  const ASTTransactionModeList* mode_list_ = nullptr;
};

// Represents a SET TRANSACTION statement.
class ASTSetTransactionStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_SET_TRANSACTION_STATEMENT;

  ASTSetTransactionStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTTransactionModeList* mode_list() const { return mode_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&mode_list_);
  }
  const ASTTransactionModeList* mode_list_ = nullptr;
};

// Represents a COMMIT statement.
class ASTCommitStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_COMMIT_STATEMENT;

  ASTCommitStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader(this);  // Triggers check that there were no children.
  }
};

// Represents a ROLLBACK statement.
class ASTRollbackStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ROLLBACK_STATEMENT;

  ASTRollbackStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader(this);  // Triggers check that there were no children.
  }
};

class ASTStartBatchStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_START_BATCH_STATEMENT;

  ASTStartBatchStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* batch_type() const { return batch_type_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&batch_type_, AST_IDENTIFIER);
  }

  const ASTIdentifier* batch_type_ = nullptr;
};

class ASTRunBatchStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RUN_BATCH_STATEMENT;

  ASTRunBatchStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }
};

class ASTAbortBatchStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ABORT_BATCH_STATEMENT;

  ASTAbortBatchStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }
};

// Represents a DROP statement.
class ASTDropStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DROP_STATEMENT;

  ASTDropStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  const ASTPathExpression* name() const { return name_; }

  const SchemaObjectKind schema_object_kind() const {
    return schema_object_kind_;
  }
  void set_schema_object_kind(SchemaObjectKind schema_object_kind) {
    schema_object_kind_ = schema_object_kind;
  }

  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
  }
  const ASTPathExpression* name_ = nullptr;
  SchemaObjectKind schema_object_kind_ =
      SchemaObjectKind::kInvalidSchemaObjectKind;
  bool is_if_exists_ = false;
};

// Represents a DROP FUNCTION statement.
class ASTDropFunctionStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DROP_FUNCTION_STATEMENT;

  ASTDropFunctionStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  const ASTPathExpression* name() const { return name_; }
  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  const ASTFunctionParameters* parameters() const { return parameters_; }

 private:
  void InitFields() override {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&parameters_, AST_FUNCTION_PARAMETERS);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTFunctionParameters* parameters_ = nullptr;
  bool is_if_exists_ = false;
};

// Represents a DROP ROW ACCESS POLICY statement.
class ASTDropRowAccessPolicyStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DROP_ROW_ACCESS_POLICY_STATEMENT;

  ASTDropRowAccessPolicyStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  const ASTIdentifier* name() const { return name_; }
  const ASTPathExpression* table_name() const { return table_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&table_name_);
  }
  bool is_if_exists_ = false;
  const ASTIdentifier* name_ = nullptr;
  const ASTPathExpression* table_name_ = nullptr;
};

// Represents a DROP ALL ROW ACCESS POLICIES statement.
class ASTDropAllRowAccessPoliciesStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DROP_ALL_ROW_ACCESS_POLICIES_STATEMENT;

  ASTDropAllRowAccessPoliciesStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  const ASTPathExpression* table_name() const { return table_name_; }

  bool has_access_keyword() const { return has_access_keyword_; }
  void set_has_access_keyword(bool value) { has_access_keyword_ = value; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&table_name_);
  }
  const ASTPathExpression* table_name_ = nullptr;
  bool has_access_keyword_ = false;
};

// Represents a DROP MATERIALIZED VIEW statement.
class ASTDropMaterializedViewStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DROP_MATERIALIZED_VIEW_STATEMENT;

  ASTDropMaterializedViewStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  const ASTPathExpression* name() const { return name_; }
  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
  }
  const ASTPathExpression* name_ = nullptr;
  bool is_if_exists_ = false;
};

// Represents a RENAME statement.
class ASTRenameStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RENAME_STATEMENT;

  ASTRenameStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* old_name() const { return old_name_; }
  const ASTPathExpression* new_name() const { return new_name_; }
  const ASTIdentifier* identifier() const { return identifier_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&identifier_);
    fl.AddRequired(&old_name_);
    fl.AddRequired(&new_name_);
  }
  const ASTPathExpression* old_name_ = nullptr;
  const ASTPathExpression* new_name_ = nullptr;
  const ASTIdentifier* identifier_ = nullptr;
};

// Represents an IMPORT statement, which currently support MODULE or PROTO kind.
// We want this statement to be a generic import at some point.
class ASTImportStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IMPORT_STATEMENT;

  ASTImportStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  enum ImportKind { MODULE, PROTO };
  void set_import_kind(ImportKind import_kind) { import_kind_ = import_kind; }
  const ImportKind import_kind() const { return import_kind_; }

  const ASTPathExpression* name() const { return name_; }
  const ASTStringLiteral* string_value() const { return string_value_; }
  const ASTAlias* alias() const { return alias_; }
  const ASTIntoAlias* into_alias() const { return into_alias_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&name_, AST_PATH_EXPRESSION);
    fl.AddOptional(&string_value_, AST_STRING_LITERAL);
    fl.AddOptional(&alias_, AST_ALIAS);
    fl.AddOptional(&into_alias_, AST_INTO_ALIAS);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
  }
  // Exactly one of 'name_' or 'string_value_' will be populated.
  const ASTPathExpression* name_ = nullptr;
  const ASTStringLiteral* string_value_ = nullptr;

  ImportKind import_kind_ = MODULE;

  // At most one of 'alias_' or 'into_alias_' will be populated.
  const ASTAlias* alias_ = nullptr;               // May be NULL.
  const ASTIntoAlias* into_alias_ = nullptr;      // May be NULL.

  const ASTOptionsList* options_list_ = nullptr;  // May be NULL.
};

class ASTModuleStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_MODULE_STATEMENT;

  ASTModuleStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
  }
  const ASTPathExpression* name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;  // May be NULL.
};

// Represents a single query statement.
class ASTQueryStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_QUERY_STATEMENT;

  ASTQueryStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&query_);
  }

  const ASTQuery* query_ = nullptr;      // Required, not NULL.
};

class ASTWithClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WITH_CLAUSE;

  ASTWithClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTWithClauseEntry* const>& with() const {
    return with_;
  }
  const ASTWithClauseEntry* with(int idx) const { return with_[idx]; }
  bool recursive() const { return recursive_; }
  void set_recursive(bool recursive) { recursive_ = recursive; }

  std::string SingleNodeDebugString() const override {
    return recursive_ ? "WithClause (recursive)" : "WithClause";
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&with_);
  }

  absl::Span<const ASTWithClauseEntry* const> with_;
  bool recursive_ = false;
};

class ASTWithClauseEntry final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WITH_CLAUSE_ENTRY;

  ASTWithClauseEntry() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* alias() const { return alias_; }
  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&alias_);
    fl.AddRequired(&query_);
  }

  const ASTIdentifier* alias_ = nullptr;
  const ASTQuery* query_ = nullptr;
};

class ASTWithConnectionClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WITH_CONNECTION_CLAUSE;

  ASTWithConnectionClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTConnectionClause* connection_clause() const {
    return connection_clause_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&connection_clause_);
  }

  const ASTConnectionClause* connection_clause_;
};

// Superclass for all query expressions.  These are top-level syntactic
// constructs (outside individual SELECTs) making up a query.  These include
// Query itself, Select, UnionAll, etc.
class ASTQueryExpression : public ASTNode {
 public:
  explicit ASTQueryExpression(ASTNodeKind kind) : ASTNode(kind) {}

  void set_parenthesized(bool parenthesized) { parenthesized_ = parenthesized; }

  bool IsQueryExpression() const override { return true; }
  bool parenthesized() const { return parenthesized_; }

 private:
  bool parenthesized_ = false;
};

class ASTQuery final : public ASTQueryExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_QUERY;

  ASTQuery() : ASTQueryExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_is_nested(bool is_nested) { is_nested_ = is_nested; }
  bool is_nested() const { return is_nested_; }

  // If present, the WITH clause wrapping this query.
  const ASTWithClause* with_clause() const { return with_clause_; }

  // The query_expr can be a single Select, or a more complex structure
  // composed out of nodes like SetOperation and Query.
  const ASTQueryExpression* query_expr() const { return query_expr_; }

  // If present, applies to the result of <query_expr_> as appropriate.
  const ASTOrderBy* order_by() const { return order_by_; }

  // If present, this applies after the result of <query_expr_> and <order_by_>.
  const ASTLimitOffset* limit_offset() const { return limit_offset_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&with_clause_, AST_WITH_CLAUSE);
    fl.AddRequired(&query_expr_);
    fl.AddOptional(&order_by_, AST_ORDER_BY);
    fl.AddOptional(&limit_offset_, AST_LIMIT_OFFSET);
  }

  const ASTWithClause* with_clause_ = nullptr;
  const ASTQueryExpression* query_expr_ = nullptr;
  const ASTOrderBy* order_by_ = nullptr;
  const ASTLimitOffset* limit_offset_ = nullptr;
  bool is_nested_ = false;
};

class ASTSetOperation final : public ASTQueryExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SET_OPERATION;

  ASTSetOperation() : ASTQueryExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  // Returns the SQL keywords for the underlying set operation eg. UNION ALL,
  // UNION DISTINCT, EXCEPT ALL, INTERSECT DISTINCT etc.
  std::string GetSQLForOperation() const;

  std::pair<std::string, std::string> GetSQLForOperationPair() const;

  enum OperationType {
    NOT_SET,
    UNION,      // UNION ALL/DISTINCT
    EXCEPT,     // EXCEPT ALL/DISTINCT
    INTERSECT,  // INTERSECT ALL/DISTINCT
  };

  OperationType op_type() const { return op_type_; }
  void set_op_type(OperationType op_type) { op_type_ = op_type; }

  bool distinct() const { return distinct_; }
  void set_distinct(bool distinct) { distinct_ = distinct; }

  const absl::Span<const ASTQueryExpression* const>& inputs() const {
    return inputs_;
  }

  const ASTHint* hint() const { return hint_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddRestAsRepeated(&inputs_);
  }

  OperationType op_type_ = NOT_SET;
  bool distinct_ = false;
  absl::Span<const ASTQueryExpression* const> inputs_;
  const ASTHint* hint_ = nullptr;
};

class ASTSelect final : public ASTQueryExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SELECT;

  ASTSelect() : ASTQueryExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  const ASTHint* hint() const { return hint_; }
  bool distinct() const { return distinct_; }
  const ASTSelectAs* select_as() const { return select_as_; }
  const ASTSelectList* select_list() const { return select_list_; }
  const ASTFromClause* from_clause() const { return from_clause_; }
  const ASTWhereClause* where_clause() const { return where_clause_; }
  const ASTGroupBy* group_by() const { return group_by_; }
  const ASTHaving* having() const { return having_; }
  const ASTWindowClause* window_clause() const { return window_clause_; }

  void set_distinct(bool distinct) { distinct_ = distinct; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddOptional(&select_as_, AST_SELECT_AS);
    fl.AddRequired(&select_list_);
    fl.AddOptional(&from_clause_, AST_FROM_CLAUSE);
    fl.AddOptional(&where_clause_, AST_WHERE_CLAUSE);
    fl.AddOptional(&group_by_, AST_GROUP_BY);
    fl.AddOptional(&having_, AST_HAVING);
    fl.AddOptional(&window_clause_, AST_WINDOW_CLAUSE);
  }

  const ASTHint* hint_ = nullptr;
  bool distinct_ = false;
  const ASTSelectAs* select_as_ = nullptr;
  const ASTSelectList* select_list_ = nullptr;
  const ASTFromClause* from_clause_ = nullptr;
  const ASTWhereClause* where_clause_ = nullptr;
  const ASTGroupBy* group_by_ = nullptr;
  const ASTHaving* having_ = nullptr;
  const ASTWindowClause* window_clause_ = nullptr;
};

// This represents a SELECT with an AS clause giving it an output type.
//   SELECT AS STRUCT ...
//   SELECT AS VALUE ...
//   SELECT AS <type_name> ...
// Exactly one of these is present.
class ASTSelectAs final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SELECT_AS;

  ASTSelectAs() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  // AS <mode> kind.
  enum AsMode {
    NOT_SET,
    STRUCT,    // AS STRUCT
    VALUE,     // AS VALUE
    TYPE_NAME  // AS <type name>
  };

  // Set if as_mode() == kTypeName;
  const ASTPathExpression* type_name() const { return type_name_; }

  AsMode as_mode() const { return as_mode_; }
  void set_as_mode(AsMode as_mode) { as_mode_ = as_mode; }

  bool is_select_as_struct() const { return as_mode_ == STRUCT; }
  bool is_select_as_value() const { return as_mode_ == VALUE; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&type_name_, AST_PATH_EXPRESSION);
  }

  const ASTPathExpression* type_name_ = nullptr;
  AsMode as_mode_ = NOT_SET;
};

class ASTSelectList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SELECT_LIST;

  ASTSelectList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTSelectColumn* const>& columns() const {
    return columns_;
  }
  const ASTSelectColumn* columns(int i) const { return columns_[i]; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&columns_);
  }

  absl::Span<const ASTSelectColumn* const> columns_;
};

class ASTSelectColumn final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SELECT_COLUMN;

  ASTSelectColumn() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }
  const ASTAlias* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTAlias* alias_ = nullptr;
};

class ASTAlias final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALIAS;

  ASTAlias() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* identifier() const { return identifier_; }

  // Get the unquoted and unescaped string value of this alias.
  std::string GetAsString() const;
  IdString GetAsIdString() const;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&identifier_);
  }

  const ASTIdentifier* identifier_ = nullptr;
};

class ASTIntoAlias final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_INTO_ALIAS;

  ASTIntoAlias() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* identifier() const { return identifier_; }

  // Get the unquoted and unescaped string value of this alias.
  std::string GetAsString() const;
  IdString GetAsIdString() const;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&identifier_);
  }

  const ASTIdentifier* identifier_ = nullptr;
};

class ASTFromClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FROM_CLAUSE;

  ASTFromClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // A FromClause has exactly one TableExpression child.
  // If the FROM clause has commas, they will be expressed as a tree
  // of ASTJoin nodes with join_type=COMMA.
  const ASTTableExpression* table_expression() const {
    return table_expression_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&table_expression_);
  }

  const ASTTableExpression* table_expression_ = nullptr;
};

class ASTWindowClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WINDOW_CLAUSE;

  ASTWindowClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTWindowDefinition* const>& windows() const {
    return windows_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&windows_);
  }

  absl::Span<const ASTWindowDefinition* const> windows_;
};

class ASTUnnestExpression final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UNNEST_EXPRESSION;

  ASTUnnestExpression() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
  }

  const ASTExpression* expression_ = nullptr;
};

class ASTWithOffset final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WITH_OFFSET;

  ASTWithOffset() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // alias may be NULL.
  const ASTAlias* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTAlias* alias_ = nullptr;
};

// A conjuction of the unnest expression and the optional alias and offset.
class ASTUnnestExpressionWithOptAliasAndOffset final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_UNNEST_EXPRESSION_WITH_OPT_ALIAS_AND_OFFSET;

  ASTUnnestExpressionWithOptAliasAndOffset() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTUnnestExpression* unnest_expression() const {
    return unnest_expression_;
  }

  const ASTAlias* optional_alias() const { return optional_alias_; }

  const ASTWithOffset* optional_with_offset() const {
    return optional_with_offset_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&unnest_expression_);
    fl.AddOptional(&optional_alias_, AST_ALIAS);
    fl.AddOptional(&optional_with_offset_, AST_WITH_OFFSET);
  }

  const ASTUnnestExpression* unnest_expression_ = nullptr;  // Required
  const ASTAlias* optional_alias_ = nullptr;                // Optional
  const ASTWithOffset* optional_with_offset_ = nullptr;     // Optional
};

// Superclass for all table expressions.  These are things that appear in the
// from clause and produce a stream of rows like a table.
// This includes table scans, joins and subqueries.
class ASTTableExpression : public ASTNode {
 public:
  explicit ASTTableExpression(ASTNodeKind kind) : ASTNode(kind) {}

  bool IsTableExpression() const override { return true; }

  // Return the alias, if the particular subclass has one.
  virtual const ASTAlias* alias() const { return nullptr; }

  // Return the ASTNode location of the alias for this table expression,
  // if applicable.
  const ASTNode* alias_location() const;
};

// TablePathExpression are the TableExpressions that introduce a single scan,
// referenced by a path expression or UNNEST, and can optionally have
// aliases, hints, and WITH OFFSET.
class ASTTablePathExpression final : public ASTTableExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_TABLE_PATH_EXPRESSION;

  ASTTablePathExpression() : ASTTableExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Exactly one of these two will be non-NULL.
  const ASTPathExpression* path_expr() const { return path_expr_; }
  const ASTUnnestExpression* unnest_expr() const { return unnest_expr_; }

  const ASTHint* hint() const { return hint_; }
  const ASTForSystemTime* for_system_time() const { return for_system_time_; }
  const ASTAlias* alias() const override { return alias_; }

  // Present if the scan had WITH OFFSET.
  const ASTWithOffset* with_offset() const { return with_offset_; }

  const ASTSampleClause* sample_clause() const { return sample_clause_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&path_expr_, AST_PATH_EXPRESSION);
    fl.AddOptional(&unnest_expr_, AST_UNNEST_EXPRESSION);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddOptional(&alias_, AST_ALIAS);
    fl.AddOptional(&for_system_time_, AST_FOR_SYSTEM_TIME);
    fl.AddOptional(&with_offset_, AST_WITH_OFFSET);
    fl.AddOptional(&sample_clause_, AST_SAMPLE_CLAUSE);
  }

  // Exactly one of these two is Required.
  const ASTPathExpression* path_expr_ = nullptr;
  const ASTUnnestExpression* unnest_expr_ = nullptr;

  const ASTHint* hint_ = nullptr;  // Optional.
  const ASTForSystemTime* for_system_time_ = nullptr;  // Optional
  const ASTAlias* alias_ = nullptr;  // Optional.
  const ASTWithOffset* with_offset_ = nullptr;  // Optional.
  const ASTSampleClause* sample_clause_ = nullptr;  // Optional.
};

class ASTTableSubquery final : public ASTTableExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TABLE_SUBQUERY;

  ASTTableSubquery() : ASTTableExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTQuery* subquery() const { return subquery_; }
  const ASTAlias* alias() const override { return alias_; }
  const ASTSampleClause* sample_clause() const { return sample_clause_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&subquery_);
    fl.AddOptional(&alias_, AST_ALIAS);
    fl.AddOptional(&sample_clause_, AST_SAMPLE_CLAUSE);
  }

  const ASTQuery* subquery_ = nullptr;              // Required.
  const ASTAlias* alias_ = nullptr;                 // Optional.
  const ASTSampleClause* sample_clause_ = nullptr;  // Optional.
};

// Joins could introduce multiple scans and cannot have aliases.
// It can also represent a JOIN with a list of consecutive ON/USING
// clauses. Such a JOIN is only for internal use, and will never show up in the
// final parse tree.
class ASTJoin final : public ASTTableExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_JOIN;

  ASTJoin() : ASTTableExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  // The join type and hint strings
  std::string GetSQLForJoinType() const;
  std::string GetSQLForJoinHint() const;

  enum JoinType { DEFAULT_JOIN_TYPE, COMMA, CROSS, FULL, INNER, LEFT, RIGHT };

  JoinType join_type() const { return join_type_; }
  void set_join_type(JoinType join_type) { join_type_ = join_type; }

  enum JoinHint { NO_JOIN_HINT, HASH, LOOKUP };

  JoinHint join_hint() const { return join_hint_; }
  void set_join_hint(JoinHint join_hint) { join_hint_ = join_hint; }

  bool natural() const { return natural_; }
  void set_natural(bool natural) { natural_ = natural; }

  const ASTHint* hint() const { return hint_; }
  const ASTTableExpression* lhs() const { return lhs_; }
  const ASTTableExpression* rhs() const { return rhs_; }
  const ASTOnClause* on_clause() const { return on_clause_; }
  const ASTUsingClause* using_clause() const { return using_clause_; }

  // The follwing block is for internal use for handling consecutive ON/USING
  // clauses. They are not used in the final AST.
  int unmatched_join_count() const { return unmatched_join_count_;}
  void set_unmatched_join_count(int count) { unmatched_join_count_ = count;}
  bool transformation_needed() const { return transformation_needed_; }
  void set_transformation_needed(bool transformation_needed)  {
    transformation_needed_ = transformation_needed;
  }
  bool contains_comma_join() const { return contains_comma_join_; }
  void set_contains_comma_join(bool contains_comma_join) {
    contains_comma_join_ = contains_comma_join;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&lhs_);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddRequired(&rhs_);
    fl.AddOptional(&on_clause_, AST_ON_CLAUSE);
    fl.AddOptional(&using_clause_, AST_USING_CLAUSE);

    // If consecutive ON/USING clauses are encountered, then they are saved as
    // clause_list_, and both on_clause_ and using_clause_ will be nullptr.
    fl.AddOptional(&clause_list_, AST_ON_OR_USING_CLAUSE_LIST);
  }

  JoinType join_type_ = DEFAULT_JOIN_TYPE;
  JoinHint join_hint_ = NO_JOIN_HINT;
  bool natural_ = false;
  const ASTHint* hint_ = nullptr;
  const ASTTableExpression* lhs_ = nullptr;
  const ASTTableExpression* rhs_ = nullptr;
  const ASTOnClause* on_clause_ = nullptr;
  const ASTUsingClause* using_clause_ = nullptr;
  const ASTOnOrUsingClauseList* clause_list_ = nullptr;

  // The number of qualified joins that do not have a matching ON/USING clause.
  // See the comment in join_processor.cc for details.
  int unmatched_join_count_ = 0;

  // Indicates if this node needs to be transformed. See the comment
  // in join_processor.cc for details.
  // This is true if contains_clause_list_ is true, or if there is a JOIN with
  // ON/USING clause list on the lhs side of the tree path.
  bool transformation_needed_ = false;

  // Indicates if this join contains a COMMA JOIN on the lhs side of the tree
  // path.
  bool contains_comma_join_ = false;
};

class ASTParenthesizedJoin final : public ASTTableExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PARENTHESIZED_JOIN;

  ASTParenthesizedJoin() : ASTTableExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTJoin* join() const { return join_; }
  const ASTSampleClause* sample_clause() const { return sample_clause_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&join_);
    fl.AddOptional(&sample_clause_, AST_SAMPLE_CLAUSE);
  }

  const ASTJoin* join_ = nullptr;                   // Required.
  const ASTSampleClause* sample_clause_ = nullptr;  // Optional.
};

class ASTOnClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ON_CLAUSE;

  ASTOnClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
  }

  const ASTExpression* expression_ = nullptr;
};

class ASTUsingClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_USING_CLAUSE;

  ASTUsingClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTIdentifier* const>& keys() const {
    return keys_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&keys_);
  }

  absl::Span<const ASTIdentifier* const> keys_;
};

class ASTOnOrUsingClauseList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ON_OR_USING_CLAUSE_LIST;

  ASTOnOrUsingClauseList(): ASTNode(kConcreteNodeKind) {}

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  absl::Span<const ASTNode* const> on_or_using_clause_list() const {
    return on_or_using_clause_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&on_or_using_clause_list_);
  }

  // Each element in the list must be either ASTOnClause or ASTUsingClause.
  absl::Span<const ASTNode* const> on_or_using_clause_list_;
};

class ASTWhereClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WHERE_CLAUSE;

  ASTWhereClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
  }

  const ASTExpression* expression_ = nullptr;
};

class ASTRollup final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ROLLUP;

  ASTRollup() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& expressions() const {
    return expressions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&expressions_);
  }

  absl::Span<const ASTExpression* const> expressions_;
};

class ASTForSystemTime final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOR_SYSTEM_TIME;

  ASTForSystemTime() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
  }

  const ASTExpression* expression_;
};

// Represents a grouping item, which is either an expression (a regular
// group by key) or a rollup list.
class ASTGroupingItem final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GROUPING_ITEM;

  ASTGroupingItem() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Exactly one of expression() and rollup() will be non-NULL.
  const ASTExpression* expression() const { return expression_; }
  const ASTRollup* rollup() const { return rollup_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&expression_);
    fl.AddOptional(&rollup_, AST_ROLLUP);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTRollup* rollup_ = nullptr;
};

class ASTGroupBy final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GROUP_BY;

  ASTGroupBy() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTHint* hint() const { return hint_; }

  const absl::Span<const ASTGroupingItem* const>& grouping_items() const {
    return grouping_items_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddRestAsRepeated(&grouping_items_);
  }

  const ASTHint* hint_ = nullptr;
  absl::Span<const ASTGroupingItem* const> grouping_items_;
};

class ASTHaving final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HAVING;

  ASTHaving() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
  }

  const ASTExpression* expression_ = nullptr;
};

class ASTCollate final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_COLLATE;

  ASTCollate() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* collation_name() const { return collation_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&collation_name_);
  }

  const ASTExpression* collation_name_ = nullptr;
};

class ASTNullOrder final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_NULL_ORDER;

  ASTNullOrder() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_nulls_first(bool nulls_first) { nulls_first_ = nulls_first; }
  bool nulls_first() const { return nulls_first_; }

  std::string SingleNodeDebugString() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }

  bool nulls_first_ = false;
};

class ASTOrderingExpression final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ORDERING_EXPRESSION;

  ASTOrderingExpression() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_descending(bool descending) { descending_ = descending; }
  bool descending() const { return descending_; }
  const ASTExpression* expression() const { return expression_; }
  const ASTCollate* collate() const { return collate_; }
  const ASTNullOrder* null_order() const { return null_order_; }

  std::string SingleNodeDebugString() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&collate_, AST_COLLATE);
    fl.AddOptional(&null_order_, AST_NULL_ORDER);
  }

  bool descending_ = false;  // The default if not explicitly in the ORDER BY.
  const ASTExpression* expression_ = nullptr;
  const ASTCollate* collate_ = nullptr;
  const ASTNullOrder* null_order_ = nullptr;
};

class ASTOrderBy final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ORDER_BY;

  ASTOrderBy() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTHint* hint() const { return hint_; }

  const absl::Span<
      const ASTOrderingExpression* const>& ordering_expressions() const
    { return ordering_expressions_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddRestAsRepeated(&ordering_expressions_);
  }

  const ASTHint* hint_ = nullptr;
  absl::Span<const ASTOrderingExpression* const> ordering_expressions_;
};

class ASTLimitOffset final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_LIMIT_OFFSET;

  ASTLimitOffset() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* limit() const { return limit_; }
  const ASTExpression* offset() const { return offset_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&limit_);
    fl.AddOptionalExpression(&offset_);
  }

  // The LIMIT value. Never NULL.
  const ASTExpression* limit_ = nullptr;
  // The OFFSET value. NULL if no OFFSET specified.
  const ASTExpression* offset_ = nullptr;
};

class ASTHavingModifier final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HAVING_MODIFIER;

  ASTHavingModifier() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  enum ModifierKind { MIN, MAX };
  void set_modifier_kind(ModifierKind modifier_kind) {
    modifier_kind_ = modifier_kind;
  }
  ModifierKind modifier_kind() const { return modifier_kind_; }

  const ASTExpression* expr() const { return expr_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
  }

  // The expression MAX or MIN applies to. Never NULL.
  const ASTExpression* expr_ = nullptr;
  ModifierKind modifier_kind_ = ModifierKind::MAX;
};

class ASTExpression : public ASTNode {
 public:
  explicit ASTExpression(ASTNodeKind kind) : ASTNode(kind) {}

  void set_parenthesized(bool parenthesized) { parenthesized_ = parenthesized; }

  bool IsExpression() const override { return true; }
  bool parenthesized() const { return parenthesized_; }

  // Returns true if this expression is allowed to occur as a child of a
  // comparison expression. This is not allowed for unparenthesized comparison
  // expressions and operators with a lower precedence level (AND, OR, and NOT).
  virtual bool IsAllowedInComparison() const { return true; }

 private:
  bool parenthesized_ = false;
};

// Parent class that corresponds to the subset of ASTExpression nodes that are
// allowed by the <generalized_path_expression> grammar rule. It allows for some
// extra type safety vs. simply passing around ASTExpression as
// <generalized_path_expression>s.
//
// Only the following node kinds are allowed:
// - AST_PATH_EXPRESSION
// - AST_DOT_GENERALIZED_FIELD where the left hand side is a
//   <generalized_path_expression>.
// - AST_DOT_IDENTIFIER where the left hand side is a
//   <generalized_path_expression>.
// - AST_ARRAY_ELEMENT where the left hand side is a
//   <generalized_path_expression>
//
// Note that the type system does not capture the "pureness constraint" that,
// e.g., the left hand side of an AST_DOT_GENERALIZED_FIELD must be a
// <generalized_path_expression> in order for the node. However, it is still
// considered a bug to create a variable with type ASTGeneralizedPathExpression
// that does not satisfy the pureness constraint (similarly, it is considered a
// bug to call a function with an ASTGeneralizedPathExpression argument that
// does not satisfy the pureness constraint).
class ASTGeneralizedPathExpression : public ASTExpression {
 public:
  explicit ASTGeneralizedPathExpression(ASTNodeKind kind)
      : ASTExpression(kind) {}

  // Returns an error if 'path' contains a node that cannot come from the
  // <generalized_path_expression> grammar rule.
  static absl::Status VerifyIsPureGeneralizedPathExpression(
      const ASTExpression* path);
};

class ASTAndExpr final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_AND_EXPR;

  ASTAndExpr() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& conjuncts() const {
    return conjuncts_;
  }

  bool IsAllowedInComparison() const override { return parenthesized(); }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&conjuncts_);
  }

  absl::Span<const ASTExpression* const> conjuncts_;
};

class ASTOrExpr final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_OR_EXPR;

  ASTOrExpr() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& disjuncts() const {
    return disjuncts_;
  }

  bool IsAllowedInComparison() const override { return parenthesized(); }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&disjuncts_);
  }

  absl::Span<const ASTExpression* const> disjuncts_;
};

class ASTBinaryExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BINARY_EXPRESSION;

  ASTBinaryExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  // Returns name of the operator in SQL, including the NOT keyword when
  // necessary.
  std::string GetSQLForOperator() const;

  // All supported operators.
  enum Op {
    NOT_SET,
    LIKE,         // "LIKE"
    IS,           // "IS"
    EQ,           // "="
    NE,           // "!="
    NE2,          // "<>"
    GT,           // ">"
    LT,           // "<"
    GE,           // ">="
    LE,           // "<="
    BITWISE_OR,   // "|"
    BITWISE_XOR,  // "^"
    BITWISE_AND,  // "&"
    PLUS,         // "+"
    MINUS,        // "-"
    MULTIPLY,     // "*"
    DIVIDE,       // "/"
    CONCAT_OP,    // "||"
  };

  void set_op(Op op) { op_ = op; }
  Op op() const { return op_; }

  // Signifies whether the binary operator has a preceding NOT to it. For NOT
  // LIKE and IS NOT.
  bool is_not() const { return is_not_; }
  void set_is_not(bool is_not) { is_not_ = is_not; }

  const ASTExpression* lhs() const { return lhs_; }
  const ASTExpression* rhs() const { return rhs_; }

  bool IsAllowedInComparison() const override {
    if (parenthesized()) return true;
    switch (op()) {
      case LIKE:
      case IS:
      case EQ:
      case NE:
      case NE2:
      case GT:
      case LT:
      case GE:
      case LE:
        return false;
      default:
        return true;
    }
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&lhs_);
    fl.AddRequired(&rhs_);
  }

  Op op_ = NOT_SET;
  bool is_not_ = false;
  const ASTExpression* lhs_ = nullptr;
  const ASTExpression* rhs_ = nullptr;
};

class ASTBitwiseShiftExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_BITWISE_SHIFT_EXPRESSION;

  ASTBitwiseShiftExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  void set_is_left_shift(bool is_left_shift) { is_left_shift_ = is_left_shift; }

  // Signifies whether the bitwise shift is of left shift type "<<" or right
  // shift type ">>".
  bool is_left_shift() const { return is_left_shift_; }
  const ASTExpression* lhs() const { return lhs_; }
  const ASTExpression* rhs() const { return rhs_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&lhs_);
    fl.AddRequired(&rhs_);
  }

  bool is_left_shift_ = false;
  const ASTExpression* lhs_ = nullptr;
  const ASTExpression* rhs_ = nullptr;
};

class ASTInExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IN_EXPRESSION;

  ASTInExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  void set_is_not(bool is_not) { is_not_ = is_not; }

  // Signifies whether the IN operator has a preceding NOT to it.
  bool is_not() const {  return is_not_; }

  const ASTExpression* lhs() const { return lhs_; }

  // Hints specified on IN clause.
  // This can be set only if IN clause has subquery as RHS.
  const ASTHint* hint() const { return hint_; }

  // Exactly one of the three fields below is present.
  const ASTInList* in_list() const { return in_list_; }
  const ASTQuery* query() const { return query_; }
  const ASTUnnestExpression* unnest_expr() const { return unnest_expr_; }

  bool IsAllowedInComparison() const override { return parenthesized(); }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&lhs_);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddOptional(&in_list_, AST_IN_LIST);
    fl.AddOptional(&query_, AST_QUERY);
    fl.AddOptional(&unnest_expr_, AST_UNNEST_EXPRESSION);
  }

  bool is_not_ = false;
  // Expression for which we need to verify whether its resolved result matches
  // any of the resolved results of the expressions present in the in_list_.
  const ASTExpression* lhs_ = nullptr;
  // Hints specified on IN clause
  const ASTHint* hint_ = nullptr;
  // List of expressions to check against for the presence of lhs_.
  const ASTInList* in_list_ = nullptr;
  // Query returns the row values to check against for the presence of lhs_.
  const ASTQuery* query_ = nullptr;
  // Check if lhs_ is an element of the array value inside Unnest.
  const ASTUnnestExpression* unnest_expr_ = nullptr;
};

class ASTInList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IN_LIST;

  ASTInList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& list() const {
    return list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&list_);
  }

  // List of expressions present in the InList node.
  absl::Span<const ASTExpression* const> list_;
};

class ASTBetweenExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BETWEEN_EXPRESSION;

  ASTBetweenExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  void set_is_not(bool is_not) { is_not_ = is_not; }

  // Signifies whether the BETWEEN operator has a preceding NOT to it.
  bool is_not() const { return is_not_; }

  const ASTExpression* lhs() const { return lhs_; }
  const ASTExpression* low() const { return low_; }
  const ASTExpression* high() const { return high_; }

  bool IsAllowedInComparison() const override { return parenthesized(); }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&lhs_);
    fl.AddRequired(&low_);
    fl.AddRequired(&high_);
  }

  bool is_not_ = false;
  // Represents <lhs_> BETWEEN <low_> AND <high_>
  const ASTExpression* lhs_ = nullptr;
  const ASTExpression* low_ = nullptr;
  const ASTExpression* high_ = nullptr;
};

class ASTUnaryExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UNARY_EXPRESSION;

  ASTUnaryExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  std::string GetSQLForOperator() const;

  enum Op {
    NOT_SET,
    NOT,          // "NOT"
    BITWISE_NOT,  // "~"
    MINUS,        // Unary "-"
    PLUS,         // Unary "+"
  };

  Op op() const { return op_; }
  void set_op(Op op) { op_ = op; }

  const ASTExpression* operand() const { return operand_; }

  bool IsAllowedInComparison() const override {
    return parenthesized() || op_ != NOT;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&operand_);
  }

  Op op_ = NOT_SET;
  const ASTExpression* operand_ = nullptr;
};

class ASTCastExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CAST_EXPRESSION;

  ASTCastExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  void set_is_safe_cast(bool is_safe_cast) {
    is_safe_cast_ = is_safe_cast;
  }

  const ASTExpression* expr() const { return expr_; }
  const ASTType* type() const { return type_; }
  bool is_safe_cast() const { return is_safe_cast_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
    fl.AddRequired(&type_);
  }

  const ASTExpression* expr_ = nullptr;
  const ASTType* type_ = nullptr;
  bool is_safe_cast_ = false;
};

class ASTCaseValueExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CASE_VALUE_EXPRESSION;

  ASTCaseValueExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<
      const ASTExpression* const>& arguments() const { return arguments_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&arguments_);
  }

  absl::Span<const ASTExpression* const> arguments_;
};

class ASTCaseNoValueExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CASE_NO_VALUE_EXPRESSION;

  ASTCaseNoValueExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<
      const ASTExpression* const>& arguments() const { return arguments_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&arguments_);
  }

  absl::Span<const ASTExpression* const> arguments_;
};

class ASTExtractExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXTRACT_EXPRESSION;

  ASTExtractExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* lhs_expr() const { return lhs_expr_; }
  const ASTExpression* rhs_expr() const { return rhs_expr_; }
  const ASTExpression* time_zone_expr() const { return time_zone_expr_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&lhs_expr_);
    fl.AddRequired(&rhs_expr_);
    fl.AddOptionalExpression(&time_zone_expr_);
  }

  const ASTExpression* lhs_expr_ = nullptr;
  const ASTExpression* rhs_expr_ = nullptr;
  const ASTExpression* time_zone_expr_ = nullptr;
};

// This is used for dotted identifier paths only, not dotting into
// arbitrary expressions (see ASTDotIdentifier below).
class ASTPathExpression final : public ASTGeneralizedPathExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PATH_EXPRESSION;

  ASTPathExpression() : ASTGeneralizedPathExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const int num_names() const { return names_.size(); }
  const absl::Span<const ASTIdentifier* const>& names() const {
    return names_;
  }
  const ASTIdentifier* name(int i) const { return names_[i]; }
  const ASTIdentifier* first_name() const { return names_.front(); }
  const ASTIdentifier* last_name() const { return names_.back(); }

  // Return this PathExpression as a dotted SQL identifier string, with
  // quoting if necessary.  If <max_prefix_size> is non-zero, include at most
  // that many identifiers from the prefix of <path>.
  std::string ToIdentifierPathString(size_t max_prefix_size = 0) const;

  // Return the vector of identifier strings (without quoting).
  std::vector<std::string> ToIdentifierVector() const;

  // Similar to ToIdentifierVector(), but returns a vector of IdString's,
  // avoiding the need to make copies.
  std::vector<IdString> ToIdStringVector() const;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&names_);
  }
  absl::Span<const ASTIdentifier* const> names_;
};

class ASTParameterExprBase : public ASTExpression {
 public:
  explicit ASTParameterExprBase(ASTNodeKind kind) : ASTExpression(kind) {}

 private:
  void InitFields() override {
    FieldLoader fl(this);
  }
};

class ASTParameterExpr final : public ASTParameterExprBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PARAMETER_EXPR;

  ASTParameterExpr() : ASTParameterExprBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* name() const { return name_; }
  int position() const { return position_; }

  void set_position(int position) { position_ = position; }

  std::string SingleNodeDebugString() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&name_, AST_IDENTIFIER);
  }

  const ASTIdentifier* name_ = nullptr;
  // 1-based position of the parameter in the query. Mutually exclusive with
  // name_.
  int position_ = 0;
};

class ASTSystemVariableExpr final : public ASTParameterExprBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SYSTEM_VARIABLE_EXPR;

  ASTSystemVariableExpr() : ASTParameterExprBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* path() const { return path_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&path_);
  }

  const ASTPathExpression* path_ = nullptr;
};


class ASTIntervalExpr final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_INTERVAL_EXPR;

  ASTIntervalExpr() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* interval_value() const { return interval_value_; }
  const ASTIdentifier* date_part_name() const { return date_part_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&interval_value_);
    fl.AddRequired(&date_part_name_);
  }

  const ASTExpression* interval_value_ = nullptr;
  const ASTIdentifier* date_part_name_ = nullptr;
};

class ASTDescriptor final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DESCRIPTOR;

  ASTDescriptor() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  ABSL_MUST_USE_RESULT zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTDescriptorColumnList* columns() const { return columns_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&columns_);
  }

  const ASTDescriptorColumnList* columns_ = nullptr;
};

// This is used for using dot to extract a field from an arbitrary expression.
// In cases where we know the left side is always an identifier path, we
// use ASTPathExpression instead.
class ASTDotIdentifier final : public ASTGeneralizedPathExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DOT_IDENTIFIER;

  ASTDotIdentifier() : ASTGeneralizedPathExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }
  const ASTIdentifier* name() const { return name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
    fl.AddRequired(&name_);
  }

  const ASTExpression* expr_ = nullptr;
  const ASTIdentifier* name_ = nullptr;
};

// This is a generalized form of extracting a field from an expression.
// It uses a parenthesized path_expression instead of a single identifier
// to select the field.
class ASTDotGeneralizedField final : public ASTGeneralizedPathExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DOT_GENERALIZED_FIELD;

  ASTDotGeneralizedField() : ASTGeneralizedPathExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }
  const ASTPathExpression* path() const { return path_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
    fl.AddRequired(&path_);
  }

  const ASTExpression* expr_ = nullptr;
  const ASTPathExpression* path_ = nullptr;
};

class ASTFunctionCall final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FUNCTION_CALL;
  enum NullHandlingModifier {
    DEFAULT_NULL_HANDLING,
    IGNORE_NULLS,
    RESPECT_NULLS
  };

  ASTFunctionCall() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  const ASTPathExpression* function() const { return function_; }
  const absl::Span<const ASTExpression* const>& arguments() const {
    return arguments_;
  }
  bool distinct() const { return distinct_; }

  void set_distinct(bool distinct) { distinct_ = distinct; }

  // If present, modifies the input behavior of aggregate functions.
  void set_null_handling_modifier(NullHandlingModifier kind) {
    null_handling_modifier_ = kind;
  }
  NullHandlingModifier null_handling_modifier() const {
    return null_handling_modifier_;
  }

  const ASTHavingModifier* having_modifier() const { return having_modifier_; }

  // If present, applies to the inputs of aggregate functions.
  const ASTOrderBy* order_by() const { return order_by_; }

  // If present, this applies to the inputs of aggregate functions.
  const ASTLimitOffset* limit_offset() const { return limit_offset_; }

  // Convenience method that returns true if any modifiers are set. Useful for
  // places in the resolver where function call syntax is used for purposes
  // other than a function call (e.g., <array>[OFFSET(<expr>) or WEEK(MONDAY)]).
  bool HasModifiers() const {
    return distinct_ || null_handling_modifier_ != DEFAULT_NULL_HANDLING ||
           having_modifier_ != nullptr ||
           order_by_ != nullptr ||
           limit_offset_ != nullptr;
  }

  // Used by the Bison parser to mark CURRENT_<date/time> functions to which no
  // parentheses have yet been applied.
  bool is_current_date_time_without_parentheses() const {
    return is_current_date_time_without_parentheses_;
  }
  void set_is_current_date_time_without_parentheses(bool value) {
    is_current_date_time_without_parentheses_ = value;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&function_);
    fl.AddRepeatedWhileIsExpression(&arguments_);
    fl.AddOptional(&having_modifier_, AST_HAVING_MODIFIER);
    fl.AddOptional(&order_by_, AST_ORDER_BY);
    fl.AddOptional(&limit_offset_, AST_LIMIT_OFFSET);
  }

  const ASTPathExpression* function_ = nullptr;
  absl::Span<const ASTExpression* const> arguments_;
  // True if the function was called with FUNC(DISTINCT args).
  bool distinct_ = false;
  // Set if the function was called with FUNC(args {IGNORE|RESPECT} NULLS).
  NullHandlingModifier null_handling_modifier_ = DEFAULT_NULL_HANDLING;
  // Set if the function was called with FUNC(args HAVING {MAX|MIN} expr).
  const ASTHavingModifier* having_modifier_ = nullptr;
  // Set if the function was called with FUNC(args ORDER BY cols).
  const ASTOrderBy* order_by_ = nullptr;
  // Set if the function was called with FUNC(args LIMIT N).
  const ASTLimitOffset* limit_offset_ = nullptr;

  // This is set by the Bison parser to indicate a parentheses-less call to
  // CURRENT_* functions. The parser parses them as function calls even without
  // the parentheses, but then still allows function call parentheses to be
  // applied.
  bool is_current_date_time_without_parentheses_ = false;
};

// Represents a named function call argument using syntax: name => expression.
// The resolver will match these against available argument names in the
// function signature.
class ASTNamedArgument final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_NAMED_ARGUMENT;

  ASTNamedArgument() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* name() const { return name_; }
  const ASTExpression* expr() const { return expr_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&expr_);
  }

  // Required, never NULL.
  const ASTIdentifier* name_ = nullptr;
  // Required, never NULL.
  const ASTExpression* expr_ = nullptr;
};

class ASTAnalyticFunctionCall final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ANALYTIC_FUNCTION_CALL;

  ASTAnalyticFunctionCall() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTFunctionCall* function() const { return function_; }
  const ASTWindowSpecification* window_spec() const {
    return window_spec_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&function_);
    fl.AddRequired(&window_spec_);
  }

  // Required, never NULL.
  const ASTFunctionCall* function_ = nullptr;
  // Required, never NULL.
  const ASTWindowSpecification* window_spec_ = nullptr;
};

class ASTPartitionBy final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PARTITION_BY;

  ASTPartitionBy() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTHint* hint() const { return hint_; }

  const absl::Span<
      const ASTExpression* const>& partitioning_expressions() const {
    return partitioning_expressions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddRestAsRepeated(&partitioning_expressions_);
  }

  const ASTHint* hint_ = nullptr;
  absl::Span<const ASTExpression* const> partitioning_expressions_;
};

class ASTClusterBy final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CLUSTER_BY;

  ASTClusterBy() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<
      const ASTExpression* const>& clustering_expressions() const {
    return clustering_expressions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&clustering_expressions_);
  }

  absl::Span<const ASTExpression* const> clustering_expressions_;
};

class ASTWindowFrameExpr final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WINDOW_FRAME_EXPR;

  enum BoundaryType {
    UNBOUNDED_PRECEDING,
    OFFSET_PRECEDING,
    CURRENT_ROW,
    OFFSET_FOLLOWING,
    UNBOUNDED_FOLLOWING,
  };

  ASTWindowFrameExpr() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  BoundaryType boundary_type() const { return boundary_type_; }
  const ASTExpression* expression() const { return expression_; }

  void set_boundary_type(BoundaryType boundary_type) {
    boundary_type_ = boundary_type;
  }

  std::string GetBoundaryTypeString() const;
  static std::string BoundaryTypeToString(BoundaryType type);

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&expression_);
  }

  BoundaryType boundary_type_ = UNBOUNDED_PRECEDING;
  // Expression to specify the boundary as a logical or physical offset
  // to current row. Cannot be NULL if boundary_type is OFFSET_PRECEDING
  // or OFFSET_FOLLOWING; otherwise, should be NULL.
  const ASTExpression* expression_ = nullptr;
};

class ASTWindowFrame final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WINDOW_FRAME;

  enum FrameUnit {
    ROWS,
    RANGE,
  };

  ASTWindowFrame() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  void set_unit(FrameUnit frame_unit) { frame_unit_ = frame_unit; }
  const ASTWindowFrameExpr* start_expr() const { return start_expr_; }
  const ASTWindowFrameExpr* end_expr() const { return end_expr_; }

  FrameUnit frame_unit() const { return frame_unit_; }
  std::string GetFrameUnitString() const;

  static std::string FrameUnitToString(FrameUnit unit);

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&start_expr_);
    fl.AddOptional(&end_expr_, AST_WINDOW_FRAME_EXPR);
  }

  FrameUnit frame_unit_ = RANGE;

  // Starting boundary expression. Never NULL.
  const ASTWindowFrameExpr* start_expr_ = nullptr;
  // Ending boundary expression. Can be NULL.
  // When this is NULL, the implicit ending boundary is CURRENT ROW.
  const ASTWindowFrameExpr* end_expr_ = nullptr;
};


class ASTWindowSpecification final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_WINDOW_SPECIFICATION;

  ASTWindowSpecification() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPartitionBy* partition_by() const { return partition_by_; }
  const ASTOrderBy* order_by() const { return order_by_; }
  const ASTWindowFrame* window_frame() const { return window_frame_; }

  const ASTIdentifier* base_window_name() const { return base_window_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&base_window_name_, AST_IDENTIFIER);
    fl.AddOptional(&partition_by_, AST_PARTITION_BY);
    fl.AddOptional(&order_by_, AST_ORDER_BY);
    fl.AddOptional(&window_frame_, AST_WINDOW_FRAME);
  }

  // All are optional, can be NULL.
  const ASTIdentifier* base_window_name_ = nullptr;
  const ASTPartitionBy* partition_by_ = nullptr;
  const ASTOrderBy* order_by_ = nullptr;
  const ASTWindowFrame* window_frame_ = nullptr;
};

class ASTWindowDefinition final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WINDOW_DEFINITION;

  ASTWindowDefinition() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* name() const { return name_; }
  const ASTWindowSpecification* window_spec() const {
    return window_spec_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&window_spec_);
  }

  // Required, never NULL.
  const ASTIdentifier* name_ = nullptr;
  // Required, never NULL.
  const ASTWindowSpecification* window_spec_ = nullptr;
};

class ASTArrayElement final : public ASTGeneralizedPathExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ARRAY_ELEMENT;

  ASTArrayElement() : ASTGeneralizedPathExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* array() const { return array_; }
  const ASTExpression* position() const { return position_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&array_);
    fl.AddRequired(&position_);
  }

  const ASTExpression* array_ = nullptr;
  const ASTExpression* position_ = nullptr;
};

// A subquery in an expression.  (Not in the FROM clause.)
class ASTExpressionSubquery final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_EXPRESSION_SUBQUERY;

  ASTExpressionSubquery() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  const ASTQuery* query() const { return query_; }
  const ASTHint* hint() const { return hint_; }

  // The syntactic modifier on this expression subquery.
  enum Modifier {
    NONE,    // (select ...)
    ARRAY,   // ARRAY(select ...)
    EXISTS,  // EXISTS(select ...)
  };

  Modifier modifier() const { return modifier_; }
  void set_modifier(Modifier modifier) { modifier_ = modifier; }

  static std::string ModifierToString(Modifier modifier);

  // Note, this is intended by called from inside bison_parser.  At this stage
  // InitFields has _not_ been set, thus we need to use only children offsets.
  // Returns null on error.
  ASTQuery* GetMutableQueryChildInternal() {
    if (num_children() == 1) {
      return mutable_child(0)->GetAsOrNull<ASTQuery>();
    } else if (num_children() == 2) {
      // Hint is the first child.
      return mutable_child(1)->GetAsOrNull<ASTQuery>();
    } else {
      return nullptr;
    }
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddRequired(&query_);
  }

  const ASTQuery* query_ = nullptr;
  const ASTHint* hint_ = nullptr;
  Modifier modifier_ = NONE;
};

class ASTLeaf : public ASTExpression {
 public:
  explicit ASTLeaf(ASTNodeKind kind) : ASTExpression(kind) {}

  std::string SingleNodeDebugString() const override {
    return absl::StrCat(std::string(ASTNode::SingleNodeDebugString()), "(",
                        image_, ")");
  }

  // image() references data with the same lifetime as this ASTLeaf object.
  absl::string_view image() const { return image_; }
  void set_image(std::string image) { image_ = std::move(image); }

  bool IsLeaf() const override { return true; }

 private:
  void InitFields() final {
    FieldLoader(this);  // Triggers check that there were no children.
  }

  std::string image_;
};

class ASTStar final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STAR;

  ASTStar() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
};

class ASTStarReplaceItem final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STAR_REPLACE_ITEM;

  ASTStarReplaceItem() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }
  const ASTIdentifier* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddRequired(&alias_);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTIdentifier* alias_ = nullptr;
};

class ASTStarExceptList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STAR_EXCEPT_LIST;

  ASTStarExceptList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTIdentifier* const>& identifiers() const {
    return identifiers_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&identifiers_);
  }

  absl::Span<const ASTIdentifier* const> identifiers_;
};

// SELECT * EXCEPT(...) REPLACE(...).  See (broken link).
class ASTStarModifiers final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STAR_MODIFIERS;

  ASTStarModifiers() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTStarExceptList* except_list() const { return except_list_; }
  const absl::Span<
      const ASTStarReplaceItem* const>& replace_items() const {
    return replace_items_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&except_list_, AST_STAR_EXCEPT_LIST);
    fl.AddRestAsRepeated(&replace_items_);
  }

  const ASTStarExceptList* except_list_ = nullptr;
  absl::Span<const ASTStarReplaceItem* const> replace_items_;
};

// SELECT * EXCEPT(...) REPLACE(...).  See (broken link).
class ASTStarWithModifiers final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STAR_WITH_MODIFIERS;

  ASTStarWithModifiers() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTStarModifiers* modifiers() const { return modifiers_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&modifiers_);
  }

  const ASTStarModifiers* modifiers_ = nullptr;
};

class ASTDotStar final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DOT_STAR;

  ASTDotStar() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
  }

  const ASTExpression* expr_ = nullptr;
};

// SELECT x.* EXCEPT(...) REPLACE(...).  See (broken link).
class ASTDotStarWithModifiers final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DOT_STAR_WITH_MODIFIERS;

  ASTDotStarWithModifiers() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }
  const ASTStarModifiers* modifiers() const { return modifiers_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
    fl.AddRequired(&modifiers_);
  }

  const ASTExpression* expr_ = nullptr;
  const ASTStarModifiers* modifiers_ = nullptr;
};

class ASTIdentifier final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IDENTIFIER;

  ASTIdentifier() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  // Set the identifier string.  Input <identifier> is the unquoted identifier.
  // There is no validity checking here.  This assumes the identifier was
  // validated and unquoted in zetasql.jjt.
  void SetIdentifier(IdString identifier) {
    id_string_ = identifier;
  }

  // Get the unquoted and unescaped string value of this identifier.
  IdString GetAsIdString() const { return id_string_; }
  std::string GetAsString() const { return id_string_.ToString(); }

 private:
  void InitFields() final {
    FieldLoader(this);  // Triggers check that there were no children.
  }

  IdString id_string_;
};

class ASTNewConstructorArg final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_NEW_CONSTRUCTOR_ARG;

  ASTNewConstructorArg() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

  // At most one of 'optional_identifier' and 'optional_path_expression' are
  // set.
  const ASTIdentifier* optional_identifier() const {
    return optional_identifier_;
  }
  const ASTPathExpression* optional_path_expression() const {
    return optional_path_expression_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&optional_identifier_, AST_IDENTIFIER);
    fl.AddOptional(&optional_path_expression_, AST_PATH_EXPRESSION);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTIdentifier* optional_identifier_ = nullptr;
  const ASTPathExpression* optional_path_expression_ = nullptr;
};

class ASTNewConstructor final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_NEW_CONSTRUCTOR;

  ASTNewConstructor() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTSimpleType* type_name() const { return type_name_; }

  const absl::Span<const ASTNewConstructorArg* const>& arguments() const {
    return arguments_;
  }
  const ASTNewConstructorArg* argument(int idx) const {
    return arguments_[idx];
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&type_name_);
    fl.AddRestAsRepeated(&arguments_);
  }

  const ASTSimpleType* type_name_ = nullptr;
  absl::Span<const ASTNewConstructorArg* const> arguments_;
};

class ASTArrayConstructor final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ARRAY_CONSTRUCTOR;

  ASTArrayConstructor() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // May return NULL. Occurs only if the array is constructed through
  // ARRAY<type>[...] syntax and not ARRAY[...] or [...].
  const ASTArrayType* type() const { return type_; }

  const absl::Span<const ASTExpression* const>& elements() const {
    return elements_;
  }
  const ASTExpression* element(int idx) const { return elements_[idx]; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&type_, AST_ARRAY_TYPE);
    fl.AddRestAsRepeated(&elements_);
  }

  const ASTArrayType* type_ = nullptr;
  absl::Span<const ASTExpression* const> elements_;
};

class ASTStructConstructorArg final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STRUCT_CONSTRUCTOR_ARG;

  ASTStructConstructorArg() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }
  const ASTAlias* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTAlias* alias_ = nullptr;
};

// This node results from structs constructed with (expr, expr, ...).
// This will only occur when there are at least two expressions.
class ASTStructConstructorWithParens final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_STRUCT_CONSTRUCTOR_WITH_PARENS;

  ASTStructConstructorWithParens() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& field_expressions() const {
    return field_expressions_;
  }
  const ASTExpression* field_expression(int idx) const {
    return field_expressions_[idx];
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&field_expressions_);
  }

  absl::Span<const ASTExpression* const> field_expressions_;
};

// This node results from structs constructed with the STRUCT keyword.
//   STRUCT(expr [AS alias], ...)
//   STRUCT<...>(expr [AS alias], ...)
// Both forms support empty field lists.
// The struct_type_ child will be non-NULL for the second form,
// which includes the struct's field list.
class ASTStructConstructorWithKeyword final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_STRUCT_CONSTRUCTOR_WITH_KEYWORD;

  ASTStructConstructorWithKeyword() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTStructType* struct_type() const { return struct_type_; }

  const absl::Span<const ASTStructConstructorArg* const>& fields() const {
    return fields_;
  }
  const ASTStructConstructorArg* field(int idx) const { return fields_[idx]; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&struct_type_, AST_STRUCT_TYPE);
    fl.AddRestAsRepeated(&fields_);
  }

  // May be NULL.
  const ASTStructType* struct_type_ = nullptr;

  absl::Span<const ASTStructConstructorArg* const> fields_;
};

class ASTIntLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_INT_LITERAL;

  ASTIntLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  bool is_hex() const;
};

class ASTNumericLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_NUMERIC_LITERAL;

  ASTNumericLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
};

class ASTBigNumericLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BIGNUMERIC_LITERAL;

  ASTBigNumericLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
};

class ASTStringLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STRING_LITERAL;

  ASTStringLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // The parsed and validated value of this literal. The raw input value can be
  // found in image().
  const std::string& string_value() const { return string_value_; }
  void set_string_value(std::string string_value) {
    string_value_ = std::move(string_value);
  }

 private:
  std::string string_value_;
};

class ASTBytesLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BYTES_LITERAL;

  ASTBytesLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // The parsed and validated value of this literal. The raw input value can be
  // found in image().
  const std::string& bytes_value() const { return bytes_value_; }
  void set_bytes_value(std::string bytes_value) {
    bytes_value_ = std::move(bytes_value);
  }

 private:
  std::string bytes_value_;
};

class ASTBooleanLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BOOLEAN_LITERAL;

  ASTBooleanLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_value(bool value) { value_ = value; }

  bool value() const { return value_; }

 private:
  bool value_ = false;
};

class ASTFloatLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FLOAT_LITERAL;

  ASTFloatLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
};

class ASTNullLiteral final : public ASTLeaf {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_NULL_LITERAL;

  ASTNullLiteral() : ASTLeaf(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
};

class ASTDateOrTimeLiteral final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DATE_OR_TIME_LITERAL;

  ASTDateOrTimeLiteral() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  const TypeKind type_kind() const { return type_kind_; }
  void set_type_kind(const TypeKind type_kind) { type_kind_ = type_kind; }

  const ASTStringLiteral* string_literal() const {
    return string_literal_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&string_literal_);
  }

  TypeKind type_kind_ = TYPE_UNKNOWN;
  const ASTStringLiteral* string_literal_ = nullptr;
};

class ASTHint final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HINT;

  ASTHint() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // This is the @num_shards hint shorthand that can occur anywhere that a
  // hint can occur, prior to @{...} hints.
  // At least one of num_shards_hints is non-NULL or hint_entries is non-empty.
  const ASTIntLiteral* num_shards_hint() const { return num_shards_hint_; }

  const absl::Span<const ASTHintEntry* const>& hint_entries() const {
    return hint_entries_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&num_shards_hint_, AST_INT_LITERAL);
    fl.AddRestAsRepeated(&hint_entries_);
  }

  const ASTIntLiteral* num_shards_hint_ = nullptr;
  absl::Span<const ASTHintEntry* const> hint_entries_;
};

class ASTHintEntry final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HINT_ENTRY;

  ASTHintEntry() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* qualifier() const { return qualifier_; }
  const ASTIdentifier* name() const { return name_; }

  // Value is always an identifier, literal, or parameter.
  const ASTExpression* value() const { return value_; }

 private:
  void InitFields() final {
    // We need a special case here because we have two children that both have
    // type ASTIdentifier and the first one is optional.
    if (num_children() == 2) {
      FieldLoader fl(this);
      fl.AddRequired(&name_);
      fl.AddRequired(&value_);
    } else {
      FieldLoader fl(this);
      fl.AddRequired(&qualifier_);
      fl.AddRequired(&name_);
      fl.AddRequired(&value_);
    }
  }

  const ASTIdentifier* qualifier_ = nullptr;  // May be NULL
  const ASTIdentifier* name_ = nullptr;
  const ASTExpression* value_ = nullptr;
};

class ASTOptionsList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_OPTIONS_LIST;

  ASTOptionsList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTOptionsEntry* const>& options_entries() const {
    return options_entries_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&options_entries_);
  }

  absl::Span<const ASTOptionsEntry* const> options_entries_;
};

class ASTOptionsEntry final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_OPTIONS_ENTRY;

  ASTOptionsEntry() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* name() const { return name_; }

  // Value is always an identifier, literal, or parameter.
  const ASTExpression* value() const { return value_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&value_);
  }

  const ASTIdentifier* name_ = nullptr;
  const ASTExpression* value_ = nullptr;
};

// Common superclass of CREATE statements supporting the common
// modifiers:
//   CREATE [OR REPLACE] [TEMP|PUBLIC|PRIVATE] <object> [IF NOT EXISTS].
class ASTCreateStatement : public ASTStatement {
 public:
  explicit ASTCreateStatement(ASTNodeKind kind) : ASTStatement(kind) {}
  enum Scope {
    DEFAULT_SCOPE = 0,
    PRIVATE,
    PUBLIC,
    TEMPORARY,
  };

  enum SqlSecurity {
    SQL_SECURITY_UNSPECIFIED = 0,
    SQL_SECURITY_DEFINER,
    SQL_SECURITY_INVOKER,
  };

  // This adds the modifiers is_temp, etc, to the node name.
  std::string SingleNodeDebugString() const override;

  Scope scope() const {
    return scope_;
  }
  void set_scope(Scope scope) {
    scope_ = scope;
  }

  bool is_default_scope() const { return scope_ == DEFAULT_SCOPE; }
  bool is_private() const { return scope_ == PRIVATE; }
  bool is_public() const { return scope_ == PUBLIC; }
  bool is_temp() const { return scope_ == TEMPORARY; }

  bool is_or_replace() const { return is_or_replace_; }
  void set_is_or_replace(bool value) { is_or_replace_ = value; }

  bool is_if_not_exists() const { return is_if_not_exists_; }
  void set_is_if_not_exists(bool value) { is_if_not_exists_ = value; }

 protected:
  virtual void CollectModifiers(std::vector<std::string>* modifiers) const;

 private:
  Scope scope_ = DEFAULT_SCOPE;
  bool is_or_replace_ = false;
  bool is_if_not_exists_ = false;
};

class ASTFunctionParameter final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FUNCTION_PARAMETER;

  ASTFunctionParameter() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  const ASTIdentifier* name() const { return name_; }
  const ASTType* type() const { return type_; }
  const ASTTemplatedParameterType* templated_parameter_type() const {
    return templated_parameter_type_;
  }
  const ASTTVFSchema* tvf_schema() const { return tvf_schema_; }
  const ASTAlias* alias() const { return alias_; }

  bool IsTableParameter() const;
  bool IsTemplated() const {
    return templated_parameter_type_ != nullptr;
  }

  bool is_not_aggregate() const { return is_not_aggregate_; }
  void set_is_not_aggregate(bool value) { is_not_aggregate_ = value; }

  enum class ProcedureParameterMode {
    NOT_SET = 0,
    IN,
    OUT,
    INOUT,
  };
  static std::string ProcedureParameterModeToString(
      ProcedureParameterMode mode);

  ProcedureParameterMode procedure_parameter_mode() const {
    return procedure_parameter_mode_;
  }
  void set_procedure_parameter_mode(ProcedureParameterMode mode) {
    procedure_parameter_mode_ = mode;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&name_, AST_IDENTIFIER);
    fl.AddOptionalType(&type_);
    fl.AddOptional(&templated_parameter_type_,
                   AST_TEMPLATED_PARAMETER_TYPE);
    fl.AddOptional(&tvf_schema_, AST_TVF_SCHEMA);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTIdentifier* name_ = nullptr;
  const ASTAlias* alias_ = nullptr;

  // Only one of <type_>, <templated_parameter_type_>, or <tvf_schema_>
  // will be set.
  //
  // This is the type for concrete scalar parameters.
  const ASTType* type_ = nullptr;
  // This indicates a templated parameter type, which may be either a
  // templated scalar type (ANY PROTO, ANY STRUCT, etc.) or templated table
  // type as indicated by its kind().
  const ASTTemplatedParameterType* templated_parameter_type_ = nullptr;
  // Only allowed for table-valued functions, indicating a table type
  // parameter.
  const ASTTVFSchema* tvf_schema_ = nullptr;

  // True if the NOT AGGREGATE modifier is present.
  bool is_not_aggregate_ = false;

  // Function parameter doesn't use this field and always has value NOT_SET.
  // Procedure parameter should have this field set during parsing.
  ProcedureParameterMode procedure_parameter_mode_ =
      ProcedureParameterMode::NOT_SET;
};

class ASTFunctionParameters final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_FUNCTION_PARAMETERS;

  ASTFunctionParameters() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<
      const ASTFunctionParameter* const>& parameter_entries() const {
    return parameter_entries_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&parameter_entries_);
  }

  absl::Span<const ASTFunctionParameter* const> parameter_entries_;
};

class ASTFunctionDeclaration final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_FUNCTION_DECLARATION;

  ASTFunctionDeclaration() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTFunctionParameters* parameters() const { return parameters_; }

  // Returns whether or not any of the <parameters_> are templated.
  bool IsTemplated() const;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&parameters_);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTFunctionParameters* parameters_ = nullptr;
};

class ASTSqlFunctionBody final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SQL_FUNCTION_BODY;

  ASTSqlFunctionBody() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&expression_);
  }

  const ASTExpression* expression_ = nullptr;
};

// This represents a call to a table-valued function (TVF). Each TVF returns an
// entire output relation instead of a single scalar value. The enclosing query
// may refer to the TVF as if it were a table subquery. The TVF may accept
// scalar arguments and/or other input relations.
class ASTTVF final : public ASTTableExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TVF;

  ASTTVF() : ASTTableExpression(kConcreteNodeKind) {}

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const absl::Span<const ASTTVFArgument* const>& argument_entries() const {
    return argument_entries_;
  }
  const ASTHint* hint() const { return hint_; }
  const ASTAlias* alias() const override { return alias_; }
  const ASTSampleClause* sample() const { return sample_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRepeatedWhileIsNodeKind(&argument_entries_, AST_TVF_ARGUMENT);
    fl.AddOptional(&hint_, AST_HINT);
    fl.AddOptional(&alias_, AST_ALIAS);
    fl.AddOptional(&sample_, AST_SAMPLE_CLAUSE);
  }

  const ASTPathExpression* name_ = nullptr;
  absl::Span<const ASTTVFArgument* const> argument_entries_;
  const ASTHint* hint_ = nullptr;
  const ASTAlias* alias_ = nullptr;
  const ASTSampleClause* sample_ = nullptr;
};

// This represents a clause of form "TABLE <target>", where <target> is either
// a path expression representing a table name, or <target> is a TVF call.
// It is currently only supported for relation arguments to table-valued
// functions.
class ASTTableClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TABLE_CLAUSE;

  ASTTableClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* table_path() const { return table_path_; }
  const ASTTVF* tvf() const { return tvf_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&table_path_, AST_PATH_EXPRESSION);
    fl.AddOptional(&tvf_, AST_TVF);
  }

  // Exactly one of these will be non-null.
  const ASTPathExpression* table_path_ = nullptr;
  const ASTTVF* tvf_ = nullptr;
};

// This represents a clause of form "MODEL <target>", where <target> is a model
// name.
class ASTModelClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_MODEL_CLAUSE;

  ASTModelClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* model_path() const { return model_path_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&model_path_);
  }

  // Exactly one of these will be non-null.
  const ASTPathExpression* model_path_ = nullptr;
};

// This represents a clause of form "CONNECTION <target>", where <target> is a
// connection name.
class ASTConnectionClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CONNECTION_CLAUSE;

  ASTConnectionClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* connection_path() const { return connection_path_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&connection_path_);
  }

  const ASTPathExpression* connection_path_ = nullptr;
};

// This represents an argument to a table-valued function (TVF). ZetaSQL can
// parse the argument in one of the following ways:
//
// (1) ZetaSQL parses the argument as an expression; if any arguments are
//     table subqueries then ZetaSQL will parse them as subquery expressions
//     and the resolver may interpret them as needed later. In this case the
//     expr_ of this class is filled.
//
// (2) ZetaSQL parses the argument as "TABLE path"; this syntax represents a
//     table argument including all columns in the named table. In this case the
//     table_clause_ of this class is non-empty.
//
// (3) ZetaSQL parses the argument as "MODEL path"; this syntax represents a
//     model argument. In this case the model_clause_ of this class is
//     non-empty.
//
// (4) ZetaSQL parses the argument as "CONNECTION path"; this syntax
//     represents a connection argument. In this case the connection_clause_ of
//     this class is non-empty.
//
// (5) ZetaSQL parses the argument as a named argument; this behaves like when
//     the argument is an expression with the extra requirement that the
//     resolver rearranges the provided named arguments to match the required
//     argument names from the function signature, if present. The named
//     argument is stored in the expr_ of this class in this case since an
//     ASTNamedArgument is a subclass of ASTExpression.
// (6) ZetaSQL parses the argument as "DESCRIPTOR"; this syntax represents a
//    descriptor on a list of columns with optional types.
class ASTTVFArgument final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TVF_ARGUMENT;

  ASTTVFArgument() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }
  const ASTTableClause* table_clause() const { return table_clause_; }
  const ASTModelClause* model_clause() const { return model_clause_; }
  const ASTConnectionClause* connection_clause() const {
    return connection_clause_;
  }
  const ASTDescriptor* descriptor() const { return descriptor_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&expr_);
    fl.AddOptional(&table_clause_, AST_TABLE_CLAUSE);
    fl.AddOptional(&model_clause_, AST_MODEL_CLAUSE);
    fl.AddOptional(&connection_clause_, AST_CONNECTION_CLAUSE);
    fl.AddOptional(&descriptor_, AST_DESCRIPTOR);
  }

  // Only one of the following may be non-null.
  const ASTExpression* expr_ = nullptr;
  const ASTTableClause* table_clause_ = nullptr;
  const ASTModelClause* model_clause_ = nullptr;
  const ASTConnectionClause* connection_clause_ = nullptr;
  const ASTDescriptor* descriptor_ = nullptr;
};

// This represents a CREATE CONSTANT statement, i.e.,
// CREATE [OR REPLACE] [TEMP|TEMPORARY|PUBLIC|PRIVATE] CONSTANT
//   [IF NOT EXISTS] <name_path> = <expression>;
class ASTCreateConstantStatement final : public ASTCreateStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_CONSTANT_STATEMENT;

  ASTCreateConstantStatement() : ASTCreateStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTExpression* expr() const { return expr_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&expr_);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTExpression* expr_ = nullptr;
};

// This represents a CREATE DATABASE statement, i.e.,
// CREATE DATABASE <name> [OPTIONS (name=value, ...)];
class ASTCreateDatabaseStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_DATABASE_STATEMENT;

  ASTCreateDatabaseStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
};

// This is the common superclass of CREATE FUNCTION and CREATE TABLE FUNCTION
// statements. It contains all fields shared between the two types of
// statements, including the function declaration, return type, OPTIONS list,
// and string body (if present).
class ASTCreateFunctionStmtBase : public ASTCreateStatement {
 public:
  enum DeterminismLevel {
    DETERMINISM_UNSPECIFIED = 0,
    DETERMINISTIC,
    NOT_DETERMINISTIC,
    IMMUTABLE,
    STABLE,
    VOLATILE,
  };
  explicit ASTCreateFunctionStmtBase(ASTNodeKind kind)
      : ASTCreateStatement(kind) {}

  std::string SingleNodeDebugString() const override;

  const ASTFunctionDeclaration* function_declaration() const {
    return function_declaration_;
  }
  const ASTIdentifier* language() const { return language_; }
  const ASTStringLiteral* code() const { return code_; }
  const ASTOptionsList* options_list() const { return options_list_; }

  const DeterminismLevel determinism_level() const {
    return determinism_level_;
  }
  void set_determinism_level(DeterminismLevel level) {
    determinism_level_ = level;
  }

  SqlSecurity sql_security() const { return sql_security_; }
  void set_sql_security(SqlSecurity sql_security) {
    sql_security_ = sql_security;
  }
  std::string GetSqlForSqlSecurity() const;
  std::string GetSqlForDeterminismLevel() const;

 protected:
  const ASTFunctionDeclaration* function_declaration_ = nullptr;
  const ASTIdentifier* language_ = nullptr;
  // For external-language functions.
  const ASTStringLiteral* code_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  SqlSecurity sql_security_;
  DeterminismLevel determinism_level_ =
      DeterminismLevel::DETERMINISM_UNSPECIFIED;
};

// This may represent an "external language" function (e.g., implemented in a
// non-SQL programming language such as JavaScript) or a "sql" function.
// Note that some combinations of field setting can represent functions that are
// not actually valid due to optional members that would be inappropriate for
// one type of function or another; validity of the parsed function must be
// checked by the analyzer.
class ASTCreateFunctionStatement final : public ASTCreateFunctionStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_FUNCTION_STATEMENT;

  ASTCreateFunctionStatement() : ASTCreateFunctionStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  bool is_aggregate() const { return is_aggregate_; }
  void set_is_aggregate(bool value) { is_aggregate_ = value; }

  const ASTType* return_type() const { return return_type_; }

  const ASTSqlFunctionBody* sql_function_body() const {
    return sql_function_body_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&function_declaration_);
    fl.AddOptionalType(&return_type_);
    fl.AddOptional(&language_, AST_IDENTIFIER);
    fl.AddOptional(&code_, AST_STRING_LITERAL);
    fl.AddOptional(&sql_function_body_, AST_SQL_FUNCTION_BODY);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
  }

  bool is_aggregate_ = false;
  const ASTType* return_type_ = nullptr;
  // For SQL functions.
  const ASTSqlFunctionBody* sql_function_body_ = nullptr;
};

class ASTCreateProcedureStatement final : public ASTCreateStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_PROCEDURE_STATEMENT;

  ASTCreateProcedureStatement() : ASTCreateStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  const ASTPathExpression* name() const { return name_; }
  const ASTFunctionParameters* parameters() const { return parameters_; }
  const ASTOptionsList* options_list() const { return options_list_; }
  const ASTBeginEndBlock* begin_end_block() const { return begin_end_block_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&parameters_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddRequired(&begin_end_block_);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTFunctionParameters* parameters_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  const ASTBeginEndBlock* begin_end_block_ = nullptr;
};

// This represents a table-valued function declaration statement in ZetaSQL,
// using the CREATE TABLE FUNCTION syntax. Note that some combinations of field
// settings can represent functions that are not actually valid, since optional
// members may be inappropriate for one type of function or another; validity of
// the parsed function must be checked by the analyzer.
class ASTCreateTableFunctionStatement final : public ASTCreateFunctionStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_TABLE_FUNCTION_STATEMENT;

  ASTCreateTableFunctionStatement()
      : ASTCreateFunctionStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  const ASTTVFSchema* return_tvf_schema() const {
    return return_tvf_schema_;
  }
  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&function_declaration_);
    fl.AddOptional(&return_tvf_schema_, AST_TVF_SCHEMA);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&language_, AST_IDENTIFIER);
    fl.AddOptional(&code_, AST_STRING_LITERAL);
    fl.AddOptional(&query_, AST_QUERY);
  }

  const ASTTVFSchema* return_tvf_schema_ = nullptr;
  const ASTQuery* query_ = nullptr;
};

class ASTCreateTableStmtBase : public ASTCreateStatement {
 public:
  explicit ASTCreateTableStmtBase(const ASTNodeKind kConcreteNodeKind)
      : ASTCreateStatement(kConcreteNodeKind) {}

  const ASTPathExpression* name() const { return name_; }
  const ASTTableElementList* table_element_list() const {
    return table_element_list_;
  }
  const ASTPartitionBy* partition_by() const { return partition_by_; }
  const ASTClusterBy* cluster_by() const { return cluster_by_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 protected:
  void InitCreateTableStmtBaseProperties(FieldLoader* fl,
                                         bool is_option_list_required) {
    fl->AddRequired(&name_);
    fl->AddOptional(&table_element_list_, AST_TABLE_ELEMENT_LIST);
    fl->AddOptional(&partition_by_, AST_PARTITION_BY);
    fl->AddOptional(&cluster_by_, AST_CLUSTER_BY);
    if (is_option_list_required) {
      fl->AddRequired(&options_list_);
    } else {
      fl->AddOptional(&options_list_, AST_OPTIONS_LIST);
    }
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTTableElementList* table_element_list_ = nullptr;  // May be NULL.
  const ASTPartitionBy* partition_by_ = nullptr;             // May be NULL.
  const ASTClusterBy* cluster_by_ = nullptr;                 // May be NULL.
  const ASTOptionsList* options_list_ = nullptr;             // May be NULL.
};

class ASTCreateTableStatement final : public ASTCreateTableStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CREATE_TABLE_STATEMENT;

  ASTCreateTableStatement() : ASTCreateTableStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitCreateTableStmtBaseProperties(&fl,
                                      /* is_option_list_required= */ false);
    fl.AddOptional(&query_, AST_QUERY);
  }

  const ASTQuery* query_ = nullptr;                          // May be NULL.
};

class ASTTransformClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TRANSFORM_CLAUSE;

  ASTTransformClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTSelectList* select_list() const { return select_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&select_list_);
  }

  const ASTSelectList* select_list_ = nullptr;
};

class ASTCreateModelStatement final : public ASTCreateStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CREATE_MODEL_STATEMENT;

  ASTCreateModelStatement() : ASTCreateStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTTransformClause* transform_clause() const {
    return transform_clause_;
  }
  const ASTOptionsList* options_list() const { return options_list_; }
  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&transform_clause_, AST_TRANSFORM_CLAUSE);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&query_, AST_QUERY);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTTransformClause* transform_clause_ = nullptr;  // May be NULL.
  const ASTOptionsList* options_list_ = nullptr;          // May be NULL.
  const ASTQuery* query_ = nullptr;                       // May be NULL.
};

// Represents the list of expressions used to order an index.
class ASTIndexItemList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_INDEX_ITEM_LIST;

  ASTIndexItemList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<
      const ASTOrderingExpression* const>& ordering_expressions() const {
    return ordering_expressions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&ordering_expressions_);
  }

  absl::Span<const ASTOrderingExpression* const> ordering_expressions_;
};

// Represents the list of expressions being used in the STORING clause of an
// index.
class ASTIndexStoringExpressionList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_INDEX_STORING_EXPRESSION_LIST;

  ASTIndexStoringExpressionList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& expressions() const {
    return expressions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&expressions_);
  }

  absl::Span<const ASTExpression* const> expressions_;
};

// Represents the list of unnest expressions for create_index.
class ASTIndexUnnestExpressionList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_INDEX_UNNEST_EXPRESSION_LIST;

  ASTIndexUnnestExpressionList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTUnnestExpressionWithOptAliasAndOffset* const>&
  unnest_expressions() const {
    return unnest_expressions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&unnest_expressions_);
  }

  absl::Span<const ASTUnnestExpressionWithOptAliasAndOffset* const>
      unnest_expressions_;
};

// Represents a CREATE INDEX statement.
class ASTCreateIndexStatement final : public ASTCreateStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_INDEX_STATEMENT;

  ASTCreateIndexStatement() : ASTCreateStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTPathExpression* table_name() const { return table_name_; }
  const ASTAlias* optional_table_alias() const { return optional_table_alias_; }
  const ASTIndexUnnestExpressionList* optional_index_unnest_expression_list()
      const {
    return optional_index_unnest_expression_list_;
  }
  const ASTIndexItemList* index_item_list() const {
    return index_item_list_;
  }
  const ASTIndexStoringExpressionList* optional_index_storing_expressions()
      const {
    return optional_index_storing_expressions_;
  }
  const ASTOptionsList* options_list() const { return options_list_; }
  bool is_unique() const { return is_unique_; }

  void set_is_unique(bool is_unique) { is_unique_ = is_unique; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&table_name_);
    fl.AddOptional(&optional_table_alias_, AST_ALIAS);
    fl.AddOptional(&optional_index_unnest_expression_list_,
                   AST_INDEX_UNNEST_EXPRESSION_LIST);
    fl.AddRequired(&index_item_list_);
    fl.AddOptional(&optional_index_storing_expressions_,
                   AST_INDEX_STORING_EXPRESSION_LIST);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTPathExpression* table_name_ = nullptr;
  const ASTAlias* optional_table_alias_ = nullptr;  // Optional
  const ASTIndexUnnestExpressionList* optional_index_unnest_expression_list_ =
      nullptr;  // Optional
  const ASTIndexItemList* index_item_list_ = nullptr;
  const ASTIndexStoringExpressionList* optional_index_storing_expressions_ =
      nullptr;  // Optional
  const ASTOptionsList* options_list_ = nullptr;
  bool is_unique_ = false;
};

class ASTCreateRowAccessPolicyStatement final : public ASTCreateStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_ROW_ACCESS_POLICY_STATEMENT;

  ASTCreateRowAccessPolicyStatement() : ASTCreateStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* name() const { return name_; }
  const ASTPathExpression* target_path() const { return target_path_; }
  const ASTGrantToClause* grant_to() const { return grant_to_; }
  const ASTFilterUsingClause* filter_using() const { return filter_using_; }

  bool has_access_keyword() const { return has_access_keyword_; }
  void set_has_access_keyword(bool value) { has_access_keyword_ = value; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&name_, AST_IDENTIFIER);
    fl.AddRequired(&target_path_);
    fl.AddOptional(&grant_to_, AST_GRANT_TO_CLAUSE);
    fl.AddRequired(&filter_using_);
  }

  const ASTIdentifier* name_ = nullptr;                 // Optional
  const ASTPathExpression* target_path_ = nullptr;      // Required
  const ASTGrantToClause* grant_to_ = nullptr;          // Optional
  const ASTFilterUsingClause* filter_using_ = nullptr;  // Required

  bool has_access_keyword_ = false;
};

class ASTCreateViewStatementBase : public ASTCreateStatement {
 public:
  explicit ASTCreateViewStatementBase(const ASTNodeKind kind)
      : ASTCreateStatement(kind) {}

  std::string GetSqlForSqlSecurity() const;

  const ASTPathExpression* name() const { return name_; }
  const ASTOptionsList* options_list() const { return options_list_; }
  const ASTQuery* query() const { return query_; }
  bool recursive() const { return recursive_; }

  SqlSecurity sql_security() const { return sql_security_; }
  void set_sql_security(SqlSecurity sql_security) {
    sql_security_ = sql_security;
  }
  void set_recursive(bool recursive) { recursive_ = recursive; }

 protected:
  void CollectModifiers(std::vector<std::string>* modifiers) const override;

  const ASTPathExpression* name_ = nullptr;
  SqlSecurity sql_security_;
  const ASTOptionsList* options_list_ = nullptr;
  const ASTQuery* query_ = nullptr;
  bool recursive_ = false;
};

class ASTCreateViewStatement final : public ASTCreateViewStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CREATE_VIEW_STATEMENT;

  ASTCreateViewStatement() : ASTCreateViewStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddRequired(&query_);
  }
};

// The gen_extra_files.sh can only parse the class definition that is on the
// single line.
class ASTCreateMaterializedViewStatement final : public ASTCreateViewStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_MATERIALIZED_VIEW_STATEMENT;

  ASTCreateMaterializedViewStatement()
      : ASTCreateViewStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPartitionBy* partition_by() const { return partition_by_; }
  const ASTClusterBy* cluster_by() const { return cluster_by_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddOptional(&partition_by_, AST_PARTITION_BY);
    fl.AddOptional(&cluster_by_, AST_CLUSTER_BY);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddRequired(&query_);
  }

  const ASTPartitionBy* partition_by_ = nullptr;             // May be NULL.
  const ASTClusterBy* cluster_by_ = nullptr;                 // May be NULL.
};

class ASTExportDataStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_EXPORT_DATA_STATEMENT;

  ASTExportDataStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTOptionsList* options_list() const { return options_list_; }
  const ASTWithConnectionClause* with_connection_clause() const {
    return with_connection_;
  }
  const ASTQuery* query() const { return query_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&with_connection_, AST_WITH_CONNECTION_CLAUSE);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddRequired(&query_);
  }

  const ASTWithConnectionClause* with_connection_ = nullptr;  // May be NULL.
  const ASTOptionsList* options_list_ = nullptr;  // May be NULL.
  const ASTQuery* query_ = nullptr;
};

class ASTCallStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CALL_STATEMENT;

  ASTCallStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* procedure_name() const { return procedure_name_; }
  const absl::Span<const ASTTVFArgument* const>& arguments() const {
    return arguments_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&procedure_name_);
    fl.AddRestAsRepeated(&arguments_);
  }

  const ASTPathExpression* procedure_name_ = nullptr;
  absl::Span<const ASTTVFArgument* const> arguments_;
};

class ASTDefineTableStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_DEFINE_TABLE_STATEMENT;

  ASTDefineTableStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* name() const { return name_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&options_list_);
  }

  const ASTPathExpression* name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
};

class ASTCreateExternalTableStatement final : public ASTCreateTableStmtBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_CREATE_EXTERNAL_TABLE_STATEMENT;
  ASTCreateExternalTableStatement()
      : ASTCreateTableStmtBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitCreateTableStmtBaseProperties(&fl, /* is_option_list_required= */ true);
  }
};

class ASTType : public ASTNode {
 public:
  explicit ASTType(ASTNodeKind kind) : ASTNode(kind) {}

  bool IsType() const override { return true; }
};

// TODO This takes a PathExpression and isn't really a simple type.
// Calling this NamedType or TypeName may be more appropriate.
class ASTSimpleType final : public ASTType {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SIMPLE_TYPE;

  ASTSimpleType() : ASTType(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* type_name() const { return type_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&type_name_);
  }

  const ASTPathExpression* type_name_ = nullptr;
};

class ASTArrayType final : public ASTType {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ARRAY_TYPE;

  ASTArrayType() : ASTType(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTType* element_type() const { return element_type_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&element_type_);
  }

  const ASTType* element_type_ = nullptr;
};

class ASTStructType final : public ASTType {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STRUCT_TYPE;

  ASTStructType() : ASTType(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTStructField* const>& struct_fields() const {
    return struct_fields_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&struct_fields_);
  }

  absl::Span<const ASTStructField* const> struct_fields_;
};

class ASTStructField final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STRUCT_FIELD;

  ASTStructField() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // name_ will be NULL for anonymous fields like in STRUCT<int, string>.
  const ASTIdentifier* name() const { return name_; }
  const ASTType* type() const { return type_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&name_, AST_IDENTIFIER);
    fl.AddRequired(&type_);
  }

  const ASTIdentifier* name_ = nullptr;
  const ASTType* type_ = nullptr;
};

class ASTTemplatedParameterType final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_TEMPLATED_PARAMETER_TYPE;
  enum TemplatedTypeKind {
    UNINITIALIZED = 0,
    ANY_TYPE,
    ANY_PROTO,
    ANY_ENUM,
    ANY_STRUCT,
    ANY_ARRAY,
    ANY_TABLE,
  };

  ASTTemplatedParameterType() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  TemplatedTypeKind kind() const { return kind_; }
  void set_kind(TemplatedTypeKind kind) { kind_ = kind; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }

  TemplatedTypeKind kind_ = UNINITIALIZED;
};

// This represents a relation argument or return type for a table-valued
// function (TVF). The resolver can convert each ASTTVFSchema directly into a
// TVFRelation object suitable for use in TVF signatures. For more information
// about the TVFRelation object, please refer to public/table_valued_function.h.
// TODO: Change the names of these objects to make them generic and
// re-usable wherever we want to represent the schema of some intermediate or
// final table. Same for ASTTVFSchemaColumn.
class ASTTVFSchema final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TVF_SCHEMA;

  ASTTVFSchema() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTTVFSchemaColumn* const>& columns() const {
    return columns_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&columns_);
  }

  absl::Span<const ASTTVFSchemaColumn* const> columns_;
};

// This represents one column of a relation argument or return value for a
// table-valued function (TVF). It contains the name and type of the column.
class ASTTVFSchemaColumn final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TVF_SCHEMA_COLUMN;

  ASTTVFSchemaColumn() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // name_ will be NULL for value tables.
  const ASTIdentifier* name() const { return name_; }
  const ASTType* type() const { return type_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&name_, AST_IDENTIFIER);
    fl.AddRequired(&type_);
  }

  const ASTIdentifier* name_ = nullptr;
  const ASTType* type_ = nullptr;
};

// This represents the value DEFAULT that shows up in DML statements.
// It will not show up as a general expression anywhere else.
class ASTDefaultLiteral final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DEFAULT_LITERAL;

  ASTDefaultLiteral() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader(this);  // Triggers check that there were no children.
  }
};

class ASTAssertStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ASSERT_STATEMENT;

  ASTAssertStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }
  const ASTStringLiteral* description() const { return description_; }

 private:
  void InitFields() final {
    FieldLoader f1(this);
    f1.AddRequired(&expr_);
    f1.AddOptional(&description_, AST_STRING_LITERAL);
  }

  const ASTExpression* expr_ = nullptr;            // Required
  const ASTStringLiteral* description_ = nullptr;  // Optional
};

class ASTAssertRowsModified final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ASSERT_ROWS_MODIFIED;

  ASTAssertRowsModified() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* num_rows() const { return num_rows_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&num_rows_);
  }

  const ASTExpression* num_rows_ = nullptr;  // Required
};

// This is used for both top-level DELETE statements and for nested DELETEs
// inside ASTUpdateItem. When used at the top-level, the target is always a
// path expression.
class ASTDeleteStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DELETE_STATEMENT;

  ASTDeleteStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested DELETE.
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;

  const ASTGeneralizedPathExpression* GetTargetPathForNested() const {
    return target_path_;
  }
  const ASTAlias* alias() const { return alias_; }
  const ASTWithOffset* offset() const { return offset_; }
  const ASTExpression* where() const { return where_; }
  const ASTAssertRowsModified* assert_rows_modified() const {
    return assert_rows_modified_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&target_path_);
    fl.AddOptional(&alias_, AST_ALIAS);
    fl.AddOptional(&offset_, AST_WITH_OFFSET);
    fl.AddOptionalExpression(&where_);
    fl.AddOptional(&assert_rows_modified_, AST_ASSERT_ROWS_MODIFIED);
  }

  const ASTGeneralizedPathExpression* target_path_ = nullptr;    // Required
  const ASTAlias* alias_ = nullptr;                              // Optional
  const ASTWithOffset* offset_ = nullptr;                        // Optional
  const ASTExpression* where_ = nullptr;                         // Optional
  const ASTAssertRowsModified* assert_rows_modified_ = nullptr;  // Optional
};

class ASTColumnAttribute : public ASTNode {
 public:
  explicit ASTColumnAttribute(ASTNodeKind kind) : ASTNode(kind) {}
  virtual std::string SingleNodeSqlString() const = 0;
};

class ASTNotNullColumnAttribute final : public ASTColumnAttribute {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_NOT_NULL_COLUMN_ATTRIBUTE;

  ASTNotNullColumnAttribute() : ASTColumnAttribute(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeSqlString() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }
};

class ASTHiddenColumnAttribute final : public ASTColumnAttribute {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_HIDDEN_COLUMN_ATTRIBUTE;

  ASTHiddenColumnAttribute() : ASTColumnAttribute(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeSqlString() const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }
};

class ASTPrimaryKeyColumnAttribute final : public ASTColumnAttribute {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_PRIMARY_KEY_COLUMN_ATTRIBUTE;

  ASTPrimaryKeyColumnAttribute() : ASTColumnAttribute(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeSqlString() const override;

  bool enforced() const { return enforced_; }
  void set_enforced(bool enforced) { enforced_ = enforced; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }

  bool enforced_ = true;
};

class ASTForeignKeyColumnAttribute final : public ASTColumnAttribute {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_FOREIGN_KEY_COLUMN_ATTRIBUTE;

  ASTForeignKeyColumnAttribute() : ASTColumnAttribute(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeSqlString() const override;

  const ASTIdentifier* constraint_name() const { return constraint_name_; }
  const ASTForeignKeyReference* reference() const { return reference_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&constraint_name_, AST_IDENTIFIER);
    fl.AddRequired(&reference_);
  }

  const ASTIdentifier* constraint_name_ = nullptr;
  const ASTForeignKeyReference* reference_ = nullptr;
};

class ASTColumnAttributeList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_COLUMN_ATTRIBUTE_LIST;

  ASTColumnAttributeList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTColumnAttribute* const>& values() const {
    return values_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&values_);
  }
  absl::Span<const ASTColumnAttribute* const> values_;
};

// A column schema identifies the column type and the column annotations.
// The annotations consist of the column attributes and the column options.
//
// This class is used only in column definitions of CREATE TABLE statements,
// and is unrelated to CREATE SCHEMA despite the usage of the overloaded term
// "schema".
//
// The hierarchy of column schema is similar to the type hierarchy.
// The annotations can be applied on struct fields or array elements, for
// example, as in STRUCT<x INT64 NOT NULL, y STRING OPTIONS(foo="bar")>.
// In this case, some column attributes, such as PRIMARY KEY and HIDDEN, are
// disallowed as field attributes.
class ASTColumnSchema : public ASTNode {
 public:
  explicit ASTColumnSchema(ASTNodeKind kind) : ASTNode(kind) {}
  const ASTColumnAttributeList* attributes() const {
    return attributes_;
  }
  const ASTGeneratedColumnInfo* generated_column_info() const {
    return generated_column_info_;
  }
  const ASTOptionsList* options_list() const { return options_list_; }

  // Helper method that returns true if the attributes()->values() contains an
  // ASTColumnAttribute with the node->kind() equal to 'node_kind'.
  bool ContainsAttribute(ASTNodeKind node_kind) const;

  template <typename T>
  std::vector<const T*> FindAttributes(ASTNodeKind node_kind) const {
    std::vector<const T*> found;
    if (attributes() == nullptr) {
      return found;
    }
    for (const ASTColumnAttribute* attribute : attributes()->values()) {
      if (attribute->node_kind() == node_kind) {
        found.push_back(static_cast<const T*>(attribute));
      }
    }
    return found;
  }

 protected:
  const ASTColumnAttributeList** mutable_attributes_ptr() {
    return &attributes_;
  }
  const ASTGeneratedColumnInfo** mutable_generated_column_info_ptr() {
    return &generated_column_info_;
  }
  const ASTOptionsList** mutable_options_list_ptr() {
    return &options_list_;
  }

 private:
  const ASTColumnAttributeList* attributes_ = nullptr;
  const ASTGeneratedColumnInfo* generated_column_info_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
};

class ASTSimpleColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SIMPLE_COLUMN_SCHEMA;

  ASTSimpleColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* type_name() const { return type_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&type_name_);
    fl.AddOptional(mutable_generated_column_info_ptr(),
                   AST_GENERATED_COLUMN_INFO);
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }

  const ASTPathExpression* type_name_ = nullptr;
};

class ASTArrayColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ARRAY_COLUMN_SCHEMA;

  ASTArrayColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTColumnSchema* element_schema() const { return element_schema_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&element_schema_);
    fl.AddOptional(mutable_generated_column_info_ptr(),
                   AST_GENERATED_COLUMN_INFO);
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }

  const ASTColumnSchema* element_schema_ = nullptr;
};

class ASTStructColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STRUCT_COLUMN_SCHEMA;

  ASTStructColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTStructColumnField* const>& struct_fields() const {
    return struct_fields_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRepeatedWhileIsNodeKind(&struct_fields_, AST_STRUCT_COLUMN_FIELD);
    fl.AddOptional(mutable_generated_column_info_ptr(),
                   AST_GENERATED_COLUMN_INFO);
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }

  absl::Span<const ASTStructColumnField* const> struct_fields_;
};

class ASTInferredTypeColumnSchema final : public ASTColumnSchema {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_INFERRED_TYPE_COLUMN_SCHEMA;

  ASTInferredTypeColumnSchema() : ASTColumnSchema(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(mutable_generated_column_info_ptr());
    fl.AddOptional(mutable_attributes_ptr(), AST_COLUMN_ATTRIBUTE_LIST);
    fl.AddOptional(mutable_options_list_ptr(), AST_OPTIONS_LIST);
  }
};

class ASTStructColumnField final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STRUCT_COLUMN_FIELD;

  ASTStructColumnField() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // name_ will be NULL for anonymous fields like in STRUCT<int, string>.
  const ASTIdentifier* name() const { return name_; }
  const ASTColumnSchema* schema() const { return schema_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&name_, AST_IDENTIFIER);
    fl.AddRequired(&schema_);
  }

  const ASTIdentifier* name_ = nullptr;
  const ASTColumnSchema* schema_ = nullptr;
};

class ASTGeneratedColumnInfo final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GENERATED_COLUMN_INFO;

  ASTGeneratedColumnInfo() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  // Adds is_stored (if needed) to the debug string.
  std::string SingleNodeDebugString() const override;

  const ASTExpression* expression() const { return expression_; }
  bool is_stored() const { return is_stored_; }
  void set_is_stored(bool is_stored) { is_stored_ = is_stored; }
  bool is_on_write() const { return is_on_write_; }
  void set_is_on_write(bool is_on_write) { is_on_write_ = is_on_write; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
  }

  const ASTExpression* expression_ = nullptr;
  bool is_stored_ = false;
  bool is_on_write_ = false;
};

// Base class for CREATE TABLE elements, including column definitions and
// table constraints.
class ASTTableElement : public ASTNode {
 public:
  explicit ASTTableElement(ASTNodeKind kind) : ASTNode(kind) {}
};

class ASTColumnDefinition final : public ASTTableElement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_COLUMN_DEFINITION;

  ASTColumnDefinition() : ASTTableElement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* name() const { return name_; }
  const ASTColumnSchema* schema() const { return schema_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    fl.AddRequired(&schema_);
  }

  const ASTIdentifier* name_ = nullptr;
  const ASTColumnSchema* schema_ = nullptr;
};

// Base class for constraints, including primary key, foreign key and check
// constraints.
class ASTTableConstraint : public ASTTableElement {
 public:
  explicit ASTTableConstraint(ASTNodeKind kind) : ASTTableElement(kind) {}

  const ASTIdentifier* constraint_name() const {
    return constraint_name_;
  }

 protected:
  const ASTIdentifier* constraint_name_ = nullptr;
};

class ASTPrimaryKey final : public ASTTableConstraint {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PRIMARY_KEY;

  ASTPrimaryKey() : ASTTableConstraint(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  void set_enforced(bool enforced) { enforced_ = enforced; }

  const ASTColumnList* column_list() const { return column_list_; }
  const ASTOptionsList* options_list() const { return options_list_; }
  bool enforced() const { return enforced_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&column_list_, AST_COLUMN_LIST);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
  }

  const ASTColumnList* column_list_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  bool enforced_ = true;
};

class ASTForeignKey final : public ASTTableConstraint {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOREIGN_KEY;

  ASTForeignKey() : ASTTableConstraint(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTColumnList* column_list() const { return column_list_; }
  const ASTForeignKeyReference* reference() const { return reference_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_list_);
    fl.AddRequired(&reference_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&constraint_name_, AST_IDENTIFIER);
  }

  const ASTColumnList* column_list_ = nullptr;
  const ASTForeignKeyReference* reference_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
};

class ASTCheckConstraint final : public ASTTableConstraint {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CHECK_CONSTRAINT;

  ASTCheckConstraint() : ASTTableConstraint(kConcreteNodeKind) {}

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;
  const ASTExpression* expression() const { return expression_; }
  const ASTOptionsList* options_list() const { return options_list_; }
  const bool is_enforced() const { return is_enforced_; }
  void set_is_enforced(bool is_enforced) { is_enforced_ = is_enforced; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&options_list_, AST_OPTIONS_LIST);
    fl.AddOptional(&constraint_name_, AST_IDENTIFIER);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  bool is_enforced_ = true;
};

class ASTTableElementList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TABLE_ELEMENT_LIST;

  ASTTableElementList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTTableElement* const>& elements() const {
    return elements_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&elements_);
  }

  absl::Span<const ASTTableElement* const> elements_;
};

class ASTColumnList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_COLUMN_LIST;

  ASTColumnList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTIdentifier* const>& identifiers() const {
    return identifiers_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&identifiers_);
  }

  absl::Span<const ASTIdentifier* const> identifiers_;
};

class ASTColumnPosition final : public ASTNode {
 public:
  enum RelativePositionType {
    PRECEDING,
    FOLLOWING,
  };
  static constexpr ASTNodeKind kConcreteNodeKind = AST_COLUMN_POSITION;

  ASTColumnPosition() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  void set_type(RelativePositionType type) {
    position_type_ = type;
  }
  RelativePositionType type() const { return position_type_; }

  const ASTIdentifier* identifier() const { return identifier_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&identifier_);
  }

  RelativePositionType position_type_;
  const ASTIdentifier* identifier_;
};

class ASTInsertValuesRow final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_INSERT_VALUES_ROW;

  ASTInsertValuesRow() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // A row of values in a VALUES clause.  May include ASTDefaultLiteral.
  const absl::Span<const ASTExpression* const>& values() const {
    return values_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&values_);
  }

  absl::Span<const ASTExpression* const> values_;
};

class ASTInsertValuesRowList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_INSERT_VALUES_ROW_LIST;

  ASTInsertValuesRowList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTInsertValuesRow* const>& rows() const {
    return rows_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&rows_);
  }

  absl::Span<const ASTInsertValuesRow* const> rows_;
};

// This is used for both top-level INSERT statements and for nested INSERTs
// inside ASTUpdateItem. When used at the top-level, the target is always a
// path expression.
class ASTInsertStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_INSERT_STATEMENT;

  ASTInsertStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  std::string GetSQLForInsertMode() const;

  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested INSERT.
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;

  const ASTGeneralizedPathExpression* GetTargetPathForNested() const {
    return target_path_;
  }
  const ASTColumnList* column_list() const { return column_list_; }
  const ASTAssertRowsModified* assert_rows_modified() const {
    return assert_rows_modified_;
  }

  enum InsertMode {
    DEFAULT_MODE,  // plain INSERT
    REPLACE,       // INSERT OR REPLACE
    UPDATE,        // INSERT OR UPDATE
    IGNORE         // INSERT OR IGNORE
  };

  InsertMode insert_mode() const { return insert_mode_; }
  void set_insert_mode(InsertMode mode) { insert_mode_ = mode; }

  // Non-NULL rows() means we had a VALUES clause.
  // This is mutually exclusive with query() and with().
  const ASTInsertValuesRowList* rows() const { return rows_; }

  const ASTQuery* query() const { return query_; }

  // This is used by the Bison parser to store the latest element of the INSERT
  // syntax that was seen. The INSERT statement is extremely complicated to
  // parse in bison because it is very free-form, almost everything is optional
  // and almost all of the keywords are also usable as identifiers. So we parse
  // it in a very free-form way, and enforce the grammar in code during/after
  // parsing.
  enum ParseProgress {
    kInitial,
    kSeenOrIgnoreReplaceUpdate,
    kSeenTargetPath,
    kSeenColumnList,
    kSeenValuesList,
  };

  ParseProgress parse_progress() const { return parse_progress_; }
  void set_parse_progress(ParseProgress progress) {
    parse_progress_ = progress;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&target_path_);
    fl.AddOptional(&column_list_, AST_COLUMN_LIST);
    fl.AddOptional(&rows_, AST_INSERT_VALUES_ROW_LIST);
    fl.AddOptional(&query_, AST_QUERY);
    fl.AddOptional(&assert_rows_modified_, AST_ASSERT_ROWS_MODIFIED);
  }

  ParseProgress parse_progress_ = kInitial;

  InsertMode insert_mode_ = DEFAULT_MODE;
  const ASTGeneralizedPathExpression* target_path_ = nullptr;
  const ASTColumnList* column_list_ = nullptr;
  const ASTAssertRowsModified* assert_rows_modified_ = nullptr;  // Optional

  // Exactly one of rows_ or query_ will be present.
  // with_ can be present if query_ is present.
  const ASTInsertValuesRowList* rows_ = nullptr;

  const ASTQuery* query_ = nullptr;
};

class ASTUpdateSetValue final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UPDATE_SET_VALUE;

  ASTUpdateSetValue() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTGeneralizedPathExpression* path() const { return path_; }

  // The rhs of SET X=Y.  May be ASTDefaultLiteral.
  const ASTExpression* value() const { return value_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&path_);
    fl.AddRequired(&value_);
  }

  const ASTGeneralizedPathExpression* path_ = nullptr;
  const ASTExpression* value_ = nullptr;
};

class ASTUpdateItem final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UPDATE_ITEM;

  ASTUpdateItem() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTUpdateSetValue* set_value() const { return set_value_; }
  const ASTInsertStatement* insert_statement() const { return insert_; }
  const ASTDeleteStatement* delete_statement() const { return delete_; }
  const ASTUpdateStatement* update_statement() const { return update_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&set_value_, AST_UPDATE_SET_VALUE);
    fl.AddOptional(&insert_, AST_INSERT_STATEMENT);
    fl.AddOptional(&delete_, AST_DELETE_STATEMENT);
    fl.AddOptional(&update_, AST_UPDATE_STATEMENT);
  }

  // Exactly one of these will be non-NULL.
  const ASTUpdateSetValue* set_value_ = nullptr;
  const ASTInsertStatement* insert_ = nullptr;
  const ASTDeleteStatement* delete_ = nullptr;
  const ASTUpdateStatement* update_ = nullptr;
};

class ASTUpdateItemList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UPDATE_ITEM_LIST;

  ASTUpdateItemList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTUpdateItem* const>& update_items() const {
    return update_items_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&update_items_);
  }

  absl::Span<const ASTUpdateItem* const> update_items_;
};

// This is used for both top-level UPDATE statements and for nested UPDATEs
// inside ASTUpdateItem. When used at the top-level, the target is always a
// path expression.
class ASTUpdateStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_UPDATE_STATEMENT;

  ASTUpdateStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested UPDATE.
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;

  const ASTGeneralizedPathExpression* GetTargetPathForNested() const {
    return target_path_;
  }
  const ASTAlias* alias() const { return alias_; }

  const ASTWithOffset* offset() const { return offset_; }

  const ASTExpression* where() const { return where_; }
  const ASTAssertRowsModified* assert_rows_modified() const {
    return assert_rows_modified_;
  }
  const ASTFromClause* from_clause() const { return from_clause_; }

  const ASTUpdateItemList* update_item_list() const {
    return update_item_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&target_path_);
    fl.AddOptional(&alias_, AST_ALIAS);
    fl.AddOptional(&offset_, AST_WITH_OFFSET);
    fl.AddRequired(&update_item_list_);
    fl.AddOptional(&from_clause_, AST_FROM_CLAUSE);
    fl.AddOptionalExpression(&where_);
    fl.AddOptional(&assert_rows_modified_, AST_ASSERT_ROWS_MODIFIED);
  }

  const ASTGeneralizedPathExpression* target_path_ = nullptr;    // Required
  const ASTAlias* alias_ = nullptr;                              // Optional
  const ASTWithOffset* offset_ = nullptr;                        // Optional
  const ASTExpression* where_ = nullptr;                         // Optional
  const ASTAssertRowsModified* assert_rows_modified_ = nullptr;  // Optional
  const ASTUpdateItemList* update_item_list_ = nullptr;          // Required
  const ASTFromClause* from_clause_ = nullptr;                   // Optional
};

class ASTTruncateStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_TRUNCATE_STATEMENT;

  ASTTruncateStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Verifies that the target path is an ASTPathExpression and, if so, returns
  // it. The behavior is undefined when called on a node that represents a
  // nested TRUNCATE (but this is not allowed by the parser).
  zetasql_base::StatusOr<const ASTPathExpression*> GetTargetPathForNonNested() const;

  const ASTPathExpression* target_path() const {
    return target_path_;
  }
  const ASTExpression* where() const { return where_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&target_path_);
    fl.AddOptionalExpression(&where_);
  }

  const ASTPathExpression* target_path_ = nullptr;  // Required
  const ASTExpression* where_ = nullptr;            // Optional
};

class ASTMergeAction final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_MERGE_ACTION;

  explicit ASTMergeAction() : ASTNode(kConcreteNodeKind) {}

  ASTMergeAction(const ASTMergeAction&) = delete;
  ASTMergeAction& operator=(const ASTMergeAction&) = delete;

  ~ASTMergeAction() override {}

  enum ActionType { NOT_SET, INSERT, UPDATE, DELETE };

  std::string SingleNodeDebugString() const override;

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Exactly one of the INSERT/UPDATE/DELETE operation must be defined in
  // following ways,
  //   -- INSERT, action_type() is INSERT. The insert_column_list() is optional.
  //      The insert_row() must be non-null, but may have an empty value list.
  //   -- UPDATE, action_type() is UPDATE. update_item_list() is non-null.
  //   -- DELETE, action_type() is DELETE.
  const ASTColumnList* insert_column_list() const {
    return insert_column_list_;
  }
  const ASTInsertValuesRow* insert_row() const { return insert_row_; }
  const ASTUpdateItemList* update_item_list() const {
    return update_item_list_;
  }

  ActionType action_type() const { return action_type_; }
  void set_action_type(ActionType action) { action_type_ = action; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&insert_column_list_, AST_COLUMN_LIST);
    fl.AddOptional(&insert_row_, AST_INSERT_VALUES_ROW);
    fl.AddOptional(&update_item_list_, AST_UPDATE_ITEM_LIST);
  }

  // For INSERT operation.
  const ASTColumnList* insert_column_list_ = nullptr;  // Optional
  const ASTInsertValuesRow* insert_row_ = nullptr;     // Optional

  // For UPDATE operation.
  const ASTUpdateItemList* update_item_list_ = nullptr;  // Optional

  // Merge action type.
  ActionType action_type_ = NOT_SET;
};

class ASTMergeWhenClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_MERGE_WHEN_CLAUSE;

  explicit ASTMergeWhenClause() : ASTNode(kConcreteNodeKind) {}
  ASTMergeWhenClause(const ASTMergeWhenClause&) = delete;
  ASTMergeWhenClause& operator=(const ASTMergeWhenClause&) = delete;
  ~ASTMergeWhenClause() override {}

  enum MatchType {
    NOT_SET,
    MATCHED,
    NOT_MATCHED_BY_SOURCE,
    NOT_MATCHED_BY_TARGET
  };

  std::string SingleNodeDebugString() const override;

  std::string GetSQLForMatchType() const;

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* search_condition() const { return search_condition_; }

  const ASTMergeAction* action() const { return action_; }

  MatchType match_type() const { return match_type_; }
  void set_match_type(MatchType match_type) { match_type_ = match_type; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&search_condition_);
    fl.AddRequired(&action_);
  }

  MatchType match_type_ = NOT_SET;
  const ASTExpression* search_condition_ = nullptr;  // Optional
  const ASTMergeAction* action_ = nullptr;           // Required
};

class ASTMergeWhenClauseList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_MERGE_WHEN_CLAUSE_LIST;

  explicit ASTMergeWhenClauseList() : ASTNode(kConcreteNodeKind) {}
  ASTMergeWhenClauseList(const ASTMergeWhenClauseList&) = delete;
  ASTMergeWhenClauseList& operator=(const ASTMergeWhenClauseList&) = delete;
  ~ASTMergeWhenClauseList() override {}

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTMergeWhenClause* const>& clause_list() const {
    return clause_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&clause_list_);
  }

  absl::Span<const ASTMergeWhenClause* const> clause_list_;
};

class ASTMergeStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_MERGE_STATEMENT;

  explicit ASTMergeStatement() : ASTStatement(kConcreteNodeKind) {}
  ASTMergeStatement(const ASTMergeStatement&) = delete;
  ASTMergeStatement& operator=(const ASTMergeStatement&) = delete;
  ~ASTMergeStatement() override {}

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* target_path() const { return target_path_; }
  const ASTAlias* alias() const { return alias_; }
  const ASTTableExpression* table_expression() const {
    return table_expression_;
  }
  const ASTExpression* merge_condition() const { return merge_condition_; }
  const ASTMergeWhenClauseList* when_clauses() const { return match_clauses_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&target_path_);
    fl.AddOptional(&alias_, AST_ALIAS);
    fl.AddRequired(&table_expression_);
    fl.AddRequired(&merge_condition_);
    fl.AddRequired(&match_clauses_);
  }

  const ASTPathExpression* target_path_ = nullptr;         // Required
  const ASTAlias* alias_ = nullptr;                        // Optional
  const ASTTableExpression* table_expression_ = nullptr;   // Required
  const ASTExpression* merge_condition_ = nullptr;         // Required
  const ASTMergeWhenClauseList* match_clauses_ = nullptr;  // Required
};

class ASTPrivilege final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PRIVILEGE;

  ASTPrivilege() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* privilege_action() const { return privilege_action_; }
  const ASTColumnList* column_list() const { return column_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&privilege_action_);
    fl.AddOptional(&column_list_, AST_COLUMN_LIST);
  }

  const ASTIdentifier* privilege_action_ = nullptr;  // Required
  const ASTColumnList* column_list_ = nullptr;       // Optional
};

// Represents privileges to be granted or revoked. It can be either or a
// non-empty list of ASTPrivilege, or "ALL PRIVILEGES" in which case the list
// will be empty.
class ASTPrivileges final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PRIVILEGES;

  ASTPrivileges() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTPrivilege* const>& privileges() const {
    return privileges_;
  }

  bool is_all_privileges() const {
    // Empty Span means ALL PRIVILEGES.
    return privileges_.empty();
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&privileges_);
  }

  absl::Span<const ASTPrivilege* const> privileges_;
};

class ASTGranteeList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GRANTEE_LIST;

  ASTGranteeList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTExpression* const>& grantee_list() const {
    return grantee_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&grantee_list_);
  }

  // An ASTGranteeList element may either be a string literal or
  // parameter.
  absl::Span<const ASTExpression* const> grantee_list_;
};

class ASTGrantStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GRANT_STATEMENT;

  ASTGrantStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPrivileges* privileges() const { return privileges_; }
  const ASTIdentifier* target_type() const { return target_type_; }
  const ASTPathExpression* target_path() const { return target_path_; }
  const ASTGranteeList* grantee_list() const { return grantee_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&privileges_);
    fl.AddOptional(&target_type_, AST_IDENTIFIER);
    fl.AddRequired(&target_path_);
    fl.AddRequired(&grantee_list_);
  }

  const ASTPrivileges* privileges_ = nullptr;       // Required
  const ASTIdentifier* target_type_ = nullptr;      // Optional
  const ASTPathExpression* target_path_ = nullptr;  // Required
  const ASTGranteeList* grantee_list_ = nullptr;    // Required
};

class ASTRevokeStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REVOKE_STATEMENT;

  ASTRevokeStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPrivileges* privileges() const { return privileges_; }
  const ASTIdentifier* target_type() const { return target_type_; }
  const ASTPathExpression* target_path() const { return target_path_; }
  const ASTGranteeList* grantee_list() const { return grantee_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&privileges_);
    fl.AddOptional(&target_type_, AST_IDENTIFIER);
    fl.AddRequired(&target_path_);
    fl.AddRequired(&grantee_list_);
  }

  const ASTPrivileges* privileges_ = nullptr;
  const ASTIdentifier* target_type_ = nullptr;
  const ASTPathExpression* target_path_ = nullptr;
  const ASTGranteeList* grantee_list_ = nullptr;
};

class ASTRepeatableClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REPEATABLE_CLAUSE;

  ASTRepeatableClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* argument() const { return argument_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&argument_);
  }

  const ASTExpression* argument_ = nullptr;  // Required
};

class ASTReplaceFieldsArg final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REPLACE_FIELDS_ARG;

  ASTReplaceFieldsArg() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }

  const ASTGeneralizedPathExpression* path_expression() const {
    return path_expression_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddRequired(&path_expression_);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTGeneralizedPathExpression* path_expression_ = nullptr;
};

class ASTReplaceFieldsExpression final : public ASTExpression {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_REPLACE_FIELDS_EXPRESSION;

  ASTReplaceFieldsExpression() : ASTExpression(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expr() const { return expr_; }

  const absl::Span<const ASTReplaceFieldsArg* const>& arguments() const {
    return arguments_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expr_);
    fl.AddRestAsRepeated(&arguments_);
  }

  const ASTExpression* expr_ = nullptr;  // Required
  absl::Span<const ASTReplaceFieldsArg* const> arguments_;
};

class ASTSampleSize final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SAMPLE_SIZE;

  ASTSampleSize() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Returns the SQL keyword for the sample-size unit, i.e. "ROWS" or "PERCENT".
  std::string GetSQLForUnit() const;

  enum Unit { NOT_SET, ROWS, PERCENT };

  // Returns the token kind corresponding to the sample-size unit, i.e.
  // parser::ROWS or parser::PERCENT.
  Unit unit() const { return unit_; }
  void set_unit(Unit unit) { unit_ = unit; }

  const ASTExpression* size() const { return size_; }
  const ASTPartitionBy* partition_by() const { return partition_by_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&size_);
    fl.AddOptional(&partition_by_, AST_PARTITION_BY);
  }

  const ASTExpression* size_ = nullptr;           // Required
  // Can only be non-NULL when 'unit_' is parser::ROWS.
  const ASTPartitionBy* partition_by_ = nullptr;  // Optional
  Unit unit_ = NOT_SET;
};

class ASTWithWeight final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WITH_WEIGHT;

  ASTWithWeight() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // alias may be NULL.
  const ASTAlias* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTAlias* alias_ = nullptr;  // Optional
};

class ASTSampleSuffix final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SAMPLE_SUFFIX;

  ASTSampleSuffix() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // weight and repeat may be NULL.
  const ASTWithWeight* weight() const { return weight_; }
  const ASTRepeatableClause* repeat() const { return repeat_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&weight_, AST_WITH_WEIGHT);
    fl.AddOptional(&repeat_, AST_REPEATABLE_CLAUSE);
  }

  const ASTWithWeight* weight_ = nullptr;        // Optional
  const ASTRepeatableClause* repeat_ = nullptr;  // Optional
};

class ASTSampleClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SAMPLE_CLAUSE;

  ASTSampleClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* sample_method() const { return sample_method_; }
  const ASTSampleSize* sample_size() const { return sample_size_; }
  const ASTSampleSuffix* sample_suffix() const { return sample_suffix_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&sample_method_);
    fl.AddRequired(&sample_size_);
    fl.AddOptional(&sample_suffix_, AST_SAMPLE_SUFFIX);
  }

  const ASTIdentifier* sample_method_ = nullptr;    // Required
  const ASTSampleSize* sample_size_ = nullptr;      // Required
  const ASTSampleSuffix* sample_suffix_ = nullptr;  // Optional
};

// Common parent for all actions in ALTER statements
class ASTAlterAction : public ASTNode {
 public:
  explicit ASTAlterAction(ASTNodeKind kind) : ASTNode(kind) {}
  virtual std::string GetSQLForAlterAction() const = 0;
};

// ALTER action for "SET OPTIONS ()" clause
class ASTSetOptionsAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SET_OPTIONS_ACTION;

  ASTSetOptionsAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&options_list_);
  }

  const ASTOptionsList* options_list_ = nullptr;
};

// ALTER table action for "ADD CONSTRAINT" clause
class ASTAddConstraintAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ADD_CONSTRAINT_ACTION;

  ASTAddConstraintAction() : ASTAlterAction(kConcreteNodeKind) {}
  std::string SingleNodeDebugString() const override;
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  bool is_if_not_exists() const { return is_if_not_exists_; }
  void set_is_if_not_exists(bool value) { is_if_not_exists_ = value; }
  const ASTTableConstraint* constraint() const {
    return constraint_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_);
  }

  bool is_if_not_exists_ = false;
  const ASTTableConstraint* constraint_ = nullptr;
};

// ALTER table action for "DROP CONSTRAINT" clause
class ASTDropConstraintAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DROP_CONSTRAINT_ACTION;

  ASTDropConstraintAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  std::string GetSQLForAlterAction() const override;

  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  const ASTIdentifier* constraint_name() const { return constraint_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_name_);
  }

  const ASTIdentifier* constraint_name_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ALTER CONSTRAINT identifier [NOT] ENFORCED" clause
class ASTAlterConstraintEnforcementAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_CONSTRAINT_ENFORCEMENT_ACTION;

  ASTAlterConstraintEnforcementAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  std::string GetSQLForAlterAction() const override;

  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  const ASTIdentifier* constraint_name() const { return constraint_name_; }
  void set_is_enforced(bool enforced) { is_enforced_ = enforced; }
  bool is_enforced() const { return is_enforced_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_name_);
  }

  const ASTIdentifier* constraint_name_ = nullptr;
  bool is_if_exists_ = false;
  bool is_enforced_ = true;
};

// ALTER table action for "ALTER CONSTRAINT identifier SET OPTIONS" clause
class ASTAlterConstraintSetOptionsAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_CONSTRAINT_SET_OPTIONS_ACTION;

  ASTAlterConstraintSetOptionsAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  std::string GetSQLForAlterAction() const override;

  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }
  const ASTIdentifier* constraint_name() const { return constraint_name_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&constraint_name_);
    fl.AddRequired(&options_list_);
  }

  const ASTIdentifier* constraint_name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ADD COLUMN" clause
class ASTAddColumnAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ADD_COLUMN_ACTION;

  ASTAddColumnAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  std::string GetSQLForAlterAction() const override;

  const ASTColumnDefinition* column_definition() const {
    return column_definition_;
  }
  bool is_if_not_exists() const { return is_if_not_exists_; }
  void set_is_if_not_exists(bool value) { is_if_not_exists_ = value; }

  // Optional children.
  const ASTColumnPosition* column_position() const { return column_position_; }
  const ASTExpression* fill_expression() const { return fill_expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_definition_);
    fl.AddOptional(&column_position_, AST_COLUMN_POSITION);
    fl.AddOptionalExpression(&fill_expression_);
  }

  const ASTColumnDefinition* column_definition_ = nullptr;
  const ASTColumnPosition* column_position_ = nullptr;
  const ASTExpression* fill_expression_ = nullptr;
  bool is_if_not_exists_ = false;
};

// ALTER table action for "DROP COLUMN" clause
class ASTDropColumnAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_DROP_COLUMN_ACTION;

  ASTDropColumnAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  std::string SingleNodeDebugString() const override;

  std::string GetSQLForAlterAction() const override;

  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }

  const ASTIdentifier* column_name() const { return column_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
  }

  const ASTIdentifier* column_name_ = nullptr;
  bool is_if_exists_ = false;
};

// ALTER table action for "ALTER COLUMN SET TYPE" clause
class ASTAlterColumnTypeAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_COLUMN_TYPE_ACTION;

  ASTAlterColumnTypeAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  const ASTIdentifier* column_name() const { return column_name_; }
  const ASTColumnSchema* schema() const { return schema_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
    fl.AddRequired(&schema_);
  }

  const ASTIdentifier* column_name_ = nullptr;
  const ASTColumnSchema* schema_ = nullptr;
};

// ALTER table action for "ALTER COLUMN SET OPTIONS" clause
class ASTAlterColumnOptionsAction final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_COLUMN_OPTIONS_ACTION;

  ASTAlterColumnOptionsAction() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  const ASTIdentifier* column_name() const { return column_name_; }
  const ASTOptionsList* options_list() const { return options_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&column_name_);
    fl.AddRequired(&options_list_);
  }

  const ASTIdentifier* column_name_ = nullptr;
  const ASTOptionsList* options_list_ = nullptr;
};

// ALTER ROW ACCESS POLICY action for "GRANT TO (<grantee_list>)" or "TO
// <grantee_list>" clause, also used by CREATE ROW ACCESS POLICY
class ASTGrantToClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_GRANT_TO_CLAUSE;

  ASTGrantToClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  const ASTGranteeList* grantee_list() const { return grantee_list_; }

  bool has_grant_keyword_and_parens() const {
    return has_grant_keyword_and_parens_;
  }
  void set_has_grant_keyword_and_parens(bool value) {
    has_grant_keyword_and_parens_ = value;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&grantee_list_);
  }

  const ASTGranteeList* grantee_list_ = nullptr;  // Required.
  bool has_grant_keyword_and_parens_ = false;
};

// ALTER ROW ACCESS POLICY action for "[FILTER] USING (<expression>)" clause,
// also used by CREATE ROW ACCESS POLICY
class ASTFilterUsingClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FILTER_USING_CLAUSE;

  ASTFilterUsingClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  const ASTExpression* predicate() const { return predicate_; }

  bool has_filter_keyword() const { return has_filter_keyword_; }
  void set_has_filter_keyword(bool value) { has_filter_keyword_ = value; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&predicate_);
  }

  const ASTExpression* predicate_ = nullptr;  // Required.
  bool has_filter_keyword_ = false;
};

// ALTER ROW ACCESS POLICY action for "REVOKE FROM (<grantee_list>)|ALL" clause
class ASTRevokeFromClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_REVOKE_FROM_CLAUSE;

  ASTRevokeFromClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  std::string GetSQLForAlterAction() const override;

  const ASTGranteeList* revoke_from_list() const { return revoke_from_list_; }

  bool is_revoke_from_all() const { return is_revoke_from_all_; }
  void set_is_revoke_from_all(bool value) { is_revoke_from_all_ = value; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptional(&revoke_from_list_, AST_GRANTEE_LIST);
  }

  const ASTGranteeList* revoke_from_list_ = nullptr;  // Optional.
  bool is_revoke_from_all_ = false;
};

// ALTER ROW ACCESS POLICY action for "RENAME TO <new_name>" clause
class ASTRenameToClause final : public ASTAlterAction {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RENAME_TO_CLAUSE;

  ASTRenameToClause() : ASTAlterAction(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string GetSQLForAlterAction() const override;

  const ASTIdentifier* new_name() const { return new_name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&new_name_);
  }

  const ASTIdentifier* new_name_ = nullptr;  // Required
};

class ASTAlterActionList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_ACTION_LIST;

  ASTAlterActionList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTAlterAction* const>& actions() const {
    return actions_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&actions_);
  }

  absl::Span<const ASTAlterAction* const> actions_;
};

// Common parent class for ALTER statement, e.g., ALTER TABLE/ALTER VIEW
class ASTAlterStatementBase : public ASTStatement {
 public:
  explicit ASTAlterStatementBase(ASTNodeKind kind) : ASTStatement(kind) {}

  // This adds the "if exists" modifier to the node name.
  std::string SingleNodeDebugString() const override;

  const ASTPathExpression* path() const { return path_; }
  const ASTAlterActionList* action_list() const { return action_list_; }
  bool is_if_exists() const { return is_if_exists_; }
  void set_is_if_exists(bool value) { is_if_exists_ = value; }

 protected:
  void InitPathAndAlterActions(FieldLoader* field_loader) {
    field_loader->AddRequired(&path_);
    field_loader->AddRequired(&action_list_);
  }

 private:
  const ASTPathExpression* path_ = nullptr;
  const ASTAlterActionList* action_list_ = nullptr;
  bool is_if_exists_ = false;
};

class ASTAlterDatabaseStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_DATABASE_STATEMENT;

  ASTAlterDatabaseStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
  }
};

class ASTAlterTableStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_TABLE_STATEMENT;

  ASTAlterTableStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
  }
};

class ASTAlterViewStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ALTER_VIEW_STATEMENT;

  ASTAlterViewStatement() : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
  }
};

class ASTAlterMaterializedViewStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_MATERIALIZED_VIEW_STATEMENT;

  ASTAlterMaterializedViewStatement()
      : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {
    FieldLoader fl(this);
    InitPathAndAlterActions(&fl);
  }
};

class ASTAlterRowAccessPolicyStatement final : public ASTAlterStatementBase {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_ROW_ACCESS_POLICY_STATEMENT;

  ASTAlterRowAccessPolicyStatement()
      : ASTAlterStatementBase(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields.
  const ASTIdentifier* name() const { return name_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&name_);
    InitPathAndAlterActions(&fl);
  }

  const ASTIdentifier* name_ = nullptr;  // Required
};

class ASTAlterAllRowAccessPoliciesStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_ALTER_ALL_ROW_ACCESS_POLICIES_STATEMENT;

  ASTAlterAllRowAccessPoliciesStatement()
      : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTPathExpression* table_name_path() const { return table_name_path_; }
  const ASTAlterAction* alter_action() const {
    return alter_action_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&table_name_path_);
    fl.AddRequired(&alter_action_);
  }

  const ASTAlterAction* alter_action_ = nullptr;  // Required
  const ASTPathExpression* table_name_path_ = nullptr;  // Required
};

class ASTForeignKeyActions final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOREIGN_KEY_ACTIONS;

  ASTForeignKeyActions() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  enum Action { NO_ACTION, RESTRICT, CASCADE, SET_NULL };
  void set_update_action(Action action) { update_action_ = action; }
  void set_delete_action(Action action) { delete_action_ = action; }
  const Action update_action() const { return update_action_; }
  const Action delete_action() const { return delete_action_; }

  static std::string GetSQLForAction(Action action);

 private:
  void InitFields() final {
    FieldLoader fl(this);
  }

  Action update_action_ = NO_ACTION;
  Action delete_action_ = NO_ACTION;
};

class ASTForeignKeyReference final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_FOREIGN_KEY_REFERENCE;

  ASTForeignKeyReference() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  std::string SingleNodeDebugString() const override;

  const ASTPathExpression* table_name() const { return table_name_; }
  const ASTColumnList* column_list() const { return column_list_; }
  const ASTForeignKeyActions* actions() const { return actions_; }

  enum Match { SIMPLE, FULL, NOT_DISTINCT };
  void set_match(Match match) { match_ = match; }
  const Match match() const { return match_; }
  std::string GetSQLForMatch() const;

  void set_enforced(bool enforced) { enforced_ = enforced; }
  bool enforced() const { return enforced_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&table_name_);
    fl.AddRequired(&column_list_);
    fl.AddRequired(&actions_);
  }

  const ASTPathExpression* table_name_ = nullptr;
  const ASTColumnList* column_list_ = nullptr;
  const ASTForeignKeyActions* actions_ = nullptr;
  Match match_ = SIMPLE;
  bool enforced_ = true;
};

// Contains a list of statements.  Variable declarations allowed only at the
// start of the list, and only if variable_declarations_allowed() is true.
class ASTStatementList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_STATEMENT_LIST;

  ASTStatementList() : ASTNode(kConcreteNodeKind) {}

  absl::Span<const ASTStatement* const> statement_list() const {
    return statement_list_;
  }

  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;
  bool variable_declarations_allowed() const {
    return variable_declarations_allowed_;
  }
  void set_variable_declarations_allowed(bool allowed) {
    variable_declarations_allowed_ = allowed;
  }

 protected:
  explicit ASTStatementList(ASTNodeKind node_kind) : ASTNode(node_kind) {}

 private:
  void InitFields() override {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&statement_list_);
  }

  absl::Span<const ASTStatement* const> statement_list_;  // Repeated
  bool variable_declarations_allowed_ = false;
};

// A top-level script.
class ASTScript final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SCRIPT;

  ASTScript() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTStatementList* statement_list_node() const {
    return statement_list_;
  }

  absl::Span<const ASTStatement* const> statement_list() const {
    return statement_list_->statement_list();
  }

 private:
  void InitFields() override {
    FieldLoader fl(this);
    fl.AddRequired(&statement_list_);
  }
  const ASTStatementList* statement_list_ = nullptr;
};

// Represents an ELSEIF clause in an IF statement.
class ASTElseifClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ELSEIF_CLAUSE;

  ASTElseifClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Returns the ASTIfStatement that this ASTElseifClause belongs to.
  const ASTIfStatement* if_stmt() const {
    return parent()->parent()->GetAsOrDie<ASTIfStatement>();
  }

  // Required fields
  const ASTExpression* condition() const { return condition_; }
  const ASTStatementList* body() const { return body_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&condition_);
    fl.AddRequired(&body_);
  }

  const ASTExpression* condition_ = nullptr;  // Required
  const ASTStatementList* body_ = nullptr;    // Required
};

// Represents a list of ELSEIF clauses.  Note that this list is never empty,
// as the grammar will not create an ASTElseifClauseList object unless there
// exists at least one ELSEIF clause.
class ASTElseifClauseList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ELSEIF_CLAUSE_LIST;

  ASTElseifClauseList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const absl::Span<const ASTElseifClause* const>& elseif_clauses() const {
    return elseif_clauses_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&elseif_clauses_);
  }
  absl::Span<const ASTElseifClause* const> elseif_clauses_;
};

class ASTIfStatement final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IF_STATEMENT;

  ASTIfStatement() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields.
  const ASTExpression* condition() const { return condition_; }
  const ASTStatementList* then_list() const { return then_list_; }

  // Optional; nullptr if no ELSEIF clauses are specified.  If present, the
  // list will never be empty.
  const ASTElseifClauseList* elseif_clauses() const { return elseif_clauses_; }

  // Optional; nullptr if no ELSE clause is specified
  const ASTStatementList* else_list() const { return else_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&condition_);
    fl.AddRequired(&then_list_);
    fl.AddOptional(&elseif_clauses_, AST_ELSEIF_CLAUSE_LIST);
    fl.AddOptional(&else_list_, AST_STATEMENT_LIST);
  }

  const ASTExpression* condition_ = nullptr;     // Required
  const ASTStatementList* then_list_ = nullptr;  // Required
  const ASTElseifClauseList* elseif_clauses_ = nullptr;  // Required
  const ASTStatementList* else_list_ = nullptr;  // Optional
};

class ASTRaiseStatement final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RAISE_STATEMENT;

  ASTRaiseStatement() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* message() const { return message_; }

  // A RAISE statement rethrows an existing exception, as opposed to creating
  // a new exception, when none of the properties are set.  Currently, the only
  // property is the message.  However, for future proofing, as more properties
  // get added to RAISE later, code should call this function to check for a
  // rethrow, rather than checking for the presence of a message, directly.
  bool is_rethrow() const { return message_ == nullptr; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&message_);
  }
  const ASTExpression* message_ = nullptr;  // Optional
};

class ASTExceptionHandler final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXCEPTION_HANDLER;

  ASTExceptionHandler() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required field; even an empty block still contains an empty statement list.
  const ASTStatementList* statement_list() const { return statement_list_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&statement_list_);
  }
  const ASTStatementList* statement_list_ = nullptr;
};

// Represents a list of exception handlers in a block.  Currently restricted
// to one element, but may contain multiple elements in the future, once there
// are multiple error codes for a block to catch.
class ASTExceptionHandlerList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXCEPTION_HANDLER_LIST;

  ASTExceptionHandlerList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  absl::Span<const ASTExceptionHandler* const> exception_handler_list() const {
    return exception_handler_list_;
  }

 private:
  void InitFields() override {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&exception_handler_list_);
  }

  absl::Span<const ASTExceptionHandler* const> exception_handler_list_;
};

class ASTBeginEndBlock final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BEGIN_END_BLOCK;
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  ASTBeginEndBlock() : ASTScriptStatement(kConcreteNodeKind) {}

  const ASTStatementList* statement_list_node() const {
    return statement_list_;
  }

  absl::Span<const ASTStatement* const> statement_list() const {
    return statement_list_->statement_list();
  }

  bool has_exception_handler() const {
    return handler_list_ != nullptr &&
           !handler_list_->exception_handler_list().empty();
  }

  // Optional; nullptr indicates a BEGIN block without an EXCEPTION clause.
  const ASTExceptionHandlerList* handler_list() const { return handler_list_; }

 private:
  void InitFields() override {
    FieldLoader fl(this);
    fl.AddRequired(&statement_list_);
    fl.AddOptional(&handler_list_, AST_EXCEPTION_HANDLER_LIST);
  }
  const ASTStatementList* statement_list_ = nullptr;  // Required, never null.
  const ASTExceptionHandlerList* handler_list_ = nullptr;  // Optional
};

class ASTIdentifierList final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_IDENTIFIER_LIST;

  ASTIdentifierList() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Guaranteed by the parser to never be empty.
  absl::Span<const ASTIdentifier* const> identifier_list() const {
    return identifier_list_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&identifier_list_);
  }
  absl::Span<const ASTIdentifier* const> identifier_list_;
};

class ASTVariableDeclaration final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_VARIABLE_DECLARATION;

  ASTVariableDeclaration() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // Required fields
  const ASTIdentifierList* variable_list() const { return variable_list_; }

  // Optional fields; at least one of <type> and <default_value> must be
  // present.
  const ASTType* type() const { return type_; }
  const ASTExpression* default_value() const { return default_value_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&variable_list_);
    fl.AddOptionalType(&type_);
    fl.AddOptionalExpression(&default_value_);
  }
  const ASTIdentifierList* variable_list_ = nullptr;
  const ASTType* type_ = nullptr;
  const ASTExpression* default_value_ = nullptr;  // Optional
};

// Base class for all loop statements (loop/end loop, while, foreach, etc.).
// Every loop has a body.
class ASTLoopStatement : public ASTScriptStatement {
 public:
  explicit ASTLoopStatement(ASTNodeKind kind) : ASTScriptStatement(kind) {}

  bool IsLoopStatement() const override { return true; }

  // Required fields
  const ASTStatementList* body() const { return body_; }

 protected:
  void InitBodyField(FieldLoader* field_loader) {
    field_loader->AddRequired(&body_);
  }

 private:
  const ASTStatementList* body_ = nullptr;
};

// Represents either:
//  - LOOP...END LOOP (if condition is nullptr).  This is semantically
//      equivelant to WHILE(true)...END WHILE.
//  - WHILE...END WHILE (if condition is not nullptr)
//
class ASTWhileStatement final : public ASTLoopStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_WHILE_STATEMENT;

  ASTWhileStatement() : ASTLoopStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  // The <condition> is optional.  A null <condition> indicates a
  // LOOP...END LOOP construct.
  const ASTExpression* condition() const { return condition_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddOptionalExpression(&condition_);
    InitBodyField(&fl);
  }

  const ASTExpression* condition_ = nullptr;
};

// Base class shared by break and continue statements.
class ASTBreakContinueStatement : public ASTScriptStatement {
 public:
  enum BreakContinueKeyword { BREAK, LEAVE, CONTINUE, ITERATE };

  ASTBreakContinueStatement(ASTNodeKind node_kind, BreakContinueKeyword keyword)
      : ASTScriptStatement(node_kind), keyword_(keyword) {}

  BreakContinueKeyword keyword() const { return keyword_; }
  void set_keyword(BreakContinueKeyword keyword) { keyword_ = keyword; }

  // Returns text representing the keyword used for this BREAK/CONINUE
  // statement.  All letters are in uppercase.
  absl::string_view GetKeywordText() const {
    switch (keyword_) {
      case BREAK:
        return "BREAK";
      case LEAVE:
        return "LEAVE";
      case CONTINUE:
        return "CONTINUE";
      case ITERATE:
        return "ITERATE";
    }
  }

 private:
  void InitFields() final {}
  BreakContinueKeyword keyword_;
};

class ASTBreakStatement final : public ASTBreakContinueStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_BREAK_STATEMENT;

  ASTBreakStatement() : ASTBreakContinueStatement(kConcreteNodeKind, BREAK) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
};

class ASTContinueStatement final : public ASTBreakContinueStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_CONTINUE_STATEMENT;

  ASTContinueStatement()
      : ASTBreakContinueStatement(kConcreteNodeKind, CONTINUE) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
};

class ASTReturnStatement final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_RETURN_STATEMENT;

  ASTReturnStatement() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

 private:
  void InitFields() final {}
};

// A statement which assigns to a single variable from an expression.
// Example:
//   SET x = 3;
class ASTSingleAssignment final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_SINGLE_ASSIGNMENT;

  ASTSingleAssignment() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifier* variable() const { return variable_; }
  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&variable_);
    fl.AddRequired(&expression_);
  }

  const ASTIdentifier* variable_ = nullptr;
  const ASTExpression* expression_ = nullptr;
};

// A statement which assigns to a query parameter from an expression.
// Example:
//   SET @x = 3;
class ASTParameterAssignment final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_PARAMETER_ASSIGNMENT;

  ASTParameterAssignment() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTParameterExpr* parameter() const { return parameter_; }
  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&parameter_);
    fl.AddRequired(&expression_);
  }

  const ASTParameterExpr* parameter_ = nullptr;
  const ASTExpression* expression_ = nullptr;
};

// A statement which assigns to a system variable from an expression.
// Example:
//   SET @@x = 3;
class ASTSystemVariableAssignment final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_SYSTEM_VARIABLE_ASSIGNMENT;

  ASTSystemVariableAssignment() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTSystemVariableExpr* system_variable() const {
    return system_variable_;
  }
  const ASTExpression* expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&system_variable_);
    fl.AddRequired(&expression_);
  }

  const ASTSystemVariableExpr* system_variable_ = nullptr;
  const ASTExpression* expression_ = nullptr;
};

// A statement which assigns multiple variables to fields in a struct,
// which each variable assigned to one field.
// Example:
//   SET (x, y) = (5, 10);
class ASTAssignmentFromStruct final : public ASTScriptStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_ASSIGNMENT_FROM_STRUCT;

  ASTAssignmentFromStruct() : ASTScriptStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifierList* variables() const { return variables_; }
  const ASTExpression* struct_expression() const { return expression_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&variables_);
    fl.AddRequired(&expression_);
  }

  const ASTIdentifierList* variables_ = nullptr;  // Required, not NULL
  const ASTExpression* expression_ = nullptr;  // Required, not NULL
};

class ASTExecuteIntoClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXECUTE_INTO_CLAUSE;

  ASTExecuteIntoClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTIdentifierList* identifiers() const { return identifiers_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&identifiers_);
  }

  const ASTIdentifierList* identifiers_ = nullptr;
};

class ASTExecuteUsingArgument final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXECUTE_USING_ARGUMENT;

  ASTExecuteUsingArgument() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* expression() const { return expression_; }
  // Optional. Absent if this argument is positional. Present if it is named.
  const ASTAlias* alias() const { return alias_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&expression_);
    fl.AddOptional(&alias_, AST_ALIAS);
  }

  const ASTExpression* expression_ = nullptr;
  const ASTAlias* alias_ = nullptr;
};

class ASTExecuteUsingClause final : public ASTNode {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind = AST_EXECUTE_USING_CLAUSE;

  ASTExecuteUsingClause() : ASTNode(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  absl::Span<const ASTExecuteUsingArgument* const> arguments() const {
    return arguments_;
  }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRestAsRepeated(&arguments_);
  }

  absl::Span<const ASTExecuteUsingArgument* const> arguments_;
};

class ASTExecuteImmediateStatement final : public ASTStatement {
 public:
  static constexpr ASTNodeKind kConcreteNodeKind =
      AST_EXECUTE_IMMEDIATE_STATEMENT;

  ASTExecuteImmediateStatement() : ASTStatement(kConcreteNodeKind) {}
  void Accept(ParseTreeVisitor* visitor, void* data) const override;
  zetasql_base::StatusOr<VisitResult> Accept(
      NonRecursiveParseTreeVisitor* visitor) const override;

  const ASTExpression* sql() const { return sql_; }
  const ASTExecuteIntoClause* into_clause() const { return into_clause_; }
  const ASTExecuteUsingClause* using_clause() const { return using_clause_; }

 private:
  void InitFields() final {
    FieldLoader fl(this);
    fl.AddRequired(&sql_);
    fl.AddOptional(&into_clause_, AST_EXECUTE_INTO_CLAUSE);
    fl.AddOptional(&using_clause_, AST_EXECUTE_USING_CLAUSE);
  }

  const ASTExpression* sql_ = nullptr;
  const ASTExecuteIntoClause* into_clause_ = nullptr;
  const ASTExecuteUsingClause* using_clause_ = nullptr;
};

inline IdString ASTAlias::GetAsIdString() const {
  return identifier()->GetAsIdString();
}

namespace parse_tree_internal {

// Concrete types (the 'leaves' of the hierarchy) must be constructible.
template <typename T>
using EnableIfConcrete =
    typename std::enable_if<std::is_constructible<T>::value, int>::type;

// Non Concrete types (internal nodes) of the hierarchy must _not_ be
// constructible.
template <typename T>
using EnableIfNotConcrete =
    typename std::enable_if<!std::is_constructible<T>::value, int>::type;

// GetAsOrNull implementation optimized for concrete types.  We assume that all
// concrete types define:
//   static constexpr Type kConcreteNodeKind;
//
// This allows us to avoid invoking dynamic_cast.
template <typename T, typename MaybeConstRoot, EnableIfConcrete<T> = 0>
inline T* GetAsOrNullImpl(MaybeConstRoot* n) {
  if (n->node_kind() == T::kConcreteNodeKind) {
    return static_cast<T*>(n);
  } else {
    return nullptr;
  }
}

// GetAsOrNull implemented simply with dynamic_cast.  This is used for
// intermediate nodes (such as ASTExpression).
template <typename T, typename MaybeConstRoot, EnableIfNotConcrete<T> = 0>
inline T* GetAsOrNullImpl(MaybeConstRoot* r) {
  // Note, if this proves too slow, it could be implemented with ancestor enums
  // sets.
  return dynamic_cast<T*>(r);
}

}  // namespace parse_tree_internal

template <typename NodeType>
inline const NodeType* ASTNode::GetAsOrNull() const {
  static_assert(std::is_base_of<ASTNode, NodeType>::value,
                "NodeType must be a member of the ASTNode class hierarchy");
  return parse_tree_internal::GetAsOrNullImpl<const NodeType, const ASTNode>(
      this);
}

template <typename NodeType>
inline NodeType* ASTNode::GetAsOrNull() {
  static_assert(std::is_base_of<ASTNode, NodeType>::value,
                "NodeType must be a member of the ASTNode class hierarchy");
  return parse_tree_internal::GetAsOrNullImpl<NodeType, ASTNode>(this);
}

}  // namespace zetasql

#endif  // ZETASQL_PARSER_PARSE_TREE_H_
