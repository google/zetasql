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

#ifndef ZETASQL_PARSER_BISON_PARSER_H_
#define ZETASQL_PARSER_BISON_PARSER_H_

#include <memory>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/arena_allocator.h"
#include "zetasql/base/logging.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/flex_tokenizer.h"
#include "zetasql/parser/location.hh"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/position.hh"
#include "zetasql/parser/statement_properties.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"

namespace zetasql_bison_parser {
class BisonParserImpl;
}  // namespace zetasql_bison_parser

namespace zetasql {
namespace parser {

// Wrapper class for the Bison parser for ZetaSQL. Contains the context data
// for the parser. The actual Bison parser is generated as
// zetasql_bison_parser::BisonParserImpl.
class BisonParser {
 public:
  BisonParser();
  BisonParser(const BisonParser&) = delete;
  BisonParser& operator=(const BisonParser&) = delete;

  // Creates a BisonParser that is intended to be used stand-alone to call a
  // generated parser function, instead of with the provided Parse() function
  // which does that for you. This situation should be rare and only relevant
  // if you have a different parser implementation than the default ZetaSQL
  // parser.
  BisonParser(zetasql_base::UnsafeArena* arena,
              std::unique_ptr<std::vector<std::unique_ptr<ASTNode>>>
                  allocated_ast_nodes,
              zetasql::IdStringPool* id_string_pool, absl::string_view input,
              const LanguageOptions& language_options)
      : id_string_pool_(id_string_pool),
        arena_(arena),
        language_options_(&language_options),
        allocated_ast_nodes_(std::move(allocated_ast_nodes)),
        input_(input) {}

  ~BisonParser();

  // Parses 'input' in mode 'mode', starting at byte offset 'start_byte_offset'.
  // Returns the output tree in 'output', or returns an annotated error.
  // Neither this object nor the returned output retain any pointers to
  // 'filename' or 'input' after the function returns.
  //
  // Memory allocation:
  // - The 'filename' is copied into 'id_string_pool' for reference by the AST.
  // - Identifiers are allocated from 'id_string_pool'.
  // - ASTNodes are allocated from 'arena'.
  // - ASTNodes still need to be deleted before the memory pools are destroyed.
  //   Ownership of all allocated ASTNodes except for the root output is
  //   returned in 'other_allocated_ast_nodes'.
  // The caller should keep 'id_string_pool' and 'arena' alive until all the
  // returned ASTNodes have been deallocated.
  //
  // If mode is kNextStatementKind, then the next statement kind is returned in
  // 'ast_statement_properties', and no parse tree is returned. This may still
  // allocate into 'id_string_pool' if the prefix of the statement that needs to
  // be parsed includes identifiers, and may allocate ASTNodes into
  // 'other_allocated_ast_nodes' if statement level hints are present. In this
  // mode, 'statement_end_byte_offset' is *not* set.
  //
  // If mode is kNextStatement, the byte offset past the current statement's
  // closing semicolon is returned in 'statement_end_byte_offset'. If the
  // statement did not end in a semicolon, then 'statement_end_byte_offset' is
  // set to -1, and the input was guaranteed to be parsed to the end.
  //
  // If mode is kStatement, then 'statement_end_byte_offset' is not set.
  absl::Status Parse(
      BisonParserMode mode, absl::string_view filename, absl::string_view input,
      int start_byte_offset, IdStringPool* id_string_pool, zetasql_base::UnsafeArena* arena,
      const LanguageOptions& language_options,
      std::unique_ptr<zetasql::ASTNode>* output,
      std::vector<std::unique_ptr<ASTNode>>* other_allocated_ast_nodes,
      ASTStatementProperties* ast_statement_properties,
      int* statement_end_byte_offset);

  // Returns the characters in the input range given by 'bison_location'. The
  // returned characters will remain valid throughout Parse().
  absl::string_view GetInputText(
      const zetasql_bison_parser::location& bison_location) const {
    ZETASQL_DCHECK_GE(bison_location.end.column, bison_location.begin.column);
    return absl::string_view(
        input_.data() + bison_location.begin.column,
        bison_location.end.column - bison_location.begin.column);
  }

  // Creates an ASTNode of type T with a unique id. Sets its location to
  // the ZetaSQL location equivalent of 'bison_location'. Stores the returned
  // pointer in allocated_ast_nodes_.
  template <typename T>
  T* CreateASTNode(const zetasql_bison_parser::location& bison_location) {
    T* result = new (zetasql_base::AllocateInArena, arena_) T;
    SetNodeLocation(bison_location, result);
    allocated_ast_nodes_->push_back(std::unique_ptr<ASTNode>(result));
    return result;
  }

  // Creates an ASTNode of type T. Sets its location to the ZetaSQL location
  // equivalent of 'bison_location'. Then adds 'children' to the node, without
  // calling InitFields(). Stores the returned pointer in ast_node_pool_.
  template <typename T>
  T* CreateASTNode(
      const zetasql_bison_parser::location& bison_location,
      absl::Span<ASTNode* const> children) {
    T* result = new (zetasql_base::AllocateInArena, arena_) T;
    SetNodeLocation(bison_location, result);
    allocated_ast_nodes_->push_back(std::unique_ptr<ASTNode>(result));
    result->AddChildren(children);
    return result;
  }

  // Same as the previous overload, but sets the start location to the start of
  // 'bison_location_start', and the end location to the end of
  // 'bison_location_end'.
  template <typename T>
  T* CreateASTNode(
      const zetasql_bison_parser::location& bison_location_start,
      const zetasql_bison_parser::location& bison_location_end,
      absl::Span<ASTNode* const> children) {
    T* result = new (zetasql_base::AllocateInArena, arena_) T;
    SetNodeLocation(bison_location_start, bison_location_end, result);
    allocated_ast_nodes_->push_back(std::unique_ptr<ASTNode>(result));
    result->AddChildren(children);
    return result;
  }

  // Creates an ASTIdentifier with text 'name' and location 'location'.
  ASTIdentifier* MakeIdentifier(
      const zetasql_bison_parser::location& location,
      absl::string_view name) {
    auto* identifier = CreateASTNode<ASTIdentifier>(location);
    identifier->SetIdentifier(id_string_pool()->Make(name));
    return identifier;
  }

  // Sets the node location of 'node' to the ZetaSQL equivalent of
  // 'bison_location'.
  void SetNodeLocation(const zetasql_bison_parser::location& bison_location,
                       ASTNode* node) {
    node->set_start_location(zetasql::ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), bison_location.begin.column));
    node->set_end_location(zetasql::ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), bison_location.end.column));
  }

  // Sets the node location of 'node' to the ZetaSQL equivalent of the
  // beginning of 'bison_location_start' through the end of
  // 'bison_location_end'.
  void SetNodeLocation(
      const zetasql_bison_parser::location& bison_location_start,
      const zetasql_bison_parser::location& bison_location_end,
      ASTNode* node) {
    node->set_start_location(zetasql::ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), bison_location_start.begin.column));
    node->set_end_location(zetasql::ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), bison_location_end.end.column));
  }

  static zetasql_bison_parser::location GetBisonLocation(
      const zetasql::ParseLocationRange& location_range) {
    zetasql_bison_parser::location result;
    result.begin.column = location_range.start().GetByteOffset();
    result.end.column = location_range.end().GetByteOffset();
    return result;
  }

  // Sets the start location of 'node' to the start of 'location', and returns
  // 'node'.
  template <typename ASTNodeType>
  ASTNodeType* WithStartLocation(
      ASTNodeType* node,
      const zetasql_bison_parser::location& location) {
    node->set_start_location(ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), location.begin.column));
    return node;
  }

  // Sets the end location of 'node' to the end of 'location', and returns
  // 'node'.
  template <typename ASTNodeType>
  ASTNodeType* WithEndLocation(
      ASTNodeType* node,
      const zetasql_bison_parser::location& location) {
    node->set_end_location(ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), location.end.column));
    return node;
  }

  // Sets the location of 'node' to 'location', and returns 'node'.
  template <typename ASTNodeType>
  ASTNodeType* WithLocation(
      ASTNodeType* node,
      const zetasql_bison_parser::location& location) {
    node->set_start_location(ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), location.begin.column));
    node->set_end_location(ParseLocationPoint::FromByteOffset(
        filename_.ToStringView(), location.end.column));
    return node;
  }

  IdString filename() const { return filename_; }
  IdStringPool* id_string_pool() const { return id_string_pool_; }

  const LanguageOptions& language_options() { return *language_options_; }

  // Returns the next 1-based parameter position.
  int GetNextPositionalParameterPosition() {
    return ++previous_positional_parameter_position_;
  }

  // Move allocated_ast_nodes_ into ast_nodes and reset it. This releases
  // ownership of allocated_ast_nodes_ so the AST can live beyond the lifetime
  // of the parser. This is only intended to be used by a BisonParser that calls
  // CreateASTNode directly instead of using the provided Parse() function. This
  // situation should be rare and only relevant if you have a different parser
  // implementation than the default ZetaSQL parser.
  void ReleaseAllocatedASTNodes(
      std::vector<std::unique_ptr<ASTNode>>* ast_nodes) {
    *ast_nodes = std::move(*allocated_ast_nodes_);
  }

  // Returns true if there is whitespace between `left` and `right`.
  bool HasWhitespace(const zetasql_bison_parser::location& left,
                     const zetasql_bison_parser::location& right) {
    return left.end.column != right.begin.column;
  }

  absl::string_view GetFirstTokenOfNode(
      zetasql_bison_parser::location& bison_location) const;

 private:
  // Identifiers and literal values are allocated from this arena. Not owned.
  // Only valid during Parse().
  IdStringPool* id_string_pool_;

  // The parser allocates all ASTNodes in this arena. Not owned. Only valid
  // during Parse().
  zetasql_base::UnsafeArena* arena_;

  // LanguageOptions to control parser's behavior. Not owned. Only valid
  // during Parse().
  const LanguageOptions* language_options_ = nullptr;

  // ASTNodes that are allocated by the parser are added to this vector during
  // parsing. The root node itself is added when it is allocated, but it is then
  // taken out of the vector again in Parse(). Only valid during Parse().
  std::unique_ptr<std::vector<std::unique_ptr<ASTNode>>> allocated_ast_nodes_;

  // The Flex tokenizer to use.  Only valid during Parse().
  std::unique_ptr<ZetaSqlFlexTokenizer> tokenizer_;

  // The file that the parser input is from.  Can be empty.  Owned by
  // <id_string_pool_>.
  IdString filename_;

  // The parser input. This is used to derive the images for tokens. Only valid
  // during Parse().
  absl::string_view input_;

  // 1-based position of the previous generated positional parameter.
  int previous_positional_parameter_position_ = 0;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_BISON_PARSER_H_
