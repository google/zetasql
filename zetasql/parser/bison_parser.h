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
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/arena_allocator.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser_runtime_info.h"
#include "zetasql/parser/statement_properties.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"

namespace zetasql_bison_parser {}  // namespace zetasql_bison_parser

namespace zetasql {
namespace parser {

// Wrapper class for the Bison parser for ZetaSQL. Contains the context data
// for the parser. The actual Bison parser is generated as
// zetasql_bison_parser::BisonParserImpl.
class BisonParser {
 public:
  BisonParser() = default;
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
        input_(input),
        parser_runtime_info_(
            std::make_unique<ParserRuntimeInfo>(language_options)) {}

  ~BisonParser() = default;

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
  // If mode is `kNextStatementKind`, then the next statement kind is returned
  // in `ast_statement_properties`, and statement level hints are returned in
  // `output`. In this mode, `statement_end_byte_offset` is *not* set.
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
      const macros::MacroCatalog* macro_catalog,
      std::unique_ptr<zetasql::ASTNode>* output,
      std::vector<std::unique_ptr<ASTNode>>* other_allocated_ast_nodes,
      ASTStatementProperties* ast_statement_properties,
      int* statement_end_byte_offset);

  // Returns the characters in the input range given by 'bison_location'. The
  // returned characters will remain valid throughout Parse().
  template <typename Location>
  absl::string_view GetInputText(const Location& bison_location) const {
    return bison_location.GetTextFrom(input_);
  }

  // Creates an ASTNode of type T with a unique id. Sets its location to
  // the ZetaSQL location equivalent of 'bison_location'. Stores the returned
  // pointer in allocated_ast_nodes_.
  template <typename T, typename Location>
  T* CreateASTNode(const Location& bison_location) {
    T* result = new (zetasql_base::AllocateInArena, arena_) T;
    SetNodeLocation(bison_location, result);
    allocated_ast_nodes_->push_back(std::unique_ptr<ASTNode>(result));
    return result;
  }

  // Creates an ASTNode of type T. Sets its location to the ZetaSQL location
  // equivalent of 'bison_location'. Then adds 'children' to the node, without
  // calling InitFields(). Stores the returned pointer in ast_node_pool_.
  template <typename T, typename Location>
  T* CreateASTNode(const Location& bison_location,
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
  template <typename T, typename Location>
  T* CreateASTNode(const Location& bison_location_start,
                   const Location& bison_location_end,
                   absl::Span<ASTNode* const> children) {
    T* result = new (zetasql_base::AllocateInArena, arena_) T;
    SetNodeLocation(bison_location_start, bison_location_end, result);
    allocated_ast_nodes_->push_back(std::unique_ptr<ASTNode>(result));
    result->AddChildren(children);
    return result;
  }

  // Creates an ASTIdentifier with text 'name' and location 'location'.
  template <typename Location>
  ASTIdentifier* MakeIdentifier(const Location& location,
                                absl::string_view name) {
    auto* identifier = CreateASTNode<ASTIdentifier>(location);
    identifier->SetIdentifier(id_string_pool()->Make(name));
    return identifier;
  }

  // Creates an ASTIdentifier with text 'name' and location 'location'.
  template <typename Location>
  ASTLocation* MakeLocation(const Location& location) {
    auto* loc = CreateASTNode<ASTLocation>(location);
    return loc;
  }

  // Sets the node location of 'node' to the ZetaSQL equivalent of
  // 'bison_location'.
  template <typename Location>
  void SetNodeLocation(const Location& bison_location, ASTNode* node) {
    node->set_start_location(bison_location.start());
    node->set_end_location(bison_location.end());
  }

  // Sets the node location of 'node' to the ZetaSQL equivalent of the
  // beginning of 'bison_location_start' through the end of
  // 'bison_location_end'.
  template <typename Location>
  void SetNodeLocation(Location& bison_location_start,
                       Location& bison_location_end, ASTNode* node) {
    node->set_start_location(bison_location_start.start());
    node->set_end_location(bison_location_end.end());
  }

  // Sets the start location of 'node' to the start of 'location', and returns
  // 'node'.
  template <typename ASTNodeType, typename Location>
  ASTNodeType* WithStartLocation(ASTNodeType* node, const Location& location) {
    node->set_start_location(location.start());
    return node;
  }

  // Sets the end location of 'node' to the end of 'location', and returns
  // 'node'.
  template <typename ASTNodeType, typename Location>
  ASTNodeType* WithEndLocation(ASTNodeType* node, const Location& location) {
    node->set_end_location(location.end());
    return node;
  }

  // Sets the location of 'node' to 'location', and returns 'node'.
  template <typename ASTNodeType, typename Location>
  ASTNodeType* WithLocation(ASTNodeType* node, const Location& location) {
    node->set_start_location(location.start());
    node->set_end_location(location.end());
    return node;
  }

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
  template <typename Location>
  bool HasWhitespace(const Location& left, const Location& right) {
    return left.end().GetByteOffset() != right.start().GetByteOffset();
  }

  template <typename Location>
  absl::string_view GetFirstTokenOfNode(const Location& bison_location) const {
    absl::string_view text = GetInputText(bison_location);
    for (int i = 0; i < text.size(); i++) {
      if (absl::ascii_isblank(text[i])) {
        return text.substr(0, i);
      }
    }
    return text;
  }

  absl::Status GenerateWarning(const absl::string_view text,
                               const int byte_offset) {
    return MakeSqlErrorAtPoint(zetasql::ParseLocationPoint::FromByteOffset(
               filename_.ToStringView(), byte_offset))
           << text;
  }

  absl::Status GenerateWarningForFutureKeywordReservation(
      const absl::string_view keyword, const int byte_offset) {
    return GenerateWarning(
        absl::Substitute(
            "$0 is used as an identifier. $0 may become a reserved word "
            "in the future. To make this statement robust, add backticks "
            "around $0 to make the identifier unambiguous",
            keyword),
        byte_offset);
  }

  void AddWarning(const absl::Status& status) { warnings_->push_back(status); }

  std::unique_ptr<std::vector<absl::Status>> release_warnings() {
    return std::move(warnings_);
  }

  std::unique_ptr<ParserRuntimeInfo> release_runtime_info() {
    return std::move(parser_runtime_info_);
  }

 private:
  absl::Status ParseInternal(
      BisonParserMode mode, absl::string_view filename, absl::string_view input,
      int start_byte_offset, IdStringPool* id_string_pool, zetasql_base::UnsafeArena* arena,
      const LanguageOptions& language_options,
      const macros::MacroCatalog* macro_catalog,
      std::unique_ptr<zetasql::ASTNode>* output,
      std::vector<std::unique_ptr<ASTNode>>* other_allocated_ast_nodes,
      ASTStatementProperties* ast_statement_properties,
      int* statement_end_byte_offset);

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

  // The file that the parser input is from.  Can be empty.  Owned by
  // <id_string_pool_>.
  IdString filename_;

  // The parser input. This is used to derive the images for tokens. Only valid
  // during Parse().
  absl::string_view input_;

  // 1-based position of the previous generated positional parameter.
  int previous_positional_parameter_position_ = 0;

  // Warnings found during parsing. Useful when monitoring usage of particular
  // syntax usage (e.g., to assess the impact of reserving a keyword)
  std::unique_ptr<std::vector<absl::Status>> warnings_ =
      std::make_unique<std::vector<absl::Status>>();

  std::unique_ptr<ParserRuntimeInfo> parser_runtime_info_;
};

// These are defined here because some of our tests rely on grabbing quoted
// words in order to ensure test completeness of tokens. Indirecting these
// strings as globals here allows us to use them in generating warnings without
// interrupting those tests.
inline constexpr absl::string_view kQualify = "QUALIFY";

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_BISON_PARSER_H_
