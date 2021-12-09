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

#include "zetasql/scripting/parsed_script.h"

#include <cstdint>
#include <stack>
#include <string>
#include <utility>

#include "zetasql/common/errors.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "zetasql/scripting/control_flow_graph.h"
#include "zetasql/scripting/error_helpers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/variant.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

constexpr int kDefaultMaxNestingLevel = 50;
ABSL_FLAG(int, zetasql_scripting_max_nesting_level, kDefaultMaxNestingLevel,
          "Maximum supported number of nested scripting statements in a "
          "ZetaSQL script, such as BEGIN...END.");

namespace zetasql {

namespace {

// Visitor to verify the maximum depth of scripting nodes within the AST is
// within the allowable limit.  This check exists to ensure that deeply nested
// scripts will cleanly and determanistically, rather than potentially causing
// a stack overflow later, while traversing the tree.
class VerifyMaxScriptingDepthVisitor : public NonRecursiveParseTreeVisitor {
 public:
  absl::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    // We limit the nesting level of script constructs to a fixed depth,
    // so that which scripts can and cannot execute is stable across
    // implementation changes to either the script executor code or the
    // compiler.
    //
    // To make the depth checking more intuitive to the user, we don't increment
    // the depth counter for purely internal nodes, such as ASTStatementList.
    //
    // We also avoid incrementing the depth counter for expression nodes, in
    // order to maintain compatibility with pre-existing behavior for standalone
    // statements, for which expression depth is limited only by available stack
    // space.  As much as possible, we want any statement that is able to
    // execute without a script to execute successfully, within a script.
    if (node->IsScriptStatement()) {
      if (++depth_ > max_depth_) {
        return MakeSqlErrorAt(node) << "Script statement nesting level "
                                       "exceeds maximum supported limit of "
                                    << max_depth_;
      }
    }

    return VisitResult::VisitChildren(node, [this, node]() {
      if (node->IsScriptStatement()) {
        --depth_;
      }
      return absl::OkStatus();
    });
  }

 private:
  int depth_ = 0;
  const int max_depth_ =
      absl::GetFlag(FLAGS_zetasql_scripting_max_nesting_level);
};

// Visitor to verify that RAISE statements which rethrow an existing exception
// are used only within an exception handler.
class ValidateRaiseStatementsVisitor : public NonRecursiveParseTreeVisitor {
 public:
  ~ValidateRaiseStatementsVisitor() override {
    ZETASQL_DCHECK_EQ(exception_handler_nesting_level_, 0);
  }

  absl::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    if (node->IsExpression() || node->IsSqlStatement()) {
      return VisitResult::Empty();
    }
    return VisitResult::VisitChildren(node);
  }

  absl::StatusOr<VisitResult> visitASTExceptionHandler(
      const ASTExceptionHandler* node) override {
    ++exception_handler_nesting_level_;
    return VisitResult::VisitChildren(node, [this]() {
      --exception_handler_nesting_level_;
      return absl::OkStatus();
    });
  }

  absl::StatusOr<VisitResult> visitASTRaiseStatement(
      const ASTRaiseStatement* node) override {
    if (node->is_rethrow() && exception_handler_nesting_level_ == 0) {
      return MakeSqlErrorAt(node)
             << "Cannot re-raise an existing exception outside of an exception "
                "handler";
    }
    return VisitResult::Empty();
  }

 private:
  int exception_handler_nesting_level_ = 0;
};

// Visitor to verify that:
// 1) Variable declarations occur only at the start of either a BEGIN block or
//    the entire script.
// 2) A variable is not declared which shadows a routine argument or another
//    variable of the same name in either the same scope, or any enclosing scope
class ValidateVariableDeclarationsVisitor
    : public NonRecursiveParseTreeVisitor {
 public:
  explicit ValidateVariableDeclarationsVisitor(
      const ParsedScript* parsed_script)
      : parsed_script_(parsed_script) {}

  absl::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    return VisitResult::VisitChildren(node);
  }

  static bool CanHaveDeclareStmtAsChild(const ASTNode* node) {
    const ASTStatementList* stmt_list = node->GetAsOrNull<ASTStatementList>();
    if (stmt_list == nullptr) {
      return false;
    }
    return stmt_list->variable_declarations_allowed();
  }

  absl::StatusOr<VisitResult> visitASTStatementList(
      const ASTStatementList* node) override {
    bool found_non_variable_decl = false;

    // Check for variable declaration outside of a block and for declaring
    // a variable that already exists.
    for (const ASTStatement* statement : node->statement_list()) {
      if (statement->node_kind() == AST_VARIABLE_DECLARATION) {
        if (found_non_variable_decl || !CanHaveDeclareStmtAsChild(node)) {
          return MakeVariableDeclarationError(
              statement,
              "Variable declarations are allowed only at the start of a "
              "block or script");
        }
        // Check for variable redeclaration.
        for (const ASTIdentifier* id :
             statement->GetAs<ASTVariableDeclaration>()
                 ->variable_list()
                 ->identifier_list()) {
          ZETASQL_RETURN_IF_ERROR(CheckForVariableRedeclaration(
              id, /*check_predefined_vars=*/(node->parent()->node_kind() ==
                                             AST_BEGIN_END_BLOCK)));
        }
      } else {
        found_non_variable_decl = true;
      }
    }
    return VisitResult::VisitChildren(node, [this, node]() {
      // Remove variables declared in this block so that a subsequent block
      // can reuse the variable name.
      for (const ASTStatement* statement : node->statement_list()) {
        if (statement->node_kind() == AST_VARIABLE_DECLARATION) {
          for (const ASTIdentifier* id :
               statement->GetAs<ASTVariableDeclaration>()
                   ->variable_list()
                   ->identifier_list()) {
            variables_.erase(id->GetAsIdString());
          }
        }
      }
      return absl::OkStatus();
    });
  }

  absl::StatusOr<VisitResult> visitASTForInStatement(
      const ASTForInStatement* node) override {
    ZETASQL_RETURN_IF_ERROR(CheckForVariableRedeclaration(
        node->variable(), /*check_predefined_vars=*/true));
    return VisitResult::VisitChildren(node, [this, node]() {
      // Remove FOR variable created here so that subsequent statements
      // can reuse the variable name.
      variables_.erase(node->variable()->GetAsIdString());
      return absl::OkStatus();
    });
  }

  absl::Status MakeVariableDeclarationError(const ASTNode* node,
                                            absl::string_view error_message) {
    return MakeSqlErrorAtNode(node, true) << error_message;
  }

  absl::Status MakeVariableDeclarationError(
      const ASTNode* node, const std::string& error_message,
      absl::string_view source_message,
      const ParseLocationPoint& source_location) {
    std::string script_text(parsed_script_->script_text());
    const InternalErrorLocation location = SetErrorSourcesFromStatus(
        MakeInternalErrorLocation(node),
        ConvertInternalErrorLocationToExternal(
            MakeSqlErrorAtPoint(source_location) << source_message,
            script_text),
        parsed_script_->error_message_mode(), script_text);
    return MakeSqlError().Attach(location) << error_message;
  }

  absl::Status MakeVariableDeclarationErrorSkipSourceLocation(
      const ASTNode* node, const std::string& error_message,
      absl::string_view source_message) {
    std::string script_text(parsed_script_->script_text());
    const InternalErrorLocation location = SetErrorSourcesFromStatus(
        MakeInternalErrorLocation(node),
        ConvertInternalErrorLocationToExternal(MakeSqlError() << source_message,
                                               script_text),
        parsed_script_->error_message_mode(), script_text);
    return MakeSqlError().Attach(location) << error_message;
  }

  // Check with existing variables for variable redeclaration. Local variables
  // are not allowed to have same name as top level variables, including
  // predefined variables.
  absl::Status CheckForVariableRedeclaration(const ASTIdentifier* id,
                                             bool check_predefined_vars) {
    if (check_predefined_vars) {
      if (parsed_script_->GetPredefinedVariables().contains(
              id->GetAsIdString())) {
        return MakeVariableDeclarationErrorSkipSourceLocation(
            id,
            absl::StrCat("Variable '", id->GetAsString(), "' redeclaration"),
            absl::StrCat(id->GetAsString(), "redeclaration"));
      }
    }
    if (!zetasql_base::InsertIfNotPresent(&variables_, id->GetAsIdString(),
                                 id->GetParseLocationRange().start())) {
      return MakeVariableDeclarationError(
          id,
          absl::StrCat("Variable '", id->GetAsString(),
                       "' redeclaration"),
          absl::StrCat(id->GetAsString(), " previously declared here"),
          variables_[id->GetAsIdString()]);
    }
    if (parsed_script_->routine_arguments().contains(id->GetAsIdString())) {
      return MakeVariableDeclarationError(
          id, absl::StrCat("Variable '", id->GetAsString(),
                           "' previously declared as an argument"));
    }
    return absl::OkStatus();
  }

  // Associates each active variable with the location of its declaration.
  // Used to generate the error message if the script later attempts to declare
  // a variable of the same name.
  absl::flat_hash_map<IdString, ParseLocationPoint, IdStringCaseHash,
                      IdStringCaseEqualFunc>
      variables_;

  const ParsedScript* parsed_script_;
};

// Visitor which records a mapping of each node's index as a child of its
// parent.  This is used by the script executor implementation to locate the
// "next" statement or elseif clause after each node finishes running.
class PopulateIndexMapsVisitor : public NonRecursiveParseTreeVisitor {
 public:
  // Caller retains ownership of map_statement_index.
  explicit PopulateIndexMapsVisitor(ParsedScript::NodeIndexMap* map_node_index)
      : map_node_index_(map_node_index) {}

  absl::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    for (int i = 0; i < node->num_children(); i++) {
      (*map_node_index_)[node->child(i)] = i;
    }
    if (node->IsExpression() || node->IsSqlStatement()) {
      return VisitResult::Empty();
    }
    return VisitResult::VisitChildren(node);
  }

 private:
  ParsedScript::NodeIndexMap* map_node_index_;
};

// Visitor to find the node within a script that matches a given position.
class FindNodeFromPositionVisitor : public NonRecursiveParseTreeVisitor {
 public:
  explicit FindNodeFromPositionVisitor(const ParseLocationPoint& location)
      : location_(location) {}

  const ASTNode* match() const { return match_; }

  absl::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    if (match_ != nullptr) {
      return VisitResult::Empty();
    }
    if (node->IsStatement()
        || node->node_kind() == AST_ELSEIF_CLAUSE
        || node->node_kind() == AST_WHEN_THEN_CLAUSE
        || node->node_kind() == AST_UNTIL_CLAUSE
        || node->node_kind() == AST_QUERY) {
      const ParseLocationRange& stmt_range = node->GetParseLocationRange();
      if (stmt_range.start() == location_) {
        match_ = node;
        return VisitResult::Empty();
      }
      return VisitResult::VisitChildren(node);
    }
    return VisitResult::Empty();
  }

  absl::StatusOr<VisitResult> visitASTExceptionHandlerList(
      const ASTExceptionHandlerList* node) override {
    return VisitResult::VisitChildren(node);
  }

  absl::StatusOr<VisitResult> visitASTExceptionHandler(
      const ASTExceptionHandler* node) override {
    return VisitResult::VisitChildren(node);
  }

  absl::StatusOr<VisitResult> visitASTElseifClauseList(
      const ASTElseifClauseList* node) override {
    return VisitResult::VisitChildren(node);
  }

  absl::StatusOr<VisitResult> visitASTWhenThenClauseList(
      const ASTWhenThenClauseList* node) override {
    return VisitResult::VisitChildren(node);
  }

  absl::StatusOr<VisitResult> visitASTStatementList(
      const ASTStatementList* node) override {
    return VisitResult::VisitChildren(node);
  }

  absl::StatusOr<VisitResult> visitASTScript(const ASTScript* node) override {
    return VisitResult::VisitChildren(node);
  }

 private:
  const ParseLocationPoint location_;
  const ASTNode* match_ = nullptr;
};
}  // namespace

absl::StatusOr<const ASTNode*> ParsedScript::FindScriptNodeFromPosition(
    const ParseLocationPoint& start_pos) const {
  FindNodeFromPositionVisitor visitor(start_pos);
  ZETASQL_RETURN_IF_ERROR(script()->TraverseNonRecursive(&visitor));
  return visitor.match();
}

absl::StatusOr<ParsedScript::VariableCreationMap>
ParsedScript::GetVariablesInScopeAtNode(
    const ControlFlowNode * node) const {
  VariableCreationMap variables;
  const ASTNode * ast_node = node->ast_node();

  // If ast_node is an on-going FOR...IN loop, add variable to scope.
  if (ast_node->node_kind() == AST_FOR_IN_STATEMENT
      && node->kind() == ControlFlowNode::Kind::kForAdvance) {
    const ASTForInStatement* for_stmt =
            ast_node->GetAs<ASTForInStatement>();
    variables.insert_or_assign(for_stmt->variable()->GetAsIdString(), for_stmt);
  }

  for (const ASTNode* node_it = ast_node->parent(); node_it != nullptr;
       node_it = node_it->parent()) {
    if (node_it->node_kind() == AST_FOR_IN_STATEMENT) {
      const ASTForInStatement* for_stmt =
            node_it->GetAs<ASTForInStatement>();
      variables.insert_or_assign(for_stmt->variable()->GetAsIdString(),
                                 for_stmt);
    } else if (node_it->node_kind() == AST_STATEMENT_LIST
               && node_it->GetAs<ASTStatementList>()
                      ->variable_declarations_allowed()) {
      // Add variables declared in variable-creating statements up to, but not
      // including, the current statement.
      for (const ASTStatement* stmt :
               node_it->GetAs<ASTStatementList>()->statement_list()) {
        if (stmt == ast_node) {
          // Skip over DECLARE statements that haven't run yet when
          // <statement> is about to begin.
          break;
        }
        if (stmt->node_kind() == AST_VARIABLE_DECLARATION) {
          const ASTVariableDeclaration* decl =
              stmt->GetAs<ASTVariableDeclaration>();
          for (const ASTIdentifier* variable :
               decl->variable_list()->identifier_list()) {
            variables.insert_or_assign(variable->GetAsIdString(), decl);
          }
        } else {
          // Variable declarations are only allowed at the start of a block,
          // before any other statements, so no need to check further.
          break;
        }
      }
    }
  }
  return variables;
}

absl::Status ParsedScript::GatherInformationAndRunChecksInternal() {
  // Check the maximum-depth constraint first, to ensure that other checks
  // do not cause a stack overflow in the case of a deeply nested script.
  VerifyMaxScriptingDepthVisitor max_depth_visitor;
  ZETASQL_RETURN_IF_ERROR(script()->TraverseNonRecursive(&max_depth_visitor));

  ValidateVariableDeclarationsVisitor var_decl_visitor(this);
  ZETASQL_RETURN_IF_ERROR(script()->TraverseNonRecursive(&var_decl_visitor));

  ValidateRaiseStatementsVisitor raise_visitor;
  ZETASQL_RETURN_IF_ERROR(script()->TraverseNonRecursive(&raise_visitor));

  // Walk the parse tree, constructing a StatementIndexMap, associating each
  // statement in the script with its index in the child list of the statement's
  // parent.  This is used to transfer control when advancing through the
  // script.
  PopulateIndexMapsVisitor populate_index_visitor(&node_index_map_);
  ZETASQL_RETURN_IF_ERROR(script()->TraverseNonRecursive(&populate_index_visitor));

  // Callers interested in validating query parameters should call
  // CheckQueryParameters.
  ZETASQL_RETURN_IF_ERROR(PopulateQueryParameters());

  // Generates the control-flow graph.  Also, emits errors if the script
  // contains a BREAK or CONTINUE statement outside of a loop.
  ZETASQL_ASSIGN_OR_RETURN(control_flow_graph_,
                   ControlFlowGraph::Create(script(), script_text()));

  return absl::OkStatus();
}

absl::Status ParsedScript::GatherInformationAndRunChecks() {
  return ConvertInternalErrorLocationAndAdjustErrorString(
      error_message_mode(), script_text(),
      GatherInformationAndRunChecksInternal());
}

ParsedScript::ParsedScript(
    absl::string_view script_string, const ASTScript* ast_script,
    std::unique_ptr<ParserOutput> parser_output,
    ErrorMessageMode error_message_mode, ArgumentTypeMap routine_arguments,
    bool is_procedure,
    const VariableWithTypeParameterMap& predefined_variable_names)
    : parser_output_(std::move(parser_output)),
      ast_script_(ast_script),
      script_string_(script_string),
      error_message_mode_(error_message_mode),
      routine_arguments_(std::move(routine_arguments)),
      is_procedure_(is_procedure),
      predefined_variable_names_(predefined_variable_names) {}

absl::StatusOr<std::unique_ptr<ParsedScript>> ParsedScript::CreateInternal(
    absl::string_view script_string, const ParserOptions& parser_options,
    ErrorMessageMode error_message_mode, ArgumentTypeMap routine_arguments,
    bool is_procedure,
    const VariableWithTypeParameterMap& predefined_variable_names) {
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(ParseScript(script_string, parser_options, error_message_mode,
                              &parser_output));
  const ASTScript* ast_script = parser_output->script();
  std::unique_ptr<ParsedScript> parsed_script = absl::WrapUnique(
      new ParsedScript(script_string, ast_script, std::move(parser_output),
                       error_message_mode, std::move(routine_arguments),
                       is_procedure, predefined_variable_names));
  ZETASQL_RETURN_IF_ERROR(parsed_script->GatherInformationAndRunChecks());
  return parsed_script;
}

absl::StatusOr<std::unique_ptr<ParsedScript>> ParsedScript::Create(
    absl::string_view script_string, const ParserOptions& parser_options,
    ErrorMessageMode error_message_mode,
    const VariableWithTypeParameterMap& predefined_variable_names) {
  return CreateInternal(script_string, parser_options, error_message_mode, {},
                        false, predefined_variable_names);
}

absl::StatusOr<std::unique_ptr<ParsedScript>> ParsedScript::CreateForRoutine(
    absl::string_view script_string, const ParserOptions& parser_options,
    ErrorMessageMode error_message_mode, ArgumentTypeMap routine_arguments,
    const VariableWithTypeParameterMap& predefined_variable_names) {
  return CreateInternal(script_string, parser_options, error_message_mode,
                        std::move(routine_arguments),
                        /*is_procedure=*/true, predefined_variable_names);
}

absl::StatusOr<std::unique_ptr<ParsedScript>> ParsedScript::CreateForRoutine(
    absl::string_view script_string, const ASTScript* ast_script,
    ErrorMessageMode error_message_mode, ArgumentTypeMap routine_arguments,
    const VariableWithTypeParameterMap& predefined_variable_names) {
  std::unique_ptr<ParsedScript> parsed_script = absl::WrapUnique(
      new ParsedScript(script_string, ast_script, /*parser_output=*/nullptr,
                       error_message_mode, std::move(routine_arguments),
                       /*is_procedure=*/true, predefined_variable_names));
  ZETASQL_RETURN_IF_ERROR(parsed_script->GatherInformationAndRunChecks());
  return parsed_script;
}

absl::StatusOr<std::unique_ptr<ParsedScript>> ParsedScript::Create(
    absl::string_view script_string, const ASTScript* ast_script,
    ErrorMessageMode error_message_mode,
    const VariableWithTypeParameterMap& predefined_variable_names) {
  std::unique_ptr<ParsedScript> parsed_script = absl::WrapUnique(
      new ParsedScript(script_string, ast_script, /*parser_output=*/nullptr,
                       error_message_mode, {}, /*is_procedure=*/false,
                       predefined_variable_names));
  ZETASQL_RETURN_IF_ERROR(parsed_script->GatherInformationAndRunChecks());
  return parsed_script;
}

absl::Status ParsedScript::PopulateQueryParameters() {
  std::vector<const ASTNode*> query_parameters;
  script()->GetDescendantSubtreesWithKinds({AST_PARAMETER_EXPR},
                                           &query_parameters);
  std::set<ParseLocationPoint> positional_points;
  for (const ASTNode* node : query_parameters) {
    const ASTParameterExpr* query_parameter =
        node->GetAsOrDie<ASTParameterExpr>();
    const ParseLocationRange& range = query_parameter->GetParseLocationRange();
    const ParseLocationPoint& point = range.start();
    if (query_parameter->name() == nullptr) {
      positional_points.insert(point);
    } else {
      named_query_parameters_.insert(
          {point, query_parameter->name()->GetAsIdString()});
    }
  }

  if (!positional_points.empty() && !named_query_parameters_.empty()) {
    return MakeScriptExceptionAt(script())
           << "Cannot mix named and positional parameters in scripts";
  }

  // Add the proper indices now that we know the sort order.
  int i = 0;
  for (auto itr = positional_points.begin(); itr != positional_points.end();
       ++itr, i++) {
    positional_query_parameters_.insert({*itr, i});
  }

  return absl::OkStatus();
}

std::pair<int64_t, int64_t> ParsedScript::GetPositionalParameters(
    const ParseLocationRange& range) const {
  auto lower = positional_query_parameters_.lower_bound(range.start());
  auto upper = positional_query_parameters_.upper_bound(range.end());

  // Attempt to return something sane if there are no positional parameters in
  // the segment.
  if (lower == upper) {
    return {0, 0};
  }

  int64_t start = lower->second;
  int64_t end = upper == positional_query_parameters_.end()
                    ? positional_query_parameters_.size()
                    : upper->second;
  return {start, end - start};
}

ParsedScript::StringSet ParsedScript::GetNamedParameters(
    const ParseLocationRange& range) const {
  ParsedScript::StringSet result;

  auto lower = named_query_parameters_.lower_bound(range.start());
  auto upper = named_query_parameters_.upper_bound(range.end());
  for (; lower != upper; ++lower) {
    result.insert(lower->second.ToStringView());
  }

  return result;
}

ParsedScript::StringSet ParsedScript::GetAllNamedParameters() const {
  ParsedScript::StringSet result;

  for (auto itr = named_query_parameters_.begin();
       itr != named_query_parameters_.end(); ++itr) {
    result.insert(itr->second.ToStringView());
  }

  return result;
}

absl::Status ParsedScript::CheckQueryParameters(
    const ParsedScript::QueryParameters& parameters) const {
  return ConvertInternalErrorLocationAndAdjustErrorString(
      error_message_mode(), script_text(),
      CheckQueryParametersInternal(parameters));
}

absl::Status ParsedScript::CheckQueryParametersInternal(
    const ParsedScript::QueryParameters& parameters) const {
  // TODO: Remove this check once everywhere else uses parameters.
  if (!parameters.has_value()) {
    return absl::OkStatus();
  }

  int64_t num_positionals = positional_query_parameters_.size();
  ParsedScript::StringSet named_parameters = GetAllNamedParameters();

  if (num_positionals > 0) {
    int64_t known_num_positionals = 0;
    if (parameters.has_value() &&
        absl::holds_alternative<int64_t>(parameters.value())) {
      known_num_positionals = absl::get<int64_t>(parameters.value());
    }
    if (num_positionals > known_num_positionals) {
      return MakeScriptExceptionAt(script())
             << "Script has " << num_positionals
             << " positional parameters but only " << known_num_positionals
             << " were supplied";
    }
  } else if (!named_parameters.empty()) {
    const ParsedScript::StringSet* known_named_parameters = nullptr;
    if (parameters.has_value()) {
      known_named_parameters =
          absl::get_if<ParsedScript::StringSet>(&parameters.value());
    }
    IdStringPool pool;
    for (absl::string_view parsed_name : named_parameters) {
      if (known_named_parameters == nullptr ||
          known_named_parameters->find(parsed_name) ==
              known_named_parameters->end()) {
        return MakeScriptExceptionAt(script())
               << "Unknown named query parameter: " << parsed_name;
      }
    }
  }

  return absl::OkStatus();
}

}  // namespace zetasql
