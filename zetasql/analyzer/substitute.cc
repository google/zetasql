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

#include "zetasql/analyzer/substitute.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/analyzer_impl.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/multi_catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// A ColumnRefLayer represents the columns, keyed by column id, referred to by
// subqueries below a certain subquery. These references end up getting added to
// the parameter list of the corresponding subquery.
// These ColumnRef pointers point to ColumnRefs in parameter_list of lambdas,
// they are valid during the AnalyzeSubstituteCall.
using ColumnRefLayer = absl::flat_hash_map<int, const ResolvedColumnRef*>;

// Adds a new ColumnRefLayer to a stack of such layers, representing the columns
// referenced by a stack of subqueries. The new layer represents the innermost
// subquery. On destruction, if the subquery is not the outermost subquery,
// propagates any new column refs from the inner layers to the next layer out.
// This is done because any columns referenced by inner subqueries which are not
// created in those subqueries must also be referenced in the parameter lists of
// subqueries higher in the stack.
class ScopedColumnRefLayer {
 public:
  explicit ScopedColumnRefLayer(std::vector<ColumnRefLayer>& column_refs_stack)
      : column_refs_stack_(column_refs_stack) {
    // Adds a new layer to the column refs stack.
    column_refs_stack_.emplace_back();
  }

  // Pops the top layer of the column refs stack.
  ~ScopedColumnRefLayer() {
    ZETASQL_DCHECK(!column_refs_stack_.empty());
    if (column_refs_stack_.empty()) {
      return;
    }
    auto layer_count = column_refs_stack_.size();
    if (layer_count > 1) {
      // Move column refs in the current layer into the next upper layer.
      const ColumnRefLayer& current_layer = column_refs_stack_[layer_count - 1];
      ColumnRefLayer& upper_layer = column_refs_stack_[layer_count - 2];
      for (auto& col : current_layer) {
        upper_layer.insert({col.first, col.second});
      }
    }
    column_refs_stack_.pop_back();
  }

  std::vector<ColumnRefLayer>& column_refs_stack_;
};

// For the lambda body, we only need to replace column refs with the
// corresponding invoke args. This is separate from the
// VariableReplacementInserter to avoid unintended interactions.
class ColumnRefReplacer : public ResolvedASTDeepCopyVisitor {
 public:
  // 'column_ref_map' is a map of column IDs to the column that should replace
  // references to that column ID. The column IDs will be the IDs of the
  // lambda's args columns. The ResolvedColumns will be the corresponding args
  // to the INVOKE function from the expression.
  explicit ColumnRefReplacer(
      const absl::flat_hash_map<int, const ResolvedColumnRef*>& column_ref_map)
      : lambda_column_ref_map_(column_ref_map) {}

  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override;

 private:
  const absl::flat_hash_map<int, const ResolvedColumnRef*>&
      lambda_column_ref_map_;
};

absl::Status ColumnRefReplacer::VisitResolvedColumnRef(
    const ResolvedColumnRef* node) {
  const ResolvedColumn& column = node->column();
  auto it = lambda_column_ref_map_.find(column.column_id());
  if (it == lambda_column_ref_map_.end()) {
    return CopyVisitResolvedColumnRef(node);
  }

  // Map lambda argument columns to columns that they are representing.
  // If either node or target_column is correlated, the new column ref is
  // correlated.
  const ResolvedColumnRef* target_column_ref = it->second;
  const ResolvedColumn& target_column = target_column_ref->column();
  PushNodeToStack(MakeResolvedColumnRef(
      target_column.type(), target_column,
      node->is_correlated() || target_column_ref->is_correlated()));
  return absl::OkStatus();
}

// Replaces ColumnRefs in 'node' according to 'column_ref_map' based on column
// id.
absl::StatusOr<std::unique_ptr<ResolvedExpr>> ReplaceColumnRefs(
    const ResolvedExpr* node,
    absl::flat_hash_map<int, const ResolvedColumnRef*>& column_ref_map) {
  ColumnRefReplacer column_map_replacer(column_ref_map);
  ZETASQL_RETURN_IF_ERROR(node->Accept(&column_map_replacer));
  return column_map_replacer.ConsumeRootNode<ResolvedExpr>();
}

class VariableReplacementInserter : public ResolvedASTDeepCopyVisitor {
 public:
  // 'referenced_columns' is inserted into the parameter list for the
  // outermost subquery.
  //
  // We use named parameters as placeholders where we incorporate
  // 'projected_vars' and 'lambdas' by name. This deep copy visitor does that
  // replacement work. We use projection for 'projected_vars', rather than
  // in-place substitution, to prevent the input expressions from being
  // expressed more than once in our output AST. We use in-place substitution
  // for 'lambdas' as they need to be evaluated at the location specified by
  // 'expression'.
  VariableReplacementInserter(
      const std::vector<std::unique_ptr<const ResolvedColumnRef>>&
          referenced_columns,
      const absl::flat_hash_map<std::string, const ResolvedExpr*>&
          projected_vars,
      const Function* invoke_function,
      const absl::flat_hash_map<std::string, const ResolvedInlineLambda*>&
          lambdas);

  absl::Status VisitResolvedParameter(const ResolvedParameter* node) override;

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override;

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override;

 private:
  // Returns if the enclosing SubqueryExpr is the outermost.
  bool InOutermostSubquery() { return column_refs_stack_.size() == 1; }

  // Adds collected lambda column refs to parameter_list of 'subquery_expr'.
  // If 'subquery_expr' is the outermost, column refs are added as it is in
  // original lambda parameter_list.
  // If 'subquery_expr' is not the outermost, column refs are added to
  // parameter_list as correlated.
  absl::Status AddLambdaColumnRefsToSubqueryExpr(
      ResolvedSubqueryExpr* subquery_expr);

  const std::vector<std::unique_ptr<const ResolvedColumnRef>>&
      referenced_columns_;
  const absl::flat_hash_map<std::string, const ResolvedExpr*>& projected_vars_;
  const Function* invoke_function_;
  const absl::flat_hash_map<std::string, const ResolvedInlineLambda*>& lambdas_;

  // A stack of ColumnRef collection for recording lambda column refs.
  // One layer is created for each SubqueryExpr. ColumnRefs collected are
  // added to the parameter_list of the SubqueryExpr.
  std::vector<ColumnRefLayer> column_refs_stack_;
};

// This object holds options, catalog and type_factory so the interior functions
// don't each have to take them as params.
class ExpressionSubstitutor {
 public:
  ExpressionSubstitutor(AnalyzerOptions options,
                        Catalog& catalog, TypeFactory& type_factory);

  absl::StatusOr<std::unique_ptr<ResolvedExpr>> Substitute(
      absl::string_view expression,
      const absl::flat_hash_map<std::string, const ResolvedExpr*>& variables,
      const absl::flat_hash_map<std::string, const ResolvedInlineLambda*>&
          lambdas);

 private:
  // Sets up the invoke catalog for invoking lambda.
  absl::Status SetupInvokeCatalog();

  AnalyzerOptions options_;
  TypeFactory& type_factory_;

  // The following are used to setup a catalog for INVOKE function. INVOKE
  // function is initialized only when lambdas are involved.
  std::unique_ptr<Function> invoke_function_;
  std::unique_ptr<SimpleCatalog> invoke_catalog_;
  std::unique_ptr<MultiCatalog> catalog_with_invoke_;

  // Points the catalog in for methods of this class.
  // Points to input_catalog if there is no lambdas or the lazily initialized
  // catalog_with_invoke_ if there is any lambda.
  Catalog* catalog_;
};

ExpressionSubstitutor::ExpressionSubstitutor(AnalyzerOptions options,
                                             Catalog& catalog,
                                             TypeFactory& type_factory)
    : options_(std::move(options)),
      type_factory_(type_factory),
      catalog_(&catalog) {
  options_.set_parameter_mode(ParameterMode::PARAMETER_NAMED);

  // We expect options_ to come from the outer analysis of the overall query.
  // It would not be unusual for the analyzer to have some parameters, but we
  // don't need them here, because this API disallows expressions with
  // parameters.
  options_.clear_positional_query_parameters();
  options_.clear_query_parameters();

  // Do not record location info. The SQL is generated so any location info
  // returned would be meaningless to the end user.
  options_.set_record_parse_locations(false);
}

// Lambdas are invoked with a INVOKE function call in 'expression' parameter of
// AnalyzeSubstitute. INVOKE function is a implementation detail of this file
// and not a ZetaSQL standard function. A catalog has to be setup before
// analyzing the input expression.
absl::Status ExpressionSubstitutor::SetupInvokeCatalog() {
  ZETASQL_RETURN_IF_ERROR(MultiCatalog::Create(catalog_->FullName(), {catalog_},
                                       &catalog_with_invoke_));
  invoke_function_ = absl::make_unique<Function>("invoke", "", Function::SCALAR,
                                                 FunctionOptions());
  invoke_function_->AddSignature(FunctionSignature(
      /*result_type=*/{SignatureArgumentKind::ARG_TYPE_ANY_1},
      /*arguments=*/
      {{SignatureArgumentKind::ARG_TYPE_ANY_1, FunctionArgumentType::REQUIRED},
       {SignatureArgumentKind::ARG_TYPE_ARBITRARY,
        FunctionArgumentType::REPEATED}},
      /*context_id=*/-1));

  invoke_catalog_ = absl::make_unique<SimpleCatalog>("invoke_catalog");
  invoke_catalog_->AddFunction(invoke_function_.get());
  catalog_with_invoke_->AppendCatalog(invoke_catalog_.get());
  // Point the catalog in use to the new catalog.
  catalog_ = catalog_with_invoke_.get();
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<ResolvedExpr>> ExpressionSubstitutor::Substitute(
    absl::string_view expression,
    const absl::flat_hash_map<std::string, const ResolvedExpr*>& variables,
    const absl::flat_hash_map<std::string, const ResolvedInlineLambda*>&
        lambdas) {
  // Set up catalog making INVOKE available if there is any lambda.
  if (!lambdas.empty()) {
    ZETASQL_RETURN_IF_ERROR(SetupInvokeCatalog());
  }

  // This select list introduces the aliases for the 'variables' that are in
  // turn used in 'expression' as columns.
  std::vector<std::string> select_list;
  select_list.reserve(variables.size());

  // Any referenced column from 'select_list' will need to be inserted into
  // the outermost subquery in the resulting expression, so that it can be
  // used in the project scan.
  std::vector<std::unique_ptr<const ResolvedColumnRef>> referenced_columns;

  for (const auto& var : variables) {
    ZETASQL_RETURN_IF_ERROR(CollectColumnRefs(*var.second, &referenced_columns));

    // We represent each variable as a named parameter in the inner query.
    // After analyzing the query, we will go back through and replace
    // named parameters with the corresponding resolved expressions.
    select_list.push_back(absl::StrCat("@", var.first, " AS ", var.first));
    ZETASQL_RETURN_IF_ERROR(options_.AddQueryParameter(var.first, var.second->type()));
  }

  // Compare two referenced columns.
  auto cmp = [](const std::unique_ptr<const ResolvedColumnRef>& l,
                const std::unique_ptr<const ResolvedColumnRef>& r) {
    if (l->column().column_id() != r->column().column_id()) {
      return l->column().column_id() < r->column().column_id();
    }
    return l->is_correlated() < r->is_correlated();
  };

  auto eq = [](const std::unique_ptr<const ResolvedColumnRef>& l,
               const std::unique_ptr<const ResolvedColumnRef>& r) {
    return l->column().column_id() == r->column().column_id() &&
           l->is_correlated() == r->is_correlated();
  };

  // Erase any duplicates from the referenced columns list.
  std::sort(referenced_columns.begin(), referenced_columns.end(), cmp);
  referenced_columns.erase(
      std::unique(referenced_columns.begin(), referenced_columns.end(), eq),
      referenced_columns.end());

  // In 'expression', lambdas are referenced as named parameters instead of
  // columns.
  for (const auto& lambda : lambdas) {
    ZETASQL_RETURN_IF_ERROR(options_.AddQueryParameter(lambda.first,
                                               lambda.second->body()->type()));
  }

  // Sort <select_list> so that the AST we generate is stable.
  std::sort(select_list.begin(), select_list.end());
  // Produces:
  // ( SELECT <expression> FROM ( SELECT <joined_select_list> ) )
  // The inner select sees the aliases introduced in the select list. After
  // analysis, we will replace the positional parameters with the
  // corresponding ResolvedExprs from variables.
  std::string sql;
  if (select_list.empty()) {
    sql = absl::StrCat("( SELECT ", expression, " )");
  } else {
    sql = absl::StrCat("( SELECT ", expression, " FROM ( SELECT ",
                       absl::StrJoin(select_list, ", "), " ) )");
  }
  std::unique_ptr<const AnalyzerOutput> output;
  ZETASQL_RETURN_IF_ERROR(InternalAnalyzeExpression(sql, options_, catalog_,
                                            &type_factory_, nullptr, &output))
      << "while parsing substitution sql: " << sql;
  ZETASQL_VLOG(1) << "Initial ast: " << output->resolved_expr()->DebugString();

  VariableReplacementInserter replacer(referenced_columns, variables,
                                       invoke_function_.get(), lambdas);
  ZETASQL_RETURN_IF_ERROR(output->resolved_expr()->Accept(&replacer));
  return replacer.ConsumeRootNode<ResolvedExpr>();
}

VariableReplacementInserter::VariableReplacementInserter(
    const std::vector<std::unique_ptr<const ResolvedColumnRef>>&
        referenced_columns,
    const absl::flat_hash_map<std::string, const ResolvedExpr*>& projected_vars,
    const Function* invoke_function,
    const absl::flat_hash_map<std::string, const ResolvedInlineLambda*>&
        lambdas)
    : referenced_columns_(referenced_columns),
      projected_vars_(projected_vars),
      invoke_function_(invoke_function),
      lambdas_(lambdas) {}

absl::Status VariableReplacementInserter::VisitResolvedFunctionCall(
    const ResolvedFunctionCall* node) {
  if (node->function() != invoke_function_) {
    return CopyVisitResolvedFunctionCall(node);
  }

  // The first argument of the INVOKE function call is a named parameter
  // indicating the lambda arg.
  if (!node->argument_list(0)->Is<ResolvedParameter>()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "First argument to invoke must be a parameter with the name of "
              "the lambda to be invoked, got: "
           << node->argument_list(0)->DebugString();
  }
  const auto* resolved_parameter =
      node->argument_list(0)->GetAs<ResolvedParameter>();
  auto it = lambdas_.find(resolved_parameter->name());
  if (it == lambdas_.end()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "No lambda named " << resolved_parameter->name() << " is found.";
  }
  const ResolvedInlineLambda* lambda = it->second;

  // Record lambda column refs for the enclosing SubqueryExpr.
  // The recorded ColumnRefs would be put into parameter_list of the enclosing
  // SubqueryExpr.
  ZETASQL_RET_CHECK(!column_refs_stack_.empty()) << node->DebugString();
  ColumnRefLayer& ref_layer = column_refs_stack_.back();
  for (const auto& ref : lambda->parameter_list()) {
    ref_layer.emplace(ref->column().column_id(), ref.get());
  }

  // The arguments following the first argument of the INVOKE call are a list of
  // columns to replace the lambda columns.
  ZETASQL_RET_CHECK_GE(node->argument_list_size() - 1, lambda->argument_list_size());
  // Build map from lambda argument column to INVOKE argument column.
  absl::flat_hash_map<int, const ResolvedColumnRef*> lambda_columns_map;
  for (int i = 0; i < lambda->argument_list_size(); i++) {
    ZETASQL_RET_CHECK(node->argument_list(i + 1)->Is<ResolvedColumnRef>())
        << "INVOKE arguments after the first one must be a ColumnRef.";
    const ResolvedColumnRef* invoke_column =
        node->argument_list(i + 1)->GetAs<ResolvedColumnRef>();
    lambda_columns_map.emplace(lambda->argument_list(i).column_id(),
                               invoke_column);
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> body,
                   ReplaceColumnRefs(lambda->body(), lambda_columns_map));

  PushNodeToStack(std::move(body));
  return absl::OkStatus();
}

absl::Status VariableReplacementInserter::VisitResolvedParameter(
    const ResolvedParameter* node) {
  ZETASQL_RET_CHECK_EQ(node->position(), 0)
      << "Only named parameters should be present in the input, got: "
      << node->DebugString();

  std::unique_ptr<ResolvedExpr> replacement;
  auto it = projected_vars_.find(node->name());
  if (it == projected_vars_.end()) {
    if (lambdas_.contains(node->name())) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Lambda can only be used as first argument of INVOKE: "
             << node->name();
    }
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Parameter not found: " << node->name();
  }
  ZETASQL_ASSIGN_OR_RETURN(replacement, CorrelateColumnRefs(*it->second));
  PushNodeToStack(std::move(replacement));
  return absl::OkStatus();
}

absl::Status VariableReplacementInserter::VisitResolvedSubqueryExpr(
    const ResolvedSubqueryExpr* node) {
  ScopedColumnRefLayer scoped_column_ref_layer(column_refs_stack_);

  ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedSubqueryExpr(node));
  std::unique_ptr<ResolvedSubqueryExpr> subquery_expr =
      ConsumeTopOfStack<ResolvedSubqueryExpr>();
  if (InOutermostSubquery()) {
    // Add column refs in projected 'variables' to outermost SubqueryExpr.
    for (const auto& ref : referenced_columns_) {
      ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedColumnRef(ref.get()));
      subquery_expr->add_parameter_list(ConsumeTopOfStack<ResolvedColumnRef>());
    }
  }

  ZETASQL_RETURN_IF_ERROR(AddLambdaColumnRefsToSubqueryExpr(subquery_expr.get()));

  PushNodeToStack(std::move(subquery_expr));
  return absl::OkStatus();
}

absl::Status VariableReplacementInserter::AddLambdaColumnRefsToSubqueryExpr(
    ResolvedSubqueryExpr* subquery_expr) {
  // Build a set of added column ids to avoid duplicates.
  absl::flat_hash_set<int> added_column_id_set;
  for (const auto& ref : subquery_expr->parameter_list()) {
    added_column_id_set.emplace(ref->column().column_id());
  }

  for (const auto& id_and_column : column_refs_stack_.back()) {
    if (!added_column_id_set.insert(id_and_column.first).second) {
      continue;
    }

    ZETASQL_ASSIGN_OR_RETURN(auto copy, ProcessNode(id_and_column.second));
    if (!InOutermostSubquery()) {
      copy->set_is_correlated(true);
    }
    subquery_expr->add_parameter_list(std::move(copy));
  }
  return absl::OkStatus();
}
}  // namespace

absl::Status ExpectAnalyzeSubstituteSuccess(
    zetasql_base::StatusBuilder status_builder) {
  ZETASQL_RET_CHECK_OK(status_builder) << "Unexpected error in AnalyzeSubstitute()";
  return status_builder;
}

absl::StatusOr<std::unique_ptr<ResolvedExpr>> AnalyzeSubstitute(
    AnalyzerOptions options, Catalog& catalog, TypeFactory& type_factory,
    absl::string_view expression,
    const absl::flat_hash_map<std::string, const ResolvedExpr*>& variables,
    const absl::flat_hash_map<std::string, const ResolvedInlineLambda*>&
        lambdas) {
  constexpr absl::string_view arenas_msg =
      "AnalyzeSubstitute: All arenas and the column sequence number must be "
      "set on the input options, and they must be the same arenas used to "
      "generate the variables and lambdas. Otherwise, the result of "
      "substitution will be subtly broken";
  ZETASQL_RET_CHECK(options.id_string_pool()) << arenas_msg;
  ZETASQL_RET_CHECK(options.arena()) << arenas_msg;
  ZETASQL_RET_CHECK(options.column_id_sequence_number()) << arenas_msg;

  return ExpressionSubstitutor(std::move(options), catalog, type_factory)
      .Substitute(expression, variables, lambdas);
}

}  // namespace zetasql
