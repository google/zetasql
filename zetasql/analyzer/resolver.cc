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

#include "zetasql/analyzer/resolver.h"

#include <algorithm>
#include <memory>
#include <set>
#include <utility>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/options.pb.h"
#include <cstdint>
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/function_resolver.h"
#include "absl/container/flat_hash_map.h"
// This includes common macro definitions to define in the resolver cc files.
#include "zetasql/analyzer/resolver_common_inl.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parser.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/convert_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/make_node_vector.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/types/variant.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/status_payload.h"

namespace zetasql {

const std::map<int, Resolver::SpecialArgumentType>* const
    Resolver::kEmptyArgumentOptionMap =
        new std::map<int, Resolver::SpecialArgumentType>();

Resolver::Resolver(Catalog* catalog, TypeFactory* type_factory,
                   const AnalyzerOptions* analyzer_options)
    : catalog_(catalog),
      type_factory_(type_factory),
      analyzer_options_(*analyzer_options),
      coercer_(type_factory, analyzer_options_.default_time_zone(),
               &analyzer_options_.language()),
      empty_name_list_(new NameList),
      empty_name_scope_(new NameScope(*empty_name_list_)),
      id_string_pool_(analyzer_options_.id_string_pool().get()) {
  function_resolver_ =
      absl::make_unique<FunctionResolver>(catalog, type_factory, this);
  DCHECK(analyzer_options_.AllArenasAreInitialized());
}

Resolver::~Resolver() {
}

void Resolver::Reset(absl::string_view sql) {
  sql_ = sql;
  named_subquery_map_.clear();
  unique_with_alias_names_.clear();
  next_subquery_id_ = 1;
  next_unnest_id_ = 1;
  analyzing_expression_ = false;
  analyzing_partition_by_clause_name_ = nullptr;
  disallowing_query_parameters_with_error_ = {};
  // generated_column_cycle_detector_ contains a pointer to a local object, so
  // there is no need to deallocate.
  generated_column_cycle_detector_ = nullptr;
  analyzing_stored_expression_columns_ = false;
  analyzing_check_constraint_expression_ = false;
  unique_deprecation_warnings_.clear();
  deprecation_warnings_.clear();
  function_arguments_.clear();
  function_table_arguments_.clear();
  resolved_columns_from_table_scans_.clear();

  if (analyzer_options_.column_id_sequence_number() != nullptr) {
    next_column_id_sequence_ = analyzer_options_.column_id_sequence_number();
  } else {
    owned_column_id_sequence_ = absl::make_unique<zetasql_base::SequenceNumber>();
    next_column_id_sequence_ = owned_column_id_sequence_.get();
  }
}

int Resolver::AllocateColumnId() {
  int64_t id = next_column_id_sequence_->GetNext();
  if (id == 0) {  // Avoid using column_id 0.
    id = next_column_id_sequence_->GetNext();
    DCHECK_NE(id, 0);
  }
  // Should be impossible for this to happen unless sharing across huge
  // numbers of queries.  If it does, column_ids will wrap around as int32s.
  DCHECK_LE(id, std::numeric_limits<int32_t>::max());
  return static_cast<int>(id);
}

IdString Resolver::AllocateSubqueryName() {
  return MakeIdString(absl::StrCat("$subquery", next_subquery_id_++));
}

IdString Resolver::AllocateUnnestName() {
  return MakeIdString(absl::StrCat("$unnest", next_unnest_id_++));
}

IdString Resolver::MakeIdString(absl::string_view str) const {
  return id_string_pool_->Make(str);
}

std::unique_ptr<const ResolvedLiteral> Resolver::MakeResolvedLiteral(
    const ASTNode* ast_location, const Value& value,
    bool set_has_explicit_type) const {
  auto resolved_literal = zetasql::MakeResolvedLiteral(value.type(), value,
                                                         set_has_explicit_type);
  MaybeRecordParseLocation(ast_location, resolved_literal.get());
  return resolved_literal;
}

std::unique_ptr<const ResolvedLiteral> Resolver::MakeResolvedLiteral(
    const ASTNode* ast_location, const Type* type, const Value& value,
    bool has_explicit_type) const {
  auto resolved_literal =
      zetasql::MakeResolvedLiteral(type, value, has_explicit_type);
  MaybeRecordParseLocation(ast_location, resolved_literal.get());
  return resolved_literal;
}

std::unique_ptr<const ResolvedLiteral> Resolver::MakeResolvedFloatLiteral(
    const ASTNode* ast_location, const Type* type, const Value& value,
    bool has_explicit_type, absl::string_view image) {
  if (!language().LanguageFeatureEnabled(FEATURE_NUMERIC_TYPE) &&
      !language().LanguageFeatureEnabled(FEATURE_BIGNUMERIC_TYPE)) {
    return MakeResolvedLiteral(ast_location, type, value, has_explicit_type);
  }
  const int float_literal_id = next_float_literal_image_id_++;
  auto resolved_literal = zetasql::MakeResolvedLiteral(
      type, value, has_explicit_type, float_literal_id);
  float_literal_images_[float_literal_id] = std::string(image);
  MaybeRecordParseLocation(ast_location, resolved_literal.get());
  return resolved_literal;
}

// static
std::unique_ptr<const ResolvedLiteral>
Resolver::MakeResolvedLiteralWithoutLocation(const Value& value) {
  return zetasql::MakeResolvedLiteral(value);
}

absl::Status Resolver::AddAdditionalDeprecationWarningsForCalledFunction(
    const ASTNode* ast_location, const FunctionSignature& signature,
    const std::string& function_name, bool is_tvf) {
  std::set<DeprecationWarning_Kind> warning_kinds_seen;
  for (const FreestandingDeprecationWarning& warning :
       signature.AdditionalDeprecationWarnings()) {
    const DeprecationWarning_Kind warning_kind =
        warning.deprecation_warning().kind();
    // To facilitate log analysis, we record a warning for every deprecation
    // warning kind present in the body. But to avoid creating too many
    // warnings, we only record the first warning for each kind.
    if (zetasql_base::InsertIfNotPresent(&warning_kinds_seen, warning_kind)) {
      ZETASQL_RETURN_IF_ERROR(AddDeprecationWarning(
          ast_location, warning_kind,
          // For non-TVFs, 'function_name' starts with "Function ".
          absl::StrCat(is_tvf ? "Table-valued function " : "", function_name,
                       " triggers a deprecation warning with kind ",
                       DeprecationWarning_Kind_Name(warning_kind)),
          &warning));
    }
  }

  return absl::OkStatus();
}

absl::Status Resolver::AddDeprecationWarning(
    const ASTNode* ast_location, DeprecationWarning::Kind kind,
    const std::string& message,
    const FreestandingDeprecationWarning* source_warning) {
  if (zetasql_base::InsertIfNotPresent(&unique_deprecation_warnings_,
                              std::make_pair(kind, message))) {
    DeprecationWarning warning_proto;
    warning_proto.set_kind(kind);

    absl::Status warning = MakeSqlErrorAt(ast_location) << message;
    zetasql::internal::AttachPayload(&warning, warning_proto);
    if (source_warning != nullptr) {
      ZETASQL_RET_CHECK_EQ(kind, source_warning->deprecation_warning().kind());

      ZETASQL_RET_CHECK(zetasql::internal::HasPayloadWithType<InternalErrorLocation>(
          warning));
      auto internal_error_location =
          zetasql::internal::GetPayload<InternalErrorLocation>(warning);

      // Add the error sources from 'source_warning' to
      // 'internal_error_location'.
      google::protobuf::RepeatedPtrField<ErrorSource>* error_sources =
          internal_error_location.mutable_error_source();
      ZETASQL_RET_CHECK(error_sources->empty());
      error_sources->CopyFrom(source_warning->error_location().error_source());

      // Add a new error source corresponding to 'source_warning'.
      ErrorSource* new_error_source = error_sources->Add();
      new_error_source->set_error_message(source_warning->message());
      new_error_source->set_error_message_caret_string(
          source_warning->caret_string());
      *new_error_source->mutable_error_location() =
          source_warning->error_location();

      // Overwrites the previous InternalErrorLocation.
      zetasql::internal::AttachPayload(&warning, internal_error_location);
    }

    deprecation_warnings_.push_back(warning);
  }

  return absl::OkStatus();
}

bool Resolver::TypeSupportsGrouping(const Type* type,
                                    std::string* no_grouping_type) const {
  return type->SupportsGrouping(language(), no_grouping_type);
}

absl::Status Resolver::ResolveStandaloneExpr(
    absl::string_view sql, const ASTExpression* ast_expr,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  Reset(sql);
  analyzing_expression_ = true;

  // target_column_types is only allowed on statements
  if (!analyzer_options().get_target_column_types().empty()) {
    return MakeSqlError() << "AnalyzerOptions contain target column types, "
                          << "which are not currently supported when resolving "
                          << "standalone expressions";
  }

  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_expr, empty_name_scope_.get(),
                                    "standalone expression",
                                    resolved_expr_out));
  ZETASQL_RETURN_IF_ERROR(ValidateUndeclaredParameters(resolved_expr_out->get()));
  ZETASQL_RETURN_IF_ERROR(PruneColumnLists(resolved_expr_out->get()));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveExprWithFunctionArguments(
    absl::string_view sql, const ASTExpression* ast_expr,
    IdStringHashMapCase<std::unique_ptr<ResolvedArgumentRef>>*
        function_arguments,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* output) {
  Reset(sql);
  for (std::pair<const IdString, std::unique_ptr<ResolvedArgumentRef>>& kv :
       *function_arguments) {
    // Take ownership of the unique pointers in 'function_arguments'.
    ZETASQL_RET_CHECK(!zetasql_base::ContainsKey(function_arguments_, kv.first));
    function_arguments_[kv.first] = std::move(kv.second);
  }
  disallowing_query_parameters_with_error_ =
      "Query parameters cannot be used inside SQL function bodies";
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(ConvertInternalErrorLocationToExternal(
      ResolveExpr(ast_expr, expr_resolution_info, &resolved_expr), sql));
  *output = std::move(resolved_expr);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveQueryStatementWithFunctionArguments(
    absl::string_view sql, const ASTQueryStatement* query_stmt,
    const absl::optional<TVFRelation>& specified_output_schema,
    bool allow_query_parameters,
    IdStringHashMapCase<std::unique_ptr<ResolvedArgumentRef>>*
        function_arguments,
    IdStringHashMapCase<TVFRelation>* function_table_arguments,
    std::unique_ptr<const ResolvedStatement>* output_stmt,
    std::shared_ptr<const NameList>* output_name_list) {
  Reset(sql);
  for (std::pair<const IdString, std::unique_ptr<ResolvedArgumentRef>>& kv :
       *function_arguments) {
    // Take ownership of the unique pointers in 'function_arguments'.
    ZETASQL_RET_CHECK(!zetasql_base::ContainsKey(function_arguments_, kv.first));
    function_arguments_[kv.first] = std::move(kv.second);
  }
  function_table_arguments_ = *function_table_arguments;
  if (!allow_query_parameters) {
    disallowing_query_parameters_with_error_ =
        "Query parameters cannot be used inside SQL function bodies";
  }
  std::unique_ptr<ResolvedStatement> resolved_statement;
  ZETASQL_RETURN_IF_ERROR(ConvertInternalErrorLocationToExternal(
      ResolveQueryStatement(query_stmt, &resolved_statement, output_name_list),
      sql));

  // If the function signature specified a required output schema that the TVF
  // must return, this method compares it against <return_tvf_relation> and
  // returns an error if the expected and provided column types are incompatible
  // or otherwise adds coercions for column types that are not Equals().
  if (specified_output_schema) {
    ZETASQL_RET_CHECK_EQ(RESOLVED_QUERY_STMT, resolved_statement->node_kind());
    auto* resolved_query =
        static_cast<ResolvedQueryStmt*>(resolved_statement.get());
    std::vector<std::unique_ptr<const ResolvedOutputColumn>>
        output_column_list = resolved_query->release_output_column_list();
    std::unique_ptr<const ResolvedScan> output_query =
        resolved_query->release_query();

    ZETASQL_RETURN_IF_ERROR(ConvertInternalErrorLocationToExternal(
        CheckSQLBodyReturnTypesAndCoerceIfNeeded(
            /*statement_location=*/nullptr, *specified_output_schema,
            output_name_list->get(), &output_query, &output_column_list),
        sql));
    resolved_statement = MakeResolvedQueryStmt(
        std::move(output_column_list),
        specified_output_schema->is_value_table(), std::move(output_query));
  }

  ZETASQL_RETURN_IF_ERROR(PruneColumnLists(resolved_statement.get()));
  *output_stmt = std::move(resolved_statement);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveTypeName(const std::string& type_name,
                                       const Type** type) {
  // Reset state because ResolveTypeName is a public entry-point.
  Reset(type_name);
  return ResolveTypeNameInternal(type_name, type);
}

absl::Status Resolver::ResolveTypeNameInternal(const std::string& type_name,
                                               const Type** type) {
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(ParseType(type_name, analyzer_options_.GetParserOptions(),
                            &parser_output));
  return ResolveType(parser_output->type(), type);
}

ResolvedColumnList Resolver::ConcatColumnLists(
    const ResolvedColumnList& left, const ResolvedColumnList& right) {
  ResolvedColumnList out = left;
  for (const ResolvedColumn& column : right) {
    out.emplace_back(column);
  }
  return out;
}

ResolvedColumnList Resolver::ConcatColumnListWithComputedColumnsAndSort(
    const ResolvedColumnList& column_list,
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
        computed_columns) {
  ResolvedColumnList out = column_list;
  for (const std::unique_ptr<const ResolvedComputedColumn>& computed_column :
       computed_columns) {
    out.push_back(computed_column->column());
  }
  std::sort(out.begin(), out.end());
  return out;
}

zetasql_base::StatusOr<bool> Resolver::MaybeAssignTypeToUndeclaredParameter(
    std::unique_ptr<const ResolvedExpr>* expr, const Type* type) {
  if (expr->get()->node_kind() != RESOLVED_PARAMETER) {
    return false;
  }
  const ResolvedParameter* parameter = expr->get()->GetAs<ResolvedParameter>();
  if (!parameter->is_untyped()) {
    return false;
  }
  const ParseLocationRange* location = parameter->GetParseLocationRangeOrNULL();
  ZETASQL_RET_CHECK(location != nullptr);
  ZETASQL_RETURN_IF_ERROR(AssignTypeToUndeclaredParameter(location->start(), type));
  auto coerced_parameter = MakeResolvedParameter(type, parameter->name(),
                                                 parameter->position(), false);
  if (parameter->GetParseLocationRangeOrNULL() != nullptr) {
    coerced_parameter->SetParseLocationRange(
        *parameter->GetParseLocationRangeOrNULL());
  }
  *expr = std::move(coerced_parameter);
  return true;
}

absl::Status Resolver::AssignTypeToUndeclaredParameter(
    const ParseLocationPoint& location, const Type* type) {
  const auto it = untyped_undeclared_parameters_.find(location);
  ZETASQL_RET_CHECK(it != untyped_undeclared_parameters_.end());
  const absl::variant<std::string, int> name_or_position = it->second;
  untyped_undeclared_parameters_.erase(it);

  const Type* previous_type = nullptr;

  if (absl::holds_alternative<std::string>(name_or_position)) {
    const std::string& name = absl::get<std::string>(name_or_position);
    const zetasql::Type*& stored_type = undeclared_parameters_[name];
    previous_type = stored_type;
    if (stored_type == nullptr) {
      stored_type = type;
    }
  } else {
    const int position = absl::get<int>(name_or_position);
    if (position - 1 >= undeclared_positional_parameters_.size()) {
      // The resolver has not visited this undeclared positional parameter
      // before. The resolver may visit an undeclared parameter multiple times
      // and assign a different type, such as when coercing parameters to a
      // common supertype in a BETWEEN expression.
      undeclared_positional_parameters_.resize(position);
    }
    undeclared_positional_parameters_[position - 1] = type;

    return absl::OkStatus();
  }

  ZETASQL_RET_CHECK(absl::holds_alternative<std::string>(name_or_position));
  if (previous_type != nullptr && !previous_type->Equals(type)) {
    // Currently, we require the types to agree exactly, even for different
    // version of protos. This can be relaxed in the future, incl. using
    // common supertypes.
    if (previous_type->Equivalent(type)) {
      return MakeSqlErrorAtPoint(location)
             << "Undeclared parameter '"
             << absl::get<std::string>(name_or_position)
             << "' is used assuming different versions of the same type ("
             << type->ShortTypeName(product_mode()) << ")";
    } else {
      return MakeSqlErrorAtPoint(location)
             << "Undeclared parameter '"
             << absl::get<std::string>(name_or_position)
             << "' is used assuming different types ("
             << previous_type->ShortTypeName(product_mode()) << " vs "
             << type->ShortTypeName(product_mode()) << ")";
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ValidateUndeclaredParameters(const ResolvedNode* node) {
  if (!analyzer_options_.allow_undeclared_parameters()) {
    return absl::OkStatus();
  }

  // Copying avoids modifying the collection while iterating over it.
  const auto copy = untyped_undeclared_parameters_;
  for (const auto& location_and_name : copy) {
    ZETASQL_RETURN_IF_ERROR(AssignTypeToUndeclaredParameter(location_and_name.first,
                                                    types::Int64Type()));
  }
  return absl::OkStatus();
}

absl::Status Resolver::MakeEqualityComparison(
    const ASTNode* ast_location, std::unique_ptr<const ResolvedExpr> expr1,
    std::unique_ptr<const ResolvedExpr> expr2,
    std::unique_ptr<const ResolvedExpr>* output_expr) {
  std::unique_ptr<ResolvedFunctionCall> resolved_function_call;
  ZETASQL_RETURN_IF_ERROR(function_resolver_->ResolveGeneralFunctionCall(
      ast_location, {ast_location, ast_location}, "$equal",
      false /* is_analytic */,
      MakeNodeVector(std::move(expr1), std::move(expr2)),
      /*named_arguments=*/{}, nullptr /* expected_result_type */,
      &resolved_function_call));

  *output_expr = std::move(resolved_function_call);
  return absl::OkStatus();
}

absl::Status Resolver::MakeNotExpr(
    const ASTNode* ast_location, std::unique_ptr<const ResolvedExpr> expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* expr_out) {
  ZETASQL_RET_CHECK(expr->type()->IsBool())
      << "MakeNotExpr can only be called on bool: "
      << expr->type()->ShortTypeName(product_mode());
  std::vector<std::unique_ptr<const ResolvedExpr>> arguments;
  arguments.push_back(std::move(expr));
  return ResolveFunctionCallWithResolvedArguments(
      ast_location, {ast_location}, "$not", std::move(arguments),
      /*named_arguments=*/{}, expr_resolution_info, expr_out);
}

absl::Status Resolver::MakeCoalesceExpr(
    const ASTNode* ast_location, const ResolvedColumnList& columns,
    std::unique_ptr<const ResolvedExpr>* output_expr) {
  ZETASQL_RET_CHECK_GE(columns.size(), 1);

  std::vector<std::unique_ptr<const ResolvedExpr>> exprs;
  for (const ResolvedColumn& column : columns) {
    exprs.push_back(MakeColumnRef(column));
  }

  std::unique_ptr<ResolvedFunctionCall> resolved_function_call;
  // Coerces the arguments to a common supertype, if necessary.
  size_t exprs_size = exprs.size();
  ZETASQL_RETURN_IF_ERROR(function_resolver_->ResolveGeneralFunctionCall(
      ast_location, std::vector<const ASTNode*>(exprs_size, ast_location),
      "coalesce", false /* is_analytic */, std::move(exprs),
      /*named_arguments=*/{}, nullptr /* expected_result_type */,
      &resolved_function_call));

  *output_expr = std::move(resolved_function_call);
  return absl::OkStatus();
}

absl::Status Resolver::MakeAndExpr(
    const ASTNode* ast_location,
    std::vector<std::unique_ptr<const ResolvedExpr>> exprs,
    std::unique_ptr<const ResolvedExpr>* output_expr) const {
  ZETASQL_RET_CHECK_GE(exprs.size(), 1);
  for (const std::unique_ptr<const ResolvedExpr>& expr : exprs) {
    ZETASQL_RET_CHECK(expr->type()->IsBool()) << expr->DebugString();
  }

  if (exprs.size() == 1) {
    *output_expr = std::move(exprs[0]);
  } else {
    int expr_count = exprs.size();
    // Construct the AND expression and resolve a concrete signature.
    std::unique_ptr<ResolvedFunctionCall> resolved_function_call;
    ZETASQL_RETURN_IF_ERROR(function_resolver_->ResolveGeneralFunctionCall(
        ast_location, std::vector<const ASTNode*>(expr_count, ast_location),
        "$and", false /* is_analytic */, std::move(exprs),
        /*named_arguments=*/{}, nullptr /* expected_result_type */,
        &resolved_function_call));

    ZETASQL_RET_CHECK_EQ(resolved_function_call->function()->mode(),
                 Function::SCALAR);

    *output_expr = std::move(resolved_function_call);
  }
  return absl::OkStatus();
}

std::unique_ptr<ResolvedColumnRef> Resolver::MakeColumnRef(
    const ResolvedColumn& column, bool is_correlated,
    ResolvedStatement::ObjectAccess access_flags) {
  RecordColumnAccess(column, access_flags);
  return MakeResolvedColumnRef(column.type(), column, is_correlated);
}

std::unique_ptr<ResolvedColumnRef> Resolver::MakeColumnRefWithCorrelation(
    const ResolvedColumn& column,
    const CorrelatedColumnsSetList& correlated_columns_sets,
    ResolvedStatement::ObjectAccess access_flags) {
  bool is_correlated = false;
  if (!correlated_columns_sets.empty()) {
    is_correlated = true;
    for (CorrelatedColumnsSet* column_set : correlated_columns_sets) {
      // If we are referencing a variable correlated through more than one
      // level of subquery, the sets are ordered so that the set for
      // the outermost query is last.
      const bool is_already_correlated =
          (column_set != correlated_columns_sets.back());
      if (!zetasql_base::InsertIfNotPresent(column_set, column, is_already_correlated)) {
        // is_already_correlated should always be computed consistently.
        DCHECK_EQ((*column_set)[column], is_already_correlated);
      }
    }
  }
  return MakeColumnRef(column, is_correlated, access_flags);
}

// static
std::unique_ptr<const ResolvedColumnRef> Resolver::CopyColumnRef(
    const ResolvedColumnRef* column_ref) {
  return MakeResolvedColumnRef(column_ref->type(), column_ref->column(),
                               column_ref->is_correlated());
}

absl::Status Resolver::ResolvePathExpressionAsType(
    const ASTPathExpression* path_expr,
    bool is_single_identifier,
    const Type** resolved_type) const {
  const std::vector<std::string> identifier_path =
      path_expr->ToIdentifierVector();

  // Check for SimpleTypes.
  if (identifier_path.size() == 1) {
    TypeKind type_kind =
        Type::GetTypeKindIfSimple(identifier_path[0], language());
    if (type_kind != TYPE_UNKNOWN) {
      *resolved_type = type_factory_->MakeSimpleType(type_kind);
      DCHECK((*resolved_type)->IsSupportedType(language()))
          << identifier_path[0];
      return absl::OkStatus();
    }
  }

  std::string single_name;
  if (is_single_identifier) {
    single_name = absl::StrJoin(path_expr->ToIdentifierVector(), ".");
  }

  // Named types are ENUM and PROTO, which are not available in external mode.
  if (product_mode() == PRODUCT_EXTERNAL) {
    return MakeSqlErrorAt(path_expr)
           << "Type not found: "
           << (is_single_identifier ? ToIdentifierLiteral(single_name)
                                    : path_expr->ToIdentifierPathString());
  }

  const absl::Status status = catalog_->FindType(
      (is_single_identifier ? std::vector<std::string>{single_name}
                            : identifier_path),
      resolved_type, analyzer_options_.find_options());
  if (status.code() == absl::StatusCode::kNotFound) {
    return MakeSqlErrorAt(path_expr)
           << "Type not found: "
           << (is_single_identifier ? ToIdentifierLiteral(single_name)
                                    : path_expr->ToIdentifierPathString());
  }
  return status;
}

// Get name of a hint or option formatted appropriately for error messages.
static std::string HintName(const std::string& qualifier,
                            const std::string& name) {
  return absl::StrCat((qualifier.empty() ? "" : ToIdentifierLiteral(qualifier)),
                      (qualifier.empty() ? "" : "."),
                      ToIdentifierLiteral(name));
}

absl::Status Resolver::ResolveHintOrOptionAndAppend(
    const ASTExpression* ast_value, const ASTIdentifier* ast_qualifier,
    const ASTIdentifier* ast_name, bool is_hint,
    const AllowedHintsAndOptions& allowed,
    std::vector<std::unique_ptr<const ResolvedOption>>* option_list) {
  ZETASQL_RET_CHECK(ast_name != nullptr);

  const std::string qualifier =
      ast_qualifier == nullptr ? "" : ast_qualifier->GetAsString();
  const std::string name = ast_name->GetAsString();

  std::unique_ptr<const ResolvedExpr> resolved_expr;

  // Single identifiers are accepted as hint values, and are stored as string
  // values.  These show up in the AST as path expressions with one element.
  if (ast_value->node_kind() == AST_PATH_EXPRESSION) {
    const ASTPathExpression* path_expr
        = static_cast<const ASTPathExpression*>(ast_value);
    if (path_expr->num_names() == 1 && !path_expr->parenthesized()) {
      // For backward compatibility, standalone identifier names need to be
      // treated as a literal string.  But, if the name happens to resolve
      // as an expression, emit an error, since it's not clear whether the user
      // is referring to a literal string "foo" or a constant symbol named
      // "foo".  The user can resolve the error by either adding parentheses or
      // enclosing the name in quotation marks.
      const char* context = is_hint ? "hint" : "option";
      if (ResolveScalarExpr(ast_value, empty_name_scope_.get(), context,
                            &resolved_expr)
              .ok()) {
        return MakeSqlErrorAt(ast_value)
               << "Unable to determine if "
               << path_expr->name(0)->GetAsIdString().ToStringView()
               << " is a string or expression.  If a string is intended, "
               << "please enclose it with quotation marks.  If an expression "
               << "is intended, please enclose it with parentheses.";
      }

      resolved_expr = MakeResolvedLiteral(
          ast_value, Value::String(path_expr->first_name()->GetAsString()));
    }
  }

  // Otherwise, we parse the value as a constant expression,
  // with no names visible in scope.
  if (resolved_expr == nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_value, empty_name_scope_.get(),
                                      is_hint ? "hint" : "option",
                                      &resolved_expr));
    // We try collapsing the resolved_expr early as we want to be sure that a
    // literal/parameter is passed as a hint value when possible.
    TryCollapsingExpressionsAsLiterals(
        ast_value,
        reinterpret_cast<std::unique_ptr<const ResolvedNode>*>(&resolved_expr));
  }

  // <found_ptr> points at the Type* of the hint or option we found.
  // If it points at a NULL, any type is allowed.
  const Type* const* found_ptr = nullptr;
  if (is_hint) {
    found_ptr = zetasql_base::FindOrNull(allowed.hints_lower,
                                std::make_pair(absl::AsciiStrToLower(qualifier),
                                               absl::AsciiStrToLower(name)));

    // If we have a qualifier that we are supposed to validate, then give
    // an error on unknown hints.  Otherwise, let them through.
    if (found_ptr == nullptr &&
        zetasql_base::ContainsKey(allowed.disallow_unknown_hints_with_qualifiers,
                         qualifier)) {
      return MakeSqlErrorAt(ast_name)
             << "Unknown hint: " << HintName(qualifier, name);
    }
  } else {
    ZETASQL_RET_CHECK(qualifier.empty());
    found_ptr =
        zetasql_base::FindOrNull(allowed.options_lower, absl::AsciiStrToLower(name));

    if (found_ptr == nullptr && allowed.disallow_unknown_options) {
      return MakeSqlErrorAt(ast_name)
             << "Unknown option: " << HintName(qualifier, name);
    }
  }

  // If we found the option, try to coerce the value to the appropriate type.
  if (found_ptr != nullptr && *found_ptr != nullptr) {
    const Type* expected_type = *found_ptr;
    SignatureMatchResult result;
    if (!coercer_.CoercesTo(
            GetInputArgumentTypeForExpr(resolved_expr.get()), expected_type,
            false /* is_explicit */, &result)) {
      return MakeSqlErrorAt(ast_value)
             << (is_hint ? "Hint " : "Option ") << HintName(qualifier, name)
             << " value has type "
             << resolved_expr->type()->ShortTypeName(product_mode())
             << " which cannot be coerced to expected type "
             << expected_type->ShortTypeName(product_mode());
    }

    ZETASQL_RETURN_IF_ERROR(
        function_resolver_->AddCastOrConvertLiteral(
            ast_value, expected_type, nullptr /* scan */,
            false /* set_has_explicit_type */, false /* return_null_on_error */,
            &resolved_expr));
  }

  option_list->push_back(
      MakeResolvedOption(qualifier, name, std::move(resolved_expr)));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveHintAndAppend(
    const ASTHint* ast_hint,
    std::vector<std::unique_ptr<const ResolvedOption>>* hints) {
  // Resolve the @<num_shards> hint if we have one, and turn it into the
  // long-form hint @{ num_shards: <integer> }.
  if (ast_hint->num_shards_hint() != nullptr) {
    const ASTIntLiteral* int_literal = ast_hint->num_shards_hint();
    int64_t num_shards;
    if (!functions::StringToNumeric(int_literal->image(), &num_shards,
                                    nullptr)) {
      return MakeSqlErrorAt(int_literal)
             << "Invalid int64_t literal in @num_shards hint: "
             << int_literal->image();
    }
    // @<num_shards> hint cannot be rewritten using a parameter; don't record
    // parse location.
    hints->push_back(MakeResolvedOption(
        "" /* qualifier */, "num_shards",
        MakeResolvedLiteralWithoutLocation(Value::Int64(num_shards))));
  }

  for (const ASTHintEntry* ast_hint_entry : ast_hint->hint_entries()) {
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(ResolveHintOrOptionAndAppend(
        ast_hint_entry->value(),
        ast_hint_entry->qualifier(),
        ast_hint_entry->name(),
        true /* is_hint */,
        analyzer_options_.allowed_hints_and_options(),
        hints));
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveOptionsList(
    const ASTOptionsList* options_list,
    std::vector<std::unique_ptr<const ResolvedOption>>* resolved_options) {
  if (options_list != nullptr) {
    for (const ASTOptionsEntry* options_entry :
         options_list->options_entries()) {
      ZETASQL_RETURN_IF_ERROR(ResolveHintOrOptionAndAppend(
          options_entry->value(), /*ast_qualifier=*/nullptr ,
          options_entry->name(), /*is_hint=*/false,
          analyzer_options_.allowed_hints_and_options(), resolved_options));
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveType(const ASTType* type,
                                   const Type** resolved_type) const {
  switch (type->node_kind()) {
    case AST_SIMPLE_TYPE: {
      return ResolveSimpleType(type->GetAsOrDie<ASTSimpleType>(),
                               resolved_type);
    }

    case AST_ARRAY_TYPE: {
      const ArrayType* array_type;
      ZETASQL_RETURN_IF_ERROR(
          ResolveArrayType(type->GetAsOrDie<ASTArrayType>(), &array_type));
      *resolved_type = array_type;
      return absl::OkStatus();
    }

    case AST_STRUCT_TYPE: {
      const StructType* struct_type;
      ZETASQL_RETURN_IF_ERROR(
          ResolveStructType(type->GetAsOrDie<ASTStructType>(), &struct_type));
      *resolved_type = struct_type;
      return absl::OkStatus();
    }

    default:
      break;
  }

  ZETASQL_RET_CHECK_FAIL() << type->DebugString();
}

absl::Status Resolver::ResolveSimpleType(const ASTSimpleType* type,
                                         const Type** resolved_type) const {
  return ResolvePathExpressionAsType(type->type_name(),
                                     false /* is_single_identifier */,
                                     resolved_type);
}

absl::Status Resolver::ResolveArrayType(const ASTArrayType* array_type,
                                        const ArrayType** resolved_type) const {
  const Type* resolved_element_type;

  ZETASQL_RETURN_IF_ERROR(ResolveType(array_type->element_type(),
                              &resolved_element_type));

  if (resolved_element_type->IsArray()) {
    return MakeSqlErrorAt(array_type) << "Arrays of arrays are not supported";
  }

  return type_factory_->MakeArrayType(resolved_element_type, resolved_type);
}

absl::Status Resolver::ResolveStructType(
    const ASTStructType* struct_type,
    const StructType** resolved_type) const {
  std::vector<StructType::StructField> struct_fields;
  for (auto struct_field : struct_type->struct_fields()) {
    const Type* field_type;
    ZETASQL_RETURN_IF_ERROR(ResolveType(struct_field->type(), &field_type));

    struct_fields.emplace_back(StructType::StructField(
        struct_field->name() != nullptr
            ? struct_field->name()->GetAsString() : "",
        field_type));
  }

  return type_factory_->MakeStructType(struct_fields, resolved_type);
}

void Resolver::RecordColumnAccess(
    const ResolvedColumn& column,
    ResolvedStatement::ObjectAccess access_flags) {
  ResolvedStatement::ObjectAccess& access = referenced_column_access_[column];
  access = static_cast<ResolvedStatement::ObjectAccess>(access_flags | access);
}

void Resolver::RecordColumnAccess(
    const std::vector<ResolvedColumn>& columns,
    ResolvedStatement::ObjectAccess access_flags) {
  for (const ResolvedColumn& column : columns) {
    RecordColumnAccess(column, access_flags);
  }
}

absl::Status Resolver::RecordImpliedAccess(const ResolvedStatement* statement) {
  // ResolvedUpdateStmt and ResolvedMergeStmt are both supported and we return
  // immediately if statement is not one of these two. This saves us from
  // wasteful parsing of the AST and potentially unintended actions on
  // statements where this was not designed to run.
  if (statement->node_kind() != zetasql::RESOLVED_UPDATE_STMT &&
      statement->node_kind() != zetasql::RESOLVED_MERGE_STMT) {
    return absl::OkStatus();
  }

  std::vector<const ResolvedNode*> update_items;
  // Call GetDescendantsWithKinds to find all ResolvedUpdateItems to analyze.
  // For ResolvedUpdateStmt, these include the set of ResolvedUpdateItems
  // specified directly after UPDATE.
  // For ResolvedMergeStmt, it will return the set of ResolvedUpdateItems
  // under the WHEN clause if the MERGE includes an UPDATE statement.
  statement->GetDescendantsWithKinds({zetasql::RESOLVED_UPDATE_ITEM},
                                &update_items);
  for (auto node : update_items) {
    const ResolvedUpdateItem* updateItem = node->GetAs<ResolvedUpdateItem>();
    // For every encountered update item, determine whether we need to
    // mark the associated column as an 'implied' READ. These are accesses that
    // imply READ even if the AST does not directly have a read operation.
    // If any of delete_list_size(), update_list_size(), or insert_list_size()
    // are greater than zero, it indicates a nested DML statement and we should
    // mark it as a READ because the user could use a UDF or other indirect
    // means and infer information about the array that should only be
    // accessible via READ.
    // If array_update_list_size() is greater than zero, we also mark it as a
    // READ because the lack of an exception gives information that the array
    // size is at least as big as the supplied offset.
    // Finally, if the target is not directly a ResolvedColumnRef, then it's a
    // GetProto/StructField and should be treated as a READ because the proto
    // will need to be read before it is modified and written back.
    bool is_implied_read =
        updateItem->array_update_list_size() > 0 ||
        updateItem->insert_list_size() > 0 ||
        updateItem->update_list_size() > 0 ||
        updateItem->delete_list_size() > 0 ||
        updateItem->target()->node_kind() != zetasql::RESOLVED_COLUMN_REF;

    if (is_implied_read) {
      std::vector<const ResolvedNode*> target_column;
      // The target of an update must be a column, possibly wrapped with
      // Get{Proto,Struct}Field operators.  We can use this to extract that
      // column rather than writing a traversal method that supports only those
      // specific operators.
      updateItem->target()->GetDescendantsWithKinds(
          {zetasql::RESOLVED_COLUMN_REF}, &target_column);
      ZETASQL_RET_CHECK_EQ(target_column.size(), 1);
      RecordColumnAccess(
          target_column.front()->GetAs<ResolvedColumnRef>()->column(),
          ResolvedStatement::READ);
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::SetColumnAccessList(ResolvedStatement* statement) {
  const ResolvedTableScan* scan = nullptr;

  // Currently, we are only setting column access info on nodes that support it,
  // including Update and Merge.
  std::vector<ResolvedStatement::ObjectAccess>* mutable_access_list = nullptr;
  if (statement->node_kind() == zetasql::RESOLVED_UPDATE_STMT) {
    auto update_stmt = static_cast<ResolvedUpdateStmt*>(statement);
    scan = update_stmt->table_scan();
    mutable_access_list = update_stmt->mutable_column_access_list();
  } else if (statement->node_kind() == zetasql::RESOLVED_MERGE_STMT) {
    auto merge_stmt = static_cast<ResolvedMergeStmt*>(statement);
    scan = merge_stmt->table_scan();
    mutable_access_list = merge_stmt->mutable_column_access_list();
  } else {
    return absl::OkStatus();
  }

  ZETASQL_RET_CHECK(scan != nullptr);
  ZETASQL_RET_CHECK(mutable_access_list != nullptr);
  ZETASQL_RETURN_IF_ERROR(RecordImpliedAccess(statement));

  std::vector<ResolvedStatement::ObjectAccess> column_access_list;
  for (int i = 0; i < scan->column_list().size(); ++i) {
    const ResolvedColumn& column = scan->column_list(i);
    if (zetasql_base::ContainsKey(referenced_column_access_, column)) {
      column_access_list.push_back(referenced_column_access_.at(column));
    } else {
      // This should only happen if this method is used for an unpruned
      // statement.
      column_access_list.push_back(ResolvedStatement::NONE);
    }
  }
  *mutable_access_list = column_access_list;
  return absl::OkStatus();
}

absl::Status Resolver::PruneColumnLists(const ResolvedNode* node) const {
  if (!analyzer_options_.prune_unused_columns()) {
    return absl::OkStatus();
  }

  // Validate that SetColumnAccessList was called first.
  ZETASQL_RET_CHECK(node->node_kind() != zetasql::RESOLVED_UPDATE_STMT ||
            node->GetAs<ResolvedUpdateStmt>()->column_access_list_size() == 0)
      << "SetColumnAccessList was called before PruneColumnList";

  ZETASQL_RET_CHECK(node->node_kind() != zetasql::RESOLVED_MERGE_STMT ||
          node->GetAs<ResolvedMergeStmt>()->column_access_list_size() == 0)
      << "SetColumnAccessList was called before PruneColumnList";

  std::vector<const ResolvedNode*> scan_nodes;
  node->GetDescendantsSatisfying(&ResolvedNode::IsScan, &scan_nodes);

  std::vector<ResolvedColumn> pruned_column_list;
  std::vector<int> pruned_column_index_list;
  for (const ResolvedNode* scan_node : scan_nodes) {
    const ResolvedScan* scan = scan_node->GetAs<ResolvedScan>();
    bool has_column_index_list = false;
    const ResolvedTableScan* table_scan = nullptr;
    if (scan->node_kind() == RESOLVED_TABLE_SCAN) {
      table_scan = scan->GetAs<ResolvedTableScan>();
      has_column_index_list =
          table_scan->column_index_list_size() ==
              table_scan->column_list_size();
    }

    pruned_column_list.clear();
    pruned_column_index_list.clear();
    for (int i = 0; i < scan->column_list().size(); ++i) {
      const ResolvedColumn& column = scan->column_list(i);
      if (zetasql_base::ContainsKey(referenced_column_access_, column)) {
        pruned_column_list.push_back(column);
        if (has_column_index_list) {
          const int column_index = table_scan->column_index_list(i);
          pruned_column_index_list.push_back(column_index);
        }
      }
    }

    if (pruned_column_list.size() < scan->column_list_size()) {
      // We use const_cast to mutate the column_list vector on Scan nodes.
      // This is only called right at the end, after we've done all resolving,
      // and before we transfer ownership to the caller.
      ResolvedScan* mutable_scan = const_cast<ResolvedScan*>(scan);
      mutable_scan->set_column_list(pruned_column_list);
      if (has_column_index_list) {
        ResolvedTableScan* mutable_table_scan =
            const_cast<ResolvedTableScan*>(table_scan);
        mutable_table_scan->set_column_index_list(pruned_column_index_list);
      }
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::FindTable(
    const ASTPathExpression* name, const Table** table) {
  ZETASQL_RET_CHECK(name != nullptr);
  ZETASQL_RET_CHECK(table != nullptr);

  absl::Status status = catalog_->FindTable(
      name->ToIdentifierVector(), table, analyzer_options_.find_options());
  if (status.code() == absl::StatusCode::kNotFound) {
    std::string message;
    absl::StrAppend(
        &message, "Table not found: ", name->ToIdentifierPathString());
    std::string suggestion(catalog_->SuggestTable(name->ToIdentifierVector()));
    if (!suggestion.empty()) {
      absl::StrAppend(&message, "; did you mean: ", suggestion, "?");
    }
    status = MakeSqlErrorAt(name) << message;
  }
  return status;
}

void Resolver::FindColumnIndex(const Table* table, const std::string& name,
                               int* index, bool* duplicate) {
  DCHECK(table != nullptr);
  DCHECK(index != nullptr);
  DCHECK(duplicate != nullptr);

  *index = -1;
  *duplicate = false;
  for (int i = 0; i < table->NumColumns(); i++) {
    if (zetasql_base::CaseEqual(table->GetColumn(i)->Name(), name)) {
      if (*index == -1) {
        *index = i;
      } else {
        *duplicate = true;
      }
    }
  }
}

bool Resolver::SupportsEquality(const Type* type1, const Type* type2) {
  DCHECK(type1 != nullptr);
  DCHECK(type2 != nullptr);

  // Quick check for a common case.
  if (type1->Equals(type2)) {
    return type1->SupportsEquality(analyzer_options_.language_options());
  }

  // INT64 and UINT64 support equality but cannot be coerced to a common type.
  // INT32 also implicitly coerces to INT64 and supports equality with UINT64.
  // Although not all numerical types are coerceable to all other numerical
  // types, we nonetheless support equality between all numerical types.
  if (type1->IsNumerical() && type2->IsNumerical()) {
    return type1->SupportsEquality(analyzer_options_.language_options())
        && type2->SupportsEquality(analyzer_options_.language_options());
  }

  // Check if values of these types can be coerced to a common supertype that
  // support equality.
  InputArgumentType arg1(type1);
  InputArgumentType arg2(type2);
  InputArgumentTypeSet arg_set;
  arg_set.Insert(arg1);
  arg_set.Insert(arg2);
  const Type* supertype = coercer_.GetCommonSuperType(arg_set);
  return supertype != nullptr
      && supertype->SupportsEquality(analyzer_options_.language_options());
}

}  // namespace zetasql
