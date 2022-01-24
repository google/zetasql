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

// This file contains the implementation of expression-related resolver methods
// from resolver.h.
#include <stddef.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stack>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/analytic_function_resolver.h"
#include "zetasql/analyzer/column_cycle_detector.h"
#include "zetasql/analyzer/expr_matching_helpers.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/filter_fields_path_validator.h"
#include "zetasql/analyzer/function_resolver.h"
#include "zetasql/analyzer/input_argument_type_resolver_helper.h"
#include "zetasql/analyzer/lambda_util.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/resolver_common_inl.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/catalog_helper.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/constant.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/functions/convert_string.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/normalize_mode.pb.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/enum_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/case.h"
#include "zetasql/base/string_numbers.h"  
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "zetasql/base/general_trie.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// These are constant identifiers used mostly for generated column or table
// names.  We use a single IdString for each so we never have to allocate
// or copy these strings again.
STATIC_IDSTRING(kAggregateId, "$aggregate");
STATIC_IDSTRING(kExprSubqueryId, "$expr_subquery");
STATIC_IDSTRING(kOrderById, "$orderby");
STATIC_IDSTRING(kInSubqueryCastId, "$in_subquery_cast");

namespace {

// Verifies that 'field_descriptor' is an extension corresponding to the same
// message as descriptor, and then returns 'field_descriptor'.
absl::StatusOr<const google::protobuf::FieldDescriptor*> VerifyFieldExtendsMessage(
    const ASTNode* ast_node, const google::protobuf::FieldDescriptor* field_descriptor,
    const google::protobuf::Descriptor* descriptor) {
  const google::protobuf::Descriptor* containing_type_descriptor =
      field_descriptor->containing_type();
  // Verify by full_name rather than by pointer equality to allow for extensions
  // that come from different DescriptorPools. This is tested in the
  // ExternalExtension test in analyzer_test.cc.
  if (descriptor->full_name() != containing_type_descriptor->full_name()) {
    return MakeSqlErrorAt(ast_node)
           << "Proto extension " << field_descriptor->full_name()
           << " extends message " << containing_type_descriptor->full_name()
           << " so cannot be used on an expression with message type "
           << descriptor->full_name();
  }

  return field_descriptor;
}

// If type is an array, returns the array type element. Otherwise returns type.
const Type* ArrayElementTypeOrType(const Type* type) {
  if (type->IsArray()) {
    return type->AsArray()->element_type();
  }
  return type;
}

// Adds 'expr' to the get_field_list for the passed in flatten node.
// Updates the flatten result type accordingly.
absl::Status AddGetFieldToFlatten(std::unique_ptr<const ResolvedExpr> expr,
                                  TypeFactory* type_factory,
                                  ResolvedFlatten* flatten) {
  const Type* type = expr->type();
  if (!type->IsArray()) {
    ZETASQL_RETURN_IF_ERROR(type_factory->MakeArrayType(expr->type(), &type));
  }
  flatten->set_type(type);
  flatten->add_get_field_list(std::move(expr));
  return absl::OkStatus();
}

std::string GetNodePrefixToken(absl::string_view sql,
                               const ASTLeaf& leaf_node) {
  const ParseLocationRange& parse_location_range =
      leaf_node.GetParseLocationRange();
  absl::string_view type_token = absl::StripAsciiWhitespace(
      absl::ClippedSubstr(sql, parse_location_range.start().GetByteOffset(),
                          parse_location_range.end().GetByteOffset() -
                              parse_location_range.start().GetByteOffset() -
                              leaf_node.image().size()));
  return absl::AsciiStrToUpper(type_token);
}

inline std::unique_ptr<ResolvedCast> MakeResolvedCast(
    const Type* type, std::unique_ptr<const ResolvedExpr> expr,
    std::unique_ptr<const ResolvedExpr> format,
    std::unique_ptr<const ResolvedExpr> time_zone,
    const TypeParameters& type_params, bool return_null_on_error,
    const ExtendedCompositeCastEvaluator& extended_conversion_evaluator) {
  auto result = MakeResolvedCast(type, std::move(expr), return_null_on_error,
                                 /*extended_cast=*/nullptr, std::move(format),
                                 std::move(time_zone), type_params);

  if (extended_conversion_evaluator.is_valid()) {
    std::vector<std::unique_ptr<const ResolvedExtendedCastElement>>
        conversion_list;
    for (const ConversionEvaluator& evaluator :
         extended_conversion_evaluator.evaluators()) {
      conversion_list.push_back(MakeResolvedExtendedCastElement(
          evaluator.from_type(), evaluator.to_type(), evaluator.function()));
    }

    result->set_extended_cast(
        MakeResolvedExtendedCast(std::move(conversion_list)));
  }

  return result;
}

bool IsFilterFields(const Function* function) {
  return zetasql_base::CaseEqual(function->SQLName(), "FILTER_FIELDS");
}

}  // namespace

absl::Status Resolver::ResolveBuildProto(
    const ASTNode* ast_type_location, const ProtoType* proto_type,
    const ResolvedScan* input_scan, const std::string& argument_description,
    const std::string& query_description,
    std::vector<ResolvedBuildProtoArg>* arguments,
    std::unique_ptr<const ResolvedExpr>* output) {
  const google::protobuf::Descriptor* descriptor = proto_type->descriptor();

  // Map of case-insensitive name to proto field.  nullptr if ambiguous.
  IdStringHashMapCase<const google::protobuf::FieldDescriptor*> fields_by_name;

  // Required fields we haven't found so far.
  absl::flat_hash_set<const google::protobuf::FieldDescriptor*> missing_required_fields;

  for (int i = 0; i < descriptor->field_count(); ++i) {
    const google::protobuf::FieldDescriptor* field = descriptor->field(i);
    const IdString field_name = MakeIdString(field->name());
    if (!zetasql_base::InsertIfNotPresent(&fields_by_name, field_name, field)) {
      fields_by_name[field_name] = nullptr;
    }

    if (field->is_required()) {
      missing_required_fields.insert(field);
    }
  }

  std::map<int, const google::protobuf::FieldDescriptor*> added_tag_number_to_field_map;

  std::vector<std::unique_ptr<const ResolvedMakeProtoField>>
      resolved_make_fields;

  for (int i = 0; i < arguments->size(); ++i) {
    ResolvedBuildProtoArg& argument = (*arguments)[i];
    const AliasOrASTPathExpression& alias_or_ast_path_expr =
        *argument.alias_or_ast_path_expr;
    IdString field_alias;  // Empty if we are using a path expression.
    const google::protobuf::FieldDescriptor* field = nullptr;
    switch (alias_or_ast_path_expr.kind()) {
      case AliasOrASTPathExpression::ALIAS: {
        field_alias = alias_or_ast_path_expr.alias();
        ZETASQL_RET_CHECK(!field_alias.empty());
        ZETASQL_RET_CHECK(!IsInternalAlias(field_alias));

        field = zetasql_base::FindPtrOrNull(fields_by_name, field_alias);

        if (field == nullptr) {
          return MakeSqlErrorAt(argument.ast_location)
                 << argument_description << " " << (i + 1) << " has name "
                 << ToIdentifierLiteral(field_alias)
                 << " which is not a field in proto "
                 << descriptor->full_name();
        }
        if (field->is_required()) {
          // Note that required fields may be listed twice, so this erase can
          // be a no-op.  This condition will eventually trigger a duplicate
          // column error below.
          missing_required_fields.erase(field);
        }
        break;
      }
      case AliasOrASTPathExpression::AST_PATH_EXPRESSION: {
        const ASTPathExpression* ast_path_expr =
            alias_or_ast_path_expr.ast_path_expr();
        ZETASQL_ASSIGN_OR_RETURN(
            field, FindExtensionFieldDescriptor(ast_path_expr, descriptor));
        // Don't check whether 'field' is required, because extensions cannot be
        // required.
        break;
      }
    }

    auto insert_result = added_tag_number_to_field_map.insert(
        std::make_pair(field->number(), field));
    if (!insert_result.second) {
      // We can't set the same tag number twice, so this is an error. We make an
      // effort to print simple error messages for common cases.
      const google::protobuf::FieldDescriptor* other_field = insert_result.first->second;
      if (other_field->full_name() == field->full_name()) {
        if (!field_alias.empty()) {
          // Very common case (regular field accessed twice) or a very weird
          // case (regular field accessed sometime after accessing an extension
          // field with the same tag number and name).
          return MakeSqlErrorAt(argument.ast_location)
                 << query_description << " has duplicate column name "
                 << ToIdentifierLiteral(field_alias)
                 << " so constructing proto field " << field->full_name()
                 << " is ambiguous";
        } else {
          // Less common case (field accessed twice, the second time as an
          // extension field).
          return MakeSqlErrorAt(argument.ast_location)
                 << query_description << " has duplicate extension field "
                 << field->full_name();
        }
      } else {
        // Very strange case (tag number accessed twice, somehow with different
        // names).
        return MakeSqlErrorAt(argument.ast_location)
               << query_description << " refers to duplicate tag number "
               << field->number() << " with proto field names "
               << other_field->full_name() << " and " << field->full_name();
      }
    }

    std::unique_ptr<const ResolvedExpr> expr = std::move(argument.expr);

    Value default_value;
    const Type* proto_field_type;
    RETURN_SQL_ERROR_AT_IF_ERROR(
        argument.ast_location,
        GetProtoFieldTypeAndDefault(
            ProtoFieldDefaultOptions::FromFieldAndLanguage(field, language()),
            field, type_factory_, &proto_field_type, &default_value));
    if (!proto_field_type->IsSupportedType(language())) {
      return MakeSqlErrorAt(argument.ast_location)
             << "Proto field " << field->full_name() << " has unsupported type "
             << proto_field_type->TypeName(language().product_mode());
    }

    // Add coercion if necessary.
    // TODO: Remove input_scan arg and convert to CoerceExprToType
    //     This will be a separate change because it churns test files.
    // TODO: Check coercer->AssignableTo
    if (const absl::Status cast_status =
            function_resolver_->AddCastOrConvertLiteral(
                argument.ast_location, proto_field_type, /*format=*/nullptr,
                /*time_zone=*/nullptr, TypeParameters(), input_scan,
                /*set_has_explicit_type=*/false,
                /*return_null_on_error=*/false, &expr);
        !cast_status.ok()) {
      // Propagate "Out of stack space" errors.
      // TODO
      if (cast_status.code() == absl::StatusCode::kResourceExhausted) {
        return cast_status;
      }
      return MakeSqlErrorAt(argument.ast_location)
             << "Could not store value with type "
             << GetInputArgumentTypeForExpr(expr.get())
                    .UserFacingName(product_mode())
             << " into proto field " << field->full_name()
             << " which has SQL type "
             << proto_field_type->ShortTypeName(product_mode());
    }

    resolved_make_fields.emplace_back(MakeResolvedMakeProtoField(
        field, ProtoType::GetFormatAnnotation(field), std::move(expr)));
  }

  if (!missing_required_fields.empty()) {
    std::set<std::string> field_names;  // Sorted list of fields.
    for (const google::protobuf::FieldDescriptor* field : missing_required_fields) {
      field_names.insert(field->name());
    }
    return MakeSqlErrorAt(ast_type_location)
           << "Cannot construct proto " << descriptor->full_name()
           << " because required field"
           << (missing_required_fields.size() > 1 ? "s" : "") << " "
           << absl::StrJoin(field_names, ",") << " "
           << (missing_required_fields.size() > 1 ? "are" : "is") << " missing";
  }

  auto resolved_make_proto =
      MakeResolvedMakeProto(proto_type, std::move(resolved_make_fields));
  MaybeRecordParseLocation(ast_type_location, resolved_make_proto.get());
  *output = std::move(resolved_make_proto);
  return absl::OkStatus();
}

// The extension name can be written in any of these forms.
//   (1) package.ProtoName.field_name
//   (2) catalog.package.ProtoName.field_name
//   (3) `package.ProtoName`.field_name
//   (4) catalog.`package.ProtoName`.field_name
//   (5) package.field_name
//
// The package and catalog names are optional, and could also be multi-part.
// The field_name is always written as the last identifier and cannot be part of
// a quoted name. (The Catalog lookup interface can only find message names, not
// extension field names.)
//
// We'll first try to resolve the full path as a fully qualified extension name
// by looking up the extension in the DescriptorPool of the relevant proto. This
// will resolve scoped extensions that are written in form (1) as well as
// top-level extensions written in form (5).
//
// If a resolution of extension with form (5) fails and weak field fallback
// lookup (FEATURE_WEAK_FIELD_FALLBACK_LOOKUP) is enabled then we attempt to
// find a weak field by field_name such that if a weak field was converted to
// extension it would have matched form (5) exactly.
//
// If we can't find the extension this way, we'll resolve the first N-1 names as
// a type name, which must come out to a ProtoType, and then look for the last
// name as an extension field name. This will find extensions written in form
// (2), (3) or (4).
//
// We look for the message name first using the DescriptorPool attached to the
// proto we are reading from. This lets some cases work where the proto does not
// exist or has an unexpected name in the catalog, like a global_proto_db
// qualified name.
absl::StatusOr<const google::protobuf::FieldDescriptor*>
Resolver::FindExtensionFieldDescriptor(const ASTPathExpression* ast_path_expr,
                                       const google::protobuf::Descriptor* descriptor) {
  // First try to find the extension in the DescriptorPool of the relevant proto
  // using the fully qualified name specified in the path.
  // TODO Ideally we should do this lookup using the Catalog, which will
  // enable finding top-level extensions that are inside a Catalog.
  const std::vector<std::string> extension_path =
      ast_path_expr->ToIdentifierVector();

  const std::string extension_name =
      Catalog::ConvertPathToProtoName(extension_path);

  const google::protobuf::DescriptorPool* descriptor_pool = descriptor->file()->pool();
  if (!extension_name.empty()) {
    const google::protobuf::FieldDescriptor* field_descriptor =
        descriptor_pool->FindExtensionByName(extension_name);
    if (field_descriptor != nullptr) {
      return VerifyFieldExtendsMessage(ast_path_expr, field_descriptor,
                                       descriptor);
    }
  }

  // If we couldn't find the extension in the pool using the specified path, we
  // try to look for the extension inside a message (and point
  // 'extension_scope_descriptor' to that message). But that only works if there
  // are at least two identifiers in the path (e.g., path.to.message.extension).
  const google::protobuf::Descriptor* extension_scope_descriptor = nullptr;
  if (extension_path.size() >= 2) {
    // The type name path is the full extension path without the last element
    // (which is the field name).
    std::vector<std::string> type_name_path = extension_path;
    type_name_path.pop_back();

    ZETASQL_ASSIGN_OR_RETURN(extension_scope_descriptor,
                     FindMessageTypeForExtension(
                         ast_path_expr, type_name_path, descriptor_pool,
                         /*return_error_for_non_message=*/true));

    if (extension_scope_descriptor == nullptr) {
      // We didn't find the scope message for the extension. If the extension
      // was written as just (package.ProtoName), without specifying a field,
      // then we can fail with a more helpful error.
      ZETASQL_ASSIGN_OR_RETURN(extension_scope_descriptor,
                       FindMessageTypeForExtension(
                           ast_path_expr, extension_path, descriptor_pool,
                           /*return_error_for_non_message=*/false));
      if (extension_scope_descriptor != nullptr) {
        return MakeSqlErrorAt(ast_path_expr)
               << "Expected extension name of the form "
               << "(MessageName.extension_field_name), but "
               << ast_path_expr->ToIdentifierPathString()
               << " is a full message name.  Add the extension field name.";
      }
    } else {
      const google::protobuf::FieldDescriptor* field_descriptor =
          extension_scope_descriptor->FindExtensionByName(
              ast_path_expr->last_name()->GetAsString());
      if (field_descriptor != nullptr) {
        return VerifyFieldExtendsMessage(ast_path_expr, field_descriptor,
                                         descriptor);
      }
    }
  }

  // If the extension was written as a single quoted identifier containing the
  // fully qualified extension name, we try to resolve the extension this way,
  // and if we can, we issue a more specific error message. Otherwise we'll
  // issue the generic extension not found error.
  if (extension_path.size() == 1 && extension_name.empty() &&
      descriptor_pool->FindExtensionByName(
          ast_path_expr->last_name()->GetAsString()) != nullptr) {
    return MakeSqlErrorAt(ast_path_expr)
           << "Specifying the fully qualified extension name as a quoted "
           << "identifier is disallowed: "
           << ast_path_expr->ToIdentifierPathString();
  }

  // We couldn't find the extension. If we found the scope message for the
  // extension we can issue a more specific error mentioning the scope message.
  // Otherwise we don't know if the user was looking for an extension scoped
  // within a message or a top-level extension, so we'll issue a generic message
  // that the extension was not found.
  if (extension_scope_descriptor != nullptr) {
    return MakeSqlErrorAt(ast_path_expr->last_name())
           << "Extension "
           << ToIdentifierLiteral(ast_path_expr->last_name()->GetAsIdString())
           << " not found in proto message "
           << extension_scope_descriptor->full_name();
  }
  return MakeSqlErrorAt(ast_path_expr)
         << "Extension " << ast_path_expr->ToIdentifierPathString()
         << " not found";
}

absl::StatusOr<const google::protobuf::Descriptor*> Resolver::FindMessageTypeForExtension(
    const ASTPathExpression* ast_path_expr,
    const std::vector<std::string>& type_name_path,
    const google::protobuf::DescriptorPool* descriptor_pool,
    bool return_error_for_non_message) {
  const std::string message_name =
      Catalog::ConvertPathToProtoName(type_name_path);
  if (!message_name.empty()) {
    const google::protobuf::Descriptor* found_descriptor =
        descriptor_pool->FindMessageTypeByName(message_name);
    if (found_descriptor != nullptr) {
      ZETASQL_VLOG(2) << "Found message in proto's DescriptorPool: "
              << found_descriptor->DebugString();
      return found_descriptor;
    }
  }

  const Type* found_type = nullptr;
  const absl::Status find_type_status = catalog_->FindType(
      type_name_path, &found_type, analyzer_options_.find_options());
  if (find_type_status.code() == absl::StatusCode::kNotFound) {
    // We don't give an error if it wasn't found.  That will happen in
    // the caller so it has a chance to try generating a better error.
    return nullptr;
  }
  ZETASQL_RETURN_IF_ERROR(find_type_status);
  ZETASQL_RET_CHECK(found_type != nullptr);

  if (!found_type->IsProto()) {
    if (return_error_for_non_message) {
      return MakeSqlErrorAt(ast_path_expr)
             << "Path "
             << ast_path_expr->ToIdentifierPathString(
                    type_name_path.size() /* max_prefix_size */)
             << " resolves to type "
             << found_type->ShortTypeName(product_mode())
             << " but a PROTO type was expected for reading an extension field";
    } else {
      return nullptr;
    }
  }

  return found_type->AsProto()->descriptor();
}

absl::Status Resolver::MakeResolvedDateOrTimeLiteral(
    const ASTExpression* ast_expr, const TypeKind type_kind,
    absl::string_view literal_string_value,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::string string_value;
  if (Type::IsSimpleType(type_kind) &&
      !Type::IsSupportedSimpleTypeKind(type_kind, language())) {
    return MakeSqlErrorAt(ast_expr)
           << "Type not found: "
           << Type::TypeKindToString(type_kind, language().product_mode());
  }

  switch (type_kind) {
    case TYPE_DATE: {
      int32_t date;
      if (functions::ConvertStringToDate(literal_string_value, &date).ok()) {
        *resolved_expr_out =
            MakeResolvedLiteral(ast_expr, Value::Date(date),
                                /*set_has_explicit_type=*/true);
        return absl::OkStatus();
      }
      break;
    }
    case TYPE_TIMESTAMP: {
      if (language().LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
        absl::Time timestamp;
        if (functions::ConvertStringToTimestamp(
                literal_string_value, default_time_zone(),
                functions::kNanoseconds, /*allow_tz_in_str=*/true, &timestamp)
                .ok()) {
          *resolved_expr_out =
              MakeResolvedLiteral(ast_expr, Value::Timestamp(timestamp),
                                  /*set_has_explicit_type=*/true);
          return absl::OkStatus();
        }
      } else {
        int64_t timestamp;
        if (functions::ConvertStringToTimestamp(
                literal_string_value, default_time_zone(),
                functions::kMicroseconds, &timestamp)
                .ok()) {
          *resolved_expr_out = MakeResolvedLiteral(
              ast_expr, Value::TimestampFromUnixMicros(timestamp),
              /*set_has_explicit_type=*/true);
          return absl::OkStatus();
        }
      }
      break;
    }
    case TYPE_TIME: {
      TimeValue time;
      functions::TimestampScale scale =
          language().LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)
              ? functions::kNanoseconds
              : functions::kMicroseconds;
      if (functions::ConvertStringToTime(literal_string_value, scale, &time)
              .ok() &&
          time.IsValid()) {
        *resolved_expr_out =
            MakeResolvedLiteral(ast_expr, Value::Time(time),
                                /*set_has_explicit_type=*/true);
        return absl::OkStatus();
      }
      break;
    }
    case TYPE_DATETIME: {
      DatetimeValue datetime;
      functions::TimestampScale scale =
          language().LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)
              ? functions::kNanoseconds
              : functions::kMicroseconds;
      if (functions::ConvertStringToDatetime(literal_string_value, scale,
                                             &datetime)
              .ok() &&
          datetime.IsValid()) {
        *resolved_expr_out =
            MakeResolvedLiteral(ast_expr, Value::Datetime(datetime),
                                /*set_has_explicit_type=*/true);
        return absl::OkStatus();
      }
      break;
    }
    default:
      break;
  }
  return MakeSqlErrorAt(ast_expr)
         << "Invalid " << Type::TypeKindToString(type_kind, product_mode())
         << " literal";
}

absl::Status Resolver::ResolveScalarExpr(
    const ASTExpression* ast_expr, const NameScope* name_scope,
    const char* clause_name,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  ExprResolutionInfo expr_resolution_info(name_scope, clause_name);
  return ResolveExpr(ast_expr, &expr_resolution_info, resolved_expr_out);
}

absl::StatusOr<std::unique_ptr<const ResolvedLiteral>>
Resolver::ResolveJsonLiteral(const ASTJSONLiteral* json_literal) {
  std::string unquoted_image;
  ZETASQL_RETURN_IF_ERROR(ParseStringLiteral(json_literal->image(), &unquoted_image));
  if (language().LanguageFeatureEnabled(FEATURE_JSON_NO_VALIDATION)) {
    return MakeResolvedLiteral(
        json_literal, types::JsonType(),
        Value::UnvalidatedJsonString(std::move(unquoted_image)),
        /*has_explicit_type=*/true);
  } else {
    auto status_or_value = JSONValue::ParseJSONString(
        unquoted_image,
        JSONParsingOptions{
            language().LanguageFeatureEnabled(FEATURE_JSON_LEGACY_PARSE),
            language().LanguageFeatureEnabled(
                FEATURE_JSON_STRICT_NUMBER_PARSING)});
    if (!status_or_value.ok()) {
      return MakeSqlErrorAt(json_literal)
             << "Invalid JSON literal: " << status_or_value.status().message();
    }
    return MakeResolvedLiteral(json_literal, types::JsonType(),
                               Value::Json(std::move(status_or_value.value())),
                               /*has_explicit_type=*/true);
  }
}

absl::Status Resolver::ResolveExpr(
    const ASTExpression* ast_expr,
    ExprResolutionInfo* parent_expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  ZETASQL_DCHECK(parent_expr_resolution_info != nullptr);

  // Use a separate ExprAggregationInfo for the child because we don't
  // want it to observe <has_aggregation>, <has_analytic>, or <can_flatten> from
  // a sibling. <has_aggregation> and <has_analytic> need to flow up the tree
  // only, and not across. <can_flatten> needs to be selectively propagated.
  std::unique_ptr<ExprResolutionInfo> expr_resolution_info(  // Save stack for
      new ExprResolutionInfo(parent_expr_resolution_info));  // nested exprs.

  switch (ast_expr->node_kind()) {
    // These cases are extracted into a separate method to reduce stack usage.
    case AST_INT_LITERAL:
    case AST_STRING_LITERAL:
    case AST_BYTES_LITERAL:
    case AST_BOOLEAN_LITERAL:
    case AST_FLOAT_LITERAL:
    case AST_NULL_LITERAL:
    case AST_DATE_OR_TIME_LITERAL:
    case AST_NUMERIC_LITERAL:
    case AST_BIGNUMERIC_LITERAL:
    case AST_JSON_LITERAL:
      return ResolveLiteralExpr(ast_expr, resolved_expr_out);

    case AST_STAR:
      return MakeSqlErrorAt(ast_expr)
             << "Argument * can only be used in COUNT(*)"
             << (language().LanguageFeatureEnabled(FEATURE_ANONYMIZATION)
                     ? " or ANON_COUNT(*)"
                     : "");

    case AST_DOT_STAR:
    case AST_DOT_STAR_WITH_MODIFIERS:
      // This is expected to be unreachable as parser allows creation of
      // dot star nodes only inside SELECT expression.
      return MakeSqlErrorAt(ast_expr)
             << "Dot-star is only supported in SELECT expression";

    case AST_PATH_EXPRESSION:
      expr_resolution_info->flatten_state.SetParent(
          &parent_expr_resolution_info->flatten_state);
      return ResolvePathExpressionAsExpression(
          ast_expr->GetAsOrDie<ASTPathExpression>(), expr_resolution_info.get(),
          ResolvedStatement::READ, resolved_expr_out);

    case AST_PARAMETER_EXPR:
      return ResolveParameterExpr(ast_expr->GetAsOrDie<ASTParameterExpr>(),
                                  resolved_expr_out);

    case AST_DOT_IDENTIFIER:
      expr_resolution_info->flatten_state.SetParent(
          &parent_expr_resolution_info->flatten_state);
      return ResolveDotIdentifier(ast_expr->GetAsOrDie<ASTDotIdentifier>(),
                                  expr_resolution_info.get(),
                                  resolved_expr_out);

    case AST_DOT_GENERALIZED_FIELD:
      expr_resolution_info->flatten_state.SetParent(
          &parent_expr_resolution_info->flatten_state);
      return ResolveDotGeneralizedField(
          ast_expr->GetAsOrDie<ASTDotGeneralizedField>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_UNARY_EXPRESSION:
      return ResolveUnaryExpr(ast_expr->GetAsOrDie<ASTUnaryExpression>(),
                              expr_resolution_info.get(), resolved_expr_out);

    case AST_BINARY_EXPRESSION:
      return ResolveBinaryExpr(ast_expr->GetAsOrDie<ASTBinaryExpression>(),
                               expr_resolution_info.get(), resolved_expr_out);

    case AST_BITWISE_SHIFT_EXPRESSION:
      return ResolveBitwiseShiftExpr(
          ast_expr->GetAsOrDie<ASTBitwiseShiftExpression>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_IN_EXPRESSION:
      return ResolveInExpr(ast_expr->GetAsOrDie<ASTInExpression>(),
                           expr_resolution_info.get(), resolved_expr_out);

    case AST_LIKE_EXPRESSION:
      return ResolveLikeExpr(ast_expr->GetAsOrDie<ASTLikeExpression>(),
                             expr_resolution_info.get(), resolved_expr_out);
    case AST_BETWEEN_EXPRESSION:
      return ResolveBetweenExpr(ast_expr->GetAsOrDie<ASTBetweenExpression>(),
                                expr_resolution_info.get(), resolved_expr_out);

    case AST_AND_EXPR:
      return ResolveAndExpr(ast_expr->GetAsOrDie<ASTAndExpr>(),
                            expr_resolution_info.get(), resolved_expr_out);

    case AST_OR_EXPR:
      return ResolveOrExpr(ast_expr->GetAsOrDie<ASTOrExpr>(),
                           expr_resolution_info.get(), resolved_expr_out);

    case AST_FUNCTION_CALL:
      return ResolveFunctionCall(ast_expr->GetAsOrDie<ASTFunctionCall>(),
                                 expr_resolution_info.get(), resolved_expr_out);

    case AST_CAST_EXPRESSION:
      return ResolveExplicitCast(ast_expr->GetAsOrDie<ASTCastExpression>(),
                                 expr_resolution_info.get(), resolved_expr_out);

    case AST_ARRAY_ELEMENT:
      expr_resolution_info->flatten_state.SetParent(
          &parent_expr_resolution_info->flatten_state);
      return ResolveArrayElement(ast_expr->GetAsOrDie<ASTArrayElement>(),
                                 expr_resolution_info.get(), resolved_expr_out);

    case AST_CASE_VALUE_EXPRESSION:
      return ResolveCaseValueExpression(
          ast_expr->GetAsOrDie<ASTCaseValueExpression>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_CASE_NO_VALUE_EXPRESSION:
      return ResolveCaseNoValueExpression(
          ast_expr->GetAsOrDie<ASTCaseNoValueExpression>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_EXTRACT_EXPRESSION:
      return ResolveExtractExpression(
          ast_expr->GetAsOrDie<ASTExtractExpression>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_EXPRESSION_SUBQUERY:
      return ResolveExprSubquery(ast_expr->GetAsOrDie<ASTExpressionSubquery>(),
                                 expr_resolution_info.get(), resolved_expr_out);

    case AST_NEW_CONSTRUCTOR:
      return ResolveNewConstructor(ast_expr->GetAsOrDie<ASTNewConstructor>(),
                                   expr_resolution_info.get(),
                                   resolved_expr_out);

    case AST_ARRAY_CONSTRUCTOR:
      return ResolveArrayConstructor(
          ast_expr->GetAsOrDie<ASTArrayConstructor>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_STRUCT_CONSTRUCTOR_WITH_PARENS:
      return ResolveStructConstructorWithParens(
          ast_expr->GetAsOrDie<ASTStructConstructorWithParens>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_STRUCT_CONSTRUCTOR_WITH_KEYWORD:
      return ResolveStructConstructorWithKeyword(
          ast_expr->GetAsOrDie<ASTStructConstructorWithKeyword>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_ANALYTIC_FUNCTION_CALL:
      if (!language().LanguageFeatureEnabled(FEATURE_ANALYTIC_FUNCTIONS)) {
        return MakeSqlErrorAt(ast_expr) << "Analytic functions not supported";
      }
      if (generated_column_cycle_detector_ != nullptr) {
        return MakeSqlErrorAt(ast_expr)
               << "Analytic functions cannot be used inside generated columns";
      }
      if (default_expr_access_error_name_scope_.has_value()) {
        return MakeSqlErrorAt(ast_expr)
               << "Analytic functions cannot be used inside a column default "
               << "expression";
      }
      return ResolveAnalyticFunctionCall(
          ast_expr->GetAsOrDie<ASTAnalyticFunctionCall>(),
          expr_resolution_info.get(), resolved_expr_out);

    case AST_INTERVAL_EXPR: {
      // The functions that expect an INTERVAL expression handle them specially
      // while resolving the argument list, and only in the locations where they
      // are expected. All other cases end up here.
      if (!language().LanguageFeatureEnabled(FEATURE_INTERVAL_TYPE)) {
        return MakeSqlErrorAt(ast_expr) << "Unexpected INTERVAL expression";
      }
      return ResolveIntervalExpr(ast_expr->GetAsOrDie<ASTIntervalExpr>(),
                                 expr_resolution_info.get(), resolved_expr_out);
    }
    case AST_REPLACE_FIELDS_EXPRESSION:
      return ResolveReplaceFieldsExpression(
          ast_expr->GetAsOrDie<ASTReplaceFieldsExpression>(),
          expr_resolution_info.get(), resolved_expr_out);
    case AST_SYSTEM_VARIABLE_EXPR:
      return ResolveSystemVariableExpression(
          ast_expr->GetAsOrDie<ASTSystemVariableExpr>(),
          expr_resolution_info.get(), resolved_expr_out);
    case AST_NAMED_ARGUMENT:
      // Resolve named arguments for function calls by simply resolving the
      // expression part of the argument. The function resolver will apply
      // special handling to inspect the name part and integrate into function
      // signature matching appropriately.
      return ResolveExpr(ast_expr->GetAsOrDie<ASTNamedArgument>()->expr(),
                         expr_resolution_info.get(), resolved_expr_out);
    default:
      return MakeSqlErrorAt(ast_expr)
             << "Unhandled select-list expression for node kind "
             << ast_expr->GetNodeKindString() << ":\n"
             << ast_expr->DebugString();
  }
}

absl::Status Resolver::ResolveLiteralExpr(
    const ASTExpression* ast_expr,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  switch (ast_expr->node_kind()) {
    case AST_INT_LITERAL: {
      // Note: Negative integer literals are handled by a special case for MINUS
      // in ResolveUnaryExpr().
      const ASTIntLiteral* literal = ast_expr->GetAsOrDie<ASTIntLiteral>();
      Value value;
      if (!Value::ParseInteger(literal->image(), &value)) {
        if (literal->is_hex()) {
          return MakeSqlErrorAt(literal)
                 << "Invalid hex integer literal: " << literal->image();
        } else {
          return MakeSqlErrorAt(literal)
                 << "Invalid integer literal: " << literal->image();
        }
      }
      ZETASQL_RET_CHECK(!value.is_null());
      ZETASQL_RET_CHECK(value.type_kind() == TYPE_INT64 ||
                value.type_kind() == TYPE_UINT64)
          << value.DebugString();

      if (product_mode() == PRODUCT_EXTERNAL &&
          value.type_kind() == TYPE_UINT64) {
        if (literal->is_hex()) {
          return MakeSqlErrorAt(literal)
                 << "Invalid hex integer literal: " << literal->image();
        } else {
          return MakeSqlErrorAt(literal)
                 << "Invalid integer literal: " << literal->image();
        }
      }
      *resolved_expr_out = MakeResolvedLiteral(ast_expr, value);
      return absl::OkStatus();
    }

    case AST_STRING_LITERAL: {
      const ASTStringLiteral* literal =
          ast_expr->GetAsOrDie<ASTStringLiteral>();
      *resolved_expr_out =
          MakeResolvedLiteral(ast_expr, Value::String(literal->string_value()));
      return absl::OkStatus();
    }

    case AST_BYTES_LITERAL: {
      const ASTBytesLiteral* literal = ast_expr->GetAsOrDie<ASTBytesLiteral>();
      *resolved_expr_out =
          MakeResolvedLiteral(ast_expr, Value::Bytes(literal->bytes_value()));
      return absl::OkStatus();
    }

    case AST_BOOLEAN_LITERAL: {
      const ASTBooleanLiteral* literal =
          ast_expr->GetAsOrDie<ASTBooleanLiteral>();
      *resolved_expr_out =
          MakeResolvedLiteral(ast_expr, Value::Bool(literal->value()));
      return absl::OkStatus();
    }

    case AST_FLOAT_LITERAL: {
      const ASTFloatLiteral* literal = ast_expr->GetAsOrDie<ASTFloatLiteral>();
      double double_value;
      if (!functions::StringToNumeric(literal->image(), &double_value,
                                      nullptr)) {
        return MakeSqlErrorAt(literal)
               << "Invalid floating point literal: " << literal->image();
      }
      std::unique_ptr<const ResolvedLiteral> resolved_literal =
          MakeResolvedFloatLiteral(
              ast_expr, types::DoubleType(), Value::Double(double_value),
              /*has_explicit_type=*/false, literal->image());
      *resolved_expr_out = std::move(resolved_literal);
      return absl::OkStatus();
    }

    case AST_NUMERIC_LITERAL: {
      const ASTNumericLiteral* literal =
          ast_expr->GetAsOrDie<ASTNumericLiteral>();
      if (!language().LanguageFeatureEnabled(FEATURE_NUMERIC_TYPE)) {
        std::string error_type_token = GetNodePrefixToken(sql_, *literal);
        return MakeSqlErrorAt(literal)
               << error_type_token << " literals are not supported";
      }
      std::string unquoted_image;
      ZETASQL_RETURN_IF_ERROR(ParseStringLiteral(literal->image(), &unquoted_image));
      auto value_or_status = NumericValue::FromStringStrict(unquoted_image);
      if (!value_or_status.status().ok()) {
        std::string error_type_token = GetNodePrefixToken(sql_, *literal);
        return MakeSqlErrorAt(literal)
               << "Invalid " << error_type_token
               << " literal: " << ToStringLiteral(unquoted_image);
      }
      *resolved_expr_out =
          MakeResolvedLiteral(ast_expr, types::NumericType(),
                              Value::Numeric(value_or_status.value()),
                              /*has_explicit_type=*/true);
      return absl::OkStatus();
    }

    case AST_BIGNUMERIC_LITERAL: {
      const ASTBigNumericLiteral* literal =
          ast_expr->GetAsOrDie<ASTBigNumericLiteral>();
      if (!language().LanguageFeatureEnabled(FEATURE_BIGNUMERIC_TYPE)) {
        std::string error_type_token = GetNodePrefixToken(sql_, *literal);
        return MakeSqlErrorAt(literal)
               << error_type_token << " literals are not supported";
      }
      std::string unquoted_image;
      ZETASQL_RETURN_IF_ERROR(ParseStringLiteral(literal->image(), &unquoted_image));
      auto value_or_status = BigNumericValue::FromStringStrict(unquoted_image);
      if (!value_or_status.ok()) {
        std::string error_type_token = GetNodePrefixToken(sql_, *literal);
        return MakeSqlErrorAt(literal)
               << "Invalid " << error_type_token
               << " literal: " << ToStringLiteral(unquoted_image);
      }
      *resolved_expr_out =
          MakeResolvedLiteral(ast_expr, types::BigNumericType(),
                              Value::BigNumeric(*value_or_status),
                              /*has_explicit_type=*/true);
      return absl::OkStatus();
    }

    case AST_JSON_LITERAL: {
      const ASTJSONLiteral* literal = ast_expr->GetAsOrDie<ASTJSONLiteral>();
      if (!language().LanguageFeatureEnabled(FEATURE_JSON_TYPE)) {
        return MakeSqlErrorAt(literal) << "JSON literals are not supported";
      }
      ZETASQL_ASSIGN_OR_RETURN(*resolved_expr_out, ResolveJsonLiteral(literal));
      return absl::OkStatus();
    }

    case AST_NULL_LITERAL: {
      // NULL literals are always treated as int64_t.  Literal coercion rules
      // may make the NULL change type.
      *resolved_expr_out = MakeResolvedLiteral(
          ast_expr, Value::Null(type_factory_->get_int64()));
      return absl::OkStatus();
    }

    case AST_DATE_OR_TIME_LITERAL: {
      const ASTDateOrTimeLiteral* literal =
          ast_expr->GetAsOrDie<ASTDateOrTimeLiteral>();
      return MakeResolvedDateOrTimeLiteral(
          ast_expr, literal->type_kind(),
          literal->string_literal()->string_value(), resolved_expr_out);
    }
    default:
      return MakeSqlErrorAt(ast_expr)
             << "Unhandled select-list literal expression for node kind "
             << ast_expr->GetNodeKindString() << ":\n"
             << ast_expr->DebugString();
  }
}

// Used to map a ResolvedColumnRef to a version of it that is
// available after GROUP BY.  Updates the ResolvedColumnRef with an
// available version if necessary, and returns an error if the input
// column is not available after GROUP BY.
//
// This is currently only used for resolving SELECT STAR columns
// after GROUP BY, but could potentially be used for other contexts.
//
// Arguments:
//   path_expr - input, only used for error messaging
//   clause_name - input, the clause that references the ResolvedColumn
//   expr_resolution_info - input, contains map from pre-grouping columns
//       to post-grouping columns
//   resolved_column_ref_expr - input/output
//
// On input, <resolved_column_ref_expr> indicates a ResolvedColumnRef that
// we want to resolve to a version of the column that is available after
// GROUP BY.
//
// On output, <resolved_column_ref_expr> indicates a ResolvedColumnRef that
// is available after GROUP BY.
//
// The function logic is as follows:
//
// 1) If the input ResolvedColumn is correlated then it is visible, so
//    <resolved_column_ref_expr> is left unchanged and returns OK.
// 2) Otherwise, we look through the QueryResolutionInfo's GroupByExprInfo
//    vector to see if we map this ResolvedColumn to another ResolvedColumn,
//    with no additional name path.
// 3) If we found a matching GroupByExprInfo, then <resolved_column_ref_expr>
//    is updated to a ResolvedColumnRef for the post-grouping version of the
//    column and returns OK.
// 4) Otherwise an error is returned.
absl::Status Resolver::ResolveColumnRefExprToPostGroupingColumn(
    const ASTExpression* path_expr, absl::string_view clause_name,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_column_ref_expr) {
  ZETASQL_RET_CHECK_EQ(RESOLVED_COLUMN_REF, (*resolved_column_ref_expr)->node_kind());
  ZETASQL_DCHECK(query_resolution_info != nullptr);
  ZETASQL_DCHECK(query_resolution_info->HasGroupByOrAggregation());

  const ResolvedColumnRef* resolved_column_ref =
      (*resolved_column_ref_expr)->GetAs<ResolvedColumnRef>();

  if (resolved_column_ref->is_correlated()) {
    return absl::OkStatus();
  }

  ResolvedColumn resolved_column = resolved_column_ref->column();
  const ValidNamePathList* name_path_list;
  if (query_resolution_info->group_by_valid_field_info_map().LookupNamePathList(
          resolved_column, &name_path_list)) {
    for (const ValidNamePath& valid_name_path : *name_path_list) {
      if (valid_name_path.name_path().empty()) {
        *resolved_column_ref_expr =
            MakeColumnRef(valid_name_path.target_column());
        return absl::OkStatus();
      }
    }
  }
  return MakeSqlErrorAt(path_expr)
         << clause_name << " expression references column "
         << resolved_column.name()
         << " which is neither grouped nor aggregated";
}

namespace {
std::string GetUnrecognizedNameErrorWithCatalogSuggestion(
    absl::Span<const std::string> name_parts, Catalog* catalog,
    bool name_is_system_variable, bool suggesting_system_variable) {
  std::string name_suggestion = catalog->SuggestConstant(name_parts);
  if (!name_suggestion.empty()) {
    std::string path_prefix = name_is_system_variable ? "@@" : "";
    std::string suggestion_path_prefix = suggesting_system_variable ? "@@" : "";

    std::string error_message;
    absl::StrAppend(
        &error_message, "Unrecognized name: ", path_prefix,
        absl::StrJoin(name_parts, ".",
                      [](std::string* out, const std::string& part) {
                        absl::StrAppend(out, ToIdentifierLiteral(part));
                      }),
        "; Did you mean ", suggestion_path_prefix, name_suggestion, "?");
    return error_message;
  }
  return "";
}
}  // namespace

absl::Status Resolver::GetUnrecognizedNameError(
    const ASTPathExpression* ast_path_expr, const NameScope* name_scope) {
  std::vector<std::string> identifiers = ast_path_expr->ToIdentifierVector();
  bool is_system_variable =
      ast_path_expr->parent() != nullptr &&
      ast_path_expr->parent()->node_kind() == AST_SYSTEM_VARIABLE_EXPR;
  std::string path_prefix = is_system_variable ? "@@" : "";

  IdStringPool id_string_pool;
  const IdString first_name = id_string_pool.Make(identifiers[0]);
  std::string error_message;
  absl::StrAppend(&error_message, "Unrecognized name: ", path_prefix,
                  ToIdentifierLiteral(first_name));

  // See if we can come up with a name suggestion.  First, look for aliases in
  // the current scope.
  std::string name_suggestion;
  if (name_scope != nullptr) {
    name_suggestion = name_scope->SuggestName(first_name);
    if (!name_suggestion.empty()) {
      absl::StrAppend(&error_message, "; Did you mean ", name_suggestion, "?");
    }
  }

  // Check the catalog for either a named constant or a system variable.  We'll
  // check for both in all cases, but give priority to system variables if the
  // original expression was a system variable, or a named constant, otherwise.
  if (name_suggestion.empty()) {
    std::string suggested_error_message =
        GetUnrecognizedNameErrorWithCatalogSuggestion(
            absl::MakeConstSpan(identifiers),
            is_system_variable ? GetSystemVariablesCatalog() : catalog_,
            /*name_is_system_variable=*/is_system_variable,
            /*suggesting_system_variable=*/is_system_variable);
    if (suggested_error_message.empty()) {
      suggested_error_message = GetUnrecognizedNameErrorWithCatalogSuggestion(
          absl::MakeConstSpan(identifiers),
          is_system_variable ? catalog_ : GetSystemVariablesCatalog(),
          /*name_is_system_variable=*/is_system_variable,
          /*suggesting_system_variable=*/!is_system_variable);
    }
    if (!suggested_error_message.empty()) {
      error_message = suggested_error_message;
    }
  }

  if (is_system_variable) {
    // Use the position of the '@@', rather than that of the name following it.
    return MakeSqlErrorAt(ast_path_expr->parent()) << error_message;
  } else {
    return MakeSqlErrorAt(ast_path_expr) << error_message;
  }
}

absl::Status Resolver::ValidateColumnForAggregateOrAnalyticSupport(
    const ResolvedColumn& resolved_column, IdString first_name,
    const ASTPathExpression* path_expr,
    ExprResolutionInfo* expr_resolution_info) const {
  SelectColumnStateList* select_column_state_list =
      expr_resolution_info->query_resolution_info->select_column_state_list();
  // If this ResolvedColumn is a SELECT list column, then validate
  // that any included aggregate or analytic function is allowed in
  // this expression.
  if (select_column_state_list != nullptr) {
    for (const std::unique_ptr<SelectColumnState>& select_column_state :
         select_column_state_list->select_column_state_list()) {
      if (resolved_column == select_column_state->resolved_select_column) {
        if (select_column_state->has_aggregation) {
          expr_resolution_info->has_aggregation = true;
        }
        if (select_column_state->has_analytic) {
          expr_resolution_info->has_analytic = true;
        }
        ZETASQL_RETURN_IF_ERROR(expr_resolution_info->query_resolution_info
                            ->select_column_state_list()
                            ->ValidateAggregateAndAnalyticSupport(
                                first_name.ToStringView(), path_expr,
                                select_column_state.get(),
                                expr_resolution_info));
        break;
      }
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::MaybeResolvePathExpressionAsFunctionArgumentRef(
    const ASTPathExpression* path_expr,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out,
    int* num_parts_consumed) {
  if (function_argument_info_ == nullptr) {
    return absl::OkStatus();
  }
  const FunctionArgumentInfo::ArgumentDetails* arg_details =
      function_argument_info_->FindScalarArg(
          path_expr->first_name()->GetAsIdString());
  if (arg_details == nullptr) {
    return absl::OkStatus();
  }
  // We do not expect to analyze a template function body until the function is
  // invoked and we have concrete argument types. If a template type is found
  // that could mean engine code is using an API incorrectly.
  ZETASQL_RET_CHECK(!arg_details->arg_type.IsTemplated())
      << "Function bodies cannot be resolved with templated argument types";
  auto resolved_argument_ref = MakeResolvedArgumentRef(
      arg_details->arg_type.type(), arg_details->name.ToString(),
      arg_details->arg_kind.value());
  MaybeRecordParseLocation(path_expr, resolved_argument_ref.get());
  if (arg_details->arg_kind.value() == ResolvedArgumentDef::AGGREGATE) {
    // Save the location for aggregate arguments because we generate
    // some errors referencing them in a post-pass after resolving the full
    // function body, when we no longer have the ASTNode.
    resolved_argument_ref->SetParseLocationRange(
        path_expr->GetParseLocationRange());
  }
  *resolved_expr_out = std::move(resolved_argument_ref);
  (*num_parts_consumed)++;
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolvePathExpressionAsExpression(
    const ASTPathExpression* path_expr,
    ExprResolutionInfo* expr_resolution_info,
    ResolvedStatement::ObjectAccess access_flags,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  const ASTIdentifier* ast_first_name = path_expr->name(0);
  const IdString first_name = ast_first_name->GetAsIdString();

  // The length of the longest prefix of <path_expr> that has been resolved
  // successfully. Often 1 after successful resolution and 0 if resolution
  // fails, but can be longer for name paths and named constants. 0 while
  // <path_expr> has not be resolved yet.
  int num_names_consumed = 0;

  // The catalog object that <path_expr> resolves to, if any. Can be any of the
  // following, which will be tried in this order below:
  // (0) Function argument
  //     Only if FEATURE_FUNCTION_ARGUMENT_NAMES_HIDE_LOCAL_NAMES is on.
  //     Arguments are only set when resolving CREATE [TABLE] FUNCTION
  //     statements or resolving function templates once arguments are known.
  // (1) Name target;
  // (2) Expression column (for standalone expression evaluation only);
  // (3) Function argument
  //     Instead of (0) FEATURE_FUNCTION_ARGUMENT_NAMES_HIDE_LOCAL_NAMES is off.
  // (4) Named constant.
  std::unique_ptr<const ResolvedExpr> resolved_expr;

  // (0) Check for a function argument before looking at local names.
  if (language().LanguageFeatureEnabled(
          FEATURE_FUNCTION_ARGUMENT_NAMES_HIDE_LOCAL_NAMES)) {
    ZETASQL_RETURN_IF_ERROR(MaybeResolvePathExpressionAsFunctionArgumentRef(
        path_expr, &resolved_expr, &num_names_consumed));
  }

  // (1) Try to find a name target that matches <path_expr>.
  CorrelatedColumnsSetList correlated_columns_sets;
  NameTarget target;
  bool resolved_to_target = false;
  if (num_names_consumed == 0 && expr_resolution_info->name_scope != nullptr) {
    ZETASQL_RETURN_IF_ERROR(expr_resolution_info->name_scope->LookupNamePath(
        path_expr, expr_resolution_info->clause_name,
        expr_resolution_info->is_post_distinct(), in_strict_mode(),
        &correlated_columns_sets, &num_names_consumed, &target));
    resolved_to_target = num_names_consumed > 0;
  }
  if (resolved_to_target) {
    // We resolved (at least part of) the prefix path to a NameTarget.  Create
    // a ResolvedExpr for the resolved part of the name path.  We will
    // resolve any remaining names as field accesses just before returning.
    switch (target.kind()) {
      case NameTarget::RANGE_VARIABLE:
        if (target.scan_columns()->is_value_table()) {
          ZETASQL_RET_CHECK_EQ(target.scan_columns()->num_columns(), 1);
          ResolvedColumn resolved_column =
              target.scan_columns()->column(0).column;
          resolved_expr = MakeColumnRefWithCorrelation(
              resolved_column, correlated_columns_sets, access_flags);
        } else {
          // Creating a STRUCT is relatively expensive, so we want to
          // avoid it whenever possible.  For example, if 'path_expr'
          // references a field of a struct then we can avoid creating the
          // STRUCT.  The current implementation only creates a STRUCT
          // here if there is exactly one name in the path (in which
          // case there will be an error later if there is a second component,
          // because it would have to be a generalized field access, and those
          // only apply to PROTO fields).  It is the responsibility of
          // LookupNamePath() to facilitate this optimization by only choosing
          // 'target' to be a STRUCT RANGE_VARIABLE if the prefix path has no
          // subsequent identifiers to resolve.  We ZETASQL_RET_CHECK that condition
          // here to avoid unintentional performance regression in the future.
          ZETASQL_RET_CHECK_EQ(1, path_expr->num_names());
          std::unique_ptr<ResolvedComputedColumn> make_struct_computed_column;
          ZETASQL_RETURN_IF_ERROR(CreateStructFromNameList(
              target.scan_columns().get(), correlated_columns_sets,
              &make_struct_computed_column));
          resolved_expr = make_struct_computed_column->release_expr();
        }
        break;
      case NameTarget::EXPLICIT_COLUMN:
      case NameTarget::IMPLICIT_COLUMN: {
        ResolvedColumn resolved_column = target.column();
        auto resolved_column_ref = MakeColumnRefWithCorrelation(
            resolved_column, correlated_columns_sets, access_flags);
        MaybeRecordParseLocation(path_expr, resolved_column_ref.get());
        resolved_expr = std::move(resolved_column_ref);
        if (expr_resolution_info->query_resolution_info != nullptr &&
            !expr_resolution_info->is_post_distinct()) {
          // We resolved this to a column, which might be a SELECT list
          // column.  If so, we need to validate that any aggregate or
          // analytic functions are valid in this context.  For instance,
          // consider:
          //
          //   SELECT sum(a) as b FROM table HAVING sum(b) > 5;
          //
          // When resolving the HAVING clause expression 'sum(b) > 5', as
          // we resolve the subexpression 'sum(b)' the arguments to it must
          // not contain aggregation or analytic functions.  Since 'b' is a
          // SELECT list alias and resolved to a SELECT list ResolvedColumn,
          // we must check it for the presence of aggregate or analytic
          // functions there.
          //
          // We don't do this for post-DISTINCT column references, since SELECT
          // list references are always ok (neither aggregate nor analytic
          // functions are currently allowed in ORDER BY if DISTINCT is
          // present).  TODO: We have an open item for allowing
          // analytic functions in ORDER BY after DISTINCT.  If/when we
          // add support for that, we will need to re-enable this validation
          // when is_post_distinct() is true.
          ZETASQL_RETURN_IF_ERROR(ValidateColumnForAggregateOrAnalyticSupport(
              resolved_column, first_name, path_expr, expr_resolution_info));
        }
        break;
      }
      case NameTarget::FIELD_OF:
        ZETASQL_RETURN_IF_ERROR(ResolveFieldAccess(
            MakeColumnRefWithCorrelation(target.column_containing_field(),
                                         correlated_columns_sets, access_flags),
            path_expr, ast_first_name, &expr_resolution_info->flatten_state,
            &resolved_expr));
        break;
      case NameTarget::ACCESS_ERROR:
      case NameTarget::AMBIGUOUS:
        // As per the LookupNamePath() contract, if it returns OK and at
        // least one name was consumed then the returned target must
        // be a valid target (range variable, column, or field reference).
        ZETASQL_RET_CHECK_FAIL()
            << "LookupNamePath returned OK with unexpected NameTarget: "
            << target.DebugString();
    }
  }

  if (num_names_consumed == 0 && analyzing_expression_) {
    // (2) We still haven't found a matching name. See if we can find it in an
    // expression column (for standalone expression evaluation only).
    // TODO: Move this to a separate function to handle this
    // case out of line.
    const std::string lowercase_name =
        absl::AsciiStrToLower(first_name.ToString());
    const Type* column_type = zetasql_base::FindPtrOrNull(
        analyzer_options_.expression_columns(), lowercase_name);

    if (column_type != nullptr) {
      resolved_expr = MakeResolvedExpressionColumn(column_type, lowercase_name);
      num_names_consumed = 1;
    } else if (analyzer_options_.has_in_scope_expression_column()) {
      // We have an automatically in-scope expression column.
      // See if we can resolve this name as a field on it.
      column_type = analyzer_options_.in_scope_expression_column_type();

      // Make a ResolvedExpr for the in-scope expression column.
      // We'll only use this if we are successful resolving a field access.
      std::unique_ptr<const ResolvedExpr> value_column =
          MakeResolvedExpressionColumn(
              column_type, analyzer_options_.in_scope_expression_column_name());

      // These MaybeResolve methods with <error_if_not_found> false will
      // return OK with a NULL <*resolved_expr_out> if the field isn't found.
      if (column_type->IsProto()) {
        MaybeResolveProtoFieldOptions options;
        options.error_if_not_found = false;
        ZETASQL_RETURN_IF_ERROR(MaybeResolveProtoFieldAccess(
            path_expr, ast_first_name, options, std::move(value_column),
            &resolved_expr));
      } else if (column_type->IsStruct()) {
        ZETASQL_RETURN_IF_ERROR(MaybeResolveStructFieldAccess(
            path_expr, ast_first_name, /*error_if_not_found=*/false,
            std::move(value_column), &resolved_expr));
      }

      if (resolved_expr != nullptr) {
        num_names_consumed = 1;
      }
    }

    // Lookup the column using the callback function if set.
    if (num_names_consumed == 0 &&
        analyzer_options_.lookup_expression_column_callback() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(analyzer_options_.lookup_expression_column_callback()(
          lowercase_name, &column_type));
      if (column_type != nullptr) {
        resolved_expr =
            MakeResolvedExpressionColumn(column_type, lowercase_name);
        num_names_consumed = 1;
      }
    }
  }

  if (num_names_consumed == 0 &&
      !language().LanguageFeatureEnabled(
          FEATURE_FUNCTION_ARGUMENT_NAMES_HIDE_LOCAL_NAMES)) {
    // (3) We still haven't found a matching name. See if we can find it in
    // function arguments (for CREATE FUNCTION statements only).
    ZETASQL_RETURN_IF_ERROR(MaybeResolvePathExpressionAsFunctionArgumentRef(
        path_expr, &resolved_expr, &num_names_consumed));
  }

  if (num_names_consumed == 0) {
    // (4) We still haven't found a matching name. Try to resolve the longest
    // possible prefix of <path_expr> to a named constant.
    const Constant* constant = nullptr;
    absl::Status find_constant_with_path_prefix_status =
        catalog_->FindConstantWithPathPrefix(path_expr->ToIdentifierVector(),
                                             &num_names_consumed, &constant,
                                             analyzer_options_.find_options());

    // Handle the case where a constant was found or some internal error
    // occurred. If no constant was found, <num_names_consumed> is set to 0.
    if (find_constant_with_path_prefix_status.code() !=
        absl::StatusCode::kNotFound) {
      // If an error occurred, return immediately.
      ZETASQL_RETURN_IF_ERROR(find_constant_with_path_prefix_status);

      // A constant was found. Wrap it in an AST node and skip ahead to
      // resolving any field access in a path suffix if present.
      ZETASQL_RET_CHECK(constant != nullptr);
      ZETASQL_RET_CHECK_GT(num_names_consumed, 0);
      auto resolved_constant = MakeResolvedConstant(constant->type(), constant);
      MaybeRecordParseLocation(path_expr, resolved_constant.get());
      resolved_expr = std::move(resolved_constant);
    }
  }
  if (generated_column_cycle_detector_ != nullptr) {
    // If we are analyzing generated columns, we need to record the dependency
    // here so that we are not introducing cycles.
    // E.g. in
    // CREATE TABLE T (
    //  a as b,
    //  b INT64
    // );
    // If we are processing 'a', 'first_name' will be b in this example.
    // In this case this dependency does not introduce a cycle so it's a valid
    // reference.
    unresolved_column_name_in_generated_column_.clear();
    ZETASQL_RETURN_IF_ERROR(
        generated_column_cycle_detector_->AddDependencyOn(first_name));
  }
  if (num_names_consumed == 0) {
    // No matching name could be found, so throw an error.
    if (generated_column_cycle_detector_ != nullptr) {
      // When we are analyzing generated columns and there was an error, we
      // record the name of the column here so that the caller can try resolving
      // the column that was not found. E.g. in
      // CREATE TABLE T (
      //  a as b,
      //  b as c,
      //  c INT64
      // );
      // 'b' won't be found the first time 'a' is being resolved, so
      // 'generated_column_cycle_detector_' will be set to 'b' in this case so
      // that the resolver tries to resolve 'b' next.
      unresolved_column_name_in_generated_column_ = first_name;
    }
    return GetUnrecognizedNameError(path_expr,
                                    expr_resolution_info != nullptr
                                        ? expr_resolution_info->name_scope
                                        : nullptr);
  }
  ZETASQL_RET_CHECK(resolved_expr != nullptr);

  // Resolve any further identifiers in <path_expr> as field accesses.
  for (; num_names_consumed < path_expr->num_names(); ++num_names_consumed) {
    ZETASQL_RETURN_IF_ERROR(ResolveFieldAccess(std::move(resolved_expr), path_expr,
                                       path_expr->name(num_names_consumed),
                                       &expr_resolution_info->flatten_state,
                                       &resolved_expr));
  }

  *resolved_expr_out = std::move(resolved_expr);
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveParameterExpr(
    const ASTParameterExpr* param_expr,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  if (analyzer_options().statement_context() == CONTEXT_MODULE) {
    return MakeSqlErrorAt(param_expr)
           << "Query parameters cannot be used inside modules";
  }
  if (!disallowing_query_parameters_with_error_.empty()) {
    return MakeSqlErrorAt(param_expr)
           << disallowing_query_parameters_with_error_;
  }
  if (generated_column_cycle_detector_ != nullptr) {
    return MakeSqlErrorAt(param_expr)
           << "Query parameters cannot be used inside generated columns";
  }
  if (analyzing_check_constraint_expression_) {
    return MakeSqlErrorAt(param_expr)
           << "Query parameters cannot be used inside CHECK"
           << " constraint expression";
  }
  if (default_expr_access_error_name_scope_.has_value()) {
    return MakeSqlErrorAt(param_expr)
           << "Query parameters cannot be used inside a column default "
           << "expression";
  }

  const Type* param_type = nullptr;
  std::string lowercase_param_name;
  int position = 0;

  switch (analyzer_options_.parameter_mode()) {
    case PARAMETER_NAMED: {
      if (param_expr->name() == nullptr) {
        return MakeSqlErrorAt(param_expr)
               << "Positional parameters are not supported";
      }
      const std::string& param_name = param_expr->name()->GetAsString();
      ZETASQL_RET_CHECK(!param_name.empty());
      lowercase_param_name = absl::AsciiStrToLower(param_name);
      param_type = zetasql_base::FindPtrOrNull(analyzer_options_.query_parameters(),
                                      lowercase_param_name);

      if (param_type == nullptr) {
        if (analyzer_options_.allow_undeclared_parameters()) {
          auto param =
              MakeResolvedParameter(types::Int64Type(), lowercase_param_name,
                                    /*position=*/0, /*is_untyped=*/true);
          // Note: We always attach a parse location for a parameter
          // (regardless of whether or not the AnalyzerOptions is set to
          // record_parse_locations), because we use that parse location as
          // a key to look up the parameter when doing Type analysis for
          // untyped parameters.  See MaybeAssignTypeToUndeclaredParameter()
          // for details.
          param->SetParseLocationRange(param_expr->GetParseLocationRange());
          *resolved_expr_out = std::move(param);
          // Record the new location, no collisions are possible.
          untyped_undeclared_parameters_.emplace(
              param_expr->GetParseLocationRange().start(),
              lowercase_param_name);
          return absl::OkStatus();
        }
        return MakeSqlErrorAt(param_expr)
               << "Query parameter '" << param_name << "' not found";
      }
    } break;
    case PARAMETER_POSITIONAL: {
      if (param_expr->name() != nullptr) {
        return MakeSqlErrorAt(param_expr)
               << "Named parameters are not supported";
      }

      position = param_expr->position();
      if (position - 1 >=
          analyzer_options_.positional_query_parameters().size()) {
        if (analyzer_options_.allow_undeclared_parameters()) {
          auto param =
              MakeResolvedParameter(types::Int64Type(), lowercase_param_name,
                                    position, /*is_untyped=*/true);
          param->SetParseLocationRange(param_expr->GetParseLocationRange());
          *resolved_expr_out = std::move(param);
          // Record the new location, no collisions are possible.
          untyped_undeclared_parameters_.emplace(
              param_expr->GetParseLocationRange().start(), position);
          return absl::OkStatus();
        }
        return MakeSqlErrorAt(param_expr)
               << "Query parameter number " << position << " is not defined ("
               << analyzer_options_.positional_query_parameters().size()
               << " provided)";
      }

      param_type =
          analyzer_options_.positional_query_parameters()[position - 1];
    } break;
    case PARAMETER_NONE:
      return MakeSqlErrorAt(param_expr) << "Parameters are not supported";
  }

  *resolved_expr_out = MakeResolvedParameter(param_type, lowercase_param_name,
                                             position, /*is_untyped=*/false);
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveDotIdentifier(
    const ASTDotIdentifier* dot_identifier,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(dot_identifier->expr(), expr_resolution_info,
                              &resolved_expr));

  return ResolveFieldAccess(
      std::move(resolved_expr), dot_identifier, dot_identifier->name(),
      &expr_resolution_info->flatten_state, resolved_expr_out);
}

absl::Status Resolver::MaybeResolveProtoFieldAccess(
    const ASTNode* ast_path_expression, const ASTIdentifier* identifier,
    const MaybeResolveProtoFieldOptions& options,
    std::unique_ptr<const ResolvedExpr> resolved_lhs,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  resolved_expr_out->reset();

  const std::string dot_name = identifier->GetAsString();

  ZETASQL_RET_CHECK(resolved_lhs->type()->IsProto());
  const google::protobuf::Descriptor* lhs_proto =
      resolved_lhs->type()->AsProto()->descriptor();
  ZETASQL_ASSIGN_OR_RETURN(const google::protobuf::FieldDescriptor* field,
                   FindFieldDescriptor(identifier, lhs_proto, dot_name));
  bool get_has_bit = false;
  if (options.get_has_bit_override) {
    get_has_bit = *options.get_has_bit_override;
  } else if (absl::StartsWithIgnoreCase(dot_name, "has_")) {
    if (lhs_proto->options().map_entry()) {
      return MakeSqlErrorAt(identifier)
             << lhs_proto->full_name() << " is a synthetic map entry proto. "
             << dot_name
             << " is not allowed for map entries because all map entry fields "
                "are considered to be present by definition";
    }

    const std::string dot_name_without_has = dot_name.substr(4);
    ZETASQL_ASSIGN_OR_RETURN(
        const google::protobuf::FieldDescriptor* has_field,
        FindFieldDescriptor(identifier, lhs_proto, dot_name_without_has));
    if (has_field != nullptr) {
      if (field != nullptr) {
        // Give an error if we asked for has_X and the proto has fields
        // called both X and has_X.  Such protos won't actually make it
        // through the c++ proto compiler, but might be hackable other ways.
        return MakeSqlErrorAt(identifier)
               << "Protocol buffer " << lhs_proto->full_name()
               << " has fields called both " << dot_name << " and "
               << dot_name_without_has << ", so " << dot_name
               << " is ambiguous";
      }

      // Get the has bit rather than the field value.
      field = has_field;
      get_has_bit = true;
    }
  }

  if (field == nullptr) {
    if (options.error_if_not_found) {
      std::string error_message;
      absl::StrAppend(&error_message, "Protocol buffer ",
                      lhs_proto->full_name(), " does not have a field called ",
                      dot_name);

      if (lhs_proto->field_count() == 0) {
        absl::StrAppend(
            &error_message,
            "; Proto has no fields. Is the full proto descriptor available?");
      } else {
        std::vector<std::string> possible_names;
        possible_names.reserve(lhs_proto->field_count());
        for (int i = 0; i < lhs_proto->field_count(); ++i) {
          possible_names.push_back(lhs_proto->field(i)->name());
        }
        const std::string name_suggestion =
            ClosestName(dot_name, possible_names);
        if (!name_suggestion.empty()) {
          absl::StrAppend(&error_message, "; Did you mean ", name_suggestion,
                          "?");
        }
      }
      return MakeSqlErrorAt(identifier) << error_message;
    } else {
      // We return success with a NULL resolved_expr_out.
      ZETASQL_RET_CHECK(*resolved_expr_out == nullptr);
      return absl::OkStatus();
    }
  }

  const Type* field_type;
  Value default_value;
  if (get_has_bit) {
    if (field->is_repeated()) {
      return MakeSqlErrorAt(identifier)
             << "Protocol buffer " << lhs_proto->full_name() << " field "
             << field->name() << " is repeated, so "
             << (options.get_has_bit_override
                     ? absl::StrCat("HAS(", field->name(), ")")
                     : dot_name)
             << " is not allowed";
    }

    // Note that proto3 does not allow TYPE_GROUP.
    if (lhs_proto->file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3 &&
        field->type() != google::protobuf::FieldDescriptor::TYPE_MESSAGE &&
        language().LanguageFeatureEnabled(
            FEATURE_DEPRECATED_DISALLOW_PROTO3_HAS_SCALAR_FIELD)) {
      return MakeSqlErrorAt(identifier)
             << "Checking the presence of scalar field " << field->full_name()
             << " is not supported for proto3";
    }

    // When reading has_<field>, none of the modifiers are relevant because
    // we are checking for existence of the proto field, not checking if
    // we get a NULL after unwrapping.
    field_type = type_factory_->get_bool();
  } else {
    auto default_options =
        ProtoFieldDefaultOptions::FromFieldAndLanguage(field, language());
    if (options.ignore_format_annotations) {
      // ignore_format_annotations in the options implies a RAW() fetch.
      // RAW() fetches ignore any annotations on the fields and give you back
      // whatever you would have gotten without those annotations. This applies
      // to both type annotations and default annotations.
      default_options.ignore_format_annotations = true;
      default_options.ignore_use_default_annotations = true;
    }
    RETURN_SQL_ERROR_AT_IF_ERROR(
        identifier,
        GetProtoFieldTypeAndDefault(default_options, field, type_factory_,
                                    &field_type, &default_value));

    if (options.ignore_format_annotations && field_type->IsBytes()) {
      const Type* type_with_annotations;
      ZETASQL_RETURN_IF_ERROR(
          type_factory_->GetProtoFieldType(field, &type_with_annotations));
      if (type_with_annotations->IsGeography()) {
        return MakeSqlErrorAt(identifier)
               << "RAW() extractions of Geography fields are unsupported";
      }
    }
  }

  // ZetaSQL supports has_X() for fields that have unsupported type
  // annotations, but accessing the field value would produce an error.
  if (!get_has_bit &&
      ((field_type->UsingFeatureV12CivilTimeType() &&
        !language().LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME)) ||
       (field_type->IsNumericType() &&
        !language().LanguageFeatureEnabled(FEATURE_NUMERIC_TYPE)))) {
    return MakeSqlErrorAt(identifier)
           << "Field " << dot_name << " in protocol buffer "
           << lhs_proto->full_name() << " has unsupported type "
           << field_type->TypeName(language().product_mode());
  }

  auto resolved_get_proto_field = MakeResolvedGetProtoField(
      field_type, std::move(resolved_lhs), field, default_value, get_has_bit,
      ((get_has_bit || options.ignore_format_annotations)
           ? FieldFormat::DEFAULT_FORMAT
           : ProtoType::GetFormatAnnotation(field)),
      /*return_default_value_when_unset=*/false);
  MaybeRecordFieldAccessParseLocation(ast_path_expression, identifier,
                                      resolved_get_proto_field.get());
  *resolved_expr_out = std::move(resolved_get_proto_field);
  return absl::OkStatus();
}

absl::Status Resolver::MaybeResolveStructFieldAccess(
    const ASTNode* ast_path_expression, const ASTIdentifier* identifier,
    bool error_if_not_found, std::unique_ptr<const ResolvedExpr> resolved_lhs,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  resolved_expr_out->reset();

  const std::string dot_name = identifier->GetAsString();

  ZETASQL_RET_CHECK(resolved_lhs->type()->IsStruct());
  const StructType* struct_type = resolved_lhs->type()->AsStruct();
  bool is_ambiguous;
  int found_idx;
  const StructType::StructField* field =
      struct_type->FindField(dot_name, &is_ambiguous, &found_idx);
  if (is_ambiguous) {
    return MakeSqlErrorAt(identifier)
           << "Struct field name " << ToIdentifierLiteral(dot_name)
           << " is ambiguous";
  }
  if (field == nullptr) {
    if (error_if_not_found) {
      std::string error_message;
      absl::StrAppend(&error_message, "Field name ",
                      ToIdentifierLiteral(dot_name), " does not exist in ",
                      struct_type->ShortTypeName(product_mode()));

      std::vector<std::string> possible_names;
      for (const StructType::StructField& field : struct_type->fields()) {
        possible_names.push_back(field.name);
      }
      const std::string name_suggestion = ClosestName(dot_name, possible_names);
      if (!name_suggestion.empty()) {
        absl::StrAppend(&error_message, "; Did you mean ", name_suggestion,
                        "?");
      }
      return MakeSqlErrorAt(identifier) << error_message;
    } else {
      // We return success with a NULL resolved_expr_out.
      ZETASQL_RET_CHECK(*resolved_expr_out == nullptr);
      return absl::OkStatus();
    }
  }
  ZETASQL_DCHECK_EQ(field, &struct_type->field(found_idx));

  std::unique_ptr<ResolvedExpr> resolved_node = MakeResolvedGetStructField(
      field->type, std::move(resolved_lhs), found_idx);
  ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(/*error_node=*/nullptr,
                                               resolved_node.get()));
  MaybeRecordFieldAccessParseLocation(ast_path_expression, identifier,
                                      resolved_node.get());
  *resolved_expr_out = std::move(resolved_node);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveJsonFieldAccess(
    const ASTIdentifier* identifier,
    std::unique_ptr<const ResolvedExpr> resolved_lhs,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  *resolved_expr_out = nullptr;

  ZETASQL_RET_CHECK(resolved_lhs->type()->IsJson());

  *resolved_expr_out = MakeResolvedGetJsonField(
      types::JsonType(), std::move(resolved_lhs), identifier->GetAsString());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveFieldAccess(
    std::unique_ptr<const ResolvedExpr> resolved_lhs,
    const ASTNode* ast_path_expression, const ASTIdentifier* identifier,
    FlattenState* flatten_state,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  const Type* lhs_type = resolved_lhs->type();
  std::unique_ptr<ResolvedFlatten> resolved_flatten;
  if (lhs_type->IsArray() && flatten_state != nullptr &&
      flatten_state->can_flatten()) {
    lhs_type = ArrayElementTypeOrType(resolved_lhs->type());
    if (resolved_lhs->Is<ResolvedFlatten>() &&
        flatten_state->active_flatten() != nullptr) {
      resolved_flatten.reset(const_cast<ResolvedFlatten*>(
          resolved_lhs.release()->GetAs<ResolvedFlatten>()));
      ZETASQL_RET_CHECK_EQ(flatten_state->active_flatten(), resolved_flatten.get());
    } else {
      resolved_flatten =
          MakeResolvedFlatten(/*type=*/nullptr, std::move(resolved_lhs), {});
      analyzer_output_properties_.MarkRelevant(REWRITE_FLATTEN);
      ZETASQL_RET_CHECK_EQ(nullptr, flatten_state->active_flatten());
      flatten_state->set_active_flatten(resolved_flatten.get());
    }
    resolved_lhs = MakeResolvedFlattenedArg(lhs_type);
  }

  if (lhs_type->IsProto()) {
    ZETASQL_RETURN_IF_ERROR(MaybeResolveProtoFieldAccess(
        ast_path_expression, identifier, MaybeResolveProtoFieldOptions(),
        std::move(resolved_lhs), resolved_expr_out));
  } else if (lhs_type->IsStruct()) {
    ZETASQL_RETURN_IF_ERROR(MaybeResolveStructFieldAccess(
        ast_path_expression, identifier, /*error_if_not_found=*/true,
        std::move(resolved_lhs), resolved_expr_out));
  } else if (lhs_type->IsJson()) {
    ZETASQL_RETURN_IF_ERROR(ResolveJsonFieldAccess(identifier, std::move(resolved_lhs),
                                           resolved_expr_out));
  } else if (lhs_type->IsArray() &&
             language().LanguageFeatureEnabled(
                 FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS)) {
    return MakeSqlErrorAt(identifier)
           << "Cannot access field " << identifier->GetAsIdString()
           << " on a value with type "
           << lhs_type->ShortTypeName(product_mode()) << ". "
           << "You may need an explicit call to FLATTEN, and the flattened "
           << "argument may only contain 'dot' after the first array";
  } else {
    return MakeSqlErrorAt(identifier)
           << "Cannot access field " << identifier->GetAsIdString()
           << " on a value with type "
           << lhs_type->ShortTypeName(product_mode());
  }

  ZETASQL_RET_CHECK(*resolved_expr_out != nullptr);
  if (resolved_flatten != nullptr) {
    ZETASQL_RETURN_IF_ERROR(AddGetFieldToFlatten(
        std::move(*resolved_expr_out), type_factory_, resolved_flatten.get()));
    *resolved_expr_out = std::move(resolved_flatten);
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveExtensionFieldAccess(
    std::unique_ptr<const ResolvedExpr> resolved_lhs,
    const ResolveExtensionFieldOptions& options,
    const ASTPathExpression* ast_path_expr, FlattenState* flatten_state,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::unique_ptr<ResolvedFlatten> resolved_flatten;
  if (resolved_lhs->type()->IsArray() && flatten_state != nullptr &&
      flatten_state->can_flatten()) {
    const Type* lhs_type = ArrayElementTypeOrType(resolved_lhs->type());
    if (resolved_lhs->Is<ResolvedFlatten>() &&
        flatten_state->active_flatten() != nullptr) {
      resolved_flatten.reset(const_cast<ResolvedFlatten*>(
          resolved_lhs.release()->GetAs<ResolvedFlatten>()));
      ZETASQL_RET_CHECK_EQ(flatten_state->active_flatten(), resolved_flatten.get());
    } else {
      resolved_flatten =
          MakeResolvedFlatten(/*type=*/nullptr, std::move(resolved_lhs), {});
      analyzer_output_properties_.MarkRelevant(REWRITE_FLATTEN);
      ZETASQL_RET_CHECK_EQ(nullptr, flatten_state->active_flatten());
      flatten_state->set_active_flatten(resolved_flatten.get());
    }
    resolved_lhs = MakeResolvedFlattenedArg(lhs_type);
  }

  if (!resolved_lhs->type()->IsProto()) {
    return MakeSqlErrorAt(ast_path_expr)
           << "Generalized field access is not supported on expressions of "
           << "type " << resolved_lhs->type()->ShortTypeName(product_mode());
  }
  ZETASQL_ASSIGN_OR_RETURN(
      const google::protobuf::FieldDescriptor* extension_field,
      FindExtensionFieldDescriptor(
          ast_path_expr, resolved_lhs->type()->AsProto()->descriptor()));

  const google::protobuf::Descriptor* lhs_proto =
      resolved_lhs->type()->AsProto()->descriptor();

  const Type* field_type;
  Value default_value;
  if (options.get_has_bit) {
    if (extension_field->is_repeated() || resolved_flatten != nullptr) {
      return MakeSqlErrorAt(ast_path_expr)
             << "Protocol buffer " << lhs_proto->full_name()
             << " extension field " << extension_field->name()
             << " is repeated, so "
             << absl::StrCat("HAS((", extension_field->name(), "))")
             << " is not allowed";
    }

    field_type = type_factory_->get_bool();
  } else {
    auto default_options = ProtoFieldDefaultOptions::FromFieldAndLanguage(
        extension_field, language());
    if (options.ignore_format_annotations) {
      // ignore_format_annotations in the options implies a RAW() fetch.
      // RAW() fetches ignore any annotations on the fields and give you back
      // whatever you would have gotten without those annotations. This applies
      // to both type annotations and default annotations.
      default_options.ignore_format_annotations = true;
      default_options.ignore_use_default_annotations = true;
    }
    RETURN_SQL_ERROR_AT_IF_ERROR(
        ast_path_expr, GetProtoFieldTypeAndDefault(
                           default_options, extension_field, type_factory_,
                           &field_type, &default_value));

    if (options.ignore_format_annotations && field_type->IsBytes()) {
      const Type* type_with_annotations;
      ZETASQL_RETURN_IF_ERROR(type_factory_->GetProtoFieldType(extension_field,
                                                       &type_with_annotations));
      if (type_with_annotations->IsGeography()) {
        return MakeSqlErrorAt(ast_path_expr)
               << "RAW() extractions of Geography fields are unsupported";
      }
    }
  }

  if ((field_type->UsingFeatureV12CivilTimeType() &&
       !language().LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME)) ||
      (field_type->IsNumericType() &&
       !language().LanguageFeatureEnabled(FEATURE_NUMERIC_TYPE))) {
    return MakeSqlErrorAt(ast_path_expr)
           << "Protocol buffer extension " << extension_field->full_name()
           << " has unsupported type "
           << field_type->TypeName(language().product_mode());
  }
  auto resolved_get_proto_field = MakeResolvedGetProtoField(
      field_type, std::move(resolved_lhs), extension_field, default_value,
      options.get_has_bit,
      ((options.get_has_bit || options.ignore_format_annotations)
           ? FieldFormat::DEFAULT_FORMAT
           : ProtoType::GetFormatAnnotation(extension_field)),
      /*return_default_value_when_unset=*/false);

  MaybeRecordParseLocation(ast_path_expr, resolved_get_proto_field.get());
  *resolved_expr_out = std::move(resolved_get_proto_field);
  if (resolved_flatten != nullptr) {
    ZETASQL_RETURN_IF_ERROR(AddGetFieldToFlatten(
        std::move(*resolved_expr_out), type_factory_, resolved_flatten.get()));
    *resolved_expr_out = std::move(resolved_flatten);
  }
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveDotGeneralizedField(
    const ASTDotGeneralizedField* dot_generalized_field,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::unique_ptr<const ResolvedExpr> resolved_lhs;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(dot_generalized_field->expr(),
                              expr_resolution_info, &resolved_lhs));

  // The only supported operation using generalized field access currently
  // is reading proto extensions.
  return ResolveExtensionFieldAccess(
      std::move(resolved_lhs), ResolveExtensionFieldOptions(),
      dot_generalized_field->path(), &expr_resolution_info->flatten_state,
      resolved_expr_out);
}

absl::StatusOr<const google::protobuf::FieldDescriptor*> Resolver::FindFieldDescriptor(
    const ASTNode* ast_name_location, const google::protobuf::Descriptor* descriptor,
    absl::string_view name) {
  const google::protobuf::FieldDescriptor* field =
      ProtoType::FindFieldByNameIgnoreCase(descriptor, std::string(name));
  return field;
}

absl::Status Resolver::FindFieldDescriptors(
    absl::Span<const ASTIdentifier* const> path_vector,
    const google::protobuf::Descriptor* root_descriptor,
    std::vector<const google::protobuf::FieldDescriptor*>* field_descriptors) {
  ZETASQL_RET_CHECK(root_descriptor != nullptr);
  ZETASQL_RET_CHECK(field_descriptors != nullptr);
  // Inside the loop, 'current_descriptor' will be NULL if
  // field_descriptors->back() is not of message type.
  const google::protobuf::Descriptor* current_descriptor = root_descriptor;
  for (int path_index = 0; path_index < path_vector.size(); ++path_index) {
    if (current_descriptor == nullptr) {
      // There was an attempt to modify a field of a non-message type field.
      const Type* last_type;
      ZETASQL_RETURN_IF_ERROR(type_factory_->GetProtoFieldType(
          field_descriptors->back(), &last_type));
      return MakeSqlErrorAt(path_vector[path_index])
             << "Cannot access field " << path_vector[path_index]->GetAsString()
             << " on a value with type "
             << last_type->ShortTypeName(product_mode());
    }
    ZETASQL_ASSIGN_OR_RETURN(
        const google::protobuf::FieldDescriptor* field_descriptor,
        FindFieldDescriptor(path_vector[path_index], current_descriptor,
                            path_vector[path_index]->GetAsString()));
    if (field_descriptor == nullptr) {
      std::string error_message_prefix = "Protocol buffer ";
      if (path_index > 0) {
        error_message_prefix = absl::StrCat(
            "Field ", path_vector[path_index - 1]->GetAsString(), " of type ");
      }
      return MakeSqlErrorAt(path_vector[path_index])
             << error_message_prefix << current_descriptor->full_name()
             << " does not have a field named "
             << path_vector[path_index]->GetAsString();
    }
    field_descriptors->push_back(field_descriptor);
    // 'current_descriptor' will be NULL if 'field_descriptor' is not a message.
    current_descriptor = field_descriptor->message_type();
  }

  return absl::OkStatus();
}

static absl::Status MakeCannotAccessFieldError(
    const ASTNode* field_to_extract_location,
    const std::string& field_to_extract, const std::string& invalid_type_name,
    bool is_extension) {
  return MakeSqlErrorAt(field_to_extract_location)
         << "Cannot access " << (is_extension ? "extension (" : "field ")
         << field_to_extract << (is_extension ? ")" : "")
         << " on a value with type " << invalid_type_name;
}

static absl::Status GetLastSeenFieldTypeForReplaceFields(
    const std::vector<std::pair<int, const StructType::StructField*>>*
        struct_path,
    const std::vector<const google::protobuf::FieldDescriptor*>& field_descriptors,
    TypeFactory* type_factory, const Type** last_field_type) {
  if (field_descriptors.empty()) {
    // No proto fields have been extracted, therefore return the type of the
    // last struct field.
    ZETASQL_RET_CHECK(struct_path);
    *last_field_type = struct_path->back().second->type;
    return absl::OkStatus();
  }
  ZETASQL_RETURN_IF_ERROR(type_factory->GetProtoFieldType(field_descriptors.back(),
                                                  last_field_type));
  return absl::OkStatus();
}

absl::Status Resolver::FindFieldsFromPathExpression(
    absl::string_view function_name,
    const ASTGeneralizedPathExpression* generalized_path, const Type* root_type,
    std::vector<std::pair<int, const StructType::StructField*>>* struct_path,
    std::vector<const google::protobuf::FieldDescriptor*>* field_descriptors) {
  ZETASQL_RET_CHECK(generalized_path != nullptr);
  ZETASQL_RET_CHECK(root_type->IsStructOrProto());
  switch (generalized_path->node_kind()) {
    case AST_PATH_EXPRESSION: {
      auto path_expression = generalized_path->GetAsOrDie<ASTPathExpression>();
      if (root_type->IsProto()) {
        if (path_expression->parenthesized()) {
          ZETASQL_ASSIGN_OR_RETURN(
              const google::protobuf::FieldDescriptor* field_descriptor,
              FindExtensionFieldDescriptor(path_expression,
                                           root_type->AsProto()->descriptor()));
          field_descriptors->push_back(field_descriptor);
        } else {
          ZETASQL_RETURN_IF_ERROR(FindFieldDescriptors(
              path_expression->names(), root_type->AsProto()->descriptor(),
              field_descriptors));
        }
      } else {
        ZETASQL_RET_CHECK(struct_path);
        ZETASQL_RETURN_IF_ERROR(FindStructFieldPrefix(
            path_expression->names(), root_type->AsStruct(), struct_path));
        const Type* last_struct_field_type = struct_path->back().second->type;
        if (last_struct_field_type->IsProto() &&
            path_expression->num_names() != struct_path->size()) {
          // There are proto extractions in this path expression.
          ZETASQL_RETURN_IF_ERROR(FindFieldDescriptors(
              path_expression->names().last(path_expression->num_names() -
                                            struct_path->size()),
              last_struct_field_type->AsProto()->descriptor(),
              field_descriptors));
        }
      }
      break;
    }
    case AST_DOT_GENERALIZED_FIELD: {
      // This has to be a proto extension access since that is the only path
      // expression that gets parsed to an ASTDotGeneralizedField.
      auto dot_generalized_ast =
          generalized_path->GetAsOrDie<ASTDotGeneralizedField>();
      auto generalized_path_expr =
          dot_generalized_ast->expr()
              ->GetAsOrNull<ASTGeneralizedPathExpression>();
      ZETASQL_RETURN_IF_ERROR(FindFieldsFromPathExpression(
          function_name, generalized_path_expr, root_type, struct_path,
          field_descriptors));

      // The extension should be extracted from the last seen field in the path,
      // which must be of proto type.
      const Type* last_seen_type;
      ZETASQL_RETURN_IF_ERROR(GetLastSeenFieldTypeForReplaceFields(
          struct_path, *field_descriptors, type_factory_, &last_seen_type));
      if (!last_seen_type->IsProto()) {
        return MakeCannotAccessFieldError(
            dot_generalized_ast->path(),
            dot_generalized_ast->path()->ToIdentifierPathString(),
            last_seen_type->TypeName(product_mode()), /*is_extension=*/true);
      }
      ZETASQL_ASSIGN_OR_RETURN(const google::protobuf::FieldDescriptor* field_descriptor,
                       FindExtensionFieldDescriptor(
                           dot_generalized_ast->path(),
                           last_seen_type->AsProto()->descriptor()));
      field_descriptors->push_back(field_descriptor);
      break;
    }
    case AST_DOT_IDENTIFIER: {
      // This has to be a proto field access since path expressions that parse
      // to ASTDotIdentifier must have at least one parenthesized section of the
      // path somewhere on the left side.
      auto dot_identifier_ast =
          generalized_path->GetAsOrDie<ASTDotIdentifier>();
      auto generalized_path_expr =
          dot_identifier_ast->expr()
              ->GetAsOrNull<ASTGeneralizedPathExpression>();
      ZETASQL_RETURN_IF_ERROR(FindFieldsFromPathExpression(
          function_name, generalized_path_expr, root_type, struct_path,
          field_descriptors));

      // The field should be extracted from the last seen field in the path,
      // which must be of proto type.
      const Type* last_seen_type;
      ZETASQL_RETURN_IF_ERROR(GetLastSeenFieldTypeForReplaceFields(
          struct_path, *field_descriptors, type_factory_, &last_seen_type));
      if (!last_seen_type->IsProto()) {
        return MakeCannotAccessFieldError(
            dot_identifier_ast->name(),
            dot_identifier_ast->name()->GetAsString(),
            last_seen_type->TypeName(product_mode()), /*is_extension=*/false);
      }
      ZETASQL_RETURN_IF_ERROR(FindFieldDescriptors(
          {dot_identifier_ast->name()}, last_seen_type->AsProto()->descriptor(),
          field_descriptors));
      break;
    }
    case AST_ARRAY_ELEMENT: {
      return MakeSqlErrorAt(generalized_path) << absl::Substitute(
                 "Path expressions in $0() cannot index array fields",
                 function_name);
    }
    default: {
      ZETASQL_RET_CHECK_FAIL() << "Invalid generalized path expression input "
                       << generalized_path->DebugString();
    }
  }

  return absl::OkStatus();
}

// Builds the string representation of a field path using 'struct_path_prefix'
// and 'proto_field_path_suffix' and attempts to add it to 'field_path_trie'. If
// non-empty, the field path is expanded starting with the fields in
// 'struct_path_prefix'. Returns an error if this path string overlaps with a
// path that is already present in 'field_path_trie'. For example,
// message.nested and message.nested.field are overlapping field paths, but
// message.nested.field1 and message.nested.field2 are not overlapping. Two
// paths that modify the same OneOf field are also considered overlapping.
// 'oneof_path_to_full_path' maps from paths of OneOf fields that have already
// been modified to the corresponding path expression that accessed the OneOf
// path. If 'proto_field_path_suffix' modifies a OneOf field that has not
// already been modified, it will be added to 'oneof_path_to_full_path'.
static absl::Status AddToFieldPathTrie(
    const ASTNode* path_location,
    const std::vector<std::pair<int, const StructType::StructField*>>&
        struct_path_prefix,
    const std::vector<const google::protobuf::FieldDescriptor*>& proto_field_path_suffix,
    absl::flat_hash_map<std::string, std::string>* oneof_path_to_full_path,
    zetasql_base::GeneralTrie<const ASTNode*, nullptr>* field_path_trie) {
  std::string path_string;
  bool overlapping_oneof = false;
  std::string shortest_oneof_path;
  for (const std::pair<int, const StructType::StructField*>& struct_field :
       struct_path_prefix) {
    if (!path_string.empty()) {
      absl::StrAppend(&path_string, ".");
    }
    absl::StrAppend(&path_string, struct_field.second->name);
  }
  for (const google::protobuf::FieldDescriptor* field : proto_field_path_suffix) {
    if (field->containing_oneof() != nullptr && shortest_oneof_path.empty()) {
      shortest_oneof_path =
          absl::StrCat(path_string, field->containing_oneof()->name());
      if (zetasql_base::ContainsKey(*oneof_path_to_full_path, shortest_oneof_path)) {
        overlapping_oneof = true;
      }
    }
    if (!path_string.empty()) {
      absl::StrAppend(&path_string, ".");
    }
    if (field->is_extension()) {
      absl::StrAppend(&path_string, "(");
    }
    absl::StrAppend(&path_string,
                    field->is_extension() ? field->full_name() : field->name());
    if (field->is_extension()) {
      absl::StrAppend(&path_string, ")");
    }
  }
  if (overlapping_oneof) {
    // TODO: Allow modifying fields from the same Oneof. This should
    // be fine if only one of the new values is non-NULL.
    return MakeSqlErrorAt(path_location) << absl::StrCat(
               "Modifying multiple fields from the same OneOf is unsupported "
               "by REPLACE_FIELDS(). Field path ",
               path_string, " overlaps with field path ",
               zetasql_base::FindOrDie(*oneof_path_to_full_path, shortest_oneof_path));
  }
  if (!shortest_oneof_path.empty()) {
    ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(oneof_path_to_full_path,
                                      shortest_oneof_path, path_string));
  }

  // Determine if a prefix of 'path_string' is already present in the trie.
  int match_length = 0;
  const ASTNode* prefix_location =
      field_path_trie->GetDataForMaximalPrefix(path_string, &match_length,
                                               /*is_terminator=*/nullptr);
  std::vector<std::pair<std::string, const ASTNode*>> matching_paths;
  bool prefix_exists = false;
  if (prefix_location != nullptr) {
    // If the max prefix is equal to 'path_string' or if the next character of
    // 'path_string' after the max prefix is a "." then an overlapping path is
    // already present in the trie.
    prefix_exists =
        path_string.size() == match_length
            ? true
            : (std::strncmp(&path_string.at(match_length), ".", 1) == 0);
  } else {
    // Determine if 'path_string' is the prefix of a path already in the trie.
    field_path_trie->GetAllMatchingStrings(absl::StrCat(path_string, "."),
                                           &matching_paths);
  }
  if (prefix_exists || !matching_paths.empty()) {
    return MakeSqlErrorAt(path_location)
           << absl::StrCat("REPLACE_FIELDS() field path ",
                           prefix_exists ? path_string.substr(0, match_length)
                                         : matching_paths.at(0).first,
                           " overlaps with field path ", path_string);
  }
  field_path_trie->Insert(path_string, path_location);

  return absl::OkStatus();
}

absl::Status Resolver::FindStructFieldPrefix(
    absl::Span<const ASTIdentifier* const> path_vector,
    const StructType* root_struct,
    std::vector<std::pair<int, const StructType::StructField*>>* struct_path) {
  ZETASQL_RET_CHECK(root_struct != nullptr);
  const StructType* current_struct = root_struct;
  for (const ASTIdentifier* const current_field : path_vector) {
    if (current_struct == nullptr) {
      // There was an attempt to modify a field of a non-message type field.
      return MakeCannotAccessFieldError(
          current_field, current_field->GetAsString(),
          struct_path->back().second->type->ShortTypeName(product_mode()),
          /*is_extension=*/false);
    }
    bool is_ambiguous = false;
    int found_index;
    const StructType::StructField* field = current_struct->FindField(
        current_field->GetAsString(), &is_ambiguous, &found_index);
    if (field == nullptr) {
      if (is_ambiguous) {
        return MakeSqlErrorAt(current_field)
               << "Field name " << current_field->GetAsString()
               << " is ambiguous";
      }
      return MakeSqlErrorAt(current_field)
             << "Struct " << current_struct->ShortTypeName(product_mode())
             << " does not have field named " << current_field->GetAsString();
    }
    struct_path->emplace_back(found_index, field);
    if (field->type->IsProto()) {
      return absl::OkStatus();
    }
    current_struct = field->type->AsStruct();
  }

  return absl::OkStatus();
}

class SystemVariableConstant final : public Constant {
 public:
  SystemVariableConstant(const std::vector<std::string>& name_path,
                         const Type* type)
      : Constant(name_path), type_(type) {}

  const Type* type() const override { return type_; }
  std::string DebugString() const override { return FullName(); }

 private:
  const Type* const type_;
};

Catalog* Resolver::GetSystemVariablesCatalog() {
  if (system_variables_catalog_ != nullptr) {
    return system_variables_catalog_.get();
  }

  auto catalog = absl::make_unique<SimpleCatalog>("<system_variables>");
  for (const auto& entry : analyzer_options_.system_variables()) {
    std::vector<std::string> name_path = entry.first;
    const zetasql::Type* type = entry.second;

    // Traverse the name path, adding nested catalogs as necessary so that
    // the entry has a place to go.
    SimpleCatalog* target_catalog = catalog.get();
    for (size_t i = 0; i < name_path.size() - 1; ++i) {
      const std::string& path_elem = name_path[i];
      Catalog* nested_catalog = nullptr;
      ZETASQL_CHECK_OK(target_catalog->GetCatalog(path_elem, &nested_catalog));
      if (nested_catalog == nullptr) {
        auto new_catalog = absl::make_unique<SimpleCatalog>(path_elem);
        nested_catalog = new_catalog.get();
        target_catalog->AddOwnedCatalog(std::move(new_catalog));
      }
      target_catalog = static_cast<SimpleCatalog*>(nested_catalog);
    }

    target_catalog->AddOwnedConstant(
        name_path.back(),
        absl::make_unique<SystemVariableConstant>(name_path, type));
  }
  system_variables_catalog_ = std::move(catalog);
  return system_variables_catalog_.get();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveSystemVariableExpression(
    const ASTSystemVariableExpr* ast_system_variable_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  Catalog* system_variables_catalog = GetSystemVariablesCatalog();

  std::vector<std::string> path_parts =
      ast_system_variable_expr->path()->ToIdentifierVector();

  int num_names_consumed = 0;
  const Constant* constant = nullptr;
  absl::Status find_constant_with_path_prefix_status =
      system_variables_catalog->FindConstantWithPathPrefix(
          path_parts, &num_names_consumed, &constant,
          analyzer_options_.find_options());

  if (find_constant_with_path_prefix_status.code() ==
      absl::StatusCode::kNotFound) {
    return GetUnrecognizedNameError(ast_system_variable_expr->path(),
                                    expr_resolution_info != nullptr
                                        ? expr_resolution_info->name_scope
                                        : nullptr);
  }
  ZETASQL_RETURN_IF_ERROR(find_constant_with_path_prefix_status);

  // A constant was found.  Wrap it in an ResolvedSystemVariable node.
  std::vector<std::string> name_path(num_names_consumed);
  for (int i = 0; i < num_names_consumed; ++i) {
    name_path[i] = ast_system_variable_expr->path()->name(i)->GetAsString();
  }

  auto resolved_system_variable =
      MakeResolvedSystemVariable(constant->type(), name_path);
  MaybeRecordParseLocation(ast_system_variable_expr,
                           resolved_system_variable.get());
  *resolved_expr_out = std::move(resolved_system_variable);

  for (; num_names_consumed < ast_system_variable_expr->path()->num_names();
       ++num_names_consumed) {
    ZETASQL_RETURN_IF_ERROR(ResolveFieldAccess(
        std::move(*resolved_expr_out), ast_system_variable_expr->path(),
        ast_system_variable_expr->path()->name(num_names_consumed),
        &expr_resolution_info->flatten_state, resolved_expr_out));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveFilterFieldsFunctionCall(
    const ASTFunctionCall* ast_function,
    const std::vector<const ASTExpression*>& function_arguments,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  if (function_arguments.empty()) {
    return MakeSqlErrorAt(ast_function)
           << "FILTER_FIELDS() should have arguments";
  }
  std::unique_ptr<const ResolvedExpr> proto_to_modify;
  const ASTExpression* proto_ast = function_arguments.front();
  ZETASQL_RETURN_IF_ERROR(
      ResolveExpr(proto_ast, expr_resolution_info, &proto_to_modify));
  if (!proto_to_modify->type()->IsProto()) {
    return MakeSqlErrorAt(proto_ast)
           << "FILTER_FIELDS() expected an input proto type for first "
              "argument, but found type "
           << proto_to_modify->type()->ShortTypeName(product_mode());
  }

  const Type* field_type = proto_to_modify->type();
  std::vector<std::unique_ptr<const ResolvedFilterFieldArg>> filter_field_args;

  FilterFieldsPathValidator validator(field_type->AsProto()->descriptor());
  absl::optional<bool> reset_cleared_required_fields;
  constexpr absl::string_view kResetClearedRequiredFields =
      "RESET_CLEARED_REQUIRED_FIELDS";
  for (int i = 1; i < function_arguments.size(); ++i) {
    const ASTExpression* argument = function_arguments[i];
    if (argument->Is<ASTNamedArgument>()) {
      const ASTNamedArgument* named_argument =
          argument->GetAs<ASTNamedArgument>();
      const absl::string_view name = named_argument->name()->GetAsStringView();
      if (name != kResetClearedRequiredFields) {
        return MakeSqlErrorAt(argument) << absl::Substitute(
                   "Unsupported named argument in FILTER_FIELDS(): $0", name);
      }
      if (reset_cleared_required_fields.has_value()) {
        return MakeSqlErrorAt(argument) << "Duplicated named option";
      }
      const ASTExpression* expr = named_argument->expr();
      if (!expr->Is<ASTBooleanLiteral>()) {
        return MakeSqlErrorAt(argument) << absl::Substitute(
                   "FILTER_FIELDS()'s named argument only supports literal "
                   "bool, but got $0",
                   expr->DebugString());
      }
      reset_cleared_required_fields = expr->GetAs<ASTBooleanLiteral>()->value();
      continue;
    }
    if (!argument->Is<ASTUnaryExpression>() ||
        (argument->GetAs<ASTUnaryExpression>()->op() !=
             ASTUnaryExpression::PLUS &&
         argument->GetAs<ASTUnaryExpression>()->op() !=
             ASTUnaryExpression::MINUS)) {
      return MakeSqlErrorAt(argument) << "FILTER_FIELDS() expected each field "
                                         "path to start with \"+\" or \"-\"";
    }
    const ASTUnaryExpression* unary = argument->GetAs<ASTUnaryExpression>();
    const bool include = unary->op() == ASTUnaryExpression::PLUS;
    // Ignores error message from this function to give an appropriate error
    // message with more context.
    if (!ASTGeneralizedPathExpression::VerifyIsPureGeneralizedPathExpression(
             unary->operand())
             .ok()) {
      return MakeSqlErrorAt(unary->operand())
             << "FILTER_FIELDS() expected a field path after \"+\" or \"-\", "
                "but got "
             << unary->operand()->SingleNodeDebugString();
    }
    const ASTGeneralizedPathExpression* generalized_path_expression =
        unary->operand()->GetAs<ASTGeneralizedPathExpression>();

    std::vector<const google::protobuf::FieldDescriptor*> field_descriptor_path;

    ZETASQL_RETURN_IF_ERROR(FindFieldsFromPathExpression(
        "FILTER_FIELDS", generalized_path_expression, proto_to_modify->type(),
        /*struct_path=*/nullptr, &field_descriptor_path));
    if (absl::Status status =
            validator.ValidateFieldPath(include, field_descriptor_path);
        !status.ok()) {
      return MakeSqlErrorAt(unary) << status.message();
    }

    filter_field_args.push_back(
        MakeResolvedFilterFieldArg(include, field_descriptor_path));
  }
  // Validator also ensures that there is at least one field path.
  if (absl::Status status = validator.FinalValidation(
          reset_cleared_required_fields.value_or(false));
      !status.ok()) {
    return MakeSqlErrorAt(ast_function) << status.message();
  }

  *resolved_expr_out = MakeResolvedFilterField(
      field_type, std::move(proto_to_modify), std::move(filter_field_args),
      reset_cleared_required_fields.value_or(false));
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveReplaceFieldsExpression(
    const ASTReplaceFieldsExpression* ast_replace_fields,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_REPLACE_FIELDS)) {
    return MakeSqlErrorAt(ast_replace_fields)
           << "REPLACE_FIELDS() is not supported";
  }

  std::unique_ptr<const ResolvedExpr> expr_to_modify;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(ast_replace_fields->expr(), expr_resolution_info,
                              &expr_to_modify));
  if (!expr_to_modify->type()->IsStructOrProto()) {
    return MakeSqlErrorAt(ast_replace_fields->expr())
           << "REPLACE_FIELDS() expected an input type with fields for first "
              "argument, but found type "
           << expr_to_modify->type()->ShortTypeName(product_mode());
  }

  std::vector<std::unique_ptr<const ResolvedReplaceFieldItem>>
      resolved_modify_items;

  // This trie keeps track of the path expressions that are modified by
  // this REPLACE_FIELDS expression.
  zetasql_base::GeneralTrie<const ASTNode*, nullptr> field_path_trie;
  // This map keeps track of the OneOf fields that are modified. Modifying
  // multiple fields from the same OneOf is unsupported. This is not tracked in
  // 'field_path_trie' because field path expressions do not contain the OneOf
  // name of modified OneOf fields.
  absl::flat_hash_map<std::string, std::string> oneof_path_to_full_path;
  for (const ASTReplaceFieldsArg* replace_arg :
       ast_replace_fields->arguments()) {
    // Resolve the new value and get the field desciptors for the field to be
    // modified.
    std::unique_ptr<const ResolvedExpr> replaced_field_expr;
    ZETASQL_RETURN_IF_ERROR(ResolveExpr(replace_arg->expression(), expr_resolution_info,
                                &replaced_field_expr));

    std::vector<const google::protobuf::FieldDescriptor*> field_descriptor_path;
    std::vector<std::pair<int, const StructType::StructField*>> struct_path;
    ZETASQL_RETURN_IF_ERROR(FindFieldsFromPathExpression(
        "REPLACE_FIELDS", replace_arg->path_expression(),
        expr_to_modify->type(), &struct_path, &field_descriptor_path));
    ZETASQL_RETURN_IF_ERROR(AddToFieldPathTrie(
        replace_arg->path_expression(), struct_path, field_descriptor_path,
        &oneof_path_to_full_path, &field_path_trie));

    // Add a cast to the modified value if it needs to be coerced to the type
    // of the field.
    const Type* field_type;
    if (field_descriptor_path.empty()) {
      field_type = struct_path.back().second->type;
    } else {
      ZETASQL_RETURN_IF_ERROR(type_factory_->GetProtoFieldType(
          field_descriptor_path.back(), &field_type));
    }
    ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
        replace_arg->expression(), field_type, kImplicitAssignment,
        "Cannot replace field of type $0 with value of type $1",
        &replaced_field_expr));
    std::vector<int> struct_index_path;
    struct_index_path.reserve(struct_path.size());
    for (const std::pair<int, const StructType::StructField*> struct_field :
         struct_path) {
      struct_index_path.push_back(struct_field.first);
    }
    resolved_modify_items.push_back(
        MakeResolvedReplaceFieldItem(std::move(replaced_field_expr),
                                     struct_index_path, field_descriptor_path));
  }

  const Type* field_type = expr_to_modify->type();
  *resolved_expr_out = MakeResolvedReplaceField(
      field_type, std::move(expr_to_modify), std::move(resolved_modify_items));
  return absl::OkStatus();
}

// Return an error if <expr> is a ResolvedFunctionCall and is getting an un-CAST
// literal NULL as an argument.  <parser_op_sql> is used to make the error
// message.
static absl::Status ReturnErrorOnLiteralNullArg(
    const std::string& parser_op_sql,
    const std::vector<const ASTNode*>& arg_locations,
    const ResolvedExpr* expr) {
  const ResolvedFunctionCall* function_call;
  if (expr->node_kind() == RESOLVED_FUNCTION_CALL) {
    function_call = expr->GetAs<ResolvedFunctionCall>();
    ZETASQL_RET_CHECK_EQ(arg_locations.size(), function_call->argument_list().size());
    for (int i = 0; i < function_call->argument_list().size(); ++i) {
      if (arg_locations[i]->node_kind() == AST_NULL_LITERAL) {
        return MakeSqlErrorAt(arg_locations[i])
               << "Operands of " << parser_op_sql << " cannot be literal NULL";
      }
    }
  }
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveUnaryExpr(
    const ASTUnaryExpression* unary_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  const std::string& function_name =
      FunctionResolver::UnaryOperatorToFunctionName(unary_expr->op());

  if (unary_expr->op() == ASTUnaryExpression::MINUS &&
      !unary_expr->child(0)->GetAsOrDie<ASTExpression>()->parenthesized()) {
    if (unary_expr->child(0)->node_kind() == AST_INT_LITERAL) {
      // Try to parse this as a negative int64_t.
      const ASTIntLiteral* literal =
          unary_expr->operand()->GetAsOrDie<ASTIntLiteral>();
      int64_t int64_value;
      if (literal->is_hex()) {
        if (zetasql_base::safe_strto64_base(absl::StrCat("-", literal->image()),
                                       &int64_value, 16)) {
          *resolved_expr_out =
              MakeResolvedLiteral(unary_expr, Value::Int64(int64_value));
          return absl::OkStatus();
        } else {
          return MakeSqlErrorAt(unary_expr)
                 << "Invalid hex integer literal: -" << literal->image();
        }
      }
      if (functions::StringToNumeric(absl::StrCat("-", literal->image()),
                                     &int64_value, nullptr)) {
        *resolved_expr_out =
            MakeResolvedLiteral(unary_expr, Value::Int64(int64_value));
        return absl::OkStatus();
      } else {
        return MakeSqlErrorAt(unary_expr)
               << "Invalid integer literal: -" << literal->image();
      }
    } else if (unary_expr->child(0)->node_kind() == AST_FLOAT_LITERAL) {
      // Try to parse this as a negative double.
      const ASTFloatLiteral* literal =
          unary_expr->operand()->GetAsOrDie<ASTFloatLiteral>();
      std::string negative_image = absl::StrCat("-", literal->image());
      double double_value;
      if (functions::StringToNumeric(negative_image, &double_value, nullptr)) {
        std::unique_ptr<const ResolvedLiteral> resolved_literal =
            MakeResolvedFloatLiteral(
                unary_expr, types::DoubleType(), Value::Double(double_value),
                /*has_explicit_type=*/false, negative_image);
        *resolved_expr_out = std::move(resolved_literal);
        return absl::OkStatus();
      } else {
        return MakeSqlErrorAt(unary_expr)
               << "Invalid floating point literal: -" << literal->image();
      }
    }
  } else if (unary_expr->op() == ASTUnaryExpression::PLUS) {
    // Unary plus on NULL is an error.
    if (unary_expr->operand()->node_kind() == AST_NULL_LITERAL) {
      return MakeSqlErrorAt(unary_expr->operand())
             << "Operands of " << unary_expr->GetSQLForOperator()
             << " cannot be literal NULL";
    }

    ZETASQL_RETURN_IF_ERROR(ResolveExpr(unary_expr->operand(), expr_resolution_info,
                                resolved_expr_out));

    // Unary plus on non-numeric type is an error.
    if (!(*resolved_expr_out)->type()->IsNumerical()) {
      return MakeSqlErrorAt(unary_expr->operand())
             << "Operands of " << unary_expr->GetSQLForOperator()
             << " must be numeric type but was "
             << (*resolved_expr_out)->type()->ShortTypeName(product_mode());
    }
    return absl::OkStatus();
  } else if (unary_expr->op() == ASTUnaryExpression::IS_UNKNOWN ||
             unary_expr->op() == ASTUnaryExpression::IS_NOT_UNKNOWN) {
    // IS [NOT] UNKNOWN reuses the "is_null" function, and skips the null
    // literal validation. IS NOT UNKNOWN also handles NOT in subsequent step.
    std::unique_ptr<const ResolvedExpr> resolved_operand;
    ZETASQL_RETURN_IF_ERROR(ResolveExpr(unary_expr->operand(), expr_resolution_info,
                                &resolved_operand));

    absl::string_view coerce_msg =
        unary_expr->op() == ASTUnaryExpression::IS_UNKNOWN
            ? "Operand of IS UNKNOWN must be coercible to $0, but has type $1"
            : "Operand of IS NOT UNKNOWN must be coercible to $0, but has type "
              "$1";

    ZETASQL_RETURN_IF_ERROR(
        CoerceExprToType(unary_expr->operand(), type_factory_->get_bool(),
                         kImplicitCoercion, coerce_msg, &resolved_operand));

    std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;
    resolved_arguments.push_back(std::move(resolved_operand));

    // Explicitly casting or converting resolved literal to target type.
    // It prevents ResolveFunctionCallWithResolvedArguments from undoing
    // the coercion to boolean.
    ZETASQL_RETURN_IF_ERROR(
        UpdateLiteralsToExplicit({unary_expr->operand()}, &resolved_arguments));
    ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallWithResolvedArguments(
        unary_expr, {unary_expr->operand()}, "$is_null",
        std::move(resolved_arguments), /*named_arguments=*/{},
        expr_resolution_info, resolved_expr_out));

    if (unary_expr->op() == ASTUnaryExpression::IS_NOT_UNKNOWN) {
      return MakeNotExpr(unary_expr, std::move(*resolved_expr_out),
                         expr_resolution_info, resolved_expr_out);
    }
    return absl::OkStatus();
  }

  ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
      unary_expr, function_name, {unary_expr->operand()},
      *kEmptyArgumentOptionMap, expr_resolution_info, resolved_expr_out));

  ZETASQL_RETURN_IF_ERROR(ReturnErrorOnLiteralNullArg(unary_expr->GetSQLForOperator(),
                                              {unary_expr->operand()},
                                              resolved_expr_out->get()));
  return absl::OkStatus();
}

static const std::string* const kInvalidOperatorTypeStr =
    new std::string("$invalid_is_operator_type");
static const std::string* const kIsFalseFnName = new std::string("$is_false");
static const std::string* const kIsNullFnName = new std::string("$is_null");
static const std::string* const kIsTrueFnName = new std::string("$is_true");

static const std::string& IsOperatorToFunctionName(const ASTExpression* expr) {
  switch (expr->node_kind()) {
    case AST_NULL_LITERAL:
      return *kIsNullFnName;
    case AST_BOOLEAN_LITERAL:
      if (expr->GetAsOrDie<ASTBooleanLiteral>()->value()) {
        return *kIsTrueFnName;
      } else {
        return *kIsFalseFnName;
      }
    default:
      break;
  }

  return *kInvalidOperatorTypeStr;
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveBinaryExpr(
    const ASTBinaryExpression* binary_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  std::unique_ptr<const ResolvedExpr> resolved_binary_expr;
  // Special case to handle IS operator. Based on the rhs_ resolved type (i.e.
  // true/false/NULL), the general function name is resolved to
  // $is_true/$is_false/$is_null respectively with lhs_ as an argument.
  bool not_handled = false;
  if (binary_expr->op() == ASTBinaryExpression::IS) {
    const std::string& function_name =
        IsOperatorToFunctionName(binary_expr->rhs());
    ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
        binary_expr, function_name, {binary_expr->lhs()},
        *kEmptyArgumentOptionMap, expr_resolution_info, &resolved_binary_expr));
  } else {
    const std::string& function_name =
        FunctionResolver::BinaryOperatorToFunctionName(
            binary_expr->op(), binary_expr->is_not(), &not_handled);
    ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
        binary_expr, function_name, {binary_expr->lhs(), binary_expr->rhs()},
        *kEmptyArgumentOptionMap, expr_resolution_info, &resolved_binary_expr));

    // Give an error on literal NULL arguments to any binary expression
    // except IS or IS DISTINCT FROM
    if (binary_expr->op() != ASTBinaryExpression::DISTINCT) {
      ZETASQL_RETURN_IF_ERROR(
          ReturnErrorOnLiteralNullArg(binary_expr->GetSQLForOperator(),
                                      {binary_expr->lhs(), binary_expr->rhs()},
                                      resolved_binary_expr.get()));
    }
  }

  if (binary_expr->is_not() && !not_handled) {
    return MakeNotExpr(binary_expr, std::move(resolved_binary_expr),
                       expr_resolution_info, resolved_expr_out);
  }

  *resolved_expr_out = std::move(resolved_binary_expr);
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveBitwiseShiftExpr(
    const ASTBitwiseShiftExpression* bitwise_shift_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  const std::string& function_name = bitwise_shift_expr->is_left_shift()
                                         ? "$bitwise_left_shift"
                                         : "$bitwise_right_shift";
  ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
      bitwise_shift_expr, function_name,
      {bitwise_shift_expr->lhs(), bitwise_shift_expr->rhs()},
      *kEmptyArgumentOptionMap, expr_resolution_info, resolved_expr_out));
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveInExpr(
    const ASTInExpression* in_expr, ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  std::unique_ptr<const ResolvedExpr> resolved_in_expr;
  if (in_expr->query() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveInSubquery(in_expr, expr_resolution_info, &resolved_in_expr));
  } else if (in_expr->in_list() != nullptr) {
    std::vector<const ASTExpression*> in_arguments;
    in_arguments.reserve(1 + in_expr->in_list()->list().size());
    in_arguments.push_back(in_expr->lhs());
    for (const ASTExpression* expr : in_expr->in_list()->list()) {
      in_arguments.push_back(expr);
    }
    ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
        in_expr, "$in", in_arguments, *kEmptyArgumentOptionMap,
        expr_resolution_info, &resolved_in_expr));
  } else {
    const ASTUnnestExpression* unnest_expr = in_expr->unnest_expr();
    ZETASQL_RET_CHECK(unnest_expr != nullptr);

    std::vector<std::unique_ptr<const ResolvedExpr>> args;
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpressionArgument(in_expr->lhs(), expr_resolution_info, &args));

    {
      // The unnest expression is allowed to flatten.
      FlattenState::Restorer restorer;
      expr_resolution_info->flatten_state.set_can_flatten(true, &restorer);
      ZETASQL_RETURN_IF_ERROR(ResolveExpressionArgument(
          in_expr->unnest_expr()->expression(), expr_resolution_info, &args));
    }

    ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallWithResolvedArguments(
        in_expr, {in_expr->lhs(), in_expr->unnest_expr()->expression()},
        "$in_array", std::move(args), /*named_arguments=*/{},
        expr_resolution_info, &resolved_in_expr));
  }

  if (in_expr->is_not()) {
    return MakeNotExpr(in_expr, std::move(resolved_in_expr),
                       expr_resolution_info, resolved_expr_out);
  }
  *resolved_expr_out = std::move(resolved_in_expr);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveInSubquery(
    const ASTInExpression* in_subquery_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::unique_ptr<const ResolvedExpr> resolved_in_expr;

  const ASTExpression* in_expr = in_subquery_expr->lhs();
  const ASTQuery* in_subquery = in_subquery_expr->query();

  ZETASQL_DCHECK(in_expr != nullptr);
  ZETASQL_RETURN_IF_ERROR(
      ResolveExpr(in_expr, expr_resolution_info, &resolved_in_expr));

  CorrelatedColumnsSet correlated_columns_set;
  std::unique_ptr<const NameScope> subquery_scope(
      new NameScope(expr_resolution_info->name_scope, &correlated_columns_set));
  std::unique_ptr<const ResolvedScan> resolved_in_subquery;
  std::shared_ptr<const NameList> resolved_name_list;
  ZETASQL_DCHECK(in_subquery != nullptr);
  ZETASQL_RETURN_IF_ERROR(ResolveQuery(
      in_subquery, subquery_scope.get(), kExprSubqueryId,
      /*is_outer_query=*/false, &resolved_in_subquery, &resolved_name_list));
  if (resolved_name_list->num_columns() > 1) {
    return MakeSqlErrorAt(in_subquery)
           << "Subquery of type IN must have only one output column";
  }

  // Order preservation is not relevant for IN subqueries.
  const_cast<ResolvedScan*>(resolved_in_subquery.get())->set_is_ordered(false);

  const Type* in_expr_type = resolved_in_expr->type();
  const Type* in_subquery_type = resolved_name_list->column(0).column.type();

  // TODO: Non-equivalent STRUCTs should still be comparable
  // as long as their related field types are comparable.  Add support for
  // this.
  if (!in_expr_type->SupportsEquality(language()) ||
      !in_subquery_type->SupportsEquality(language()) ||
      (!in_expr_type->Equivalent(in_subquery_type) &&
       (!in_expr_type->IsNumerical() || !in_subquery_type->IsNumerical()))) {
    return MakeSqlErrorAt(in_expr)
           << "Cannot execute IN subquery with uncomparable types "
           << in_expr_type->ShortTypeName(product_mode()) << " and "
           << in_subquery_type->ShortTypeName(product_mode());
  }

  // The check above ensures that the two types support equality and they
  // can be compared.  If the types are Equals then they can be compared
  // directly without any coercion.  Otherwise we try to find a common
  // supertype between the two types and coerce the two expressions to the
  // common supertype before the comparison.
  //
  // TODO:  For non-Equals but Equivalent STRUCTs we are always
  // adding coercion to a common supertype.  However, sometimes the
  // supertyping is unnecessary.  In particular, if the only difference
  // between the STRUCT types is the field names, and all the field types
  // are Equals, then we do not need to do the coercion.  Optimize this.
  if (!in_expr_type->Equals(in_subquery_type)) {
    // We must find the common supertype and add cast(s) where necessary.
    // This check is basically an Equals() test, and is not an equivalence
    // test since if we have two equivalent but different field types
    // (such as two enums with the same name) we must coerce one to the other.
    InputArgumentTypeSet type_set;
    type_set.Insert(GetInputArgumentTypeForExpr(resolved_in_expr.get()));
    // The output column from the subquery column is non-literal, non-parameter.
    type_set.Insert(InputArgumentType(in_subquery_type));
    const Type* supertype = nullptr;
    ZETASQL_RETURN_IF_ERROR(coercer_.GetCommonSuperType(type_set, &supertype));
    const Type* in_expr_cast_type = nullptr;
    const Type* in_subquery_cast_type = nullptr;
    if (supertype != nullptr) {
      // We use Equals(), not Equivalent(), because we want to add the cast
      // in that case.
      if (!in_expr_type->Equals(supertype)) {
        in_expr_cast_type = supertype;
      }
      if (!in_subquery_type->Equals(supertype)) {
        in_subquery_cast_type = supertype;
      }
    } else if ((in_expr_type->IsUint64() &&
                in_subquery_type->IsSignedInteger()) ||
               (in_expr_type->IsSignedInteger() &&
                in_subquery_type->IsUint64())) {
      // We need to handle the case where one operand is signed integer and
      // the other is UINT64.
      //
      // The argument types coming out of here should match the '$equals'
      // function signatures, so they must either both be the same type
      // or one must be INT64 and the other must be UINT64.  We know here
      // that one argument is UINT64, so the other is INT64 or INT32.
      // If the signed integer is INT64 then we do not need to do anything.
      // If the signed integer is INT32 then we need to coerce it to INT64.
      if (in_expr_type->IsInt32()) {
        in_expr_cast_type = type_factory_->get_int64();
      }
      if (in_subquery_type->IsInt32()) {
        in_subquery_cast_type = type_factory_->get_int64();
      }
    } else {
      // We did not find a common supertype for the two types, so they
      // are not comparable.
      return MakeSqlErrorAt(in_expr)
             << "Cannot execute IN subquery with uncomparable types "
             << in_expr_type->DebugString() << " and "
             << in_subquery_type->DebugString();
    }
    if (in_expr_cast_type != nullptr) {
      // Add a cast to <in_expr>.
      ZETASQL_RETURN_IF_ERROR(CoerceExprToType(in_expr, in_expr_cast_type,
                                       kExplicitCoercion, &resolved_in_expr));
    }
    if (in_subquery_cast_type != nullptr) {
      // Add a project on top of the subquery scan that casts its (only)
      // column to the supertype.
      ResolvedColumnList target_columns;
      ZETASQL_RET_CHECK_EQ(1, resolved_name_list->num_columns());
      target_columns.push_back(
          ResolvedColumn(AllocateColumnId(), kInSubqueryCastId,
                         resolved_name_list->column(0).column.name_id(),
                         in_subquery_cast_type));

      ResolvedColumnList current_columns =
          resolved_name_list->GetResolvedColumns();

      ZETASQL_RETURN_IF_ERROR(CreateWrapperScanWithCasts(
          in_subquery, target_columns, kInSubqueryCastId, &resolved_in_subquery,
          &current_columns));
    }
  }

  std::vector<std::unique_ptr<const ResolvedColumnRef>> parameters;
  FetchCorrelatedSubqueryParameters(correlated_columns_set, &parameters);
  std::unique_ptr<ResolvedSubqueryExpr> resolved_expr =
      MakeResolvedSubqueryExpr(type_factory_->get_bool() /* output type */,
                               ResolvedSubqueryExpr::IN, std::move(parameters),
                               std::move(resolved_in_expr),
                               std::move(resolved_in_subquery));
  ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(
      /*error_node=*/in_subquery_expr->query(), resolved_expr.get()));
  MaybeRecordParseLocation(in_subquery_expr->query(), resolved_expr.get());
  ZETASQL_RETURN_IF_ERROR(
      ResolveHintsForNode(in_subquery_expr->hint(), resolved_expr.get()));
  ZETASQL_RETURN_IF_ERROR(MaybeResolveCollationForSubqueryExpr(
      /*error_location=*/in_subquery_expr->query(), resolved_expr.get()));
  *resolved_expr_out = std::move(resolved_expr);
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveLikeExpr(
    const ASTLikeExpression* like_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  // Regular LIKE expressions (ex. X LIKE Y) are parsed as an
  // ASTBinaryExpression, not an ASTLikeExpression.
  ZETASQL_RET_CHECK(language().LanguageFeatureEnabled(FEATURE_V_1_3_LIKE_ANY_SOME_ALL));
  std::unique_ptr<const ResolvedExpr> resolved_like_expr;
  if (like_expr->query() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveLikeExprSubquery(like_expr, expr_resolution_info,
                                            &resolved_like_expr));
  } else if (like_expr->in_list() != nullptr) {
    std::vector<const ASTExpression*> like_arguments;
    like_arguments.reserve(1 + like_expr->in_list()->list().size());
    like_arguments.push_back(like_expr->lhs());
    for (const ASTExpression* expr : like_expr->in_list()->list()) {
      like_arguments.push_back(expr);
    }
    std::string function_type = "";
    switch (like_expr->op()->op()) {
      case ASTAnySomeAllOp::kAny:
      case ASTAnySomeAllOp::kSome:
        function_type = "$like_any";
        break;
      case ASTAnySomeAllOp::kAll:
        function_type = "$like_all";
        break;
      default:
        return MakeSqlErrorAt(like_expr)
               << "Internal: Unsupported LIKE expression operation. Operation "
                  "must be of type ANY|SOME|ALL.";
        break;
    }
    ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
        like_expr, function_type, like_arguments, *kEmptyArgumentOptionMap,
        expr_resolution_info, &resolved_like_expr));
  } else if (like_expr->unnest_expr() != nullptr) {
    FlattenState::Restorer restorer;
    expr_resolution_info->flatten_state.set_can_flatten(true, &restorer);
    std::string function_type = "";
    switch (like_expr->op()->op()) {
      case ASTAnySomeAllOp::kAny:
      case ASTAnySomeAllOp::kSome:
        function_type = "$like_any_array";
        break;
      case ASTAnySomeAllOp::kAll:
        function_type = "$like_all_array";
        break;
      default:
        return MakeSqlErrorAt(like_expr)
               << "Internal: Unsupported LIKE expression operation. Operation "
                  "must be of type ANY|SOME|ALL.";
        break;
    }
    ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
        like_expr, function_type,
        {like_expr->lhs(), like_expr->unnest_expr()->expression()},
        *kEmptyArgumentOptionMap, expr_resolution_info, &resolved_like_expr));
  } else {
    return MakeSqlErrorAt(like_expr)
           << "Internal: Unsupported LIKE expression.";
  }

  if (like_expr->is_not()) {
    return MakeNotExpr(like_expr, std::move(resolved_like_expr),
                       expr_resolution_info, resolved_expr_out);
  }
  *resolved_expr_out = std::move(resolved_like_expr);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveLikeExprSubquery(
    const ASTLikeExpression* like_subquery_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::unique_ptr<const ResolvedExpr> resolved_like_expr;

  const ASTExpression* like_lhs = like_subquery_expr->lhs();
  const ASTQuery* like_subquery = like_subquery_expr->query();
  ZETASQL_RETURN_IF_ERROR(
      ResolveExpr(like_lhs, expr_resolution_info, &resolved_like_expr));

  CorrelatedColumnsSet correlated_columns_set;
  auto subquery_scope = absl::make_unique<const NameScope>(
      expr_resolution_info->name_scope, &correlated_columns_set);
  std::unique_ptr<const ResolvedScan> resolved_like_subquery;
  std::shared_ptr<const NameList> resolved_name_list;
  ZETASQL_RET_CHECK_NE(like_subquery, nullptr);
  ZETASQL_RETURN_IF_ERROR(ResolveQuery(
      like_subquery, subquery_scope.get(), kExprSubqueryId,
      /*is_outer_query=*/false, &resolved_like_subquery, &resolved_name_list));
  ZETASQL_RET_CHECK(resolved_name_list->num_columns() != 0);
  if (resolved_name_list->num_columns() > 1) {
    return MakeSqlErrorAt(like_subquery)
           << "Subquery of a LIKE expression must have only one output column";
  }
  // Order preservation is not relevant for LIKE subqueries.
  const_cast<ResolvedScan*>(resolved_like_subquery.get())
      ->set_is_ordered(false);

  const Type* like_expr_type = resolved_like_expr->type();
  const Type* like_subquery_type = resolved_name_list->column(0).column.type();
  if (!like_expr_type->Equivalent(like_subquery_type) ||
      (!like_expr_type->IsString() && !like_expr_type->IsBytes())) {
    return MakeSqlErrorAt(like_lhs)
           << "Cannot execute a LIKE expression subquery with types "
           << like_expr_type->ShortTypeName(product_mode()) << " and "
           << like_subquery_type->ShortTypeName(product_mode());
  }

  ResolvedSubqueryExpr::SubqueryType subquery_type;
  switch (like_subquery_expr->op()->op()) {
    case ASTAnySomeAllOp::kAny:
    case ASTAnySomeAllOp::kSome:
      subquery_type = ResolvedSubqueryExpr::LIKE_ANY;
      break;
    case ASTAnySomeAllOp::kAll:
      subquery_type = ResolvedSubqueryExpr::LIKE_ALL;
      break;
    default:
      return MakeSqlErrorAt(like_subquery_expr)
             << "Internal: Unsupported LIKE expression operation. Operation "
                "must be of type ANY|SOME|ALL.";
      break;
  }

  std::vector<std::unique_ptr<const ResolvedColumnRef>> parameters;
  FetchCorrelatedSubqueryParameters(correlated_columns_set, &parameters);
  std::unique_ptr<ResolvedSubqueryExpr> resolved_expr =
      MakeResolvedSubqueryExpr(type_factory_->get_bool() /* output type */,
                               subquery_type, std::move(parameters),
                               std::move(resolved_like_expr),
                               std::move(resolved_like_subquery));
  ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(
      /*error_node=*/like_subquery_expr->query(), resolved_expr.get()));
  ZETASQL_RETURN_IF_ERROR(
      ResolveHintsForNode(like_subquery_expr->hint(), resolved_expr.get()));
  *resolved_expr_out = std::move(resolved_expr);
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveBetweenExpr(
    const ASTBetweenExpression* between_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  std::vector<const ASTExpression*> between_arguments;
  between_arguments.push_back(between_expr->lhs());
  between_arguments.push_back(between_expr->low());
  between_arguments.push_back(between_expr->high());
  std::unique_ptr<const ResolvedExpr> resolved_between_expr;
  ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallWithLiteralRetry(
      between_expr, "$between", between_arguments, *kEmptyArgumentOptionMap,
      expr_resolution_info, &resolved_between_expr));
  if (between_expr->is_not()) {
    return MakeNotExpr(between_expr, std::move(resolved_between_expr),
                       expr_resolution_info, resolved_expr_out);
  }

  *resolved_expr_out = std::move(resolved_between_expr);
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveAndExpr(
    const ASTAndExpr* and_expr, ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  return ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
      and_expr, "$and", and_expr->conjuncts(), *kEmptyArgumentOptionMap,
      expr_resolution_info, resolved_expr_out);
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveOrExpr(
    const ASTOrExpr* or_expr, ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  return ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
      or_expr, "$or", or_expr->disjuncts(), *kEmptyArgumentOptionMap,
      expr_resolution_info, resolved_expr_out);
}

void Resolver::FetchCorrelatedSubqueryParameters(
    const CorrelatedColumnsSet& correlated_columns_set,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>* parameters) {
  for (const auto& item : correlated_columns_set) {
    const ResolvedColumn& column = item.first;
    const bool is_already_correlated = item.second;
    parameters->push_back(MakeColumnRef(column, is_already_correlated));
  }
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveExprSubquery(
    const ASTExpressionSubquery* expr_subquery,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  if (generated_column_cycle_detector_ != nullptr) {
    return MakeSqlErrorAt(expr_subquery)
           << "Generated column expression must not include a subquery";
  }
  if (analyzing_check_constraint_expression_) {
    return MakeSqlErrorAt(expr_subquery)
           << "CHECK constraint expression must not include a subquery";
  }
  if (default_expr_access_error_name_scope_.has_value()) {
    return MakeSqlErrorAt(expr_subquery)
           << "A column default expression must not include a subquery";
  }
  std::unique_ptr<CorrelatedColumnsSet> correlated_columns_set(
      new CorrelatedColumnsSet);
  // TODO: If the subquery appears in the HAVING, then we probably
  // need to allow the select list aliases to resolve also.  Test subqueries
  // in the HAVING.
  std::unique_ptr<const NameScope> subquery_scope(new NameScope(
      expr_resolution_info->name_scope, correlated_columns_set.get()));

  std::unique_ptr<const ResolvedScan> resolved_query;
  std::shared_ptr<const NameList> resolved_name_list;
  ZETASQL_RETURN_IF_ERROR(ResolveQuery(
      expr_subquery->query(), subquery_scope.get(), kExprSubqueryId,
      /*is_outer_query=*/false, &resolved_query, &resolved_name_list));

  ResolvedSubqueryExpr::SubqueryType subquery_type;
  switch (expr_subquery->modifier()) {
    case ASTExpressionSubquery::ARRAY:
      subquery_type = ResolvedSubqueryExpr::ARRAY;
      break;
    case ASTExpressionSubquery::NONE:
      subquery_type = ResolvedSubqueryExpr::SCALAR;
      break;
    case ASTExpressionSubquery::EXISTS:
      subquery_type = ResolvedSubqueryExpr::EXISTS;
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Invalid subquery modifier: "
                       << expr_subquery->modifier();
  }

  if (subquery_type != ResolvedSubqueryExpr::ARRAY) {
    // Order preservation is not relevant for non-ARRAY expression subqueries.
    const_cast<ResolvedScan*>(resolved_query.get())->set_is_ordered(false);
  }

  const Type* output_type;
  if (subquery_type == ResolvedSubqueryExpr::EXISTS) {
    output_type = type_factory_->get_bool();
  } else if (resolved_name_list->num_columns() == 1) {
    output_type = resolved_name_list->column(0).column.type();
  } else {
    // The subquery has more than one column, which is not allowed without
    // SELECT AS STRUCT.
    ZETASQL_RET_CHECK_GE(resolved_name_list->num_columns(), 1);
    return MakeSqlErrorAt(expr_subquery)
           << (subquery_type == ResolvedSubqueryExpr::ARRAY ? "ARRAY"
                                                            : "Scalar")
           << " subquery cannot have more than one column unless using"
              " SELECT AS STRUCT to build STRUCT values";
  }

  if (subquery_type == ResolvedSubqueryExpr::ARRAY) {
    if (output_type->IsArray()) {
      return MakeSqlErrorAt(expr_subquery)
             << "Cannot use array subquery with column of type "
             << output_type->ShortTypeName(product_mode())
             << " because nested arrays are not supported";
    }
    ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(output_type, &output_type));
  }

  std::vector<std::unique_ptr<const ResolvedColumnRef>> parameters;
  FetchCorrelatedSubqueryParameters(*correlated_columns_set, &parameters);
  std::unique_ptr<ResolvedSubqueryExpr> resolved_expr =
      MakeResolvedSubqueryExpr(output_type, subquery_type,
                               std::move(parameters), /*in_expr=*/nullptr,
                               std::move(resolved_query));
  ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(
      /*error_node=*/expr_subquery, resolved_expr.get()));
  MaybeRecordExpressionSubqueryParseLocation(expr_subquery,
                                             resolved_expr.get());
  ZETASQL_RETURN_IF_ERROR(
      ResolveHintsForNode(expr_subquery->hint(), resolved_expr.get()));
  *resolved_expr_out = std::move(resolved_expr);
  return absl::OkStatus();
}

absl::Status Resolver::MakeDatePartEnumResolvedLiteral(
    functions::DateTimestampPart date_part,
    std::unique_ptr<const ResolvedExpr>* resolved_date_part) {
  const EnumType* date_part_type;
  const google::protobuf::EnumDescriptor* date_part_descr =
      functions::DateTimestampPart_descriptor();
  ZETASQL_RET_CHECK(type_factory_->MakeEnumType(date_part_descr, &date_part_type).ok());
  // Parameter substitution cannot be used for dateparts; don't record parse
  // location.
  *resolved_date_part = MakeResolvedLiteralWithoutLocation(
      Value::Enum(date_part_type, date_part));
  return absl::OkStatus();
}

absl::Status Resolver::MakeDatePartEnumResolvedLiteralFromNames(
    IdString date_part_name, IdString date_part_arg_name,
    const ASTExpression* date_part_ast_location,
    const ASTExpression* date_part_arg_ast_location,
    std::unique_ptr<const ResolvedExpr>* resolved_date_part,
    functions::DateTimestampPart* date_part) {
  ZETASQL_RET_CHECK_EQ(date_part_arg_name.empty(),
               date_part_arg_ast_location == nullptr);
  using functions::DateTimestampPart;

  DateTimestampPart local_date_part;
  if (!functions::DateTimestampPart_Parse(
          absl::AsciiStrToUpper(date_part_name.ToStringView()),
          &local_date_part)) {
    return MakeSqlErrorAt(date_part_ast_location)
           << "A valid date part name is required but found " << date_part_name;
  }
  // Users must use WEEK(<WEEKDAY>) instead of WEEK_<WEEKDAY>.
  switch (local_date_part) {
    case functions::WEEK_MONDAY:
    case functions::WEEK_TUESDAY:
    case functions::WEEK_WEDNESDAY:
    case functions::WEEK_THURSDAY:
    case functions::WEEK_FRIDAY:
    case functions::WEEK_SATURDAY:
      return MakeSqlErrorAt(date_part_ast_location)
             << "A valid date part name is required but found "
             << date_part_name;
    default:
      break;
  }

  if (!date_part_arg_name.empty()) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_2_WEEK_WITH_WEEKDAY)) {
      return MakeSqlErrorAt(date_part_arg_ast_location)
             << "Date part arguments are not supported";
    }

    if (local_date_part != functions::WEEK) {
      return MakeSqlErrorAt(date_part_arg_ast_location)
             << "Date part arguments are not supported for "
             << functions::DateTimestampPart_Name(local_date_part)
             << ", but found " << date_part_arg_name;
    }

    // WEEK(SUNDAY) is the same as WEEK.
    static const auto* arg_name_to_date_part =
        new IdStringHashMapCase<DateTimestampPart>(
            {{IdString::MakeGlobal("SUNDAY"), DateTimestampPart::WEEK},
             {IdString::MakeGlobal("MONDAY"), DateTimestampPart::WEEK_MONDAY},
             {IdString::MakeGlobal("TUESDAY"), DateTimestampPart::WEEK_TUESDAY},
             {IdString::MakeGlobal("WEDNESDAY"),
              DateTimestampPart::WEEK_WEDNESDAY},
             {IdString::MakeGlobal("THURSDAY"),
              DateTimestampPart::WEEK_THURSDAY},
             {IdString::MakeGlobal("FRIDAY"), DateTimestampPart::WEEK_FRIDAY},
             {IdString::MakeGlobal("SATURDAY"),
              DateTimestampPart::WEEK_SATURDAY}});

    const DateTimestampPart* final_date_part =
        zetasql_base::FindOrNull(*arg_name_to_date_part, date_part_arg_name);
    if (final_date_part == nullptr) {
      return MakeSqlErrorAt(date_part_arg_ast_location)
             << "A valid date part argument for "
             << functions::DateTimestampPart_Name(local_date_part)
             << " is required, but found " << date_part_arg_name;
    }
    local_date_part = *final_date_part;
  }

  if (date_part != nullptr) {
    *date_part = local_date_part;
  }

  return MakeDatePartEnumResolvedLiteral(local_date_part, resolved_date_part);
}

absl::Status Resolver::ResolveDatePartArgument(
    const ASTExpression* date_part_ast_location,
    std::unique_ptr<const ResolvedExpr>* resolved_date_part,
    functions::DateTimestampPart* date_part) {
  IdString date_part_name;
  IdString date_part_arg_name;
  const ASTExpression* date_part_arg_ast_location = nullptr;
  switch (date_part_ast_location->node_kind()) {
    case AST_IDENTIFIER:
      date_part_name =
          date_part_ast_location->GetAsOrDie<ASTIdentifier>()->GetAsIdString();
      break;
    case AST_PATH_EXPRESSION: {
      const auto ast_path =
          date_part_ast_location->GetAsOrDie<ASTPathExpression>();
      if (ast_path->num_names() != 1) {
        return MakeSqlErrorAt(ast_path)
               << "A valid date part name is required but found "
               << ast_path->ToIdentifierPathString();
      }
      date_part_name = ast_path->first_name()->GetAsIdString();
      break;
    }
    case AST_FUNCTION_CALL: {
      const ASTFunctionCall* ast_function_call =
          date_part_ast_location->GetAsOrDie<ASTFunctionCall>();

      if (ast_function_call->function()->num_names() != 1) {
        return MakeSqlErrorAt(ast_function_call->function())
               << "A valid date part name is required, but found "
               << ast_function_call->function()->ToIdentifierPathString();
      }
      date_part_name =
          ast_function_call->function()->first_name()->GetAsIdString();

      if (ast_function_call->arguments().size() != 1 ||
          ast_function_call->HasModifiers()) {
        return MakeSqlErrorAt(ast_function_call)
               << "Found invalid date part argument function call syntax for "
               << ast_function_call->function()->ToIdentifierPathString()
               << "()";
      }

      date_part_arg_ast_location = ast_function_call->arguments()[0];
      if (date_part_arg_ast_location->node_kind() != AST_PATH_EXPRESSION) {
        // We don't allow AST_IDENTIFIER because the grammar won't allow it to
        // be used as a function argument, despite ASTIdentifier inheriting from
        // ASTExpression.
        return MakeSqlErrorAt(date_part_arg_ast_location)
               << "Found invalid date part argument syntax in argument of "
               << ast_function_call->function()->ToIdentifierPathString();
      }
      const auto ast_path =
          date_part_arg_ast_location->GetAsOrDie<ASTPathExpression>();
      if (ast_path->num_names() != 1) {
        return MakeSqlErrorAt(ast_path)
               << "A valid date part argument is required, but found "
               << ast_path->ToIdentifierPathString();
      }
      date_part_arg_name = ast_path->first_name()->GetAsIdString();
      break;
    }
    default:
      return MakeSqlErrorAt(date_part_ast_location)
             << "A valid date part name is required";
  }

  return MakeDatePartEnumResolvedLiteralFromNames(
      date_part_name, date_part_arg_name, date_part_ast_location,
      date_part_arg_name.empty() ? nullptr : date_part_arg_ast_location,
      resolved_date_part, date_part);
}

// static
absl::StatusOr<Resolver::ProtoExtractionType>
Resolver::ProtoExtractionTypeFromName(const std::string& extraction_type_name) {
  std::string upper_name = absl::AsciiStrToUpper(extraction_type_name);
  if (upper_name == "HAS") {
    return ProtoExtractionType::kHas;
  } else if (upper_name == "FIELD") {
    return ProtoExtractionType::kField;
  } else if (upper_name == "RAW") {
    return ProtoExtractionType::kRaw;
  } else {
    return MakeSqlError() << "Unable to parse " << extraction_type_name
                          << " to a valid ProtoExtractionType";
  }
}

// static
std::string Resolver::ProtoExtractionTypeName(
    ProtoExtractionType extraction_type) {
  switch (extraction_type) {
    case ProtoExtractionType::kHas:
      return "HAS";
    case ProtoExtractionType::kField:
      return "FIELD";
    case ProtoExtractionType::kRaw:
      return "RAW";
  }
}

absl::Status Resolver::ResolveProtoExtractExpression(
    const ASTExpression* field_extraction_type_ast_location,
    std::unique_ptr<const ResolvedExpr> resolved_proto_input,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  ZETASQL_RET_CHECK(
      language().LanguageFeatureEnabled(FEATURE_V_1_3_EXTRACT_FROM_PROTO));
  if (field_extraction_type_ast_location->node_kind() != AST_FUNCTION_CALL) {
    return MakeSqlErrorAt(field_extraction_type_ast_location)
           << "Invalid proto extraction function call syntax found. Extraction "
              "type and field should follow the pattern EXTRACTION_TYPE(field)";
  }

  // Determine the field and extraction type from the ACCESSOR(field) input
  // expression.
  const auto ast_function_call =
      field_extraction_type_ast_location->GetAsOrDie<ASTFunctionCall>();
  if (ast_function_call->function()->num_names() != 1) {
    return MakeSqlErrorAt(ast_function_call->function())
           << "A valid proto extraction type is required (e.g., HAS or "
              "FIELD), but found "
           << ast_function_call->function()->ToIdentifierPathString();
  }

  std::string extraction_type_name =
      ast_function_call->function()->first_name()->GetAsString();
  absl::StatusOr<const Resolver::ProtoExtractionType> field_extraction_type_or =
      ProtoExtractionTypeFromName(extraction_type_name);
  if (!field_extraction_type_or.ok()) {
    return MakeSqlErrorAt(field_extraction_type_ast_location)
           << "A valid proto extraction type is required (e.g., HAS or FIELD), "
              "but found "
           << extraction_type_name;
  }
  bool extraction_type_supported = false;
  switch (field_extraction_type_or.value()) {
    case ProtoExtractionType::kHas:
    case ProtoExtractionType::kField:
    case ProtoExtractionType::kRaw: {
      // These EXTRACT types are supported by the base
      // FEATURE_V_1_3_EXTRACT_FROM_PROTO.
      extraction_type_supported = true;
      break;
    }
  }
  if (!extraction_type_supported) {
    return MakeSqlErrorAt(field_extraction_type_ast_location)
           << "Extraction type "
           << ProtoExtractionTypeName(field_extraction_type_or.value())
           << "() is not supported";
  }

  if (ast_function_call->arguments().size() != 1 ||
      ast_function_call->HasModifiers()) {
    return MakeSqlErrorAt(ast_function_call)
           << "Found invalid argument function call syntax for "
           << ast_function_call->function()->ToIdentifierPathString() << "()";
  }

  const ASTExpression* field_ast_location = ast_function_call->arguments()[0];
  // We don't allow AST_IDENTIFIER because the grammar won't allow it to
  // be used as a function argument, despite ASTIdentifier inheriting from
  // ASTExpression.
  if (field_ast_location->node_kind() != AST_PATH_EXPRESSION) {
    return MakeSqlErrorAt(field_ast_location)
           << "Found invalid argument for "
           << ast_function_call->function()->ToIdentifierPathString()
           << "() accessor. Input must be an identifier naming a valid "
              "field";
  }

  const ASTPathExpression* field_path =
      field_ast_location->GetAsOrDie<ASTPathExpression>();
  if (field_path->names().empty() ||
      (field_path->num_names() > 1 && !field_path->parenthesized())) {
    const absl::string_view error_message =
            "A valid top level field or parenthesized extension path is "
            "required";
    return MakeSqlErrorAt(field_path)
           << error_message << ", but found '"
           << field_path->ToIdentifierPathString() << "'";
  }

  return ResolveProtoExtractWithExtractTypeAndField(
      field_extraction_type_or.value(), field_path,
      std::move(resolved_proto_input), resolved_expr_out);
}

absl::Status Resolver::ResolveProtoExtractWithExtractTypeAndField(
    ProtoExtractionType field_extraction_type,
    const ASTPathExpression* field_path,
    std::unique_ptr<const ResolvedExpr> resolved_proto_input,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  MaybeResolveProtoFieldOptions top_level_field_options;
  ResolveExtensionFieldOptions extension_options;
  switch (field_extraction_type) {
    case ProtoExtractionType::kHas: {
      top_level_field_options.get_has_bit_override = true;
      extension_options.get_has_bit = true;
      top_level_field_options.ignore_format_annotations = false;
      extension_options.ignore_format_annotations = false;
      break;
    }
    case ProtoExtractionType::kField: {
      top_level_field_options.get_has_bit_override = false;
      extension_options.get_has_bit = false;
      top_level_field_options.ignore_format_annotations = false;
      extension_options.ignore_format_annotations = false;
      break;
    }
    case ProtoExtractionType::kRaw: {
      top_level_field_options.get_has_bit_override = false;
      top_level_field_options.ignore_format_annotations = true;
      extension_options.get_has_bit = false;
      extension_options.ignore_format_annotations = true;
      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Invalid proto extraction type: "
                       << ProtoExtractionTypeName(field_extraction_type);
  }
  if (field_path->parenthesized()) {
    return ResolveExtensionFieldAccess(
        std::move(resolved_proto_input), extension_options, field_path,
        /*flatten_state=*/nullptr, resolved_expr_out);
  } else {
    ZETASQL_RET_CHECK_EQ(field_path->num_names(), 1)
        << "Non-parenthesized input to "
        << ProtoExtractionTypeName(field_extraction_type)
        << " must be a top level field, but found "
        << field_path->ToIdentifierPathString();
    return MaybeResolveProtoFieldAccess(
        field_path, field_path->first_name(), top_level_field_options,
        std::move(resolved_proto_input), resolved_expr_out);
  }
}

absl::Status Resolver::ResolveNormalizeModeArgument(
    const ASTExpression* arg,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  // Resolve the normalize mode argument.  If it is a valid normalize mode,
  // creates a NormalizeMode enum ResolvedLiteral for it.
  if (arg->node_kind() != AST_PATH_EXPRESSION) {
    return MakeSqlErrorAt(arg) << "Argument is not a valid NORMALIZE mode";
  }
  const absl::Span<const ASTIdentifier* const>& names =
      arg->GetAsOrDie<ASTPathExpression>()->names();
  if (names.size() != 1) {
    return MakeSqlErrorAt(arg) << "Argument is not a valid NORMALIZE mode";
  }
  const std::string normalize_mode_name = names[0]->GetAsString();
  functions::NormalizeMode normalize_mode;
  if (!functions::NormalizeMode_Parse(
          absl::AsciiStrToUpper(normalize_mode_name), &normalize_mode)) {
    return MakeSqlErrorAt(arg) << "Argument is not a valid NORMALIZE mode: "
                               << ToIdentifierLiteral(normalize_mode_name);
  }

  const EnumType* normalize_mode_type;
  const google::protobuf::EnumDescriptor* normalize_mode_descr =
      functions::NormalizeMode_descriptor();
  ZETASQL_RET_CHECK_OK(
      type_factory_->MakeEnumType(normalize_mode_descr, &normalize_mode_type));
  *resolved_expr_out = MakeResolvedLiteralWithoutLocation(
      Value::Enum(normalize_mode_type, normalize_mode));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveIntervalArgument(
    const ASTExpression* arg, ExprResolutionInfo* expr_resolution_info,
    std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_arguments_out,
    std::vector<const ASTExpression*>* ast_arguments_out) {
  if (arg->node_kind() != AST_INTERVAL_EXPR) {
    return MakeSqlErrorAt(arg) << "Expected INTERVAL expression";
  }
  const ASTIntervalExpr* interval_expr = arg->GetAsOrDie<ASTIntervalExpr>();

  // Resolve the INTERVAL value expression.
  const ASTExpression* interval_value_expr = interval_expr->interval_value();
  ZETASQL_RETURN_IF_ERROR(ResolveExpressionArgument(
      interval_value_expr, expr_resolution_info, resolved_arguments_out));
  ast_arguments_out->push_back(interval_value_expr);

  // Resolve the date part identifier and verify that it is a valid date part.
  const ASTIdentifier* interval_date_part_identifier =
      interval_expr->date_part_name();
  std::unique_ptr<const ResolvedExpr> resolved_date_part;
  ZETASQL_RETURN_IF_ERROR(ResolveDatePartArgument(interval_date_part_identifier,
                                          &resolved_date_part));

  // Optionally resolve the second date part identifier.
  if (interval_expr->date_part_name_to() != nullptr) {
    const ASTIdentifier* interval_date_part_identifier_to =
        interval_expr->date_part_name_to();
    // We verify that it is a valid date part to have good error message.
    std::unique_ptr<const ResolvedExpr> resolved_date_part_to;
    ZETASQL_RETURN_IF_ERROR(ResolveDatePartArgument(interval_date_part_identifier_to,
                                            &resolved_date_part_to));
    // But for backward compatibility with existing code, INTERVAL used as
    // argument to few date/time functions has to be single part.
    return MakeSqlErrorAt(arg)
           << "INTERVAL argument only support single date part field.";
  }

  // Coerce the interval value argument to INT64 if necessary.
  if (!resolved_arguments_out->back()->type()->IsInt64()) {
    std::unique_ptr<const ResolvedExpr> resolved_interval_value_arg =
        std::move(resolved_arguments_out->back());
    resolved_arguments_out->pop_back();

    SignatureMatchResult result;
    // Interval value must be either coercible to INT64 type, or be string
    // literal coercible to INT64 value.
    if (((resolved_interval_value_arg->node_kind() == RESOLVED_LITERAL ||
          resolved_interval_value_arg->node_kind() == RESOLVED_PARAMETER) &&
         resolved_interval_value_arg->type()->IsString()) ||
        coercer_.CoercesTo(
            GetInputArgumentTypeForExpr(resolved_interval_value_arg.get()),
            type_factory_->get_int64(), /*is_explicit=*/false, &result)) {
      ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
          interval_expr->interval_value(), type_factory_->get_int64(),
          kExplicitCoercion, &resolved_interval_value_arg));
      resolved_arguments_out->push_back(std::move(resolved_interval_value_arg));
    } else {
      return MakeSqlErrorAt(interval_expr->interval_value())
             << "Interval value must be coercible to INT64 type";
    }
  }

  resolved_arguments_out->push_back(std::move(resolved_date_part));

  ast_arguments_out->push_back(arg);
  return absl::OkStatus();
}

// This is used to make a map from function name to a SpecialFunctionFamily
// enum that is used inside GetFunctionNameAndArguments to pick out functions
// that need special-case handling based on name.  Using the map avoids
// having several sequential calls to zetasql_base::StringCaseEqual, which is slow.
enum SpecialFunctionFamily {
  FAMILY_NONE,
  FAMILY_COUNT,
  FAMILY_ANON,
  FAMILY_DATE_ADD,
  FAMILY_DATE_DIFF,
  FAMILY_DATE_TRUNC,
  FAMILY_STRING_NORMALIZE,
  FAMILY_GENERATE_CHRONO_ARRAY,
  FAMILY_BINARY_STATS,
};

static IdStringHashMapCase<SpecialFunctionFamily>*
InitSpecialFunctionFamilyMap() {
  auto* out = new IdStringHashMapCase<SpecialFunctionFamily>;

  // COUNT(*) has special handling to erase the * argument.
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("count"), FAMILY_COUNT);

  // These expect argument 1 to be an INTERVAL.
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("date_add"), FAMILY_DATE_ADD);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("date_sub"), FAMILY_DATE_ADD);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("datetime_add"), FAMILY_DATE_ADD);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("datetime_sub"), FAMILY_DATE_ADD);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("time_add"), FAMILY_DATE_ADD);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("time_sub"), FAMILY_DATE_ADD);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("timestamp_add"), FAMILY_DATE_ADD);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("timestamp_sub"), FAMILY_DATE_ADD);

  // These expect argument 2 to be a DATEPART.
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("date_diff"), FAMILY_DATE_DIFF);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("datetime_diff"),
                   FAMILY_DATE_DIFF);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("time_diff"), FAMILY_DATE_DIFF);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("timestamp_diff"),
                   FAMILY_DATE_DIFF);

  // These expect argument 1 to be a DATEPART.
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("date_trunc"), FAMILY_DATE_TRUNC);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("datetime_trunc"),
                   FAMILY_DATE_TRUNC);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("time_trunc"), FAMILY_DATE_TRUNC);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("timestamp_trunc"),
                   FAMILY_DATE_TRUNC);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("last_day"), FAMILY_DATE_TRUNC);

  // These expect argument 1 to be a NORMALIZE_MODE.
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("normalize"),
                   FAMILY_STRING_NORMALIZE);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("normalize_and_casefold"),
                   FAMILY_STRING_NORMALIZE);

  // These expect argument 2 to be an INTERVAL.
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("generate_date_array"),
                   FAMILY_GENERATE_CHRONO_ARRAY);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("generate_timestamp_array"),
                   FAMILY_GENERATE_CHRONO_ARRAY);

  // These don't support DISTINCT.
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("corr"), FAMILY_BINARY_STATS);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("covar_pop"), FAMILY_BINARY_STATS);
  zetasql_base::InsertOrDie(out, IdString::MakeGlobal("covar_samp"),
                   FAMILY_BINARY_STATS);

  return out;
}

static SpecialFunctionFamily GetSpecialFunctionFamily(
    const IdString& function_name) {
  static IdStringHashMapCase<SpecialFunctionFamily>* kSpecialFunctionFamilyMap =
      InitSpecialFunctionFamilyMap();
  if (absl::StartsWithIgnoreCase(function_name.ToStringView(), "anon_")) {
    return FAMILY_ANON;
  }
  return zetasql_base::FindWithDefault(*kSpecialFunctionFamilyMap, function_name,
                              FAMILY_NONE);
}

absl::Status Resolver::GetFunctionNameAndArguments(
    const ASTFunctionCall* function_call,
    std::vector<std::string>* function_name_path,
    std::vector<const ASTExpression*>* function_arguments,
    std::map<int, SpecialArgumentType>* argument_option_map,
    QueryResolutionInfo* query_resolution_info) {
  const ASTPathExpression* function = function_call->function();
  *function_name_path = function->ToIdentifierVector();
  function_arguments->assign(function_call->arguments().begin(),
                             function_call->arguments().end());

  bool is_anon_function = false;

  if (function->num_names() == 1 ||
      (function_name_path->size() == 2 &&
       zetasql_base::CaseEqual((*function_name_path)[0], "SAFE"))) {
    switch (GetSpecialFunctionFamily(function->last_name()->GetAsIdString())) {
      // A normal function with no special handling.
      case FAMILY_NONE:
        break;

      // Special case to look for COUNT(*).
      case FAMILY_COUNT:
        if (function_call->arguments().size() == 1 &&
            function_call->arguments()[0]->node_kind() == AST_STAR) {
          if (function_call->distinct()) {
            return MakeSqlErrorAt(function_call)
                   << "COUNT(*) cannot be used with DISTINCT";
          }

          // The catalog function for COUNT(*) has the name "$count_star" with
          // no arguments.
          function_name_path->back() = "$count_star";
          function_arguments->clear();
        }
        break;

      // Special case handling for differential privacy functions due to
      // CLAMPED BETWEEN syntax.
      // TODO: refactor so that the function name is first looked up
      // from the catalog, and then perform the special handling for special
      // functions. then change this code to also check
      // function->Is<AnonFunction>(), instead of checking for an 'anon_' prefix
      // in the function name.
      case FAMILY_ANON:
        if (language().LanguageFeatureEnabled(FEATURE_ANONYMIZATION)) {
          is_anon_function = true;
          // The currently supported anonymous aggregate functions each require
          // exactly one 'normal argument' (two for ANON_PERCENTILE_CONT), along
          // with an optional CLAMPED clause.  We transform the CLAMPED clause
          // into normal function call arguments here. Note that after this
          // transformation we cannot tell if the function call arguments were
          // originally derived from
          // the CLAMPED clause or not, so we won't be able to tell the
          // following apart:
          //   ANON_SUM(x CLAMPED BETWEEN 1 AND 10)
          //   ANON_SUM(x, 1, 10)
          // Since the latter is invalid, we must detect that case here and
          // provide an error.
          if (absl::AsciiStrToUpper(function->last_name()->GetAsString()) ==
              "ANON_PERCENTILE_CONT") {
            if (function_call->arguments().size() != 2) {
              size_t arg_size = function_call->arguments().size();
              return MakeSqlErrorAt(function_call)
                     << "Anonymized aggregate function anon_percentile_cont"
                     << " expects exactly 2 arguments but found " << arg_size
                     << (arg_size == 1 ? " argument" : " arguments");
            }
          } else if (function_call->arguments().size() != 1) {
            return MakeSqlErrorAt(function_call)
                   << "Anonymized aggregate function "
                   << absl::AsciiStrToUpper(
                          function->last_name()->GetAsString())
                   << " expects exactly 1 argument but found "
                   << function_call->arguments().size() << " arguments";
          }
          // Convert the CLAMPED BETWEEN lower and upper bound to
          // normal function call arguments, appended after the first
          // argument.
          if (function_call->clamped_between_modifier() != nullptr) {
            function_arguments->emplace_back(
                function_call->clamped_between_modifier()->low());
            function_arguments->emplace_back(
                function_call->clamped_between_modifier()->high());
          }
          // Special case to look for ANON_COUNT(*).
          if (function->last_name()->GetAsIdString().CaseEquals(
                  MakeIdString("anon_count")) &&
              !function_call->arguments().empty() &&
              function_call->arguments()[0]->node_kind() == AST_STAR) {
            // The catalog function for ANON_COUNT(*) has the name
            // "$anon_count_star", and the signature is without the *
            // argument.
            function_name_path->back() = "$anon_count_star";
            function_arguments->erase(function_arguments->begin());
          }
          if (query_resolution_info != nullptr) {
            query_resolution_info->set_has_anonymized_aggregation(true);
          }
        }
        // We normally record that anonymization is present when an
        // AnonymizedAggregateScan is created, which is appropriate when
        // resolving queries.  However, when resolving expressions, we
        // also need to record that anonymization is present here since
        // the expression might include an anonymized aggregate function
        // call but no related resolved scan is created for it.
        analyzer_output_properties_.MarkRelevant(REWRITE_ANONYMIZATION);
        break;

      // Special case handling for DATE_ADD, DATE_SUB, DATE_TRUNC, and
      // DATE_DIFF due to the INTERVAL and AT TIME ZONE syntax, and date parts
      // represented as identifiers.  In these cases, we massage the element
      // list to conform to the defined function signatures.
      case FAMILY_DATE_ADD:
        zetasql_base::InsertOrDie(argument_option_map, 1, SpecialArgumentType::INTERVAL);
        break;
      case FAMILY_DATE_DIFF:
        zetasql_base::InsertOrDie(argument_option_map, 2, SpecialArgumentType::DATEPART);
        break;
      case FAMILY_DATE_TRUNC:
        zetasql_base::InsertOrDie(argument_option_map, 1, SpecialArgumentType::DATEPART);
        break;
      case FAMILY_STRING_NORMALIZE:
        zetasql_base::InsertOrDie(argument_option_map, 1,
                         SpecialArgumentType::NORMALIZE_MODE);
        break;
      case FAMILY_GENERATE_CHRONO_ARRAY:
        zetasql_base::InsertOrDie(argument_option_map, 2, SpecialArgumentType::INTERVAL);
        break;

      // Special case to disallow DISTINCT with binary stats.
      case FAMILY_BINARY_STATS:
        if (function_call->distinct()) {
          // TODO: function->name(0) probably does not work right
          // if this is a SAFE function call.  This should probably be
          // function->last_name() or function_name_path->back() instead.
          return MakeSqlErrorAt(function_call)
                 << "DISTINCT is not allowed for function "
                 << absl::AsciiStrToUpper(function->name(0)->GetAsString());
        }

        break;
    }
  }
  if (function_call->clamped_between_modifier() != nullptr &&
      !is_anon_function) {
    return MakeSqlErrorAt(function_call)
           << "The CLAMPED BETWEEN clause is not allowed in the function call "
           << "arguments for function "
           << absl::AsciiStrToUpper(function->name(0)->GetAsString());
  }

  return absl::OkStatus();
}

static bool IsLetterO(char c) { return c == 'o' || c == 'O'; }

// Returns function_name without a leading "SAFE_" prefix. If no safe prefix
// is present, returns function_name.
absl::string_view StripSafeCaseInsensitive(absl::string_view function_name) {
  if ((function_name[0] == 's' || function_name[0] == 'S') &&
      zetasql_base::CaseCompare(function_name.substr(0, 5),
                                           "SAFE_") == 0) {
    return function_name.substr(5);
  }
  return function_name;
}

static bool IsSpecialArrayContextFunction(absl::string_view function_name) {
  // We try to avoid doing the zetasql_base::StringCaseEqual calls as much as possible.
  if (IsLetterO(function_name[0]) &&
      (zetasql_base::CaseCompare(function_name, "OFFSET") == 0 ||
       zetasql_base::CaseCompare(function_name, "ORDINAL") == 0)) {
    return true;
  }
  return false;
}

static bool IsSpecialMapContextFunction(absl::string_view function_name) {
  // We try to avoid doing the zetasql_base::StringCaseEqual calls as much as possible.
  char first_letter = function_name[0];
  return (first_letter == 'k' || first_letter == 'K') &&
         zetasql_base::CaseCompare(function_name, "KEY") == 0;
}

absl::Status Resolver::ResolveLambda(
    const ASTLambda* ast_lambda, absl::Span<const IdString> arg_names,
    absl::Span<const Type* const> arg_types, const Type* body_result_type,
    bool allow_argument_coercion, const NameScope* name_scope,
    std::unique_ptr<const ResolvedInlineLambda>* resolved_expr_out) {
  static constexpr char kLambda[] = "Lambda";
  // Every argument should have a corresponding type.
  ZETASQL_RET_CHECK_EQ(arg_names.size(), arg_types.size());

  // Build a NameList from lambda arguments.
  std::vector<ResolvedColumn> arg_columns;
  arg_columns.reserve(arg_names.size());
  std::shared_ptr<NameList> args_name_list = std::make_shared<NameList>();
  for (int i = 0; i < arg_names.size(); i++) {
    IdString arg_name = arg_names[i];
    const Type* arg_type = arg_types[i];
    const ResolvedColumn arg_column(AllocateColumnId(),
                                    /*table_name=*/kLambdaArgId, arg_name,
                                    arg_type);
    ZETASQL_RETURN_IF_ERROR(
        args_name_list->AddColumn(arg_name, arg_column, /*is_explicit=*/true));
    arg_columns.push_back(arg_column);

    // We need to record access to the column representing lambda argument,
    // otherwise analyzer will further remove it for the list of used columns as
    // unreferenced.
    RecordColumnAccess(arg_column);
  }

  // Combine argument names and function call site name scope to create a name
  // scope for lambda body. This enables the lambda body to access columns in
  // addition to lambda arguments.
  CorrelatedColumnsSet correlated_columns_set;
  auto body_name_scope = absl::make_unique<NameScope>(
      name_scope, args_name_list, &correlated_columns_set);

  // Resolve the body.
  std::unique_ptr<const ResolvedExpr> resolved_body;
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_lambda->body(), body_name_scope.get(),
                                    kLambda, &resolved_body));

  // If <body_result_type> is set, the body expr is expected to have
  // specific type.
  if (body_result_type != nullptr && allow_argument_coercion) {
    ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
        ast_lambda->body(), body_result_type, kImplicitCoercion,
        "Lambda should return type $0, but returns $1", &resolved_body));
  }

  // Gather the correlated column references: columns other than the lambda
  // arguments.
  std::vector<std::unique_ptr<const ResolvedColumnRef>> parameter_list;
  FetchCorrelatedSubqueryParameters(correlated_columns_set, &parameter_list);

  *resolved_expr_out = MakeResolvedInlineLambda(
      arg_columns, std::move(parameter_list), std::move(resolved_body));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveAggregateFunctionCallFirstPass(
    const ASTFunctionCall* ast_function, const Function* function,
    ResolvedFunctionCallBase::ErrorMode error_mode,
    const std::vector<const ASTExpression*>& function_arguments,
    const std::map<int, SpecialArgumentType>& argument_option_map,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::unique_ptr<ExprResolutionInfo> local_expr_resolution_info;
  std::vector<std::unique_ptr<const ResolvedColumnRef>> correlated_columns;
  if (ast_function->with_group_rows() == nullptr) {
    // Normal case, do initial function call resolution.
    local_expr_resolution_info = absl::make_unique<ExprResolutionInfo>(
        expr_resolution_info, expr_resolution_info->aggregate_name_scope,
        expr_resolution_info->clause_name,
        expr_resolution_info->allows_analytic);
    // When resolving arguments of aggregation functions, we resolve
    // against pre-grouped versions of columns only.
    local_expr_resolution_info->use_post_grouping_columns = false;

    return ResolveFunctionCallImpl(
        ast_function, function, error_mode, function_arguments,
        argument_option_map, local_expr_resolution_info.get(),
        /*with_group_rows_subquery=*/nullptr, std::move(correlated_columns),
        resolved_expr_out);
  }

  ZETASQL_RET_CHECK_NE(ast_function->with_group_rows(), nullptr);
  if (!expr_resolution_info->allows_aggregation) {
    return MakeSqlErrorAt(ast_function)
           << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
           << " not allowed in " << expr_resolution_info->clause_name;
  } else if (expr_resolution_info->query_resolution_info == nullptr) {
    return MakeSqlErrorAt(ast_function)
           << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
           << " not expected";
  }

  // Evaluate the subquery first and then use resulting name list to resolve
  // the aggregate function.
  const ASTQuery* subquery = ast_function->with_group_rows()->subquery();
  CorrelatedColumnsSet correlated_columns_set;
  // The name scope is constructed to skip expr_resolution_info's name scope,
  // which contains columns from the table being aggregated. Instead the
  // subquery will use columns from group_rows() tvf.
  // We also want to make correlation references available to the aggregate
  // function call.  To support this, we can use the previous_scope() here. Note
  // that this is only valid when the current name scope is the from clause name
  // scope.  If the from clause name scope has a previous scope, then it is the
  // correlation scope due to the way it is constructed in the resolver.  For
  // other name scopes, for instance the NameScope representing both the select
  // list aliases and from clause, the previous scope is *not* the correlation
  // scope.  So it is not in general true that the previous scope always
  // represents a correlation scope.  In this context, the current name scope is
  // the from clause scope, so we can rely on that here.
  auto subquery_scope = absl::make_unique<NameScope>(
      expr_resolution_info->name_scope->previous_scope(),
      &correlated_columns_set);
  NameListPtr with_group_rows_subquery_name_list;
  ZETASQL_RET_CHECK_NE(subquery, nullptr);
  std::unique_ptr<const ResolvedScan> resolved_with_group_rows_subquery;
  {
    ZETASQL_RET_CHECK_NE(
        expr_resolution_info->query_resolution_info->from_clause_name_list(),
        nullptr);
    name_lists_for_group_rows_.push(
        {expr_resolution_info->query_resolution_info->from_clause_name_list(),
         /*group_rows_tvf_used=*/false});
    auto cleanup =
        absl::MakeCleanup([this]() { name_lists_for_group_rows_.pop(); });
    IdString subquery_alias = AllocateSubqueryName();
    ZETASQL_RETURN_IF_ERROR(ResolveQuery(subquery, subquery_scope.get(), subquery_alias,
                                 /*is_outer_query=*/false,
                                 &resolved_with_group_rows_subquery,
                                 &with_group_rows_subquery_name_list));
    ZETASQL_RET_CHECK(!name_lists_for_group_rows_.empty());
    ZETASQL_RET_CHECK_EQ(
        expr_resolution_info->query_resolution_info->from_clause_name_list(),
        name_lists_for_group_rows_.top().name_list);
    if (!name_lists_for_group_rows_.top().group_rows_tvf_used) {
      return MakeSqlErrorAt(ast_function->with_group_rows())
             << "GROUP_ROWS() TVF must be used at least once inside WITH "
                "GROUP_ROWS";
    }
    // Support AS VALUE in the subquery to produce value table, make it
    // available to the aggregate function.
    if (with_group_rows_subquery_name_list->is_value_table()) {
      ZETASQL_RET_CHECK_EQ(with_group_rows_subquery_name_list->num_columns(), 1);
      auto new_name_list = std::make_shared<NameList>();
      ZETASQL_RETURN_IF_ERROR(new_name_list->AddValueTableColumn(
          subquery_alias, with_group_rows_subquery_name_list->column(0).column,
          subquery));
      with_group_rows_subquery_name_list = new_name_list;
    }
  }

  FetchCorrelatedSubqueryParameters(correlated_columns_set,
                                    &correlated_columns);

  MaybeRecordParseLocation(
      subquery,
      const_cast<ResolvedScan*>(resolved_with_group_rows_subquery.get()));
  // NameScope of the aggregate function arguments is the output of the WITH
  // GROUPS ROWS subquery along with correlation references.
  auto with_group_rows_subquery_scope = absl::make_unique<NameScope>(
      expr_resolution_info->name_scope->previous_scope(),
      with_group_rows_subquery_name_list);
  local_expr_resolution_info = absl::make_unique<ExprResolutionInfo>(
      with_group_rows_subquery_scope.get(),
      with_group_rows_subquery_scope.get(), /*allows_aggregation=*/true,
      /*allows_analytic=*/true, /*use_post_grouping_columns_in=*/false,
      "Function call WITH GROUP_ROWS",
      expr_resolution_info->query_resolution_info);

  return ResolveFunctionCallImpl(
      ast_function, function, error_mode, function_arguments,
      argument_option_map, local_expr_resolution_info.get(),
      std::move(resolved_with_group_rows_subquery),
      std::move(correlated_columns), resolved_expr_out);
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveFunctionCall(
    const ASTFunctionCall* ast_function,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::vector<std::string> function_name_path;
  std::vector<const ASTExpression*> function_arguments;
  std::map<int, SpecialArgumentType> argument_option_map;
  if (expr_resolution_info->use_post_grouping_columns) {
    // We have already resolved aggregate function calls for SELECT list
    // aggregate expressions.  Second pass aggregate function resolution
    // should look up these resolved aggregate function calls in the map.
    ZETASQL_RET_CHECK(expr_resolution_info->query_resolution_info != nullptr);
    const ResolvedComputedColumn* computed_aggregate_column =
        zetasql_base::FindPtrOrNull(
            expr_resolution_info->query_resolution_info->aggregate_expr_map(),
            ast_function);
    if (computed_aggregate_column != nullptr) {
      *resolved_expr_out = MakeColumnRef(computed_aggregate_column->column());
      return absl::OkStatus();
    }
    // If we do not find them in the map, then fall through
    // and resolve the aggregate function call normally.  This is required
    // when the aggregate function appears in the HAVING or ORDER BY.
  }

  // Get the catalog function name and arguments that may be different from
  // what are specified in the input query.
  ZETASQL_RETURN_IF_ERROR(GetFunctionNameAndArguments(
      ast_function, &function_name_path, &function_arguments,
      &argument_option_map, expr_resolution_info->query_resolution_info));

  // Special case for "OFFSET", "ORDINAL", and "KEY" functions, which are really
  // just special wrappers allowed inside array element access syntax only.
  if (function_name_path.size() == 1) {
    const absl::string_view function_name = function_name_path[0];
    const absl::string_view function_name_without_safe =
        StripSafeCaseInsensitive(function_name);
    const bool is_array_context_fn =
        IsSpecialArrayContextFunction(function_name_without_safe);
    const bool is_map_context_fn =
        IsSpecialMapContextFunction(function_name_without_safe);
    if (is_array_context_fn || is_map_context_fn) {
      absl::string_view type = is_map_context_fn ? "map" : "array";
      absl::string_view arg_name = is_map_context_fn ? "key" : "position";
      return MakeSqlErrorAt(ast_function)
             << absl::AsciiStrToUpper(function_name)
             << " is not a function. It can only be used for " << type
             << " element access using " << type << "["
             << absl::AsciiStrToUpper(function_name) << "(" << arg_name << ")]";
    }
  }
  const Function* function;
  ResolvedFunctionCallBase::ErrorMode error_mode;
  ZETASQL_RETURN_IF_ERROR(LookupFunctionFromCatalog(
      ast_function, function_name_path,
      FunctionNotFoundHandleMode::kReturnError, &function, &error_mode));
  if (IsFilterFields(function)) {
    return ResolveFilterFieldsFunctionCall(ast_function, function_arguments,
                                           expr_resolution_info,
                                           resolved_expr_out);
  }
  if (function->IsAggregate()) {
    return ResolveAggregateFunctionCallFirstPass(
        ast_function, function, error_mode, function_arguments,
        argument_option_map, expr_resolution_info, resolved_expr_out);
  }
  if (ast_function->distinct()) {
    return MakeSqlErrorAt(ast_function)
           << "Non-aggregate " << function->QualifiedSQLName()
           << " cannot be called with DISTINCT";
  }
  if (ast_function->null_handling_modifier() !=
      ASTFunctionCall::DEFAULT_NULL_HANDLING) {
    return MakeSqlErrorAt(ast_function)
           << "IGNORE NULLS and RESPECT NULLS are not supported on scalar "
              "functions";
  }
  if (ast_function->having_modifier() != nullptr) {
    return MakeSqlErrorAt(ast_function->having_modifier())
           << "HAVING MAX and HAVING MIN are not supported on scalar functions";
  }
  if (ast_function->clamped_between_modifier() != nullptr) {
    return MakeSqlErrorAt(ast_function->clamped_between_modifier())
           << "CLAMPED BETWEEN is not supported on scalar functions";
  }
  if (ast_function->order_by() != nullptr) {
    return MakeSqlErrorAt(ast_function->order_by())
           << "ORDER BY in arguments is not supported on scalar functions";
  }
  if (ast_function->limit_offset() != nullptr) {
    return MakeSqlErrorAt(ast_function->limit_offset())
           << "LIMIT in arguments is not supported on scalar functions";
  }

  if (ast_function->with_group_rows() != nullptr) {
    return MakeSqlErrorAt(ast_function->with_group_rows())
           << "WITH GROUP_ROWS is not supported on scalar functions";
  }

  return ResolveFunctionCallImpl(
      ast_function, function, error_mode, function_arguments,
      argument_option_map, expr_resolution_info,
      /*with_group_rows_subquery=*/nullptr,
      /*with_group_rows_correlation_references=*/{}, resolved_expr_out);
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveAnalyticFunctionCall(
    const ASTAnalyticFunctionCall* analytic_function_call,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  if (!expr_resolution_info->allows_analytic) {
    return MakeSqlErrorAt(analytic_function_call)
           << "Analytic function not allowed in "
           << expr_resolution_info->clause_name
           << (expr_resolution_info->is_post_distinct()
                   ? " after SELECT DISTINCT"
                   : "");
  }

  if (analytic_function_call->function()->with_group_rows() != nullptr) {
    // TODO: support WITH GROUP_ROWS on analytic functions.
    return MakeSqlErrorAt(analytic_function_call->function()->with_group_rows())
           << "WITH GROUP_ROWS syntax is not supported on analytic functions";
  }

  if (analytic_function_call->function()->order_by() != nullptr) {
    return MakeSqlErrorAt(analytic_function_call)
           << "ORDER BY in arguments is not supported on analytic functions";
  }

  if (analytic_function_call->function()->limit_offset() != nullptr) {
    return MakeSqlErrorAt(analytic_function_call)
           << "LIMIT in arguments is not supported on analytic functions";
  }

  if (analytic_function_call->function()->having_modifier() != nullptr) {
    return MakeSqlErrorAt(analytic_function_call)
           << "HAVING modifier is not supported on analytic functions";
  }
  if (analytic_function_call->function()->clamped_between_modifier() !=
      nullptr) {
    return MakeSqlErrorAt(analytic_function_call)
           << "CLAMPED BETWEEN is not supported on analytic functions";
  }

  std::vector<std::string> function_name_path;
  std::vector<const ASTExpression*> function_arguments;
  std::map<int, SpecialArgumentType> argument_option_map;

  // Get the catalog function name and arguments that may be different from
  // what are specified in the input query.
  ZETASQL_RETURN_IF_ERROR(GetFunctionNameAndArguments(
      analytic_function_call->function(), &function_name_path,
      &function_arguments, &argument_option_map,
      expr_resolution_info->query_resolution_info));

  // We want to report errors on invalid function names before errors on invalid
  // arguments, so pre-emptively lookup the function in the catalog.
  const Function* function;
  ResolvedFunctionCallBase::ErrorMode error_mode;
  ZETASQL_RETURN_IF_ERROR(LookupFunctionFromCatalog(
      analytic_function_call, function_name_path,
      FunctionNotFoundHandleMode::kReturnError, &function, &error_mode));

  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;

  std::vector<const ASTExpression*> ast_arguments;
  ZETASQL_RETURN_IF_ERROR(ResolveExpressionArguments(
      expr_resolution_info, function_arguments, argument_option_map,
      &resolved_arguments, &ast_arguments));

  if (expr_resolution_info->has_analytic) {
    return MakeSqlErrorAt(analytic_function_call)
           << "Analytic function cannot be an argument of another analytic "
           << "function";
  }

  expr_resolution_info->has_analytic = true;

  std::unique_ptr<ResolvedFunctionCall> resolved_function_call;
  const std::vector<const ASTNode*> arg_locations =
      ToLocations(absl::Span<const ASTExpression* const>(ast_arguments));
  ZETASQL_RETURN_IF_ERROR(function_resolver_->ResolveGeneralFunctionCall(
      analytic_function_call, arg_locations, function_name_path,
      /*is_analytic=*/true, std::move(resolved_arguments),
      /*named_arguments=*/{}, /*expected_result_type=*/nullptr,
      &resolved_function_call));
  ZETASQL_DCHECK(expr_resolution_info->query_resolution_info != nullptr);
  return expr_resolution_info->query_resolution_info->analytic_resolver()
      ->ResolveOverClauseAndCreateAnalyticColumn(
          analytic_function_call, resolved_function_call.get(),
          expr_resolution_info, resolved_expr_out);
}

bool Resolver::IsValidExplicitCast(
    const std::unique_ptr<const ResolvedExpr>& resolved_argument,
    const Type* to_type) {
  ExtendedCompositeCastEvaluator unused_extended_conversion_evaluator =
      ExtendedCompositeCastEvaluator::Invalid();
  return CheckExplicitCast(resolved_argument.get(), to_type,
                           &unused_extended_conversion_evaluator)
      .value_or(false);
}

absl::StatusOr<bool> Resolver::CheckExplicitCast(
    const ResolvedExpr* resolved_argument, const Type* to_type,
    ExtendedCompositeCastEvaluator* extended_conversion_evaluator) {
  SignatureMatchResult result;
  return coercer_.CoercesTo(GetInputArgumentTypeForExpr(resolved_argument),
                            to_type, /*is_explicit=*/true, &result,
                            extended_conversion_evaluator);
}

static absl::Status CastResolutionError(const ASTNode* ast_location,
                                        const Type* from_type,
                                        const Type* to_type, ProductMode mode) {
  std::string error_prefix;
  if (from_type->IsArray() && to_type->IsArray()) {
    // Special intro message for array casts.
    error_prefix =
        "Casting between arrays with incompatible element types "
        "is not supported: ";
  }
  return MakeSqlErrorAt(ast_location) << error_prefix << "Invalid cast from "
                                      << from_type->ShortTypeName(mode)
                                      << " to " << to_type->ShortTypeName(mode);
}

absl::Status Resolver::ResolveFormatOrTimeZoneExpr(
    const ASTExpression* expr, ExprResolutionInfo* expr_resolution_info,
    const char* clause_name,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) {
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(expr, expr_resolution_info, resolved_expr));

  auto expr_type = GetInputArgumentTypeForExpr(resolved_expr->get());
  auto make_error_msg = [clause_name](absl::string_view target_type_name,
                                      absl::string_view actual_type_name) {
    return absl::Substitute("$2 should return type $0, but returns $1",
                            target_type_name, actual_type_name, clause_name);
  };
  auto status =
      CoerceExprToType(expr, type_factory_->get_string(), kImplicitCoercion,
                       make_error_msg, resolved_expr);
  // TODO
  if (!status.ok()) {
    return MakeSqlErrorAt(expr) << status.message();
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveFormatClause(
    const ASTCastExpression* cast, ExprResolutionInfo* expr_resolution_info,
    const std::unique_ptr<const ResolvedExpr>& resolved_argument,
    const Type* resolved_cast_type,
    std::unique_ptr<const ResolvedExpr>* resolved_format,
    std::unique_ptr<const ResolvedExpr>* resolved_time_zone,
    bool* resolve_cast_to_null) {
  if (cast->format() == nullptr) {
    return absl::OkStatus();
  }

  if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_FORMAT_IN_CAST)) {
    return MakeSqlErrorAt(cast->format())
           << "CAST with FORMAT is not supported";
  }

  *resolve_cast_to_null = false;
  auto iter = internal::GetCastFormatMap().find(
      {resolved_argument->type()->kind(), resolved_cast_type->kind()});
  if (iter == internal::GetCastFormatMap().end()) {
    return MakeSqlErrorAt(cast->format())
           << "FORMAT is not allowed for cast from "
           << resolved_argument->type()->ShortTypeName(product_mode()) << " to "
           << resolved_cast_type->ShortTypeName(product_mode());
  }

  ZETASQL_RETURN_IF_ERROR(ResolveFormatOrTimeZoneExpr(
      cast->format()->format(), expr_resolution_info, "FORMAT expression",
      resolved_format));

  if (cast->format()->time_zone_expr() != nullptr) {
    // time zone is allowed only for cast from/to timestamps.
    if (resolved_argument->type()->kind() != TYPE_TIMESTAMP &&
        resolved_cast_type->kind() != TYPE_TIMESTAMP) {
      return MakeSqlErrorAt(cast->format()->time_zone_expr())
             << "AT TIME ZONE is not allowed for cast from "
             << resolved_argument->type()->ShortTypeName(product_mode())
             << " to " << resolved_cast_type->ShortTypeName(product_mode());
    }

    ZETASQL_RETURN_IF_ERROR(ResolveFormatOrTimeZoneExpr(
        cast->format()->time_zone_expr(), expr_resolution_info,
        "AT TIME ZONE expression", resolved_time_zone));
  }

  const bool return_null_on_error = cast->is_safe_cast();
  if ((*resolved_format)->node_kind() == RESOLVED_LITERAL) {
    const Value& v = (*resolved_format)->GetAs<ResolvedLiteral>()->value();

    // If format is literal and not null, validate it.
    if (!v.is_null()) {
      FormatValidationFunc func = iter->second;
      auto status = func(v.string_value());
      if (!status.ok()) {
        if (return_null_on_error) {
          *resolve_cast_to_null = true;
          return absl::OkStatus();
        }

        return MakeSqlErrorAt(cast->format()->format()) << status.message();
      }
    }
  }

  if (*resolved_time_zone != nullptr &&
      (*resolved_time_zone)->node_kind() == RESOLVED_LITERAL) {
    const Value& v = (*resolved_time_zone)->GetAs<ResolvedLiteral>()->value();

    // If time_zone is literal and not null, validate it.
    if (!v.is_null()) {
      absl::TimeZone timezone;
      auto status = functions::MakeTimeZone(v.string_value(), &timezone);
      if (!status.ok()) {
        if (return_null_on_error) {
          *resolve_cast_to_null = true;
          return absl::OkStatus();
        }

        return MakeSqlErrorAt(cast->format()->time_zone_expr())
               << status.message();
      }
    }
  }

  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveExplicitCast(
    const ASTCastExpression* cast, ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  const Type* resolved_cast_type;
  TypeParameters resolved_type_params;
  std::unique_ptr<const ResolvedExpr> resolved_argument;
  ZETASQL_RETURN_IF_ERROR(ResolveType(cast->type(), /*type_parameter_context=*/{},
                              &resolved_cast_type, &resolved_type_params));
  ZETASQL_RETURN_IF_ERROR(
      ResolveExpr(cast->expr(), expr_resolution_info, &resolved_argument));
  const bool return_null_on_error = cast->is_safe_cast();

  std::unique_ptr<const ResolvedExpr> resolved_format;
  std::unique_ptr<const ResolvedExpr> resolved_time_zone;
  bool resolve_cast_to_null = false;
  ZETASQL_RETURN_IF_ERROR(ResolveFormatClause(
      cast, expr_resolution_info, resolved_argument, resolved_cast_type,
      &resolved_format, &resolved_time_zone, &resolve_cast_to_null));

  if (resolve_cast_to_null) {
    *resolved_expr_out = MakeResolvedLiteral(cast, resolved_cast_type,
                                             Value::Null(resolved_cast_type),
                                             /*has_explicit_type=*/true);
    return absl::OkStatus();
  }

  // Handles casting literals that do not have an explicit type:
  // - For untyped NULL, converts the NULL to the target type.
  // - For untyped empty array, converts it to the target type if the target
  //   type is an array type.
  if (resolved_argument->node_kind() == RESOLVED_LITERAL &&
      !resolved_argument->GetAs<ResolvedLiteral>()->has_explicit_type()) {
    if (resolved_argument->GetAs<ResolvedLiteral>()->value().is_null()) {
      *resolved_expr_out = MakeResolvedLiteral(cast, resolved_cast_type,
                                               Value::Null(resolved_cast_type),
                                               /*has_explicit_type=*/true);
      return absl::OkStatus();
    }
    if (resolved_argument->GetAs<ResolvedLiteral>()->value().is_empty_array()) {
      if (resolved_cast_type->IsArray()) {
        *resolved_expr_out = MakeResolvedLiteral(
            cast, resolved_cast_type,
            Value::Array(resolved_cast_type->AsArray(), /*values=*/{}),
            /*has_explicit_type=*/true);
        return absl::OkStatus();
      } else {
        return CastResolutionError(cast->expr(), resolved_argument->type(),
                                   resolved_cast_type, product_mode());
      }
    }
  }

  // Don't attempt to coerce string/bytes literals to protos in the
  // analyzer. Instead, put a ResolvedCast node in the resolved AST and let the
  // engines do it. There are at least two reasons for this:
  // 1) We might not have the appropriate proto descriptor here (particularly
  //    for extensions).
  // 2) Since semantics of proto casting are not fully specified, applying these
  //    casts at analysis time may have inconsistent behavior with applying them
  //    at run-time in the engine. (One example is with garbage values, where
  //    engines may not be prepared for garbage proto Values in the resolved
  //    AST.)
  SignatureMatchResult result;
  if (resolved_argument->node_kind() == RESOLVED_LITERAL &&
      resolved_cast_type->IsProto() &&
      coercer_.CoercesTo(GetInputArgumentTypeForExpr(resolved_argument.get()),
                         resolved_cast_type, /*is_explicit=*/true, &result)) {
    ZETASQL_RETURN_IF_ERROR(ResolveCastWithResolvedArgument(
        cast->expr(), resolved_cast_type, return_null_on_error,
        &resolved_argument));
    if (resolved_argument->node_kind() == RESOLVED_CAST) {
      MaybeRecordParseLocation(
          analyzer_options_.parse_location_record_type() ==
                  PARSE_LOCATION_RECORD_FULL_NODE_SCOPE
              ? static_cast<const ASTNode*>(cast)
              : static_cast<const ASTNode*>(cast->type()),
          const_cast<ResolvedExpr*>(resolved_argument.get()));
    }
    *resolved_expr_out = std::move(resolved_argument);
    return absl::OkStatus();
  }

  if (IsValidExplicitCast(resolved_argument, resolved_cast_type)) {
    // For an explicit cast, if we are casting a literal then we will coerce it
    // to the target type and mark it as already cast.
    ZETASQL_RETURN_IF_ERROR(function_resolver_->AddCastOrConvertLiteral(
        cast->expr(), resolved_cast_type, std::move(resolved_format),
        std::move(resolved_time_zone), resolved_type_params,
        /*scan=*/nullptr, /*set_has_explicit_type=*/true, return_null_on_error,
        &resolved_argument));

    // Make the location of the resolved literal to enclose the entire CAST
    // expression.
    if (resolved_argument->node_kind() == RESOLVED_LITERAL) {
      const ResolvedLiteral* argument_literal =
          resolved_argument->GetAs<ResolvedLiteral>();
      resolved_argument = MakeResolvedLiteral(
          cast,  // Overrides parse location to be that of 'cast'.
          argument_literal->type(), argument_literal->value(),
          argument_literal->has_explicit_type());
    }
    if (resolved_argument->node_kind() == RESOLVED_CAST) {
      MaybeRecordParseLocation(
          analyzer_options_.parse_location_record_type() ==
                  PARSE_LOCATION_RECORD_FULL_NODE_SCOPE
              ? static_cast<const ASTNode*>(cast)
              : static_cast<const ASTNode*>(cast->type()),
          const_cast<ResolvedExpr*>(resolved_argument.get()));
    }
    *resolved_expr_out = std::move(resolved_argument);
    return absl::OkStatus();
  }
  return CastResolutionError(cast->expr(), resolved_argument->type(),
                             resolved_cast_type, product_mode());
}

absl::StatusOr<TypeParameters> Resolver::ResolveTypeParameters(
    const ASTTypeParameterList* type_parameters, const Type& resolved_type,
    const std::vector<TypeParameters>& child_parameter_list) {
  if (type_parameters != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_PARAMETERIZED_TYPES)) {
    return MakeSqlErrorAt(type_parameters)
           << "Parameterized types are not supported";
  }

  if (type_parameters == nullptr) {
    // A subfield of the struct or the element of an array has type parameters.
    if (!child_parameter_list.empty()) {
      return TypeParameters::MakeTypeParametersWithChildList(
          child_parameter_list);
    }
    // No type parameters in the AST.
    return TypeParameters();
  }

  // Resolve each type parameter value retrieved from the parser.
  ZETASQL_ASSIGN_OR_RETURN(std::vector<TypeParameterValue> resolved_type_parameter_list,
                   ResolveParameterLiterals(*type_parameters));

  // Validate type parameters and get the resolved TypeParameters class.
  absl::StatusOr<TypeParameters> type_params_or_status =
      resolved_type.ValidateAndResolveTypeParameters(
          resolved_type_parameter_list, product_mode());
  // TODO: Modify ValidateAndResolveTypeParameters to
  // handle the attachment of the error location. This can be done by taking an
  // argument that is a function: std::function<StatusBuilder(int)>.
  if (!type_params_or_status.ok()) {
    // We assume INVALID_ARGUMENT is never returned by
    // ValidateAndResolveTypeParameters for reasons other than MakeSqlError().
    if (absl::IsInvalidArgument(type_params_or_status.status())) {
      return MakeSqlErrorAt(type_parameters)
             << type_params_or_status.status().message();
    }
    return type_params_or_status.status();
  }
  TypeParameters type_params = type_params_or_status.value();

  if (!child_parameter_list.empty()) {
    type_params.set_child_list(child_parameter_list);
  }
  return type_params;
}

absl::Status Resolver::ResolveCastWithResolvedArgument(
    const ASTNode* ast_location, const Type* to_type, bool return_null_on_error,
    std::unique_ptr<const ResolvedExpr>* resolved_argument) {
  return ResolveCastWithResolvedArgument(
      ast_location, AnnotatedType(to_type, /*annotation_map=*/nullptr),
      /*format=*/nullptr, /*time_zone=*/nullptr, TypeParameters(),
      return_null_on_error, resolved_argument);
}

absl::Status Resolver::ResolveCastWithResolvedArgument(
    const ASTNode* ast_location, AnnotatedType to_annotated_type,
    std::unique_ptr<const ResolvedExpr> format,
    std::unique_ptr<const ResolvedExpr> time_zone,
    const TypeParameters& type_params, bool return_null_on_error,
    std::unique_ptr<const ResolvedExpr>* resolved_argument) {
  const Type* to_type = to_annotated_type.type;
  const AnnotationMap* to_type_annotation_map =
      to_annotated_type.annotation_map;

  // We can return without creating a ResolvedCast if the original type and
  // target type are the same, and original collation and target collation are
  // equal.
  if (to_type->Equals((*resolved_argument)->type()) && type_params.IsEmpty() &&
      AnnotationMap::HasEqualAnnotations(
          (*resolved_argument)->type_annotation_map(), to_type_annotation_map,
          CollationAnnotation::GetId())) {
    return absl::OkStatus();
  }

  ExtendedCompositeCastEvaluator extended_conversion_evaluator =
      ExtendedCompositeCastEvaluator::Invalid();
  ZETASQL_ASSIGN_OR_RETURN(bool cast_is_valid,
                   CheckExplicitCast(resolved_argument->get(), to_type,
                                     &extended_conversion_evaluator));
  if (!cast_is_valid) {
    return CastResolutionError(ast_location, (*resolved_argument)->type(),
                               to_type, product_mode());
  }

  // Add EXPLICIT cast.
  auto resolved_cast =
      MakeResolvedCast(to_type, std::move(*resolved_argument),
                       std::move(format), std::move(time_zone), type_params,
                       return_null_on_error, extended_conversion_evaluator);
  if (language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT) &&
      to_type_annotation_map != nullptr) {
    // TODO: Add a field inside ResolvedCast to indicate collation
    // of target type after introducing CAST(... AS STRING COLLATE '...')
    // syntax.
    resolved_cast->set_type_annotation_map(to_type_annotation_map);
  }

  *resolved_argument = std::move(resolved_cast);
  return absl::OkStatus();
}

const char Resolver::kArrayAtOffset[] = "$array_at_offset";
const char Resolver::kArrayAtOrdinal[] = "$array_at_ordinal";
const char Resolver::kProtoMapAtKey[] = "$proto_map_at_key";
const char Resolver::kSafeArrayAtOffset[] = "$safe_array_at_offset";
const char Resolver::kSafeArrayAtOrdinal[] = "$safe_array_at_ordinal";
const char Resolver::kSafeProtoMapAtKey[] = "$safe_proto_map_at_key";
const char Resolver::kSubscript[] = "$subscript";
const char Resolver::kSubscriptWithKey[] = "$subscript_with_key";
const char Resolver::kSubscriptWithOffset[] = "$subscript_with_offset";
const char Resolver::kSubscriptWithOrdinal[] = "$subscript_with_ordinal";

// TODO: Rename ResolveArrayElement to ResolveSubscriptElement.
// Rename ASTArrayElement to ASTSubscriptElement in all references.
//
// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveArrayElement(
    const ASTArrayElement* array_element,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  std::vector<std::unique_ptr<const ResolvedExpr>> args;
  // Resolve the lhs first before failing the subscript lookup.
  // This will produce errors in the input query from left to right.
  ZETASQL_RETURN_IF_ERROR(ResolveExpressionArgument(array_element->array(),
                                            expr_resolution_info, &args));
  const ResolvedExpr* resolved_lhs = args.back().get();

  // An array element access during a flattened path refers to the preceding
  // element, not to the output of flatten. As such, if we're in the middle of
  // a flatten, we have to apply the array access to the Get*Field.
  std::unique_ptr<ResolvedFlatten> resolved_flatten;
  if (resolved_lhs->Is<ResolvedFlatten>() &&
      expr_resolution_info->flatten_state.active_flatten() != nullptr) {
    ZETASQL_RET_CHECK_EQ(resolved_lhs,
                 expr_resolution_info->flatten_state.active_flatten());
    resolved_flatten.reset(const_cast<ResolvedFlatten*>(
        args.back().release()->GetAs<ResolvedFlatten>()));
    auto get_field_list = resolved_flatten->release_get_field_list();
    ZETASQL_RET_CHECK(!get_field_list.empty());
    args.back() = std::move(get_field_list.back());
    get_field_list.pop_back();
    resolved_flatten->set_get_field_list(std::move(get_field_list));
  }

  const bool is_array_subscript = resolved_lhs->type()->IsArray();
  std::vector<std::string> function_name_path;
  const ASTExpression* unwrapped_ast_position_expr;
  std::unique_ptr<const ResolvedExpr> resolved_position;
  std::string original_wrapper_name("");

  if (!is_array_subscript) {
    ZETASQL_RETURN_IF_ERROR(ResolveNonArraySubscriptElementAccess(
        resolved_lhs, array_element->position(), expr_resolution_info,
        &function_name_path, &unwrapped_ast_position_expr, &resolved_position,
        &original_wrapper_name));
  } else {
    absl::string_view function_name;
    ZETASQL_RETURN_IF_ERROR(ResolveArrayElementAccess(
        resolved_lhs, array_element->position(), expr_resolution_info,
        &function_name, &unwrapped_ast_position_expr, &resolved_position,
        &original_wrapper_name));
    function_name_path.push_back(std::string(function_name));
  }
  const std::string subscript_lhs_type =
      resolved_lhs->type()->ShortTypeName(product_mode());
  const std::string argumentType =
      resolved_position->type()->ShortTypeName(product_mode());
  args.push_back(std::move(resolved_position));

  // Engines may or may not have loaded the relevant $subscript function (or
  // friends) into their Catalog.  If they have not, then we want to provide
  // a user friendly error saying that the operator is not supported.  So we
  // first try to look up the function name in the Catalog to see if it exists.
  const Function* function;
  ResolvedFunctionCallBase::ErrorMode error_mode;
  absl::Status status = LookupFunctionFromCatalog(
      array_element, function_name_path,
      FunctionNotFoundHandleMode::kReturnNotFound, &function, &error_mode);

  // If the Catalog lookup fails then we will return an error.
  if (!status.ok()) {
    if (!absl::IsNotFound(status)) {
      return status;
    }
    // Otherwise, then the engine has not added the appropriate subscript
    // function into their Catalog.  We detect that case so we can provide
    // a good user-facing error.
    const std::string element_wrapper_string =
        original_wrapper_name.empty()
            ? ""
            : absl::StrCat(original_wrapper_name, "()");
    const std::string element_or_subscript =
        (is_array_subscript ? "Element" : "Subscript");
    return MakeSqlErrorAt(array_element->position())
           << element_or_subscript << " access using "
           << "[" << element_wrapper_string
           << "] is not supported on values of type " << subscript_lhs_type;
  }

  // This may fail due to no function signature match.  If so, then we get
  // a no signature match error (which can/should be customized as appropriate).
  ZETASQL_RETURN_IF_ERROR(ResolveFunctionCallWithResolvedArguments(
      array_element->position(),
      /*arg_locations=*/{array_element, unwrapped_ast_position_expr}, function,
      error_mode, std::move(args), /*named_arguments=*/{}, expr_resolution_info,
      /*with_group_rows_subquery=*/nullptr,
      /*with_group_rows_correlation_references=*/{}, resolved_expr_out));

  if (resolved_flatten != nullptr) {
    resolved_flatten->add_get_field_list(std::move(*resolved_expr_out));
    *resolved_expr_out = std::move(resolved_flatten);
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveNonArraySubscriptElementAccess(
    const ResolvedExpr* resolved_lhs, const ASTExpression* ast_position,
    ExprResolutionInfo* expr_resolution_info,
    std::vector<std::string>* function_name_path,
    const ASTExpression** unwrapped_ast_position_expr,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out,
    std::string* original_wrapper_name) {
  original_wrapper_name->clear();
  *unwrapped_ast_position_expr = nullptr;
  if (ast_position->node_kind() == AST_FUNCTION_CALL) {
    const ASTFunctionCall* ast_function_call =
        ast_position->GetAsOrDie<ASTFunctionCall>();
    if (ast_function_call->function()->num_names() == 1 &&
        !ast_function_call->HasModifiers()) {
      const IdString name =
          ast_function_call->function()->first_name()->GetAsIdString();
      *original_wrapper_name = name.ToString();
      static const IdStringHashMapCase<std::vector<std::string>>*
          kNameToSubscript = new IdStringHashMapCase<std::vector<std::string>>{
              {IdString::MakeGlobal("KEY"), {kSubscriptWithKey}},
              {IdString::MakeGlobal("OFFSET"), {kSubscriptWithOffset}},
              {IdString::MakeGlobal("ORDINAL"), {kSubscriptWithOrdinal}},
              {IdString::MakeGlobal("SAFE_KEY"), {"SAFE", kSubscriptWithKey}},
              {IdString::MakeGlobal("SAFE_OFFSET"),
               {"SAFE", kSubscriptWithOffset}},
              {IdString::MakeGlobal("SAFE_ORDINAL"),
               {"SAFE", kSubscriptWithOrdinal}}};

      // The function name is correctly resolved, then proceed to resolve the
      // argument.
      if (auto iter = kNameToSubscript->find(name);
          iter != kNameToSubscript->end()) {
        *function_name_path = iter->second;
        if (ast_function_call->arguments().size() != 1) {
          return MakeSqlErrorAt(ast_position)
                 << "Subscript access using [" << *original_wrapper_name
                 << "()] on value of type "
                 << resolved_lhs->type()->ShortTypeName(product_mode())
                 << " only support one argument";
        }
        *unwrapped_ast_position_expr = ast_function_call->arguments()[0];
        ZETASQL_RETURN_IF_ERROR(ResolveExpr(*unwrapped_ast_position_expr,
                                    expr_resolution_info, resolved_expr_out));
        return absl::OkStatus();
      }
    }
  }

  // If we get here then the subscript operator argument is a normal expression,
  // not one of the special cases above.
  function_name_path->push_back(std::string(kSubscript));
  *unwrapped_ast_position_expr = ast_position;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(*unwrapped_ast_position_expr,
                              expr_resolution_info, resolved_expr_out));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveArrayElementAccess(
    const ResolvedExpr* resolved_array, const ASTExpression* ast_position,
    ExprResolutionInfo* expr_resolution_info, absl::string_view* function_name,
    const ASTExpression** unwrapped_ast_position_expr,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out,
    std::string* original_wrapper_name) {
  ZETASQL_RET_CHECK(resolved_array->type()->IsArray());

  // We expect that array element positions are wrapped like array[offset(x)] or
  // array[ordinal(y)], and we give an error if neither of those wrapper
  // functions is used.  <unwrapped_ast_position_expr> will be set to non-NULL
  // if we find a valid wrapper.
  *unwrapped_ast_position_expr = nullptr;
  if (ast_position->node_kind() == AST_FUNCTION_CALL) {
    const ASTFunctionCall* ast_function_call =
        ast_position->GetAsOrDie<ASTFunctionCall>();
    if (ast_function_call->function()->num_names() == 1 &&
        ast_function_call->arguments().size() == 1 &&
        !ast_function_call->HasModifiers()) {
      const IdString name =
          ast_function_call->function()->first_name()->GetAsIdString();
      *original_wrapper_name = name.ToString();
      static const IdStringHashMapCase<std::string>* name_to_function =
          new IdStringHashMapCase<std::string>{
              {IdString::MakeGlobal("KEY"), kProtoMapAtKey},
              {IdString::MakeGlobal("OFFSET"), kArrayAtOffset},
              {IdString::MakeGlobal("ORDINAL"), kArrayAtOrdinal},
              {IdString::MakeGlobal("SAFE_KEY"), kSafeProtoMapAtKey},

              {IdString::MakeGlobal("SAFE_OFFSET"), kSafeArrayAtOffset},
              {IdString::MakeGlobal("SAFE_ORDINAL"), kSafeArrayAtOrdinal}};
      const std::string* function_name_str =
          zetasql_base::FindOrNull(*name_to_function, name);
      if (function_name_str != nullptr) {
        *function_name = *function_name_str;
        *unwrapped_ast_position_expr = ast_function_call->arguments()[0];
      }
    }
  }

  if (*unwrapped_ast_position_expr == nullptr) {
    return MakeSqlErrorAt(ast_position)
           << "Array element access with array[position] is not supported. "
              "Use array[OFFSET(zero_based_offset)] or "
              "array[ORDINAL(one_based_ordinal)]";
  }

  ZETASQL_RETURN_IF_ERROR(ResolveExpr(*unwrapped_ast_position_expr,
                              expr_resolution_info, resolved_expr_out));

  const bool is_map_at =
      *function_name == kProtoMapAtKey || *function_name == kSafeProtoMapAtKey;
  if (is_map_at) {
    analyzer_output_properties_.MarkRelevant(REWRITE_PROTO_MAP_FNS);
  }

  // Coerce to INT64 if necessary.
  if (!is_map_at) {
    ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
        *unwrapped_ast_position_expr, types::Int64Type(), kImplicitCoercion,
        "Array position in [] must be coercible to $0 type, but has type $1",
        resolved_expr_out));
  }

  if (is_map_at) {
    if (!IsProtoMap(resolved_array->type())) {
      return MakeSqlErrorAt(ast_position)
             << "Only proto maps can be accessed using KEY or SAFE_KEY; tried "
             << "to use map accessor on "
             << resolved_array->type()->ShortTypeName(product_mode());
    }

    const ProtoType* proto_type =
        resolved_array->type()->AsArray()->element_type()->AsProto();

    const Type* key_type;
    ZETASQL_RETURN_IF_ERROR(
        type_factory_->GetProtoFieldType(proto_type->map_key(), &key_type));
    ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
        *unwrapped_ast_position_expr, key_type, kImplicitCoercion,
        "Map key in [] must be coercible to type $0, but has type $1",
        resolved_expr_out));
  }

  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveCaseNoValueExpression(
    const ASTCaseNoValueExpression* case_no_value,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;
  std::vector<const ASTExpression*> ast_arguments;
  ZETASQL_RETURN_IF_ERROR(ResolveExpressionArguments(
      expr_resolution_info, case_no_value->arguments(), {}, &resolved_arguments,
      &ast_arguments));

  std::vector<const ASTNode*> arg_locations =
      ToLocations(absl::Span<const ASTExpression* const>(ast_arguments));
  if (case_no_value->arguments().size() % 2 == 0) {
    // Missing an ELSE expression.  Add a NULL literal to the arguments
    // for resolution. The literal has no parse location.
    resolved_arguments.push_back(
        MakeResolvedLiteralWithoutLocation(Value::NullInt64()));
    // Need a location for the added NULL, but it probably won't ever be used.
    arg_locations.push_back(case_no_value);
  }

  return ResolveFunctionCallWithResolvedArguments(
      case_no_value, arg_locations, "$case_no_value",
      std::move(resolved_arguments), /*named_arguments=*/{},
      expr_resolution_info, resolved_expr_out);
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveCaseValueExpression(
    const ASTCaseValueExpression* case_value,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;
  std::vector<const ASTExpression*> ast_arguments;
  ZETASQL_RETURN_IF_ERROR(
      ResolveExpressionArguments(expr_resolution_info, case_value->arguments(),
                                 {}, &resolved_arguments, &ast_arguments));

  std::vector<const ASTNode*> arg_locations =
      ToLocations(absl::Span<const ASTExpression* const>(ast_arguments));
  if (case_value->arguments().size() % 2 == 1) {
    // Missing an ELSE expression.  Add a NULL literal to the arguments
    // for resolution. No parse location.
    resolved_arguments.push_back(
        MakeResolvedLiteralWithoutLocation(Value::NullInt64()));
    // Need a location for the added NULL, but it probably won't ever be used.
    arg_locations.push_back(case_value);
  }

  return ResolveFunctionCallWithResolvedArguments(
      case_value, arg_locations, "$case_with_value",
      std::move(resolved_arguments), /*named_arguments=*/{},
      expr_resolution_info, resolved_expr_out);
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveExtractExpression(
    const ASTExtractExpression* extract_expression,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;
  std::vector<const ASTNode*> arg_locations;

  ZETASQL_RETURN_IF_ERROR(ResolveExpressionArgument(extract_expression->rhs_expr(),
                                            expr_resolution_info,
                                            &resolved_arguments));
  arg_locations.push_back(extract_expression->rhs_expr());

  if (resolved_arguments[0]->type()->IsProto()) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_EXTRACT_FROM_PROTO)) {
      return MakeSqlErrorAt(extract_expression)
             << "EXTRACT from PROTO is not supported";
    }
    if (extract_expression->time_zone_expr() != nullptr) {
      return MakeSqlErrorAt(extract_expression)
             << "EXTRACT from PROTO does not support AT TIME ZONE";
    }
    return ResolveProtoExtractExpression(extract_expression->lhs_expr(),
                                         std::move(resolved_arguments[0]),
                                         resolved_expr_out);
  }

  functions::DateTimestampPart date_part;
  std::unique_ptr<const ResolvedExpr> resolved_date_part_argument;
  absl::Status resolved_datepart_status = ResolveDatePartArgument(
      extract_expression->lhs_expr(), &resolved_date_part_argument, &date_part);
  if (!resolved_datepart_status.ok()) {
    // We prioritize invalid argument type error over invalid date part
    // name error for the EXTRACT function.
    if (!resolved_arguments[0]->type()->IsTimestamp() &&
        !resolved_arguments[0]->type()->IsCivilDateOrTimeType() &&
        !resolved_arguments[0]->type()->IsProto()) {
      return MakeSqlErrorAt(extract_expression->rhs_expr())
             << "EXTRACT does not support arguments of type: "
             << resolved_arguments[0]->type()->ShortTypeName(product_mode());
    }
    return resolved_datepart_status;
  }

  // If extracting the DATE date part, the TIME date part, or the
  // DATETIME date part, check the date part is compatible, and they will be
  // eventually resolved as '$extract_date', '$extract_time' and
  // '$extract_datetime', respectively; Otherwise, add the date part as an
  // argument for '$extract'.
  if (date_part == functions::DATE || date_part == functions::TIME) {
    if (date_part == functions::TIME &&
        !language().LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME)) {
      return MakeSqlErrorAt(extract_expression->lhs_expr())
             << "Unknown date part 'TIME'";
    }
    if (resolved_arguments[0]->type()->IsDate() ||
        resolved_arguments[0]->type()->IsTime()) {
      // We cannot extract a DATE or TIME from a DATE or TIME, provide a custom
      // error message consistent with CheckExtractPostResolutionArguments().
      return MakeSqlErrorAt(extract_expression)
             << ExtractingNotSupportedDatePart(
                    resolved_arguments[0]->type()->ShortTypeName(
                        product_mode()),
                    functions::DateTimestampPartToSQL(date_part));
    }
  } else if (date_part == functions::DATETIME) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME)) {
      return MakeSqlErrorAt(extract_expression->lhs_expr())
             << "Unknown date part 'DATETIME'";
    }
    if (!resolved_arguments[0]->type()->IsTimestamp()) {
      // We can only extract a DATETIME from a TIMESTAMP, provide a custom
      // error message consistent with CheckExtractPostResolutionArguments().
      return MakeSqlErrorAt(extract_expression)
             << ExtractingNotSupportedDatePart(
                    resolved_arguments[0]->type()->ShortTypeName(
                        product_mode()),
                    "DATETIME");
    }
  } else {
    resolved_arguments.push_back(std::move(resolved_date_part_argument));
    arg_locations.push_back(extract_expression->lhs_expr());
  }

  if (extract_expression->time_zone_expr() != nullptr) {
    // TODO: Consider always adding the 'name' of the default time
    // zone to the arguments if time_zone_expr() == nullptr and make the
    // time zone argument required, rather than let it be optional as it is
    // now (so engines must use their default time zone if not explicitly in
    // the query).  The default absl::TimeZone in the analyzer options has a
    // Name() that can be used if necessary.  Current implementations do not
    // support the time zone argument yet, so requiring it will break them
    // until the new argument is supported in all the implementations.
    if (resolved_arguments[0]->type()->IsCivilDateOrTimeType()) {
      return MakeSqlErrorAt(extract_expression)
             << "EXTRACT from "
             << resolved_arguments[0]->type()->ShortTypeName(product_mode())
             << " does not support AT TIME ZONE";
    }
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpressionArgument(extract_expression->time_zone_expr(),
                                  expr_resolution_info, &resolved_arguments));
    arg_locations.push_back(extract_expression->time_zone_expr());
  }

  std::string function_name;
  switch (date_part) {
    case functions::DATE:
      function_name = "$extract_date";
      break;
    case functions::TIME:
      function_name = "$extract_time";
      break;
    case functions::DATETIME:
      function_name = "$extract_datetime";
      break;
    case functions::YEAR:
    case functions::MONTH:
    case functions::DAY:
    case functions::DAYOFWEEK:
    case functions::DAYOFYEAR:
    case functions::QUARTER:
    case functions::HOUR:
    case functions::MINUTE:
    case functions::SECOND:
    case functions::MILLISECOND:
    case functions::MICROSECOND:
    case functions::NANOSECOND:
    case functions::WEEK:
    case functions::WEEK_MONDAY:
    case functions::WEEK_TUESDAY:
    case functions::WEEK_WEDNESDAY:
    case functions::WEEK_THURSDAY:
    case functions::WEEK_FRIDAY:
    case functions::WEEK_SATURDAY:
    case functions::ISOWEEK:
    case functions::ISOYEAR:
      function_name = "$extract";
      break;
    default:
      break;
  }
  // ZETASQL_CHECK should never fail because the date_part should always be valid now.
  ZETASQL_RET_CHECK(!function_name.empty());

  return ResolveFunctionCallWithResolvedArguments(
      extract_expression, arg_locations, function_name,
      std::move(resolved_arguments), /*named_arguments=*/{},
      expr_resolution_info, resolved_expr_out);
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveNewConstructor(
    const ASTNewConstructor* ast_new_constructor,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  const Type* resolved_type;
  ZETASQL_RET_CHECK(ast_new_constructor->type_name()->type_parameters() == nullptr)
      << "The parser does not support type parameters in new constructor "
         "syntax";
  ZETASQL_RETURN_IF_ERROR(ResolveSimpleType(ast_new_constructor->type_name(),
                                    "new constructors", &resolved_type,
                                    /*resolved_type_params=*/nullptr));

  if (!resolved_type->IsProto()) {
    return MakeSqlErrorAt(ast_new_constructor->type_name())
           << "NEW constructors are not allowed for type "
           << resolved_type->ShortTypeName(product_mode());
  }

  std::vector<ResolvedBuildProtoArg> arguments;
  for (int i = 0; i < ast_new_constructor->arguments().size(); ++i) {
    const ASTNewConstructorArg* ast_arg = ast_new_constructor->argument(i);

    std::unique_ptr<const ResolvedExpr> expr;
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpr(ast_arg->expression(), expr_resolution_info, &expr));

    std::unique_ptr<AliasOrASTPathExpression> alias_or_ast_path_expr;
    if (ast_arg->optional_identifier() != nullptr) {
      alias_or_ast_path_expr = absl::make_unique<AliasOrASTPathExpression>(
          ast_arg->optional_identifier()->GetAsIdString());
    } else if (ast_arg->optional_path_expression() != nullptr) {
      if (!language().LanguageFeatureEnabled(
              FEATURE_V_1_2_PROTO_EXTENSIONS_WITH_NEW)) {
        return MakeSqlErrorAt(ast_arg->optional_path_expression())
               << "NEW constructor does not support proto extensions";
      }
      alias_or_ast_path_expr = absl::make_unique<AliasOrASTPathExpression>(
          ast_arg->optional_path_expression());
    } else {
      alias_or_ast_path_expr = absl::make_unique<AliasOrASTPathExpression>(
          GetAliasForExpression(ast_arg->expression()));
    }
    if (alias_or_ast_path_expr->kind() == AliasOrASTPathExpression::ALIAS) {
      const IdString alias = alias_or_ast_path_expr->alias();
      if (alias.empty() || IsInternalAlias(alias)) {
        return MakeSqlErrorAt(ast_arg)
               << "Cannot construct proto because argument " << (i + 1)
               << " does not have an alias to match to a field name";
      }
    }

    arguments.emplace_back(ast_arg->expression(), std::move(expr),
                           std::move(alias_or_ast_path_expr));
  }

  ZETASQL_RETURN_IF_ERROR(ResolveBuildProto(
      ast_new_constructor->type_name(), resolved_type->AsProto(),
      /*input_scan=*/nullptr, "Argument", "Constructor", &arguments,
      resolved_expr_out));
  return absl::OkStatus();
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveArrayConstructor(
    const ASTArrayConstructor* ast_array_constructor,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  const ArrayType* array_type = nullptr;
  // Indicates whether the array constructor has an explicit element type.
  bool has_explicit_type = false;
  if (ast_array_constructor->type() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveArrayType(ast_array_constructor->type(),
                                     "literal value construction", &array_type,
                                     /*resolved_type_params=*/nullptr));
    has_explicit_type = true;
  }

  // Resolve all the element expressions and collect the set of element types.
  InputArgumentTypeSet element_type_set;
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_elements;
  resolved_elements.reserve(ast_array_constructor->elements().size());
  for (const ASTExpression* element : ast_array_constructor->elements()) {
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(ResolveExpr(element, expr_resolution_info, &resolved_expr));

    if (resolved_expr->type()->IsArray()) {
      return MakeSqlErrorAt(element)
             << "Cannot construct array with element type "
             << resolved_expr->type()->ShortTypeName(product_mode())
             << " because nested arrays are not supported";
    }

    // If at least one element looks like an expression, the array's type should
    // be as firm as an expression's type for supertyping, function signature
    // matching, etc.
    if (resolved_expr->node_kind() == RESOLVED_LITERAL &&
        resolved_expr->GetAs<ResolvedLiteral>()->has_explicit_type()) {
      has_explicit_type = true;
    }

    element_type_set.Insert(GetInputArgumentTypeForExpr(resolved_expr.get()));
    resolved_elements.push_back(std::move(resolved_expr));
  }

  // If array type is not explicitly mentioned, use the common supertype of the
  // element type set as the array element type.
  if (array_type == nullptr) {
    if (resolved_elements.empty()) {
      // If neither array_type nor element is specified, empty array [] is
      // always resolved as an ARRAY<int64_t>, which is similar to resolving
      // NULL. The coercion rules may make the empty array [] change type.
      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(type_factory_->get_int64(),
                                                   &array_type));
    } else {
      const Type* super_type = nullptr;
      ZETASQL_RETURN_IF_ERROR(
          coercer_.GetCommonSuperType(element_type_set, &super_type));
      if (super_type == nullptr) {
        // We need a special case for a literal array with a mix of INT64 and
        // UINT64 arguments.  Normally, there is no supertype.  But given that
        // we want to allow array literals like '[999999999999999999999999,1]',
        // we will define the array type as UINT64.  There still might be a
        // negative INT64 and the array literal may still not work, but it
        // enables the previous use case.
        bool all_integer_literals = true;
        for (const InputArgumentType& argument_type :
             element_type_set.arguments()) {
          if (argument_type.is_untyped()) {
            continue;
          }
          if (!argument_type.is_literal() ||
              !argument_type.type()->IsInteger()) {
            all_integer_literals = false;
            break;
          }
        }
        if (!all_integer_literals) {
          return MakeSqlErrorAt(ast_array_constructor)
                 << "Array elements of types " << element_type_set.ToString()
                 << " do not have a common supertype";
        }
        super_type = type_factory_->get_uint64();
      }
      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(super_type, &array_type));
    }
  }

  // Add casts or convert literal values to array element type if necessary.
  bool is_array_literal = true;
  for (int i = 0; i < resolved_elements.size(); ++i) {
    // We keep the original <type_annotation_map> when coercing array element.
    ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
        ast_array_constructor->element(i),
        AnnotatedType(array_type->element_type(),
                      resolved_elements[i]->type_annotation_map()),
        kImplicitCoercion, "Array element type $1 does not coerce to $0",
        &resolved_elements[i]));

    if (resolved_elements[i]->node_kind() != RESOLVED_LITERAL) {
      is_array_literal = false;
    }
  }

  if (is_array_literal) {
    std::vector<Value> element_values;
    element_values.reserve(resolved_elements.size());
    for (int i = 0; i < resolved_elements.size(); ++i) {
      const ResolvedExpr* element = resolved_elements[i].get();
      ZETASQL_RET_CHECK_EQ(element->node_kind(), RESOLVED_LITERAL)
          << element->DebugString();
      element_values.emplace_back(element->GetAs<ResolvedLiteral>()->value());
    }
    *resolved_expr_out = MakeResolvedLiteral(
        ast_array_constructor, array_type,
        Value::Array(array_type, element_values), has_explicit_type);
  } else {
    const std::vector<const ASTNode*> ast_element_locations =
        ToLocations(ast_array_constructor->elements());
    std::unique_ptr<ResolvedFunctionCall> resolved_function_call;

    ZETASQL_RETURN_IF_ERROR(function_resolver_->ResolveGeneralFunctionCall(
        ast_array_constructor, ast_element_locations, "$make_array",
        /*is_analytic=*/false, std::move(resolved_elements),
        /*named_arguments=*/{}, array_type, &resolved_function_call));
    *resolved_expr_out = std::move(resolved_function_call);
  }
  return absl::OkStatus();
}

void Resolver::TryCollapsingExpressionsAsLiterals(
    const ASTNode* ast_location,
    std::unique_ptr<const ResolvedNode>* node_ptr) {
  // We collect mutable pointers to all the nodes present inside 'node_ptr'
  // during which we ensure that all the children nodes come after their
  // parents (index-wise).
  std::vector<std::unique_ptr<const ResolvedNode>*> mutable_child_node_ptrs;
  mutable_child_node_ptrs.push_back(node_ptr);
  int consumed = 0;
  while (consumed < mutable_child_node_ptrs.size()) {
    // Remove constness so that we can modify the structure of the given
    // resolved node bottom-up. This will not change any semantics and is needed
    // to mutate the node pointers to point to their newly collapsed literal
    // form.
    ResolvedNode* node =
        const_cast<ResolvedNode*>(mutable_child_node_ptrs[consumed]->get());
    node->AddMutableChildNodePointers(&mutable_child_node_ptrs);
    consumed++;
  }

  // We traverse the vector 'mutable_child_node_ptr' from the back so that we
  // process the tree inside 'node_ptr' bottom-up. This also ensures that
  // whenever we mutate a pointer X, all of X's children have already been
  // processed.
  while (!mutable_child_node_ptrs.empty()) {
    std::unique_ptr<const ResolvedNode>* mutable_node_ptr =
        mutable_child_node_ptrs.back();
    const ResolvedNode* node = mutable_node_ptr->get();
    mutable_child_node_ptrs.pop_back();

    // Collapse the ResolvedMakeStruct into a struct literal if all the field
    // expressions inside the struct are literals.
    if (node->node_kind() == RESOLVED_MAKE_STRUCT) {
      const ResolvedMakeStruct* make_struct = node->GetAs<ResolvedMakeStruct>();
      std::vector<Value> literal_values;
      bool is_struct_literal = true;
      for (const auto& field_expr : make_struct->field_list()) {
        is_struct_literal =
            is_struct_literal && field_expr->node_kind() == RESOLVED_LITERAL;
        if (is_struct_literal) {
          literal_values.push_back(
              field_expr->GetAs<ResolvedLiteral>()->value());
        } else {
          // Break early from the loop if not a struct literal.
          break;
        }
      }

      if (is_struct_literal) {
        *mutable_node_ptr = MakeResolvedLiteral(
            ast_location, make_struct->type(),
            Value::Struct(make_struct->type()->AsStruct(), literal_values),
            /*has_explicit_type=*/false);
      }
    } else if (node->node_kind() == RESOLVED_FUNCTION_CALL &&
               node->GetAs<ResolvedFunctionCall>()->function()->Name() ==
                   "$make_array") {
      // Collapse the $make_array(...) into an array literal if all its element
      // fields are literals. This mostly handles the case where there is an
      // array of newly formed struct literals (after collapsing
      // ResolvedMakeStructs).
      const ResolvedFunctionCall* make_array =
          node->GetAs<ResolvedFunctionCall>();
      std::vector<Value> literal_values;
      bool is_array_literal = true;
      for (int i = 0; i < make_array->argument_list().size(); ++i) {
        is_array_literal =
            is_array_literal &&
            make_array->argument_list()[i]->node_kind() == RESOLVED_LITERAL;
        if (is_array_literal) {
          const ResolvedLiteral* argument_literal =
              make_array->argument_list()[i]->GetAs<ResolvedLiteral>();
          literal_values.push_back(argument_literal->value());
        } else {
          // Break early from the loop if not an array literal.
          break;
        }
      }

      if (is_array_literal) {
        *mutable_node_ptr = MakeResolvedLiteral(
            ast_location, make_array->type(),
            Value::Array(make_array->type()->AsArray(), literal_values),
            /*has_explicit_type=*/false);
      }
    }
  }
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveStructConstructorWithParens(
    const ASTStructConstructorWithParens* ast_struct_constructor,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  return ResolveStructConstructorImpl(
      ast_struct_constructor,
      /*ast_struct_type=*/nullptr, ast_struct_constructor->field_expressions(),
      {} /* ast_field_aliases */, expr_resolution_info, resolved_expr_out);
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveStructConstructorWithKeyword(
    const ASTStructConstructorWithKeyword* ast_struct_constructor,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  std::vector<const ASTExpression*> ast_field_expressions;
  std::vector<const ASTAlias*> ast_field_aliases;
  for (const ASTStructConstructorArg* arg : ast_struct_constructor->fields()) {
    ast_field_expressions.push_back(arg->expression());
    ast_field_aliases.push_back(arg->alias());
  }

  return ResolveStructConstructorImpl(ast_struct_constructor,
                                      ast_struct_constructor->struct_type(),
                                      ast_field_expressions, ast_field_aliases,
                                      expr_resolution_info, resolved_expr_out);
}

absl::Status Resolver::ResolveStructConstructorImpl(
    const ASTNode* ast_location, const ASTStructType* ast_struct_type,
    const absl::Span<const ASTExpression* const> ast_field_expressions,
    const absl::Span<const ASTAlias* const> ast_field_aliases,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  if (!ast_field_aliases.empty()) {
    ZETASQL_RET_CHECK_EQ(ast_field_expressions.size(), ast_field_aliases.size());
  }

  // If we have a type from the AST, use it.  Otherwise, we'll collect field
  // names and types and make a new StructType below.
  const StructType* struct_type = nullptr;
  if (ast_struct_type != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveStructType(ast_struct_type, "literal value construction",
                          &struct_type, /*resolved_type_params=*/nullptr));
  }

  bool struct_has_explicit_type = false;

  if (struct_type != nullptr) {
    if (struct_type->num_fields() != ast_field_expressions.size()) {
      return MakeSqlErrorAt(ast_struct_type)
             << "STRUCT type has " << struct_type->num_fields()
             << " fields but constructor call has "
             << ast_field_expressions.size() << " fields";
    }
    // The struct has an explicit type, for example STRUCT<int32_t, int64_t>(1, 2).
    struct_has_explicit_type = true;
  }

  // Resolve all the field expressions.
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_field_expressions;
  std::vector<StructType::StructField> struct_fields;

  // We create a struct literal only when all of its fields are literal
  // coercible, so that we can later treat the struct literal as field-wise
  // literal coercible.
  // NOTE: This does not change any behavior, i.e. even if we don't collapse the
  // MakeStruct into a ResolvedLiteral, we will still treat it as field-wise
  // literal coercible (for appropriate fields) by looking underneath the
  // MakeStruct.
  bool is_struct_literal = true;

  // Identifies whether all the struct fields have an explicit type.
  // Initialized to false so that non-explicitly typed structs with
  // no fields are not marked as has_explicit_type.
  bool all_fields_have_explicit_type = false;

  // The has_explicit_type() property of the first field in the struct.
  // Only relevant for a literal field.
  bool first_field_has_explicit_type = false;

  // Only filled if <is_struct_literal>.
  std::vector<Value> literal_values;

  for (int i = 0; i < ast_field_expressions.size(); ++i) {
    const ASTExpression* ast_expression = ast_field_expressions[i];
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpr(ast_expression, expr_resolution_info, &resolved_expr));

    if (struct_type != nullptr) {
      // If we have a target type, try to coerce the field value to that type.
      const Type* target_field_type = struct_type->field(i).type;
      if (!resolved_expr->type()->Equals(target_field_type) ||
          // TODO: The condition below will be comparing the input
          // field collation and target collation after
          // STRUCT<STRING COLLATE '...'> syntax is supported.
          CollationAnnotation::ExistsIn(resolved_expr->type_annotation_map())) {
        SignatureMatchResult result;
        const InputArgumentType input_argument_type =
            GetInputArgumentTypeForExpr(resolved_expr.get());
        if (!coercer_.CoercesTo(input_argument_type, target_field_type,
                                /*is_explicit=*/false, &result)) {
          return MakeSqlErrorAt(ast_expression)
                 << "Struct field " << (i + 1) << " has type "
                 << input_argument_type.DebugString()
                 << " which does not coerce to "
                 << target_field_type->ShortTypeName(product_mode());
        }

        // We cannot use CoerceExprToType in this case because we need to
        // propagate 'struct_has_explicit_type' so that NULL and [] literals
        // are typed correctly.
        ZETASQL_RETURN_IF_ERROR(function_resolver_->AddCastOrConvertLiteral(
            ast_expression, target_field_type, /*format=*/nullptr,
            /*time_zone=*/nullptr, TypeParameters(),
            /*scan=*/nullptr, struct_has_explicit_type,
            /*return_null_on_error=*/false, &resolved_expr));
      }

      // For STRUCT<TYPE> constructors, we mark the field literals with
      // has_explicit_type=true so that they do not coerce further like
      // literals.
      if (resolved_expr->node_kind() == RESOLVED_LITERAL) {
        resolved_expr = MakeResolvedLiteral(
            ast_expression, resolved_expr->type(),
            resolved_expr->GetAs<ResolvedLiteral>()->value(),
            struct_has_explicit_type);
      }

      if (!ast_field_aliases.empty() && ast_field_aliases[i] != nullptr) {
        return MakeSqlErrorAt(ast_field_aliases[i])
               << "STRUCT constructors cannot specify both an explicit "
                  "type and field names with AS";
      }
    } else {
      // Otherwise, we need to compute the struct field type to create.
      IdString alias;
      if (ast_field_aliases.empty()) {
        // We are in the (...) construction syntax and will always
        // generate anonymous fields.
      } else {
        // We are in the STRUCT(...) constructor syntax and will use the
        // explicitly provided aliases, or try to infer aliases from the
        // expression.
        if (ast_field_aliases[i] != nullptr) {
          alias = ast_field_aliases[i]->GetAsIdString();
        } else {
          alias = GetAliasForExpression(ast_field_expressions[i]);
        }
      }
      struct_fields.push_back({alias.ToString(), resolved_expr->type()});
    }

    // We create a struct literal if all the fields are literals, and either
    // <struct_has_explicit_type> (all fields will be coerced to the
    // explicit struct field types with the resulting struct marked as
    // has_explicit_type) or all the fields' has_explicit_type-ness
    // is the same.
    if (is_struct_literal) {
      if (resolved_expr->node_kind() != RESOLVED_LITERAL) {
        is_struct_literal = false;
      } else {
        const ResolvedLiteral* resolved_literal =
            resolved_expr->GetAs<ResolvedLiteral>();
        // We cannot create a literal if there is a non-explicit NULL
        // literal, since that can implicitly coerce to any type and we have
        // no way to represent that in the field Value of a Struct Value.
        // For example, consider the literal struct 'STRUCT(NULL, NULL)'.
        // If we create a Value of type STRUCT<int64_t, int64_t> then want to
        // coerce that to STRUCT<int64_t, struct<string, int32_t>> then it will
        // fail since the second field of type int64_t cannot coerce to the
        // second field of the target type struct<string, int32_t>.
        if (struct_type == nullptr) {
          // If struct_type == nullptr then we are not forcing this to be an
          // explicitly typed struct.  Otherwise, we *can* allow this to
          // be a struct literal regardless of whether a null field is
          // explicitly typed or not.
          if (resolved_literal->value().is_null() &&
              !resolved_literal->has_explicit_type()) {
            is_struct_literal = false;
          }
          if (i == 0) {
            first_field_has_explicit_type =
                resolved_literal->has_explicit_type();
            all_fields_have_explicit_type =
                resolved_literal->has_explicit_type();
          } else {
            if (!resolved_literal->has_explicit_type()) {
              all_fields_have_explicit_type = false;
            }
            if (first_field_has_explicit_type !=
                resolved_literal->has_explicit_type()) {
              // If two fields do not have the same has_explicit_type property,
              // then do not create a struct literal for this.
              is_struct_literal = false;
              literal_values.clear();
            }
          }
        }
      }
    }

    if (is_struct_literal) {
      literal_values.push_back(
          resolved_expr->GetAs<ResolvedLiteral>()->value());
    }

    resolved_field_expressions.push_back(std::move(resolved_expr));
  }

  if (struct_type == nullptr) {
    ZETASQL_RET_CHECK_EQ(struct_fields.size(), ast_field_expressions.size());
    ZETASQL_RETURN_IF_ERROR(type_factory_->MakeStructType(struct_fields, &struct_type));
  }

  if (is_struct_literal) {
    ZETASQL_RET_CHECK_EQ(struct_type->num_fields(), literal_values.size());
    *resolved_expr_out = MakeResolvedLiteral(
        ast_location, struct_type, Value::Struct(struct_type, literal_values),
        struct_has_explicit_type || all_fields_have_explicit_type);
  } else {
    auto node = MakeResolvedMakeStruct(struct_type,
                                       std::move(resolved_field_expressions));
    if (ast_struct_type != nullptr) {
      MaybeRecordParseLocation(ast_struct_type, node.get());
    }
    ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(
        /*error_node=*/ast_struct_type, node.get()));
    *resolved_expr_out = std::move(node);
  }
  return absl::OkStatus();
}

ResolvedNonScalarFunctionCallBase::NullHandlingModifier
Resolver::ResolveNullHandlingModifier(
    ASTFunctionCall::NullHandlingModifier ast_null_handling_modifier) {
  switch (ast_null_handling_modifier) {
    case ASTFunctionCall::DEFAULT_NULL_HANDLING:
      return ResolvedNonScalarFunctionCallBase::DEFAULT_NULL_HANDLING;
    case ASTFunctionCall::IGNORE_NULLS:
      return ResolvedNonScalarFunctionCallBase::IGNORE_NULLS;
    case ASTFunctionCall::RESPECT_NULLS:
      return ResolvedNonScalarFunctionCallBase::RESPECT_NULLS;
      // No "default:". Let the compilation fail in case an entry is added to
      // the enum without being handled here.
  }
}

absl::Status Resolver::FinishResolvingAggregateFunction(
    const ASTFunctionCall* ast_function_call,
    std::unique_ptr<ResolvedFunctionCall>* resolved_function_call,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedScan> with_group_rows_subquery,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>
        with_group_rows_correlation_references,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  ZETASQL_DCHECK(expr_resolution_info != nullptr);
  QueryResolutionInfo* const query_resolution_info =
      expr_resolution_info->query_resolution_info;
  const Function* function = (*resolved_function_call)->function();

  if (!expr_resolution_info->allows_aggregation) {
    return MakeSqlErrorAt(ast_function_call)
           << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
           << " not allowed in " << expr_resolution_info->clause_name
           << (expr_resolution_info->is_post_distinct()
                   ? " after SELECT DISTINCT"
                   : "");
  } else if (query_resolution_info == nullptr) {
    return MakeSqlErrorAt(ast_function_call)
           << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
           << " not expected";
  }

  const std::vector<std::unique_ptr<const ResolvedExpr>>& arg_list =
      (*resolved_function_call)->argument_list();
  if (ast_function_call->distinct()) {
    if (!function->SupportsDistinctModifier()) {
      return MakeSqlErrorAt(ast_function_call)
             << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
             << " does not support DISTINCT in arguments";
    }

    if (arg_list.empty()) {
      return MakeSqlErrorAt(ast_function_call)
             << "DISTINCT function call with no arguments is not allowed";
    }

    for (const std::unique_ptr<const ResolvedExpr>& argument : arg_list) {
      std::string no_grouping_type;
      if (!TypeSupportsGrouping(argument->type(), &no_grouping_type)) {
        return MakeSqlErrorAt(ast_function_call)
               << "Aggregate functions with DISTINCT cannot be used with "
                  "arguments of type "
               << no_grouping_type;
      }
    }
  }

  std::vector<std::unique_ptr<const ResolvedOrderByItem>>
      resolved_order_by_items;

  const ASTOrderBy* order_by_arguments = ast_function_call->order_by();
  if (order_by_arguments != nullptr) {
    if (!language().LanguageFeatureEnabled(
            FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE)) {
      return MakeSqlErrorAt(order_by_arguments)
             << "ORDER BY in aggregate function is not supported";
    }

    // Checks whether the function supports ordering in arguments if
    // there is an order by specified.
    if (!function->SupportsOrderingArguments()) {
      return MakeSqlErrorAt(order_by_arguments)
             << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
             << " does not support ORDER BY in arguments";
    }

    if (arg_list.empty()) {
      return MakeSqlErrorAt(order_by_arguments)
             << "ORDER BY in aggregate function call with no arguments "
             << "is not allowed";
    }

    // Resolves the ordering expression in arguments.
    std::vector<OrderByItemInfo> order_by_info;
    ZETASQL_RETURN_IF_ERROR(
        ResolveOrderingExprs(order_by_arguments->ordering_expressions(),
                             expr_resolution_info, &order_by_info));

    // Checks if there is any order by index.
    // Supporting order by index here makes little sense as the function
    // arguments are not resolved to columns as opposed to the order-by
    // after a select. The Postgres spec has the same restriction.
    for (const OrderByItemInfo& item_info : order_by_info) {
      if (item_info.is_select_list_index()) {
        return MakeSqlErrorAt(order_by_arguments)
               << "Aggregate functions do not allow ORDER BY by index"
               << " in arguments";
      }
    }

    if (ast_function_call->distinct()) {
      // If both DISTINCT and ORDER BY are present, the ORDER BY arguments
      // must be a subset of the DISTINCT arguments.
      for (const OrderByItemInfo& item_info : order_by_info) {
        bool is_order_by_argument_matched = false;
        bool is_same_expr;
        for (const auto& argument : arg_list) {
          ZETASQL_ASSIGN_OR_RETURN(is_same_expr, IsSameExpressionForGroupBy(
                                             argument.get(),
                                             item_info.order_expression.get()));
          if (is_same_expr) {
            is_order_by_argument_matched = true;
            break;
          }
        }
        if (!is_order_by_argument_matched) {
          return MakeSqlErrorAt(item_info.ast_location)
                 << "An aggregate function that has both DISTINCT and ORDER BY"
                 << " arguments can only ORDER BY expressions that are"
                 << " arguments to the function";
        }
      }
    }

    AddColumnsForOrderByExprs(
        kOrderById, &order_by_info,
        query_resolution_info
            ->select_list_columns_to_compute_before_aggregation());

    // We may have precomputed some ORDER BY expression columns before
    // aggregation.  If any aggregate function arguments match those
    // precomputed columns, then update the argument to reference the
    // precomputed column (and avoid recomputing the argument expression).
    if (!query_resolution_info
             ->select_list_columns_to_compute_before_aggregation()
             ->empty()) {
      std::vector<std::unique_ptr<const ResolvedExpr>> updated_args =
          (*resolved_function_call)->release_argument_list();
      bool is_same_expr;
      for (int arg_idx = 0; arg_idx < updated_args.size(); ++arg_idx) {
        for (const std::unique_ptr<const ResolvedComputedColumn>&
                 computed_column :
             *query_resolution_info
                  ->select_list_columns_to_compute_before_aggregation()) {
          ZETASQL_ASSIGN_OR_RETURN(is_same_expr, IsSameExpressionForGroupBy(
                                             updated_args[arg_idx].get(),
                                             computed_column->expr()));
          if (is_same_expr) {
            updated_args[arg_idx] = MakeColumnRef(computed_column->column());
            break;
          }
        }
      }
      (*resolved_function_call)->set_argument_list(std::move(updated_args));
    }
    ZETASQL_RETURN_IF_ERROR(
        ResolveOrderByItems(order_by_arguments, {} /* no output_column_list */,
                            order_by_info, &resolved_order_by_items));
  }
  const ASTLimitOffset* limit_offset = ast_function_call->limit_offset();
  std::unique_ptr<const ResolvedExpr> limit_expr;
  if (limit_offset != nullptr) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_1_LIMIT_IN_AGGREGATE)) {
      return MakeSqlErrorAt(limit_offset)
             << "LIMIT in aggregate function arguments is not supported";
    }

    // Checks whether the function supports limit in arguments if
    // there is a LIMIT specified.
    if (!function->SupportsLimitArguments()) {
      return MakeSqlErrorAt(limit_offset)
             << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
             << " does not support LIMIT in arguments";
    }
    // Returns an error if an OFFSET is specified.
    if (limit_offset->offset() != nullptr) {
      return MakeSqlErrorAt(limit_offset->offset())
             << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
             << " does not support OFFSET in arguments";
    }
    ZETASQL_RET_CHECK(limit_offset->limit() != nullptr);
    ExprResolutionInfo expr_resolution_info(empty_name_scope_.get(), "LIMIT");
    ZETASQL_RETURN_IF_ERROR(ResolveLimitOrOffsetExpr(
        limit_offset->limit(), "LIMIT" /* clause_name */, &expr_resolution_info,
        &limit_expr));
  }

  std::unique_ptr<const ResolvedAggregateHavingModifier> resolved_having;
  const ASTHavingModifier* having_modifier =
      ast_function_call->having_modifier();
  if (having_modifier != nullptr) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_1_HAVING_IN_AGGREGATE)) {
      return MakeSqlErrorAt(having_modifier)
             << "HAVING modifier in aggregate function is not supported";
    }

    if (!function->SupportsHavingModifier()) {
      return MakeSqlErrorAt(having_modifier)
             << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
             << " does not support HAVING in arguments";
    }

    ZETASQL_RETURN_IF_ERROR(ResolveHavingModifier(having_modifier, expr_resolution_info,
                                          &resolved_having));
  }

  const ASTClampedBetweenModifier* clamped_between_modifier =
      ast_function_call->clamped_between_modifier();
  if (clamped_between_modifier != nullptr) {
    // Checks whether the function supports clamped between in arguments if
    // there is a CLAMPED BETWEEN specified.
    if (!function->SupportsClampedBetweenModifier()) {
      return MakeSqlErrorAt(clamped_between_modifier)
             << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
             << " does not support CLAMPED BETWEEN in arguments";
    }
  }

  // Checks if there exists aggregate or analytic function after resolving
  // the aggregate arguments, ORDER BY modifier, and HAVING modifier.
  if (expr_resolution_info->has_aggregation) {
    return MakeSqlErrorAt(ast_function_call)
           << "Aggregations of aggregations are not allowed";
  }
  if (expr_resolution_info->has_analytic) {
    return MakeSqlErrorAt(ast_function_call)
           << "Analytic functions cannot be arguments to aggregate functions";
  }

  expr_resolution_info->has_aggregation = true;

  ResolvedNonScalarFunctionCallBase::NullHandlingModifier
      resolved_null_handling_modifier = ResolveNullHandlingModifier(
          ast_function_call->null_handling_modifier());
  if (resolved_null_handling_modifier !=
      ResolvedNonScalarFunctionCallBase::DEFAULT_NULL_HANDLING) {
    if (!analyzer_options_.language().LanguageFeatureEnabled(
            FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE)) {
      return MakeSqlErrorAt(ast_function_call)
             << "IGNORE NULLS and RESPECT NULLS in aggregate functions are not "
                "supported";
    }
    if (!function->SupportsNullHandlingModifier()) {
      return MakeSqlErrorAt(ast_function_call)
             << "IGNORE NULLS and RESPECT NULLS are not allowed for aggregate "
                "function "
             << function->Name();
    }
  }

  std::shared_ptr<ResolvedFunctionCallInfo> function_call_info = nullptr;
  if (function->Is<TemplatedSQLFunction>()) {
    // TODO: The core of this block is in common with code
    // in function_resolver.cc for handling templated scalar functions.
    // Refactor the common code into common helper functions.
    function_call_info.reset(new ResolvedFunctionCallInfo());
    const TemplatedSQLFunction* sql_function =
        function->GetAs<TemplatedSQLFunction>();
    std::vector<InputArgumentType> input_arguments;
    const absl::Span<const ASTExpression* const> ast_nodes =
        ast_function_call->arguments();
    GetInputArgumentTypesForGenericArgumentList(
        {ast_nodes.begin(), ast_nodes.end()},
        (*resolved_function_call)->argument_list(), &input_arguments);

    // Call the TemplatedSQLFunction::Resolve() method to get the output type.
    // Use a new empty cycle detector, or the cycle detector from this
    // Resolver if we are analyzing one or more templated function calls.
    CycleDetector owned_cycle_detector;
    AnalyzerOptions analyzer_options = analyzer_options_;
    if (analyzer_options.find_options().cycle_detector() == nullptr) {
      analyzer_options.mutable_find_options()->set_cycle_detector(
          &owned_cycle_detector);
    }

    const absl::Status resolve_status =
        function_resolver_->ResolveTemplatedSQLFunctionCall(
            ast_function_call, *sql_function, analyzer_options, input_arguments,
            &function_call_info);

    if (!resolve_status.ok()) {
      // TODO:  This code matches the code from
      // ResolveGeneralFunctionCall() in function_resolver.cc for handling
      // templated scalar SQL functions.  There is similar, but not
      // equivalent code for handling templated TVFs.  We should look
      // at making them all consistent, and maybe implementing the
      // common code through a helper function.
      //
      // The Resolve method returned an error status that is already updated
      // based on the <analyzer_options> ErrorMessageMode.  Make a new
      // ErrorSource based on the <resolve_status>, and return a new error
      // status that indicates that the function call is invalid, while
      // indicating the function call location for the error.
      return WrapNestedErrorStatus(
          ast_function_call,
          absl::StrCat("Invalid function ", sql_function->Name()),
          resolve_status, analyzer_options_.error_message_mode());
    }

    std::unique_ptr<FunctionSignature> new_signature(
        new FunctionSignature((*resolved_function_call)->signature()));

    new_signature->SetConcreteResultType(
        static_cast<const TemplatedSQLFunctionCall*>(function_call_info.get())
            ->expr()
            ->type());

    (*resolved_function_call)->set_signature(*new_signature);
    ZETASQL_RET_CHECK((*resolved_function_call)->signature().IsConcrete())
        << "result_signature: '"
        << (*resolved_function_call)->signature().DebugString() << "'";
  }

  // Build an AggregateFunctionCall to replace the regular FunctionCall.
  std::unique_ptr<ResolvedAggregateFunctionCall> resolved_agg_call =
      MakeResolvedAggregateFunctionCall(
          (*resolved_function_call)->type(), function,
          (*resolved_function_call)->signature(),
          (*resolved_function_call)->release_argument_list(),
          (*resolved_function_call)->release_generic_argument_list(),
          (*resolved_function_call)->error_mode(),
          ast_function_call->distinct(), resolved_null_handling_modifier,
          std::move(resolved_having), std::move(resolved_order_by_items),
          std::move(limit_expr), function_call_info);
  resolved_agg_call->set_with_group_rows_subquery(
      std::move(with_group_rows_subquery));
  resolved_agg_call->set_with_group_rows_parameter_list(
      std::move(with_group_rows_correlation_references));
  resolved_agg_call->set_hint_list(
      (*resolved_function_call)->release_hint_list());
  MaybeRecordFunctionCallParseLocation(ast_function_call,
                                       resolved_agg_call.get());
  ZETASQL_RETURN_IF_ERROR(MaybeResolveCollationForFunctionCallBase(
      /*error_location=*/ast_function_call, resolved_agg_call.get()));
  ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(
      /*error_node=*/ast_function_call, resolved_agg_call.get()));

  // If this <ast_function_call> is the top level function call in
  // <expr_resolution_info> and it has an alias, then use that alias.
  // Otherwise create an internal alias for this expression.
  IdString alias = GetColumnAliasForTopLevelExpression(expr_resolution_info,
                                                       ast_function_call);
  if (alias.empty() || !analyzer_options_.preserve_column_aliases()) {
    alias = MakeIdString(absl::StrCat(
        "$agg",
        query_resolution_info->aggregate_columns_to_compute().size() + 1));
  }

  // Instead of leaving the aggregate ResolvedFunctionCall inline, we pull
  // it out into <query_resolution_info->aggregate_columns_to_compute().
  // The actual ResolvedExpr we return is a ColumnRef pointing to that
  // function call.
  ResolvedColumn aggregate_column(AllocateColumnId(), kAggregateId, alias,
                                  resolved_agg_call->annotated_type());

  query_resolution_info->AddAggregateComputedColumn(
      ast_function_call, MakeResolvedComputedColumn(
                             aggregate_column, std::move(resolved_agg_call)));

  *resolved_expr_out = MakeColumnRef(aggregate_column);

  return absl::OkStatus();
}

absl::Status Resolver::ResolveExpressionArgument(
    const ASTExpression* arg, ExprResolutionInfo* expr_resolution_info,
    std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_arguments) {
  std::unique_ptr<const ResolvedExpr> resolved_arg;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(arg, expr_resolution_info, &resolved_arg));
  resolved_arguments->push_back(std::move(resolved_arg));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveExpressionArguments(
    ExprResolutionInfo* expr_resolution_info,
    const absl::Span<const ASTExpression* const> arguments,
    const std::map<int, SpecialArgumentType>& argument_option_map,
    std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_arguments_out,
    std::vector<const ASTExpression*>* ast_arguments_out) {

  // These reservations could be low for special cases like interval args
  // that turn into multiple elements.  We'll guess we have at most one
  // of those and need at most one extra slot.
  resolved_arguments_out->reserve(arguments.size() + 1);
  ast_arguments_out->reserve(arguments.size() + 1);

  for (int idx = 0; idx < arguments.size(); ++idx) {
    const ASTExpression* arg = arguments[idx];
    const SpecialArgumentType* special_argument_type =
        zetasql_base::FindOrNull(argument_option_map, idx);
    if (special_argument_type != nullptr) {
      switch (*special_argument_type) {
        case SpecialArgumentType::INTERVAL:
          ZETASQL_RETURN_IF_ERROR(ResolveIntervalArgument(arg, expr_resolution_info,
                                                  resolved_arguments_out,
                                                  ast_arguments_out));
          break;
        case SpecialArgumentType::DATEPART: {
          std::unique_ptr<const ResolvedExpr> resolved_argument;
          ZETASQL_RETURN_IF_ERROR(ResolveDatePartArgument(arg, &resolved_argument));
          resolved_arguments_out->push_back(std::move(resolved_argument));
          ast_arguments_out->push_back(arg);
          break;
        }
        case SpecialArgumentType::NORMALIZE_MODE: {
          std::unique_ptr<const ResolvedExpr> resolved_argument;
          ZETASQL_RETURN_IF_ERROR(
              ResolveNormalizeModeArgument(arg, &resolved_argument));
          resolved_arguments_out->push_back(std::move(resolved_argument));
          ast_arguments_out->push_back(arg);
          break;
        }
      }
    } else if (arg->Is<ASTLambda>()) {
      if (!language().LanguageFeatureEnabled(
              FEATURE_V_1_3_INLINE_LAMBDA_ARGUMENT)) {
        return MakeSqlErrorAt(arg) << "Lambda is not supported";
      }
      ZETASQL_RETURN_IF_ERROR(
          ValidateLambdaArgumentListIsIdentifierList(arg->GetAs<ASTLambda>()));
      // Normally all function arguments are resolved, then signatures are
      // matched against them. Lambdas, such as `e->e>0`, don't have explicit
      // types for the arguments. Types of arguments of lambda can only be
      // inferred based on types of other arguments and functions signature.
      // Then the lambda body referencing the arguments can be resolved. We put
      // nullptrs instead ResolvedExpr in place of lambdas, resolve the lambdas
      // during signature matching and fill in the generic_argument_list
      // appropriately after signature matching. The resolved argument list with
      // nullptr only hangs around during arguments resolving and signature
      // matching.
      //
      resolved_arguments_out->push_back(nullptr);
      ast_arguments_out->push_back(arg);
    } else {
      ZETASQL_RETURN_IF_ERROR(ResolveExpressionArgument(arg, expr_resolution_info,
                                                resolved_arguments_out));
      ast_arguments_out->push_back(arg);
    }
  }
  ZETASQL_RET_CHECK_EQ(ast_arguments_out->size(), resolved_arguments_out->size());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveFunctionCallWithResolvedArguments(
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations,
    const std::vector<std::string>& function_name_path,
    std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
    std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  const Function* function;
  ResolvedFunctionCallBase::ErrorMode error_mode;
  ZETASQL_RETURN_IF_ERROR(LookupFunctionFromCatalog(
      ast_location, function_name_path,
      FunctionNotFoundHandleMode::kReturnError, &function, &error_mode));
  return ResolveFunctionCallWithResolvedArguments(
      ast_location, arg_locations, function, error_mode,
      std::move(resolved_arguments), std::move(named_arguments),
      expr_resolution_info, /*with_group_rows_subquery=*/nullptr,
      /*with_group_rows_correlation_references=*/{}, resolved_expr_out);
}

absl::Status Resolver::ResolveFunctionCallWithResolvedArguments(
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations,
    absl::string_view function_name,
    std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
    std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  const std::vector<std::string> function_name_path = {
      std::string(function_name)};
  return ResolveFunctionCallWithResolvedArguments(
      ast_location, arg_locations, function_name_path,
      std::move(resolved_arguments), std::move(named_arguments),
      expr_resolution_info, resolved_expr_out);
}

absl::Status Resolver::ResolveProtoDefaultIfNull(
    const ASTNode* ast_location,
    std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  ZETASQL_RET_CHECK_EQ(resolved_arguments.size(), 1);

  std::unique_ptr<const ResolvedExpr> resolved_argument =
      std::move(resolved_arguments[0]);
  if (resolved_argument->node_kind() != RESOLVED_GET_PROTO_FIELD) {
    return MakeSqlErrorAt(ast_location) << "The PROTO_DEFAULT_IF_NULL input "
                                           "expression must end with a proto "
                                           "field access";
  }
  const ResolvedGetProtoField* resolved_field_access =
      resolved_argument->GetAs<ResolvedGetProtoField>();
  if (resolved_field_access->type()->IsProto()) {
    return MakeSqlErrorAt(ast_location)
           << "The PROTO_DEFAULT_IF_NULL input expression "
              "cannot access a field with type message; Field "
           << resolved_field_access->field_descriptor()->full_name()
           << " is of message type";
  } else if (resolved_field_access->field_descriptor()->is_required()) {
    return MakeSqlErrorAt(ast_location)
           << "The field accessed by PROTO_DEFAULT_IF_NULL input expression "
              "cannot access a required field; Field "
           << resolved_field_access->field_descriptor()->full_name()
           << " is required";
  } else if (resolved_field_access->get_has_bit()) {
    return MakeSqlErrorAt(ast_location)
           << "The PROTO_DEFAULT_IF_NULL function does not accept expressions "
              "that result in a 'has_' virtual field access";
  } else if (resolved_field_access->return_default_value_when_unset()) {
    return MakeSqlErrorAt(ast_location) << "The PROTO_DEFAULT_IF_NULL input "
                                           "expression must end with a proto "
                                           "field access";
  } else if (!ProtoType::GetUseDefaultsExtension(
                 resolved_field_access->field_descriptor()) &&
             (resolved_field_access->expr()
                      ->type()
                      ->AsProto()
                      ->descriptor()
                      ->file()
                      ->syntax() != google::protobuf::FileDescriptor::SYNTAX_PROTO3 ||
              !language().LanguageFeatureEnabled(
                  FEATURE_V_1_3_IGNORE_PROTO3_USE_DEFAULTS))) {
    return MakeSqlErrorAt(ast_location)
           << "The field accessed by PROTO_DEFAULT_IF_NULL must have a usable "
              "default value; Field "
           << resolved_field_access->field_descriptor()->full_name()
           << " is annotated to ignore proto defaults";
  }

  // We could eliminate the need for a const_cast by re-resolving the ASTNode
  // argument using ResolveFieldAccess() and ResolveExtensionFieldAccess()
  // directly, but that seems like more trouble than it's worth.
  const_cast<ResolvedGetProtoField*>(resolved_field_access)
      ->set_return_default_value_when_unset(true);

  *resolved_expr_out = std::move(resolved_argument);

  return absl::OkStatus();
}

namespace {

bool IsLetterP(char c) { return c == 'p' || c == 'P'; }

bool IsProtoDefaultIfNull(const std::vector<std::string>& function_name_path) {
  // We try to avoid doing the zetasql_base::StringCaseEqual calls as much as possible.
  if (function_name_path.size() == 1 && !function_name_path[0].empty() &&
      IsLetterP(function_name_path[0][0]) &&
      zetasql_base::CaseEqual(function_name_path[0], "PROTO_DEFAULT_IF_NULL")) {
    return true;
  }
  return false;
}

bool IsTypeOf(const Function* function) {
  return function->NumSignatures() == 1 &&
         function->signatures()[0].context_id() == FN_TYPEOF &&
         function->IsZetaSQLBuiltin();
}

bool IsFlatten(const Function* function) {
  return function->NumSignatures() == 1 &&
         function->signatures()[0].context_id() == FN_FLATTEN &&
         function->IsZetaSQLBuiltin();
}

}  // namespace

absl::Status Resolver::ResolveFunctionCallWithResolvedArguments(
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations, const Function* function,
    ResolvedFunctionCallBase::ErrorMode error_mode,
    std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments,
    std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedScan> with_group_rows_subquery,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>
        with_group_rows_correlation_references,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  // Generated columns, ZETASQL_CHECK constraints, and expressions that are stored in an
  // index have specific limitations on VOLATILE/STABLE functions. ZetaSQL
  // relies upon each engine to provide volatility information for non-builtin
  // functions (including user defined functions).
  // TODO: Checking function volalitity is still not enough. A
  // ColumnRef can be volatile if it's a logical generated column that calls
  // other volatile functions. Such column cannot be used in places where
  // volatility is not allowed.
  if (analyzing_nonvolatile_stored_expression_columns_ &&
      function->function_options().volatility == FunctionEnums::VOLATILE) {
    return MakeSqlErrorAt(ast_location)
           << function->QualifiedSQLName(/* capitalize_qualifier= */ true)
           << " is not allowed in expressions that are stored as each "
              "invocation might return a different value";
  }
  if (analyzing_check_constraint_expression_ &&
      function->function_options().volatility != FunctionEnums::IMMUTABLE) {
    return MakeSqlErrorAt(ast_location)
           << function->QualifiedSQLName(/* capitalize_qualifier= */ true)
           << " is not allowed in CHECK"
           << " constraint expression as each "
           << "invocation might return a different value";
  }

  // Now resolve the function call, including overload resolution.
  std::unique_ptr<ResolvedFunctionCall> resolved_function_call;

  ZETASQL_RETURN_IF_ERROR(function_resolver_->ResolveGeneralFunctionCall(
      ast_location, arg_locations, function, error_mode,
      /*is_analytic=*/false, std::move(resolved_arguments),
      std::move(named_arguments), /*expected_result_type=*/nullptr,
      expr_resolution_info->name_scope, &resolved_function_call));

  if (function->IsDeprecated()) {
    ZETASQL_RETURN_IF_ERROR(AddDeprecationWarning(
        ast_location, DeprecationWarning::DEPRECATED_FUNCTION,
        absl::StrCat(function->QualifiedSQLName(/*capitalize_qualifier=*/true),
                     " is deprecated")));
  }
  if (resolved_function_call->signature().IsDeprecated()) {
    ZETASQL_RETURN_IF_ERROR(AddDeprecationWarning(
        ast_location, DeprecationWarning::DEPRECATED_FUNCTION_SIGNATURE,
        absl::StrCat("Using a deprecated function signature for ",
                     function->QualifiedSQLName())));
  }

  // Down-casting the 'ast_location' pointer doesn't seem like the right thing
  // to be doing here. Probably we should be plumbing the hints into this code
  // explicitly.
  // TODO: Eliminate this bad code smell.
  const auto* ast_function = ast_location->GetAsOrNull<ASTFunctionCall>();
  if (ast_function != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(ast_function->hint(),
                                        resolved_function_call.get()));

    if (IsTypeOf(function) && resolved_function_call->hint_list_size() > 0) {
      return MakeSqlErrorAt(ast_function->hint())
             << "The TYPEOF() operator does not allow hints.";
    }
  }

  ZETASQL_RETURN_IF_ERROR(AddAdditionalDeprecationWarningsForCalledFunction(
      ast_location, resolved_function_call->signature(),
      function->QualifiedSQLName(/*capitalize_qualifier=*/true),
      /*is_tvf=*/false));

  // If the function call resolved to an aggregate function, then do the
  // extra processing required for aggregates.
  if (function->IsAggregate()) {
    const ASTFunctionCall* ast_function_call =
        ast_location->GetAsOrDie<ASTFunctionCall>();
    ZETASQL_RETURN_IF_ERROR(FinishResolvingAggregateFunction(
        ast_function_call, &resolved_function_call, expr_resolution_info,
        std::move(with_group_rows_subquery),
        std::move(with_group_rows_correlation_references), resolved_expr_out));
  } else {
    if (function->mode() == Function::ANALYTIC) {
      return MakeSqlErrorAt(ast_location)
             << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
             << " cannot be called without an OVER clause";
    }
    ZETASQL_DCHECK_EQ(function->mode(), Function::SCALAR);

    // We handle the PROTO_DEFAULT_IF_NULL() function here so that it can be
    // resolved to a ResolvedGetProtoField.
    if (IsProtoDefaultIfNull(function->FunctionNamePath())) {
      if (!language().LanguageFeatureEnabled(
              FEATURE_V_1_3_PROTO_DEFAULT_IF_NULL)) {
        return MakeSqlErrorAt(ast_location)
               << "The PROTO_DEFAULT_IF_NULL function is not supported";
      }
      ZETASQL_RETURN_IF_ERROR(ResolveProtoDefaultIfNull(
          ast_location, resolved_function_call->release_argument_list(),
          resolved_expr_out));
    } else if (IsFlatten(function)) {
      if (!language().LanguageFeatureEnabled(
              FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS)) {
        return MakeSqlErrorAt(ast_location)
               << "The FLATTEN function is not supported";
      }
      ZETASQL_RET_CHECK_EQ(1, resolved_function_call->argument_list_size());
      *resolved_expr_out =
          std::move(resolved_function_call->release_argument_list()[0]);
    } else {
      *resolved_expr_out = std::move(resolved_function_call);
    }
  }

  if ((*resolved_expr_out)->Is<ResolvedFunctionCallBase>()) {
    const auto* call = (*resolved_expr_out)->GetAs<ResolvedFunctionCallBase>();
    if (call->function()->IsZetaSQLBuiltin()) {
      switch (call->signature().context_id()) {
        case FN_ARRAY_FILTER:
        case FN_ARRAY_FILTER_WITH_INDEX:
        case FN_ARRAY_TRANSFORM:
        case FN_ARRAY_TRANSFORM_WITH_INDEX:
          analyzer_output_properties_.MarkRelevant(
              REWRITE_ARRAY_FILTER_TRANSFORM);
          break;
        case FN_ARRAY_INCLUDES:
        case FN_ARRAY_INCLUDES_LAMBDA:
        case FN_ARRAY_INCLUDES_ANY:
          analyzer_output_properties_.MarkRelevant(REWRITE_ARRAY_INCLUDES);
          break;
        case FN_CONTAINS_KEY:
        case FN_MODIFY_MAP:
          analyzer_output_properties_.MarkRelevant(REWRITE_PROTO_MAP_FNS);
          break;
        case FN_TYPEOF:
          analyzer_output_properties_.MarkRelevant(REWRITE_TYPEOF_FUNCTION);
          break;
        default:
          break;
      }
    }
  }

  return absl::OkStatus();
}

absl::Status Resolver::LookupFunctionFromCatalog(
    const ASTNode* ast_location,
    const std::vector<std::string>& function_name_path,
    FunctionNotFoundHandleMode handle_mode, const Function** function,
    ResolvedFunctionCallBase::ErrorMode* error_mode) const {
  *error_mode = ResolvedFunctionCallBase::DEFAULT_ERROR_MODE;

  // This is the function name path with SAFE stripped off, if applicable.
  auto stripped_name =
      absl::MakeSpan(function_name_path.data(), function_name_path.size());
  bool is_stripped = false;

  absl::Status find_status;

  // Look up the SAFE name first, to avoid tricky cases where a UDF could
  // be used to hide a builtin if the engine didn't block that.
  if (function_name_path.size() > 1 &&
      zetasql_base::CaseEqual(function_name_path[0], "SAFE")) {
    // Check this first because we don't want behavior to vary based
    // on feature flags, other than letting a query succeed.
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_2_SAFE_FUNCTION_CALL)) {
      return MakeSqlErrorAt(ast_location)
             << "Function calls with SAFE are not supported";
    }

    stripped_name.remove_prefix(1);
    is_stripped = true;
    find_status = catalog_->FindFunction(stripped_name, function,
                                         analyzer_options_.find_options());
    if (find_status.ok()) {
      if (!(*function)->SupportsSafeErrorMode()) {
        return MakeSqlErrorAt(ast_location)
               << "Function " << IdentifierPathToString(stripped_name)
               << " does not support SAFE error mode";
      }
      *error_mode = ResolvedFunctionCallBase::SAFE_ERROR_MODE;
    }
  } else {
    find_status = catalog_->FindFunction(function_name_path, function,
                                         analyzer_options_.find_options());
  }

  bool function_lookup_succeeded = find_status.ok();
  bool all_required_features_are_enabled = true;
  if (function_lookup_succeeded) {
    all_required_features_are_enabled =
        (*function)->function_options().check_all_required_features_are_enabled(
            language().GetEnabledLanguageFeatures());
  }

  // We return UNIMPLEMENTED errors from the Catalog directly to the caller.
  if (find_status.code() == absl::StatusCode::kUnimplemented ||
      find_status.code() == absl::StatusCode::kPermissionDenied ||
      (find_status.code() == absl::StatusCode::kNotFound &&
       handle_mode == FunctionNotFoundHandleMode::kReturnNotFound)) {
    return find_status;
  }

  if (!find_status.ok() && find_status.code() != absl::StatusCode::kNotFound) {
    // The FindFunction() call can return an invalid argument error, for
    // example, when looking up LazyResolutionFunctions (which are resolved
    // upon lookup).
    //
    // Rather than directly return the <find_status>, we update the location
    // of the error to indicate the function call in this statement.  We also
    // preserve the ErrorSource payload from <find_status>, since that
    // indicates source errors for this error.
    return WrapNestedErrorStatus(
        ast_location,
        absl::StrCat("Invalid function ",
                     IdentifierPathToString(stripped_name)),
        find_status, analyzer_options_.error_message_mode());
  } else if (find_status.code() == absl::StatusCode::kNotFound ||
             !all_required_features_are_enabled) {
    std::string error_message;
    absl::StrAppend(&error_message, "Function not found: ",
                    IdentifierPathToString(stripped_name));
    if (all_required_features_are_enabled) {
      // Do not make a suggestion if the function is disabled because of missing
      // language features. By default that function will be added to the
      // function map, thus the function itself will be the suggestion result,
      // which is confusing.
      const std::string function_suggestion =
          catalog_->SuggestFunction(stripped_name);
      if (!function_suggestion.empty()) {
        absl::StrAppend(
            &error_message, "; Did you mean ",
            is_stripped ? absl::StrCat(function_name_path[0], ".") : "",
            function_suggestion, "?");
      }
    }
    return MakeSqlErrorAt(ast_location) << error_message;
  }
  return find_status;
}

absl::Status Resolver::ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
    const ASTNode* ast_location, const std::string& function_name,
    const absl::Span<const ASTExpression* const> arguments,
    const std::map<int, SpecialArgumentType>& argument_option_map,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  const std::vector<std::string> function_name_path = {function_name};
  const Function* function;
  ResolvedFunctionCallBase::ErrorMode error_mode;
  ZETASQL_RETURN_IF_ERROR(LookupFunctionFromCatalog(
      ast_location, function_name_path,
      FunctionNotFoundHandleMode::kReturnError, &function, &error_mode));
  return ResolveFunctionCallImpl(ast_location, function, error_mode, arguments,
                                 argument_option_map, expr_resolution_info,
                                 /*with_group_rows_subquery=*/nullptr,
                                 /*with_group_rows_correlation_references=*/{},
                                 resolved_expr_out);
}

absl::Status Resolver::ResolveFunctionCallWithLiteralRetry(
    const ASTNode* ast_location, const std::string& function_name,
    const absl::Span<const ASTExpression* const> arguments,
    const std::map<int, SpecialArgumentType>& argument_option_map,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  const absl::Status status =
      ResolveFunctionCallByNameWithoutAggregatePropertyCheck(
          ast_location, function_name, arguments, argument_option_map,
          expr_resolution_info, resolved_expr_out);
  if (status.ok() || status.code() != absl::StatusCode::kInvalidArgument) {
    return status;
  }

  // If initial resolution failed, then try marking the literals as explicitly
  // typed so that we can avoid narrowing literal coercions that might fail due
  // to overflow.  This allows us to handle cases like the following:
  // select * from TestTable
  //   where KitchenSink.int32_val BETWEEN -1 AND 5000000000 OR
  //         KitchenSink.int32_val BETWEEN -4294967296 AND 10;
  // OR
  // select * from TestTable
  //   where KitchenSink.int32_val < 5000000000;
  const std::vector<std::string> function_name_path{function_name};
  const Function* function;
  ResolvedFunctionCallBase::ErrorMode error_mode;
  ZETASQL_RETURN_IF_ERROR(LookupFunctionFromCatalog(
      ast_location, function_name_path,
      FunctionNotFoundHandleMode::kReturnError, &function, &error_mode));
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;
  std::vector<const ASTExpression*> ast_arguments;
  ZETASQL_RETURN_IF_ERROR(ResolveExpressionArguments(
      expr_resolution_info, arguments, argument_option_map, &resolved_arguments,
      &ast_arguments));

  // After resolving the arguments, mark literal arguments as having an explicit
  // type.
  ZETASQL_RETURN_IF_ERROR(UpdateLiteralsToExplicit(arguments, &resolved_arguments));
  const absl::Status new_status = ResolveFunctionCallWithResolvedArguments(
      ast_location,
      ToLocations(absl::Span<const ASTExpression* const>(ast_arguments)),
      function, error_mode, std::move(resolved_arguments),
      /*named_arguments=*/{}, expr_resolution_info,
      /*with_group_rows_subquery=*/nullptr,
      /*with_group_rows_correlation_references=*/{}, resolved_expr_out);
  if (!new_status.ok()) {
    // Return the original error.
    return status;
  }
  return absl::OkStatus();
}

absl::Status Resolver::UpdateLiteralsToExplicit(
    const absl::Span<const ASTExpression* const> ast_arguments,
    std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_expr_list) {

  ZETASQL_RET_CHECK_EQ(ast_arguments.size(), resolved_expr_list->size());
  for (int i = 0; i < resolved_expr_list->size(); ++i) {
    const ResolvedExpr* expr = (*resolved_expr_list)[i].get();
    if (expr->node_kind() != RESOLVED_LITERAL) {
      continue;
    }
    const ResolvedLiteral* expr_literal = expr->GetAs<ResolvedLiteral>();
    // Skip the cast if the literal is already explicitly typed.
    if (expr_literal->has_explicit_type()) {
      continue;
    }
    ZETASQL_RETURN_IF_ERROR(function_resolver_->AddCastOrConvertLiteral(
        ast_arguments[i], expr->type(), /*format=*/nullptr,
        /*time_zone=*/nullptr, TypeParameters(), /*scan=*/nullptr,
        /*set_has_explicit_type=*/true,
        /*return_null_on_error=*/false, &(*resolved_expr_list)[i]));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveFunctionCallImpl(
    const ASTNode* ast_location, const Function* function,
    ResolvedFunctionCallBase::ErrorMode error_mode,
    const absl::Span<const ASTExpression* const> arguments,
    const std::map<int, SpecialArgumentType>& argument_option_map,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedScan> with_group_rows_subquery,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>
        with_group_rows_correlation_references,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {

  // Check if the function call contains any named arguments.
  std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments;
  for (int i = 0; i < arguments.size(); ++i) {
    const ASTExpression* arg = arguments[i];
    if (arg->node_kind() == AST_NAMED_ARGUMENT) {
      named_arguments.emplace_back(arg->GetAs<ASTNamedArgument>(), i);
    }
  }

  if ((function->Is<SQLFunctionInterface>() ||
       function->Is<TemplatedSQLFunction>()) &&
      !function->IsAggregate()) {
    analyzer_output_properties_.MarkRelevant(REWRITE_INLINE_SQL_FUNCTIONS);
  }

  // A "flatten" function allows the child to flatten.
  FlattenState::Restorer restorer;
  ZETASQL_RET_CHECK_EQ(nullptr, expr_resolution_info->flatten_state.active_flatten());
  if (IsFlatten(function)) {
    // We check early for too many arguments as otherwise we can ZETASQL_RET_CHECK for
    // resolving arguments with flatten in progress which happens before arg
    // count checking.
    if (arguments.size() != 1) {
      return MakeSqlErrorAt(ast_location)
             << "Number of arguments does not match for function FLATTEN. "
                "Supported signature: FLATTEN(ARRAY)";
    }
    expr_resolution_info->flatten_state.set_can_flatten(true, &restorer);
  }

  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;
  std::vector<const ASTExpression*> ast_arguments;
  ZETASQL_RETURN_IF_ERROR(ResolveExpressionArguments(
      expr_resolution_info, arguments, argument_option_map, &resolved_arguments,
      &ast_arguments));

  return ResolveFunctionCallWithResolvedArguments(
      ast_location,
      ToLocations(absl::Span<const ASTExpression* const>(ast_arguments)),
      function, error_mode, std::move(resolved_arguments),
      std::move(named_arguments), expr_resolution_info,
      std::move(with_group_rows_subquery),
      std::move(with_group_rows_correlation_references), resolved_expr_out);
}

IdString Resolver::GetColumnAliasForTopLevelExpression(
    ExprResolutionInfo* expr_resolution_info, const ASTExpression* ast_expr) {
  const IdString alias = expr_resolution_info->column_alias;
  if (expr_resolution_info->top_level_ast_expr == ast_expr &&
      !IsInternalAlias(expr_resolution_info->column_alias)) {
    return alias;
  }
  return IdString();
}

absl::Status Resolver::CoerceExprToBool(
    const ASTNode* ast_location, absl::string_view clause_name,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) const {
  auto make_error_msg = [clause_name](absl::string_view target_type_name,
                                      absl::string_view actual_type_name) {
    return absl::Substitute("$2 should return type $0, but returns $1",
                            target_type_name, actual_type_name, clause_name);
  };
  return CoerceExprToType(ast_location, type_factory_->get_bool(),
                          kImplicitCoercion, make_error_msg, resolved_expr);
}

absl::Status Resolver::CoerceExprToType(
    const ASTNode* ast_location, AnnotatedType annotated_target_type,
    CoercionMode mode, CoercionErrorMessageFunction make_error,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) const {
  const Type* target_type = annotated_target_type.type;
  const AnnotationMap* target_type_annotation_map =
      annotated_target_type.annotation_map;
  ZETASQL_RET_CHECK_NE(target_type, nullptr);
  ZETASQL_RET_CHECK_NE(resolved_expr, nullptr);
  ZETASQL_RET_CHECK_NE(resolved_expr->get(), nullptr);
  const AnnotationMap* source_type_annotation_map =
      resolved_expr->get()->type_annotation_map();
  if (target_type_annotation_map != nullptr) {
    ZETASQL_RET_CHECK(target_type_annotation_map->HasCompatibleStructure(target_type))
        << "The type annotation map "
        << target_type_annotation_map->DebugString()
        << " is not compatible with the target type "
        << target_type->DebugString();
  }
  if (target_type->Equals(resolved_expr->get()->type()) &&
      AnnotationMap::HasEqualAnnotations(source_type_annotation_map,
                                         target_type_annotation_map,
                                         CollationAnnotation::GetId())) {
    return absl::OkStatus();
  }
  InputArgumentType expr_arg_type =
      GetInputArgumentTypeForExpr(resolved_expr->get());
  SignatureMatchResult sig_match_result;
  Coercer coercer(type_factory_, &language(), catalog_);
  bool success;
  switch (mode) {
    case kImplicitAssignment:
      success = coercer.AssignableTo(expr_arg_type, target_type,
                                     /*is_explicit=*/false, &sig_match_result);
      break;
    case kImplicitCoercion:
      success = coercer.CoercesTo(expr_arg_type, target_type,
                                  /*is_explicit=*/false, &sig_match_result);
      break;
    case kExplicitCoercion:
      success = coercer.CoercesTo(expr_arg_type, target_type,
                                  /*is_explicit=*/true, &sig_match_result);
      break;
  }
  if (!success) {
    const std::string target_type_name =
        target_type->ShortTypeName(language().product_mode());
    const std::string expr_type_name =
        expr_arg_type.UserFacingName(language().product_mode());
    return MakeSqlErrorAt(ast_location)
           << make_error(target_type_name, expr_type_name);
  }

  // The coercion is legal, so implement it by adding a cast.  Note that
  // AddCastOrConvertLiteral() adds a cast node only when necessary.
  return function_resolver_->AddCastOrConvertLiteral(
      ast_location, annotated_target_type, /*format=*/nullptr,
      /*time_zone=*/nullptr, TypeParameters(), /*scan=*/nullptr,
      /*set_has_explicit_type=*/false,
      /*return_null_on_error=*/false, resolved_expr);
}

absl::Status Resolver::CoerceExprToType(
    const ASTNode* ast_location, const Type* target_type, CoercionMode mode,
    CoercionErrorMessageFunction make_error,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) const {
  return CoerceExprToType(
      ast_location, AnnotatedType(target_type, /*annotation_map=*/nullptr),
      mode, make_error, resolved_expr);
}

absl::Status Resolver::CoerceExprToType(
    const ASTNode* ast_location, const Type* target_type, CoercionMode mode,
    absl::string_view error_template,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) const {
  return CoerceExprToType(
      ast_location, AnnotatedType(target_type, /*annotation_map=*/nullptr),
      mode, error_template, resolved_expr);
}

absl::Status Resolver::CoerceExprToType(
    const ASTNode* ast_location, AnnotatedType annotated_target_type,
    CoercionMode mode, absl::string_view error_template,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) const {
  auto make_error_msg = [error_template](absl::string_view target_type_name,
                                         absl::string_view actual_type_name) {
    return absl::Substitute(error_template, target_type_name, actual_type_name);
  };
  return CoerceExprToType(ast_location, annotated_target_type, mode,
                          make_error_msg, resolved_expr);
}

absl::Status Resolver::CoerceExprToType(
    const ASTNode* ast_location, const Type* target_type, CoercionMode mode,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) const {
  return CoerceExprToType(
      ast_location, AnnotatedType(target_type, /*annotation_map=*/nullptr),
      mode, resolved_expr);
}

absl::Status Resolver::CoerceExprToType(
    const ASTNode* ast_location, AnnotatedType annotated_target_type,
    CoercionMode mode,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) const {
  return CoerceExprToType(ast_location, annotated_target_type, mode,
                          "Expected type $0; found $1", resolved_expr);
}

absl::Status Resolver::ResolveExecuteImmediateArgument(
    const ASTExecuteUsingArgument* argument, ExprResolutionInfo* expr_info,
    std::unique_ptr<const ResolvedExecuteImmediateArgument>* output) {
  const std::string alias =
      argument->alias() != nullptr ? argument->alias()->GetAsString() : "";

  std::unique_ptr<const ResolvedExpr> expression;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(argument->expression(), expr_info, &expression));

  *output = MakeResolvedExecuteImmediateArgument(alias, std::move(expression));

  return absl::OkStatus();
}

absl::StatusOr<functions::DateTimestampPart> Resolver::ResolveDateTimestampPart(
    const ASTIdentifier* date_part_identifier) {
  std::unique_ptr<const ResolvedExpr> resolved_date_part;
  ZETASQL_RETURN_IF_ERROR(
      ResolveDatePartArgument(date_part_identifier, &resolved_date_part));
  ZETASQL_RET_CHECK(resolved_date_part->node_kind() == RESOLVED_LITERAL &&
            resolved_date_part->type()->IsEnum());
  return static_cast<functions::DateTimestampPart>(
      resolved_date_part->GetAs<ResolvedLiteral>()->value().enum_value());
}

// TODO: The noinline attribute is to prevent the stack usage
// being added to its caller "Resolver::ResolveExpr" which is a recursive
// function. Now the attribute has to be added for all callees. Hopefully
// the fix allows replacing these with one attribute on ResolveExpr.
ABSL_ATTRIBUTE_NOINLINE
absl::Status Resolver::ResolveIntervalExpr(
    const ASTIntervalExpr* interval_expr,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  // Resolve the INTERVAL value expression.
  const ASTExpression* interval_value_expr = interval_expr->interval_value();

  std::unique_ptr<const ResolvedExpr> resolved_interval_value;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(interval_value_expr, expr_resolution_info,
                              &resolved_interval_value));

  // Resolve the date part identifier and verify that it is a valid date part.
  ZETASQL_ASSIGN_OR_RETURN(functions::DateTimestampPart date_part,
                   ResolveDateTimestampPart(interval_expr->date_part_name()));

  // Optionally resolve the second date part identifier.
  functions::DateTimestampPart date_part_to;
  if (interval_expr->date_part_name_to() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(date_part_to, ResolveDateTimestampPart(
                                       interval_expr->date_part_name_to()));
  }

  // TODO: Clarify whether integer literals (with optional sign) are
  // allowed, or only integer expressions are allowed.
  if (resolved_interval_value->type()->IsInt64()) {
    if (interval_expr->date_part_name_to() != nullptr) {
      return MakeSqlErrorAt(interval_expr->date_part_name_to())
             << "The INTERVAL keyword followed by an integer expression can "
                "only specify a single datetime field, not a field range. "
                "Consider using INTERVAL function for specifying multiple "
                "datetime fields.";
    }

    std::vector<std::unique_ptr<const ResolvedExpr>> resolved_arguments;
    std::vector<const ASTExpression*> ast_arguments;
    resolved_arguments.push_back(std::move(resolved_interval_value));
    ast_arguments.push_back(interval_expr);

    std::unique_ptr<const ResolvedExpr> resolved_date_part;
    ZETASQL_RETURN_IF_ERROR(ResolveDatePartArgument(interval_expr->date_part_name(),
                                            &resolved_date_part));
    resolved_arguments.push_back(std::move(resolved_date_part));
    ast_arguments.push_back(interval_expr->date_part_name());

    std::vector<const ASTNode*> arg_locations =
        ToLocations(absl::Span<const ASTExpression* const>(ast_arguments));

    return ResolveFunctionCallWithResolvedArguments(
        interval_expr, arg_locations, "$interval",
        std::move(resolved_arguments), /*named_arguments=*/{},
        expr_resolution_info, resolved_expr_out);
  }

  // TODO: Support <sign> <string literal> as well.
  if (resolved_interval_value->node_kind() != RESOLVED_LITERAL ||
      !resolved_interval_value->type()->IsString()) {
    return MakeSqlErrorAt(interval_value_expr)
           << "Invalid interval literal. Expected INTERVAL keyword to be "
              "followed by an INT64 expression or STRING literal";
  }

  const ResolvedLiteral* interval_value_literal =
      resolved_interval_value->GetAs<ResolvedLiteral>();
  absl::StatusOr<IntervalValue> interval_value_or_status;
  if (interval_expr->date_part_name_to() == nullptr) {
    interval_value_or_status = IntervalValue::ParseFromString(
        interval_value_literal->value().string_value(), date_part);
  } else {
    interval_value_or_status = IntervalValue::ParseFromString(
        interval_value_literal->value().string_value(), date_part,
        date_part_to);
  }
  // Not using ZETASQL_ASSIGN_OR_RETURN, because we need to translate OutOfRange error
  // into InvalidArgument error, and add location.
  if (!interval_value_or_status.ok()) {
    return MakeSqlErrorAt(interval_expr)
           << interval_value_or_status.status().message();
  }

  Value value(Value::Interval(interval_value_or_status.value()));
  *resolved_expr_out = MakeResolvedLiteral(interval_expr, value);
  return absl::OkStatus();
}

}  // namespace zetasql
