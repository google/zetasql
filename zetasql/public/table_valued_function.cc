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

#include "zetasql/public/table_valued_function.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/function_utils.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/simple_table.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_deserializer.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// static
absl::Status TableValuedFunctionOptions::Deserialize(
      const TableValuedFunctionOptionsProto& proto,
      std::unique_ptr<TableValuedFunctionOptions>* result) {
  auto options = std::make_unique<TableValuedFunctionOptions>();
  options->set_uses_upper_case_sql_name(proto.uses_upper_case_sql_name());

  *result = std::move(options);
  return absl::OkStatus();
}

void TableValuedFunctionOptions::Serialize(
    TableValuedFunctionOptionsProto* proto) const {
  proto->Clear();
  proto->set_uses_upper_case_sql_name(uses_upper_case_sql_name);
}

int64_t TableValuedFunction::NumSignatures() const {
  return signatures_.size();
}

const std::vector<FunctionSignature>& TableValuedFunction::signatures() const {
  return signatures_;
}

absl::Status TableValuedFunction::AddSignature(
    const FunctionSignature& function_signature) {
  ZETASQL_RET_CHECK_EQ(0, NumSignatures());
  ZETASQL_RETURN_IF_ERROR(function_signature.IsValidForTableValuedFunction())
      << function_signature.DebugString(FullName());
  signatures_.push_back(function_signature);
  return absl::OkStatus();
}

const FunctionSignature* TableValuedFunction::GetSignature(int64_t idx) const {
  if (idx < 0 || idx >= NumSignatures()) {
    return nullptr;
  }
  return &(signatures_[idx]);
}

std::string TableValuedFunction::GetSupportedSignaturesUserFacingText(
    const LanguageOptions& language_options,
    bool print_template_and_name_details) const {
  std::string supported_signatures;
  for (const FunctionSignature& signature : signatures()) {
    absl::StrAppend(
        &supported_signatures, (!supported_signatures.empty() ? "; " : ""),
        GetSignatureUserFacingText(signature, language_options,
                                   print_template_and_name_details));
  }
  return supported_signatures;
}

std::string TableValuedFunction::GetSignatureUserFacingText(
    const FunctionSignature& signature, const LanguageOptions& language_options,
    bool print_template_and_name_details) const {
  std::vector<std::string> argument_texts;
  for (const FunctionArgumentType& argument : signature.arguments()) {
    std::string arg_type_string = argument.UserFacingName(
        language_options.product_mode(), print_template_and_name_details);
    if (print_template_and_name_details && argument.has_argument_name()) {
      arg_type_string =
          absl::StrCat(argument.argument_name(), " => ", arg_type_string);
    }
    // If the argument is a relation argument to a table-valued function and the
    // function signature specifies a required input schema, append the types of
    // the required columns to the user-facing signature string.
    if (argument.IsRelation() &&
        argument.options().has_relation_input_schema()) {
      const TVFRelation& relation_input_schema =
          argument.options().relation_input_schema();
      std::vector<std::string> column_strings;
      column_strings.reserve(relation_input_schema.num_columns());
      for (const TVFRelation::Column& column :
           relation_input_schema.columns()) {
        column_strings.push_back(
            column.type->ShortTypeName(language_options.product_mode()));
        // Prevent concatenating value column name.
        if (!relation_input_schema.is_value_table() ||
            column.is_pseudo_column) {
          column_strings.back() =
              absl::StrCat(column.name, " ", column_strings.back());
        }
      }
      absl::StrAppend(&arg_type_string, "<",
                      absl::StrJoin(column_strings, ", "), ">");
    }
    if (argument.optional()) {
      argument_texts.push_back(absl::StrCat("[", arg_type_string, "]"));
    } else if (argument.repeated()) {
      argument_texts.push_back(absl::StrCat("[", arg_type_string, ", ...]"));
    } else {
      argument_texts.push_back(arg_type_string);
    }
  }
  return absl::StrCat(SQLName(), "(", absl::StrJoin(argument_texts, ", "), ")");
}

std::string TableValuedFunction::DebugString() const {
  return absl::StrCat(FullName(), (signatures_.empty() ? "" : "\n"),
                      FunctionSignature::SignaturesToString(signatures_));
}

std::string TableValuedFunction::GetTVFSignatureErrorMessage(
    absl::string_view tvf_name_string,
    absl::Span<const InputArgumentType> input_arg_types, int signature_idx,
    const SignatureMatchResult& signature_match_result,
    const LanguageOptions& language_options) const {
  // bad_argument_index is set for some specific tvf mismatch cases.
  if (signature_match_result.bad_argument_index() != -1) {
    // TODO: Update this error message when we support more than one
    // TVF signature.
    return absl::StrCat(signature_match_result.mismatch_message(), " of ",
                        GetSupportedSignaturesUserFacingText(
                            language_options,
                            /*print_template_and_name_details=*/false));
  } else if (!signature_match_result.mismatch_message().empty()) {
    return absl::StrCat(
        Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
            tvf_name_string, input_arg_types, language_options.product_mode(),
            {}, /*argument_types_on_new_line=*/true),
        "\n  Signature: ",
        GetSupportedSignaturesUserFacingText(
            language_options,
            /*print_template_and_name_details=*/true),
        "\n    ", signature_match_result.mismatch_message());
  } else {
    return absl::StrCat(
        Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
            tvf_name_string, input_arg_types, language_options.product_mode()),
        ". Supported signature", (NumSignatures() > 1 ? "s" : ""), ": ",
        GetSupportedSignaturesUserFacingText(
            language_options,
            /*print_template_and_name_details=*/false));
  }
}

absl::Status TableValuedFunction::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  for (const std::string& name : function_name_path()) {
    proto->add_name_path(name);
  }
  // TODO: Make proto->signature a repeated.
  ZETASQL_RET_CHECK_EQ(1, NumSignatures());
  ZETASQL_RETURN_IF_ERROR(GetSignature(0)->Serialize(file_descriptor_set_map,
                                             proto->mutable_signature()));
  tvf_options().Serialize(proto->mutable_options());

  const std::optional<const AnonymizationInfo> anonymization_info =
      this->anonymization_info();
  if (anonymization_info.has_value()) {
    SimpleAnonymizationInfoProto anonymization_info_proto;
    for (const std::string& name : anonymization_info->UserIdColumnNamePath()) {
      anonymization_info_proto.add_userid_column_name(name);
    }
    *proto->mutable_anonymization_info() = anonymization_info_proto;
  }
  if (statement_context() != CONTEXT_DEFAULT) {
    proto->set_statement_context(statement_context());
  }
  return absl::OkStatus();
}

// A TVFDeserializer for each TableValuedFunctionType. Thread safe after module
// initializers.
static std::vector<TableValuedFunction::TVFDeserializer>* TvfDeserializers() {
  static auto* tvf_deserializers =
      new std::vector<TableValuedFunction::TVFDeserializer>(
          FunctionEnums::TableValuedFunctionType_ARRAYSIZE);
  return tvf_deserializers;
}

// static
absl::Status TableValuedFunction::Deserialize(
    const TableValuedFunctionProto& proto,
    const TypeDeserializer& type_deserializer,
    std::unique_ptr<TableValuedFunction>* result) {
  auto tvf_name = [proto]() { return absl::StrJoin(proto.name_path(), "."); };
  ZETASQL_RET_CHECK(proto.has_type()) << tvf_name();
  ZETASQL_RET_CHECK_NE(FunctionEnums::INVALID, proto.type()) << tvf_name();
  TableValuedFunction::TVFDeserializer deserializer =
      (*TvfDeserializers())[proto.type()];
  ZETASQL_RET_CHECK(deserializer != nullptr) << tvf_name();
  return deserializer(proto, type_deserializer, result);
}

// static
absl::Status TableValuedFunction::Deserialize(
    const TableValuedFunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result) {
  return TableValuedFunction::Deserialize(
      proto, TypeDeserializer(factory, pools), result);
}

// static
void TableValuedFunction::RegisterDeserializer(
    FunctionEnums::TableValuedFunctionType type, TVFDeserializer deserializer) {
  // ABSL_CHECK validated -- This is used at initialization time only.
  ABSL_CHECK(FunctionEnums::TableValuedFunctionType_IsValid(type)) << type;
  // ABSL_CHECK validated -- This is used at initialization time only.
  ABSL_CHECK(!(*TvfDeserializers())[type]) << type;
  (*TvfDeserializers())[type] = std::move(deserializer);
}

// static
// Backwards compatible support for deserializers that do not support
// TypeDeserializer for extended types.
void TableValuedFunction::RegisterDeserializer(
    FunctionEnums::TableValuedFunctionType type,
    TVFDeserializerWithoutTypeDeserializer deserializer) {
  TableValuedFunction::RegisterDeserializer(
      type, [deserializer = std::move(deserializer)](
                const TableValuedFunctionProto& proto,
                const TypeDeserializer& type_deserializer,
                std::unique_ptr<TableValuedFunction>* result) {
        return deserializer(proto,
                            std::vector<const google::protobuf::DescriptorPool*>(
                                type_deserializer.descriptor_pools().begin(),
                                type_deserializer.descriptor_pools().end()),
                            type_deserializer.type_factory(), result);
      });
}

absl::Status TableValuedFunction::SetUserIdColumnNamePath(
    absl::Span<const std::string> userid_column_name_path) {
  ZETASQL_ASSIGN_OR_RETURN(anonymization_info_,
                   AnonymizationInfo::Create(userid_column_name_path));
  return absl::OkStatus();
}

// Serializes this TVFRelation column to a protocol buffer.
absl::StatusOr<TVFRelationColumnProto> TVFSchemaColumn::ToProto(
    FileDescriptorSetMap* file_descriptor_set_map) const {
  TVFRelationColumnProto proto;
  proto.set_name(name);
  proto.set_is_pseudo_column(is_pseudo_column);
  ZETASQL_RETURN_IF_ERROR(type->SerializeToProtoAndDistinctFileDescriptors(
      proto.mutable_type(), file_descriptor_set_map));
  if (annotation_map != nullptr) {
    ZETASQL_RETURN_IF_ERROR(annotation_map->Serialize(proto.mutable_annotation_map()));
  }
  if (name_parse_location_range.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(*proto.mutable_name_parse_location_range(),
                     name_parse_location_range.value().ToProto());
  }
  if (type_parse_location_range.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(*proto.mutable_type_parse_location_range(),
                     type_parse_location_range.value().ToProto());
  }
  return proto;
}

// static
absl::StatusOr<TVFSchemaColumn> TVFSchemaColumn::FromProto(
    const TVFRelationColumnProto& proto,
    const TypeDeserializer& type_deserializer) {
  ZETASQL_ASSIGN_OR_RETURN(const Type* type,
                   type_deserializer.Deserialize(proto.type()));
  const AnnotationMap* annotation_map = nullptr;
  if (proto.has_annotation_map()) {
    ZETASQL_RETURN_IF_ERROR(type_deserializer.type_factory()->DeserializeAnnotationMap(
        proto.annotation_map(), &annotation_map));
  }
  TVFRelation::Column column(proto.name(), {type, annotation_map},
                             proto.is_pseudo_column());
  ParseLocationRange location_range;
  if (proto.has_name_parse_location_range()) {
    ZETASQL_ASSIGN_OR_RETURN(
        column.name_parse_location_range,
        ParseLocationRange::Create(proto.name_parse_location_range()));
  }
  if (proto.has_type_parse_location_range()) {
    ZETASQL_ASSIGN_OR_RETURN(
        column.type_parse_location_range,
        ParseLocationRange::Create(proto.type_parse_location_range()));
  }
  return column;
}

// static
absl::StatusOr<TVFSchemaColumn> TVFSchemaColumn::FromProto(
    const TVFRelationColumnProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory) {
  return TVFSchemaColumn::FromProto(proto, TypeDeserializer(factory, pools));
}

std::string TVFRelation::GetSQLDeclaration(ProductMode product_mode) const {
  std::vector<std::string> strings;
  strings.reserve(columns().size());
  for (const Column& column : columns()) {
    strings.push_back(column.type->TypeName(product_mode));
    // Prevent concatenating value column name or empty column name
    if ((!is_value_table() || column.is_pseudo_column) &&
        !column.name.empty()) {
      strings.back() =
          absl::StrCat(ToIdentifierLiteral(column.name), " ", strings.back());
    }
  }
  return absl::StrCat("TABLE<", absl::StrJoin(strings, ", "), ">");
}

std::string TVFRelation::DebugString() const {
  std::vector<std::string> strings;
  strings.reserve(columns().size());
  for (const Column& column : columns()) {
    strings.push_back(column.DebugString(is_value_table()));
  }
  return absl::StrCat("TABLE<", absl::StrJoin(strings, ", "), ">");
}

absl::Status TVFRelation::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TVFRelationProto* proto) const {
  for (const Column& col : columns_) {
    ZETASQL_ASSIGN_OR_RETURN(*proto->add_column(),
                     col.ToProto(file_descriptor_set_map));
  }
  proto->set_is_value_table(is_value_table());
  return absl::OkStatus();
}

// static
absl::StatusOr<TVFRelation> TVFRelation::Deserialize(
    const TVFRelationProto& proto, const TypeDeserializer& type_deserializer) {
  std::vector<Column> cols;
  cols.reserve(proto.column_size());
  for (const TVFRelationColumnProto& col_proto : proto.column()) {
    ZETASQL_ASSIGN_OR_RETURN(Column column,
                     TVFSchemaColumn::FromProto(col_proto, type_deserializer));
    cols.push_back(column);
  }
  if (proto.is_value_table()) {
    AnnotatedType annotated_type = cols[0].annotated_type();
    cols.erase(cols.begin());
    return TVFRelation::ValueTable(annotated_type, cols);
  } else {
    return TVFRelation(cols);
  }
}

bool operator==(const TVFSchemaColumn& a, const TVFSchemaColumn& b) {
  return a.name == b.name && a.is_pseudo_column == b.is_pseudo_column &&
         (a.type == b.type ||
          (a.type != nullptr && b.type != nullptr && a.type->Equals(b.type))) &&
         AnnotationMap::Equals(a.annotation_map, b.annotation_map);
}

bool operator == (const TVFRelation& a, const TVFRelation& b) {
  return a.is_value_table() == b.is_value_table() &&
         std::equal(a.columns().begin(), a.columns().end(),
                    b.columns().begin(), b.columns().end());
}

std::string TVFModelArgument::DebugString() const { return "ANY MODEL"; }

std::string TVFConnectionArgument::DebugString() const {
  return "ANY CONNECTION";
}

std::string TVFDescriptorArgument::DebugString() const {
  return "ANY DESCRIPTOR";
}

absl::Status FixedOutputSchemaTVF::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  proto->set_type(FunctionEnums::FIXED_OUTPUT_SCHEMA_TVF);
  return TableValuedFunction::Serialize(file_descriptor_set_map, proto);
}

// static
absl::Status FixedOutputSchemaTVF::Deserialize(
    const TableValuedFunctionProto& proto,
    const TypeDeserializer& type_deserializer,
    std::unique_ptr<TableValuedFunction>* result) {
  std::vector<std::string> path;
  for (const std::string& name : proto.name_path()) {
    path.push_back(name);
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<FunctionSignature> signature,
      FunctionSignature::Deserialize(proto.signature(), type_deserializer));
  const TVFRelation result_schema =
      signature->result_type().options().relation_input_schema();

  std::unique_ptr<TableValuedFunctionOptions> options;
  ZETASQL_RETURN_IF_ERROR(
      TableValuedFunctionOptions::Deserialize(proto.options(), &options));

  *result = std::make_unique<FixedOutputSchemaTVF>(path, *signature,
                                                   result_schema, *options);

  if (proto.has_anonymization_info()) {
    ZETASQL_RET_CHECK(!proto.anonymization_info().userid_column_name().empty());
    const std::vector<std::string> userid_column_name_path = {
        proto.anonymization_info().userid_column_name().begin(),
        proto.anonymization_info().userid_column_name().end()};
    ZETASQL_RETURN_IF_ERROR(
        (*result)->GetAs<FixedOutputSchemaTVF>()->SetUserIdColumnNamePath(
            userid_column_name_path));
  }
  (*result)->set_statement_context(proto.statement_context());
  return absl::OkStatus();
}

absl::Status FixedOutputSchemaTVF::Resolve(
    const AnalyzerOptions* analyzer_options,
    const std::vector<TVFInputArgumentType>& actual_arguments,
    const FunctionSignature& concrete_signature, Catalog* catalog,
    TypeFactory* type_factory,
    std::shared_ptr<TVFSignature>* tvf_signature) const {
  TVFSignatureOptions options;
  options.additional_deprecation_warnings =
      concrete_signature.AdditionalDeprecationWarnings();
  tvf_signature->reset(
      new TVFSignature(actual_arguments, result_schema_, options));
  if (anonymization_info_ != nullptr) {
    auto anonymization_info =
        std::make_unique<AnonymizationInfo>(*anonymization_info_);
    tvf_signature->get()->SetAnonymizationInfo(std::move(anonymization_info));
  }
  return absl::OkStatus();
}

absl::Status ForwardInputSchemaToOutputSchemaTVF::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  proto->set_type(FunctionEnums::FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF);
  return TableValuedFunction::Serialize(file_descriptor_set_map, proto);
}

// static
absl::Status ForwardInputSchemaToOutputSchemaTVF::Deserialize(
    const TableValuedFunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result) {
  std::vector<std::string> path;
  for (const std::string& name : proto.name_path()) {
    path.push_back(name);
  }
  std::unique_ptr<FunctionSignature> signature;
  ZETASQL_ASSIGN_OR_RETURN(signature,
                   FunctionSignature::Deserialize(
                       proto.signature(), TypeDeserializer(factory, pools)));

  std::unique_ptr<TableValuedFunctionOptions> options;
  ZETASQL_RETURN_IF_ERROR(
      TableValuedFunctionOptions::Deserialize(proto.options(), &options));

  *result = std::make_unique<ForwardInputSchemaToOutputSchemaTVF>(
      path, *signature, *options);
  (*result)->set_statement_context(proto.statement_context());
  return absl::OkStatus();
}

absl::Status ForwardInputSchemaToOutputSchemaTVF::Resolve(
    const AnalyzerOptions* analyzer_options,
    const std::vector<TVFInputArgumentType>& actual_arguments,
    const FunctionSignature& concrete_signature, Catalog* catalog,
    TypeFactory* type_factory,
    std::shared_ptr<TVFSignature>* output_tvf_signature) const {
  // Check that we have at least one signature.
  ZETASQL_RET_CHECK(!signatures_.empty()) << DebugString();
  for (const FunctionSignature& signature : signatures_) {
    ZETASQL_RET_CHECK(!signature.arguments().empty()) << DebugString();
  }
  // Re-check that the function signature contains at least one argument and
  // that this argument is a relation. This should already be verified by the
  // FunctionSignature::IsValidForTableValuedFunction method.
  ZETASQL_RET_CHECK(actual_arguments[0].is_relation()) << DebugString();

  TVFSignatureOptions tvf_signature_options;
  tvf_signature_options.additional_deprecation_warnings =
      concrete_signature.AdditionalDeprecationWarnings();

  // Return the schema of the relation argument as the output schema.
  output_tvf_signature->reset(new TVFSignature(
      actual_arguments, actual_arguments[0].relation(), tvf_signature_options));
  return absl::OkStatus();
}

absl::Status ForwardInputSchemaToOutputSchemaTVF::CheckIsValid() const {
  // Check that the signature(s) actually contain a relation for the first
  // argument.
  for (const FunctionSignature& signature : signatures_) {
    ZETASQL_RET_CHECK(!signature.arguments().empty() &&
              signature.argument(0).IsRelation())
        << "Table-valued functions of type ForwardInputSchemaToOutputSchemaTVF "
        << "must accept a relation for the first argument: " << DebugString();
  }
  return absl::OkStatus();
}

absl::Status ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  proto->set_type(
      FunctionEnums::
          FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_WITH_APPENDED_COLUMNS);
  if (!extra_columns_.empty()) {
    TVFRelationProto relation_proto;
    for (const TVFSchemaColumn& column : extra_columns_) {
      TVFRelationColumnProto* column_proto_ptr = relation_proto.add_column();
      ZETASQL_ASSIGN_OR_RETURN(TVFRelationColumnProto column_proto,
                       column.ToProto(file_descriptor_set_map));
      *column_proto_ptr = column_proto;
    }
    *proto->mutable_custom_context() = relation_proto.SerializeAsString();
  }
  return TableValuedFunction::Serialize(file_descriptor_set_map, proto);
}

absl::Status ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF::Resolve(
    const AnalyzerOptions* analyzer_options,
    const std::vector<TVFInputArgumentType>& actual_arguments,
    const FunctionSignature& concrete_signature, Catalog* catalog,
    TypeFactory* type_factory,
    std::shared_ptr<TVFSignature>* output_tvf_signature) const {
  // Check that we have one signature.
  ZETASQL_RET_CHECK_EQ(signatures_.size(), 1) << DebugString();
  ZETASQL_RET_CHECK(!signatures_[0].arguments().empty()) << DebugString();
  // Check the first actual argument that is passed into the function call is a
  // relation. This should already be verified by Resolver::ResolveTVFArg
  // method.
  ZETASQL_RET_CHECK(actual_arguments[0].is_relation()) << DebugString();

  TVFSignatureOptions tvf_signature_options;
  tvf_signature_options.additional_deprecation_warnings =
      concrete_signature.AdditionalDeprecationWarnings();

  std::vector<TVFSchemaColumn> output_schema(
      actual_arguments[0].relation().columns());
  output_schema.reserve(output_schema.size() + extra_columns_.size());
  absl::flat_hash_set<std::string> input_column_names;
  input_column_names.reserve(output_schema.size());
  for (const TVFSchemaColumn& input_column : output_schema) {
    input_column_names.insert(input_column.name);
  }
  for (const TVFSchemaColumn& column : extra_columns_) {
    // Check whether extra column name is duplicated with a column from input
    // schema.
    if (input_column_names.find(column.name) != input_column_names.end()) {
      return absl::InvalidArgumentError(
          "Column name is duplicated between extra column and input schema: " +
          column.name);
    }
    output_schema.push_back(column);
  }

  // The returned schema includes the schema of the relation argument and extra
  // columns as output schema.
  output_tvf_signature->reset(new TVFSignature(
      actual_arguments, TVFRelation(output_schema), tvf_signature_options));
  return absl::OkStatus();
}

// static
absl::Status ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF::Deserialize(
    const TableValuedFunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result) {
  std::vector<std::string> path;
  path.reserve(proto.name_path_size());
  for (const std::string& name : proto.name_path()) {
    path.push_back(name);
  }
  std::unique_ptr<FunctionSignature> signature;
  ZETASQL_ASSIGN_OR_RETURN(signature,
                   FunctionSignature::Deserialize(
                       proto.signature(), TypeDeserializer(factory, pools)));

  std::vector<TVFSchemaColumn> extra_columns;
  if (proto.has_custom_context()) {
    TVFRelationProto relation_proto;
    ZETASQL_RET_CHECK(relation_proto.ParseFromString(proto.custom_context()));
    extra_columns.reserve(relation_proto.column_size());
    for (const TVFRelationColumnProto& column_proto : relation_proto.column()) {
      ZETASQL_ASSIGN_OR_RETURN(
          TVFSchemaColumn column,
          TVFSchemaColumn::FromProto(column_proto, pools, factory));
      extra_columns.push_back(column);
    }
  }

  std::unique_ptr<TableValuedFunctionOptions> options;
  ZETASQL_RETURN_IF_ERROR(
      TableValuedFunctionOptions::Deserialize(proto.options(), &options));

  *result =
      std::make_unique<ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF>(
          path, *signature, extra_columns, *options);
  (*result)->set_statement_context(proto.statement_context());
  return absl::OkStatus();
}

absl::Status ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF::
    IsValidForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
        bool isTemplated,
        absl::Span<const TVFSchemaColumn> extra_columns) const {
  ZETASQL_RET_CHECK(isTemplated) << "Does not support non-templated argument type";

  absl::flat_hash_set<std::string> name_set;
  for (const TVFSchemaColumn& column : extra_columns) {
    ZETASQL_RET_CHECK(!column.name.empty())
        << "invalid empty column name in extra columns";
    ZETASQL_RET_CHECK(!column.is_pseudo_column)
        << "extra columns cannot be pseudo column";
    ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&name_set, column.name))
        << "extra columns have duplicated column names: " + column.name;
  }

  return absl::OkStatus();
}

namespace {
static bool module_initialization_complete = []() {
  TableValuedFunction::RegisterDeserializer(
      FunctionEnums::FIXED_OUTPUT_SCHEMA_TVF,
      FixedOutputSchemaTVF::Deserialize);
  TableValuedFunction::RegisterDeserializer(
      FunctionEnums::FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF,
      ForwardInputSchemaToOutputSchemaTVF::Deserialize);
  TableValuedFunction::RegisterDeserializer(
      FunctionEnums::
          FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_WITH_APPENDED_COLUMNS,
      ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF::Deserialize);
  return true;
} ();
}  // namespace

}  // namespace zetasql
