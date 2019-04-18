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

#include "zetasql/public/table_valued_function.h"

#include <utility>

#include "zetasql/proto/function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/signature_match_result.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

int64_t TableValuedFunction::NumSignatures() const {
  return signatures_.size();
}

const std::vector<FunctionSignature>& TableValuedFunction::signatures() const {
  return signatures_;
}

zetasql_base::Status TableValuedFunction::AddSignature(
    const FunctionSignature& function_signature) {
  ZETASQL_RET_CHECK_EQ(0, NumSignatures());
  ZETASQL_RETURN_IF_ERROR(function_signature.IsValidForTableValuedFunction())
      << function_signature.DebugString(FullName());
  signatures_.push_back(function_signature);
  return zetasql_base::OkStatus();
}

const FunctionSignature* TableValuedFunction::GetSignature(int64_t idx) const {
  if (idx < 0 || idx >= NumSignatures()) {
    return nullptr;
  }
  return &(signatures_[idx]);
}

std::string TableValuedFunction::GetSupportedSignaturesUserFacingText(
    const LanguageOptions& language_options) const {
  std::string supported_signatures;
  for (const FunctionSignature& signature : signatures()) {
    absl::StrAppend(&supported_signatures,
                    (!supported_signatures.empty() ? "; " : ""),
                    GetSignatureUserFacingText(signature, language_options));
  }
  return supported_signatures;
}

std::string TableValuedFunction::GetSignatureUserFacingText(
    const FunctionSignature& signature,
    const LanguageOptions& language_options) const {
  std::vector<std::string> argument_texts;
  for (const FunctionArgumentType& argument : signature.arguments()) {
    std::string arg_type_string =
        argument.UserFacingName(language_options.product_mode());
    // If the argument is a relation argument to a table-valued function and the
    // function signature specifies a required input schema, append the types of
    // the required columns to the user-facing signature std::string.
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
  return absl::StrCat(absl::AsciiStrToUpper(FullName()), "(",
                      absl::StrJoin(argument_texts, ", "), ")");
}

std::string TableValuedFunction::DebugString() const {
  return absl::StrCat(FullName(), (signatures_.empty() ? "" : "\n"),
                      FunctionSignature::SignaturesToString(signatures_));
}

std::string TableValuedFunction::GetTVFSignatureErrorMessage(
    const std::string& tvf_name_string,
    const std::vector<InputArgumentType>& input_arg_types,
    int signature_idx,
    const SignatureMatchResult& signature_match_result,
    const LanguageOptions& language_options) const {
  if (!signature_match_result.tvf_bad_call_error_message().empty()) {
    // TODO: Update this error message when we support more than one
    // TVF signature.
    return absl::StrCat(signature_match_result.tvf_bad_call_error_message(),
                        " of ",
                        GetSupportedSignaturesUserFacingText(language_options));
  } else {
    return absl::StrCat(
        Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
            tvf_name_string, input_arg_types, language_options.product_mode()),
        ". Supported signature", (NumSignatures() > 1 ? "s" : ""), ": ",
        GetSupportedSignaturesUserFacingText(language_options));
  }
}

zetasql_base::Status TableValuedFunction::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  for (const std::string& name : function_name_path()) {
    proto->add_name_path(name);
  }
  // TODO: Make proto->signature a repeated.
  ZETASQL_RET_CHECK_EQ(1, NumSignatures());
  ZETASQL_RETURN_IF_ERROR(GetSignature(0)->Serialize(file_descriptor_set_map,
                                             proto->mutable_signature()));
  return zetasql_base::OkStatus();
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
zetasql_base::Status TableValuedFunction::Deserialize(
    const TableValuedFunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result) {
  auto tvf_name = [proto]() { return absl::StrJoin(proto.name_path(), "."); };
  ZETASQL_RET_CHECK(proto.has_type()) << tvf_name();
  ZETASQL_RET_CHECK_NE(FunctionEnums_TableValuedFunctionType_INVALID, proto.type())
      << tvf_name();
  TableValuedFunction::TVFDeserializer deserializer =
      (*TvfDeserializers())[proto.type()];
  ZETASQL_RET_CHECK(deserializer != nullptr) << tvf_name();
  return deserializer(proto, pools, factory, result);
}

// static
void TableValuedFunction::RegisterDeserializer(
    FunctionEnums::TableValuedFunctionType type, TVFDeserializer deserializer) {
  // CHECK validated -- This is used at initialization time only.
  CHECK(FunctionEnums::TableValuedFunctionType_IsValid(type)) << type;
  // CHECK validated -- This is used at initialization time only.
  CHECK(!(*TvfDeserializers())[type]) << type;
  (*TvfDeserializers())[type] = std::move(deserializer);
}

// Serializes this TVFRelation column to a protocol buffer.
zetasql_base::StatusOr<TVFRelationColumnProto> TVFSchemaColumn::ToProto(
    FileDescriptorSetMap* file_descriptor_set_map) const {
  TVFRelationColumnProto proto;
  proto.set_name(name);
  proto.set_is_pseudo_column(is_pseudo_column);
  ZETASQL_RETURN_IF_ERROR(type->SerializeToProtoAndDistinctFileDescriptors(
      proto.mutable_type(), file_descriptor_set_map));

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
zetasql_base::StatusOr<TVFSchemaColumn> TVFSchemaColumn::FromProto(
    const TVFRelationColumnProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory) {
  const Type* type = nullptr;
  ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
      proto.type(), pools, &type));
  TVFRelation::Column column(proto.name(), type, proto.is_pseudo_column());
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

std::string TVFRelation::GetSQLDeclaration(ProductMode product_mode) const {
  std::vector<std::string> strings;
  strings.reserve(columns().size());
  for (const Column& column : columns()) {
    strings.push_back(column.type->TypeName(product_mode));
    // Prevent concatenating value column name.
    if (!is_value_table() || column.is_pseudo_column) {
      strings.back() = absl::StrCat(column.name, " ", strings.back());
    }
  }
  return absl::StrCat("TABLE<", absl::StrJoin(strings, ", "), ">");
}

std::string TVFRelation::DebugString() const {
  std::vector<std::string> strings;
  strings.reserve(columns().size());
  for (const Column& column : columns()) {
    strings.push_back(column.type->DebugString());
    // Prevent concatenating value column name.
    if (!is_value_table() || column.is_pseudo_column) {
      strings.back() = absl::StrCat(column.name, " ", strings.back());
    }
  }
  return absl::StrCat("TABLE<", absl::StrJoin(strings, ", "), ">");
}

zetasql_base::Status TVFRelation::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TVFRelationProto* proto) const {
  for (const Column& col : columns_) {
    ZETASQL_ASSIGN_OR_RETURN(*proto->add_column(),
                     col.ToProto(file_descriptor_set_map));
  }
  proto->set_is_value_table(is_value_table());
  return zetasql_base::OkStatus();
}

// static
zetasql_base::StatusOr<TVFRelation> TVFRelation::Deserialize(
    const TVFRelationProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory) {
  std::vector<Column> cols;
  cols.reserve(proto.column_size());
  for (const TVFRelationColumnProto& col_proto : proto.column()) {
    ZETASQL_ASSIGN_OR_RETURN(Column column,
                     TVFSchemaColumn::FromProto(col_proto, pools, factory));
    cols.push_back(column);
  }
  if (proto.is_value_table()) {
    const Type* type = cols[0].type;
    cols.erase(cols.begin());
    return TVFRelation::ValueTable(type, cols);
  } else {
    return TVFRelation(cols);
  }
}

std::string TVFModelArgument::GetSQLDeclaration(ProductMode product_mode) const {
  return "ANY MODEL";
}

std::string TVFModelArgument::DebugString() const { return "ANY MODEL"; }

zetasql_base::Status FixedOutputSchemaTVF::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  proto->set_type(FunctionEnums::FIXED_OUTPUT_SCHEMA_TVF);
  return TableValuedFunction::Serialize(file_descriptor_set_map, proto);
}

// static
zetasql_base::Status FixedOutputSchemaTVF::Deserialize(
    const TableValuedFunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result) {
  std::vector<std::string> path;
  for (const std::string& name : proto.name_path()) {
    path.push_back(name);
  }
  std::unique_ptr<FunctionSignature> signature;
  ZETASQL_RETURN_IF_ERROR(FunctionSignature::Deserialize(proto.signature(), pools,
                                                 factory, &signature));
  const TVFRelation result_schema =
      signature->result_type().options().relation_input_schema();
  *result =
      absl::make_unique<FixedOutputSchemaTVF>(path, *signature, result_schema);
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status FixedOutputSchemaTVF::Resolve(
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
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ForwardInputSchemaToOutputSchemaTVF::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  proto->set_type(FunctionEnums::FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF);
  return TableValuedFunction::Serialize(file_descriptor_set_map, proto);
}

// static
zetasql_base::Status ForwardInputSchemaToOutputSchemaTVF::Deserialize(
    const TableValuedFunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result) {
  std::vector<std::string> path;
  for (const std::string& name : proto.name_path()) {
    path.push_back(name);
  }
  std::unique_ptr<FunctionSignature> signature;
  ZETASQL_RETURN_IF_ERROR(FunctionSignature::Deserialize(proto.signature(), pools,
                                                 factory, &signature));
  *result =
      absl::make_unique<ForwardInputSchemaToOutputSchemaTVF>(path, *signature);
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ForwardInputSchemaToOutputSchemaTVF::Resolve(
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
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status ForwardInputSchemaToOutputSchemaTVF::CheckIsValid() const {
  // Check that the signature(s) actually contain a relation for the first
  // argument.
  for (const FunctionSignature& signature : signatures_) {
    ZETASQL_RET_CHECK(!signature.arguments().empty() &&
              signature.argument(0).IsRelation())
        << "Table-valued functions of type ForwardInputSchemaToOutputSchemaTVF "
        << "must accept a relation for the first argument: " << DebugString();
  }
  return ::zetasql_base::OkStatus();
}

namespace {
static bool module_initialization_complete = []() {
  TableValuedFunction::RegisterDeserializer(
      FunctionEnums::FIXED_OUTPUT_SCHEMA_TVF,
      FixedOutputSchemaTVF::Deserialize);
  TableValuedFunction::RegisterDeserializer(
      FunctionEnums::FORWARD_INPUT_SCHEMA_TO_OUTPUT_SCHEMA_TVF,
      ForwardInputSchemaToOutputSchemaTVF::Deserialize);
  return true;
} ();
}  // namespace

}  // namespace zetasql
