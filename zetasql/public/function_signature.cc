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

#include "zetasql/public/function_signature.h"

#include <set>

#include "zetasql/common/errors.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/resolved_ast/serialization.pb.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/types/optional.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

FunctionArgumentTypeOptions::FunctionArgumentTypeOptions(
    const TVFRelation& relation_input_schema,
    bool extra_relation_input_columns_allowed)
    : relation_input_schema_(new TVFRelation(relation_input_schema)),
      extra_relation_input_columns_allowed_(
          extra_relation_input_columns_allowed) {}

bool FunctionSignatureOptions::CheckFunctionSignatureConstraints(
    const std::vector<InputArgumentType>& arguments) const {
  if (constraints_ == nullptr || constraints_(arguments)) {
    return true;
  }
  return false;
}

absl::Status FunctionSignatureOptions::Deserialize(
    const FunctionSignatureOptionsProto& proto,
    std::unique_ptr<FunctionSignatureOptions>* result) {
  *result = absl::make_unique<FunctionSignatureOptions>();
  (*result)->set_is_deprecated(proto.is_deprecated());
  (*result)->set_additional_deprecation_warnings(
      proto.additional_deprecation_warning());

  return absl::OkStatus();
}


void FunctionSignatureOptions::Serialize(
    FunctionSignatureOptionsProto* proto) const {
  proto->set_is_deprecated(is_deprecated());
  for (const FreestandingDeprecationWarning& warning :
       additional_deprecation_warnings()) {
    *proto->add_additional_deprecation_warning() = warning;
  }
}

const FunctionEnums::ArgumentCardinality FunctionArgumentType::REQUIRED;
const FunctionEnums::ArgumentCardinality FunctionArgumentType::REPEATED;
const FunctionEnums::ArgumentCardinality FunctionArgumentType::OPTIONAL;

absl::Status FunctionArgumentType::Deserialize(
    const FunctionArgumentTypeProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory,
    std::unique_ptr<FunctionArgumentType>* result) {
  FunctionArgumentTypeOptions options;
  options.set_cardinality(proto.options().cardinality());
  options.set_must_be_constant(proto.options().must_be_constant());
  options.set_must_be_non_null(proto.options().must_be_non_null());
  options.set_is_not_aggregate(proto.options().is_not_aggregate());
  options.set_must_support_equality(proto.options().must_support_equality());
  options.set_must_support_ordering(proto.options().must_support_ordering());
  if (proto.options().has_procedure_argument_mode()) {
    options.set_procedure_argument_mode(
        proto.options().procedure_argument_mode());
  }
  if (proto.options().has_min_value()) {
    options.set_min_value(proto.options().min_value());
  }
  if (proto.options().has_max_value()) {
    options.set_max_value(proto.options().max_value());
  }
  if (proto.options().has_extra_relation_input_columns_allowed()) {
    options.set_extra_relation_input_columns_allowed(
        proto.options().extra_relation_input_columns_allowed());
  }
  if (proto.options().has_relation_input_schema()) {
    ZETASQL_ASSIGN_OR_RETURN(
        TVFRelation relation,
        TVFRelation::Deserialize(proto.options().relation_input_schema(), pools,
                                 factory));
    options = FunctionArgumentTypeOptions(
        relation, options.extra_relation_input_columns_allowed());
  }
  if (proto.options().has_argument_name()) {
    options.set_argument_name(proto.options().argument_name());
  }
  if (proto.options().has_argument_name_is_mandatory()) {
    options.set_argument_name_is_mandatory(
        proto.options().argument_name_is_mandatory());
  }
  ParseLocationRange location;
  if (proto.options().has_argument_name_parse_location()) {
    ZETASQL_ASSIGN_OR_RETURN(location,
                     ParseLocationRange::Create(
                         proto.options().argument_name_parse_location()));
    options.set_argument_name_parse_location(location);
  }
  if (proto.options().has_argument_type_parse_location()) {
    ZETASQL_ASSIGN_OR_RETURN(location,
                     ParseLocationRange::Create(
                         proto.options().argument_type_parse_location()));
    options.set_argument_type_parse_location(location);
  }
  if (proto.options().has_descriptor_resolution_table_offset()) {
    options.set_resolve_descriptor_names_table_offset(
        proto.options().descriptor_resolution_table_offset());
  }
  if (proto.kind() == ARG_TYPE_FIXED) {
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(factory->DeserializeFromProtoUsingExistingPools(
        proto.type(), pools, &type));
    *result = absl::make_unique<FunctionArgumentType>(type, options,
                                                      proto.num_occurrences());
  } else {
    *result = absl::make_unique<FunctionArgumentType>(proto.kind(), options,
                                                      proto.num_occurrences());
  }
  return absl::OkStatus();
}

absl::Status FunctionArgumentType::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    FunctionArgumentTypeProto* proto) const {
  FunctionArgumentTypeOptionsProto* options_proto = proto->mutable_options();
  options_proto->set_cardinality(cardinality());
  proto->set_kind(kind());
  proto->set_num_occurrences(num_occurrences());
  if (options().procedure_argument_mode() != FunctionEnums::NOT_SET) {
    options_proto->set_procedure_argument_mode(
        options().procedure_argument_mode());
  }
  if (options().must_be_constant()) {
    options_proto->set_must_be_constant(options().must_be_constant());
  }
  if (options().must_be_non_null()) {
    options_proto->set_must_be_non_null(options().must_be_non_null());
  }
  if (options().is_not_aggregate()) {
    options_proto->set_is_not_aggregate(options().is_not_aggregate());
  }
  if (options().must_support_equality()) {
    options_proto->set_must_support_equality(options().must_support_equality());
  }
  if (options().must_support_ordering()) {
    options_proto->set_must_support_ordering(options().must_support_ordering());
  }
  if (options().has_min_value()) {
    options_proto->set_min_value(options().min_value());
  }
  if (options().has_max_value()) {
    options_proto->set_max_value(options().max_value());
  }
  if (options().get_resolve_descriptor_names_table_offset().has_value()) {
    options_proto->set_descriptor_resolution_table_offset(
        options().get_resolve_descriptor_names_table_offset().value());
  }

  if (type() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(type()->SerializeToProtoAndDistinctFileDescriptors(
        proto->mutable_type(), file_descriptor_set_map));
  }
  options_proto->set_extra_relation_input_columns_allowed(
      options().extra_relation_input_columns_allowed());
  if (options().has_relation_input_schema()) {
    ZETASQL_RETURN_IF_ERROR(options().relation_input_schema().Serialize(
        file_descriptor_set_map,
        options_proto->mutable_relation_input_schema()));
  }
  if (options().has_argument_name()) {
    options_proto->set_argument_name(options().argument_name());
  }
  if (options().argument_name_is_mandatory()) {
    options_proto->set_argument_name_is_mandatory(true);
  }
  absl::optional<ParseLocationRange> parse_location_range =
      options().argument_name_parse_location();
  if (parse_location_range.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(*options_proto->mutable_argument_name_parse_location(),
                     parse_location_range.value().ToProto());
  }
  parse_location_range = options().argument_type_parse_location();
  if (parse_location_range.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(*options_proto->mutable_argument_type_parse_location(),
                     parse_location_range.value().ToProto());
  }
  return absl::OkStatus();
}

bool Function::is_operator() const {
  // Special case override for count(*) which is a function.
  return absl::StartsWith(Name(), "$") && Name() != "$count_star" &&
         !absl::StartsWith(Name(), "$extract");
}

// static
std::string FunctionArgumentTypeOptions::OptionsDebugString() const {
  // Print the options in a format matching proto ShortDebugString.
  // In java, we just print the proto itself.
  std::vector<std::string> options;
  if (must_be_constant_) options.push_back("must_be_constant: true");
  if (must_be_non_null_) options.push_back("must_be_non_null: true");
  if (is_not_aggregate_) options.push_back("is_not_aggregate: true");
  if (procedure_argument_mode_ != FunctionEnums::NOT_SET) {
    options.push_back(absl::StrCat(
        "procedure_argument_mode: ",
        FunctionEnums::ProcedureArgumentMode_Name(procedure_argument_mode_)));
  }
  if (options.empty()) {
    return "";
  } else {
    return absl::StrCat(" {", absl::StrJoin(options, ", "), "}");
  }
}

std::string FunctionArgumentTypeOptions::GetSQLDeclaration(
    ProductMode product_mode) const {
  std::vector<std::string> options;
  // Some of these don't currently have any SQL syntax.
  // We emit a comment for those cases.
  if (must_be_constant_) options.push_back("/*must_be_constant*/");
  if (must_be_non_null_) options.push_back("/*must_be_non_null*/");
  if (is_not_aggregate_) options.push_back("NOT AGGREGATE");
  if (options.empty()) {
    return "";
  } else {
    return absl::StrCat(" ", absl::StrJoin(options, " "));
  }
}

std::string FunctionArgumentType::SignatureArgumentKindToString(
    SignatureArgumentKind kind) {
  switch (kind) {
    case ARG_TYPE_FIXED:
      return "FIXED";
    case ARG_TYPE_ANY_1:
      return "<T1>";
    case ARG_TYPE_ANY_2:
      return "<T2>";
    case ARG_ARRAY_TYPE_ANY_1:
      return "<array<T1>>";
    case ARG_ARRAY_TYPE_ANY_2:
      return "<array<T2>>";
    case ARG_PROTO_ANY:
      return "<proto>";
    case ARG_STRUCT_ANY:
      return "<struct>";
    case ARG_ENUM_ANY:
      return "<enum>";
    case ARG_TYPE_RELATION:
      return "ANY TABLE";
    case ARG_TYPE_MODEL:
      return "ANY MODEL";
    case ARG_TYPE_CONNECTION:
      return "ANY CONNECTION";
    case ARG_TYPE_DESCRIPTOR:
      return "ANY DESCRIPTOR";
    case ARG_TYPE_ARBITRARY:
      return "<arbitrary>";
    case ARG_TYPE_VOID:
      return "<void>";
    case __SignatureArgumentKind__switch_must_have_a_default__:
      break;  // Handling this case is only allowed internally.
  }
  return "UNKNOWN_ARG_KIND";
}

std::shared_ptr<const FunctionArgumentTypeOptions>
FunctionArgumentType::SimpleOptions(ArgumentCardinality cardinality) {
  static auto* options =
      new std::vector<std::shared_ptr<const FunctionArgumentTypeOptions>>{
          std::shared_ptr<const FunctionArgumentTypeOptions>(
              new FunctionArgumentTypeOptions(FunctionEnums::REQUIRED)),
          std::shared_ptr<const FunctionArgumentTypeOptions>(
              new FunctionArgumentTypeOptions(FunctionEnums::OPTIONAL)),
          std::shared_ptr<const FunctionArgumentTypeOptions>(
              new FunctionArgumentTypeOptions(FunctionEnums::REPEATED))};
  switch (cardinality) {
    case FunctionEnums::REQUIRED:
      return (*options)[0];
    case FunctionEnums::OPTIONAL:
      return (*options)[1];
    case FunctionEnums::REPEATED:
      return (*options)[2];
  }
}

FunctionArgumentType::FunctionArgumentType(
    SignatureArgumentKind kind, const Type* type,
    std::shared_ptr<const FunctionArgumentTypeOptions> options,
    int num_occurrences)
    : kind_(kind),
      type_(type),
      options_(options),
      num_occurrences_(num_occurrences) {
  DCHECK_EQ(kind == ARG_TYPE_FIXED, type != nullptr);
}

FunctionArgumentType::FunctionArgumentType(SignatureArgumentKind kind,
                                           ArgumentCardinality cardinality,
                                           int num_occurrences)
    : FunctionArgumentType(kind, /*type=*/nullptr, SimpleOptions(cardinality),
                           num_occurrences) {}

FunctionArgumentType::FunctionArgumentType(
    SignatureArgumentKind kind, const FunctionArgumentTypeOptions& options,
    int num_occurrences)
    : FunctionArgumentType(
          kind, /*type=*/nullptr,
          std::make_shared<FunctionArgumentTypeOptions>(options),
          num_occurrences) {}

FunctionArgumentType::FunctionArgumentType(SignatureArgumentKind kind,
                                           int num_occurrences)
    : FunctionArgumentType(kind, /*type=*/nullptr, SimpleOptions(),
                           num_occurrences) {}

FunctionArgumentType::FunctionArgumentType(const Type* type,
                                           ArgumentCardinality cardinality,
                                           int num_occurrences)
    : FunctionArgumentType(ARG_TYPE_FIXED, type, SimpleOptions(cardinality),
                           num_occurrences) {}

FunctionArgumentType::FunctionArgumentType(
    const Type* type, const FunctionArgumentTypeOptions& options,
    int num_occurrences)
    : FunctionArgumentType(
          ARG_TYPE_FIXED, type,
          std::make_shared<FunctionArgumentTypeOptions>(options),
          num_occurrences) {}

FunctionArgumentType::FunctionArgumentType(const Type* type,
                                           int num_occurrences)
    : FunctionArgumentType(ARG_TYPE_FIXED, type, SimpleOptions(),
                           num_occurrences) {}

bool FunctionArgumentType::IsConcrete() const {
  if (kind_ != ARG_TYPE_FIXED && kind_ != ARG_TYPE_RELATION &&
      kind_ != ARG_TYPE_MODEL && kind_ != ARG_TYPE_CONNECTION) {
    return false;
  }
  if (num_occurrences_ < 0) {
    return false;
  }
  return true;
}

absl::Status FunctionArgumentType::IsValid() const {
  if (IsConcrete()) {
    switch (cardinality()) {
      case REPEATED:
        if (num_occurrences_ < 0) {
          return MakeSqlError()
                 << "REPEATED concrete argument has " << num_occurrences_
                 << " occurrences but must have at least 0: " << DebugString();
        }
        break;
      case OPTIONAL:
        if (num_occurrences_ < 0 || num_occurrences_ > 1) {
          return MakeSqlError()
                 << "OPTIONAL concrete argument has " << num_occurrences_
                 << " occurrences but must have 0 or 1: " << DebugString();
        }
        break;
      case REQUIRED:
        if (num_occurrences_ != 1) {
          return MakeSqlError()
                 << "REQUIRED concrete argument has " << num_occurrences_
                 << " occurrences but must have exactly 1: " << DebugString();
        }
        break;
    }
  }
  return absl::OkStatus();
}

std::string FunctionArgumentType::UserFacingName(
    ProductMode product_mode) const {
  if (type() == nullptr) {
    switch (kind()) {
      case ARG_ARRAY_TYPE_ANY_1:
      case ARG_ARRAY_TYPE_ANY_2:
        return "ARRAY";
      case ARG_PROTO_ANY:
        return "PROTO";
      case ARG_STRUCT_ANY:
        return "STRUCT";
      case ARG_ENUM_ANY:
        return "ENUM";
      case ARG_TYPE_ANY_1:
      case ARG_TYPE_ANY_2:
      case ARG_TYPE_ARBITRARY:
        return "ANY";
      case ARG_TYPE_RELATION:
        return "TABLE";
      case ARG_TYPE_MODEL:
        return "MODEL";
      case ARG_TYPE_CONNECTION:
        return "CONNECTION";
      case ARG_TYPE_DESCRIPTOR:
        return "DESCRIPTOR";
      case ARG_TYPE_VOID:
        return "VOID";
      case ARG_TYPE_FIXED:
      default:
        // We really should have had type() != nullptr in this case.
        DCHECK(type() != nullptr) << DebugString();
        return "?";
    }
  } else {
    return type()->ShortTypeName(product_mode);
  }
}

std::string FunctionArgumentType::UserFacingNameWithCardinality(
    ProductMode product_mode) const {
  std::string arg_type_string = UserFacingName(product_mode);
  if (optional()) {
    return absl::StrCat("[", arg_type_string, "]");
  } else if (repeated()) {
    return absl::StrCat("[", arg_type_string, ", ...]");
  } else {
    return arg_type_string;
  }
}

std::string FunctionArgumentType::DebugString(bool verbose) const {
  // Note, an argument cannot be both repeated and optional.
  std::string cardinality(repeated() ? "repeated"
                                     : optional() ? "optional" : "");
  std::string occurrences(IsConcrete() && !required()
                              ? absl::StrCat("(", num_occurrences_, ")")
                              : "");
  std::string result =
      absl::StrCat(cardinality, occurrences, required() ? "" : " ");
  if (type_ != nullptr) {
    absl::StrAppend(&result, type_->DebugString());
  } else if (IsRelation() && options_->has_relation_input_schema()) {
    result = options_->relation_input_schema().DebugString();
  } else if (kind_ == ARG_TYPE_ARBITRARY) {
    absl::StrAppend(&result, "ANY TYPE");
  } else {
    absl::StrAppend(&result, SignatureArgumentKindToString(kind_));
  }
  if (verbose) {
    absl::StrAppend(&result, options_->OptionsDebugString());
  }
  if (options_->has_argument_name()) {
    absl::StrAppend(&result, " ", options_->argument_name());
  }
  return result;
}

std::string FunctionArgumentType::GetSQLDeclaration(
    ProductMode product_mode) const {
  // We emit comments for the things that don't have a SQL syntax currently.
  std::string cardinality(repeated() ? "/*repeated*/"
                                     : optional() ? "/*optional*/" : "");
  std::string result = absl::StrCat(cardinality, required() ? "" : " ");
  // TODO: Consider using UserFacingName() here.
  if (type_ != nullptr) {
    absl::StrAppend(&result, type_->TypeName(product_mode));
  } else if (options_->has_relation_input_schema()) {
    absl::StrAppend(
        &result,
        options_->relation_input_schema().GetSQLDeclaration(product_mode));
  } else if (kind_ == ARG_TYPE_ARBITRARY) {
    absl::StrAppend(&result, "ANY TYPE");
  } else {
    absl::StrAppend(&result, SignatureArgumentKindToString(kind_));
  }
  absl::StrAppend(&result, options_->GetSQLDeclaration(product_mode));
  return result;
}

FunctionSignature::FunctionSignature(const FunctionArgumentType& result_type,
                                     const FunctionArgumentTypeList& arguments,
                                     void* context_ptr)
    : arguments_(arguments), result_type_(result_type),
      num_repeated_arguments_(ComputeNumRepeatedArguments()),
      num_optional_arguments_(ComputeNumOptionalArguments()),
      context_ptr_(context_ptr), options_(FunctionSignatureOptions()) {
  ZETASQL_DCHECK_OK(IsValid());
  ComputeConcreteArgumentTypes();
}

FunctionSignature::FunctionSignature(const FunctionArgumentType& result_type,
                                     const FunctionArgumentTypeList& arguments,
                                     int64_t context_id)
    : FunctionSignature(result_type, arguments, context_id,
                        FunctionSignatureOptions()) {}

FunctionSignature::FunctionSignature(const FunctionArgumentType& result_type,
                                     const FunctionArgumentTypeList& arguments,
                                     int64_t context_id,
                                     const FunctionSignatureOptions& options)
    : arguments_(arguments),
      result_type_(result_type),
      num_repeated_arguments_(ComputeNumRepeatedArguments()),
      num_optional_arguments_(ComputeNumOptionalArguments()),
      context_id_(context_id),
      options_(options) {
  ZETASQL_DCHECK_OK(IsValid());
  ComputeConcreteArgumentTypes();
}

absl::Status FunctionSignature::Deserialize(
    const FunctionSignatureProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory,
    std::unique_ptr<FunctionSignature>* result) {
  FunctionArgumentTypeList arguments;
  for (const FunctionArgumentTypeProto& argument_proto : proto.argument()) {
    std::unique_ptr<FunctionArgumentType> argument;
    ZETASQL_RETURN_IF_ERROR(FunctionArgumentType::Deserialize(
        argument_proto, pools, factory, &argument));
    arguments.push_back(*argument);
  }

  std::unique_ptr<FunctionArgumentType> result_type;
  ZETASQL_RETURN_IF_ERROR(FunctionArgumentType::Deserialize(
      proto.return_type(), pools, factory, &result_type));

  std::unique_ptr<FunctionSignatureOptions> options;
  ZETASQL_RETURN_IF_ERROR(FunctionSignatureOptions::Deserialize(
      proto.options(), &options));

  *result = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                 proto.context_id(), *options);

  return absl::OkStatus();
}

absl::Status FunctionSignature::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    FunctionSignatureProto* proto) const {
  options_.Serialize(proto->mutable_options());

  ZETASQL_RETURN_IF_ERROR(result_type().Serialize(
      file_descriptor_set_map, proto->mutable_return_type()));

  for (const FunctionArgumentType& argument : arguments()) {
    ZETASQL_RETURN_IF_ERROR(argument.Serialize(
        file_descriptor_set_map, proto->add_argument()));
  }

  proto->set_context_id(context_id());
  return absl::OkStatus();
}

bool FunctionSignature::HasUnsupportedType(
    const LanguageOptions& language_options) const {
  // The 'result_type()->type()' can be nullptr for templated
  // arguments.
  if (result_type().type() != nullptr &&
      !result_type().type()->IsSupportedType(language_options)) {
    return true;
  }
  for (const FunctionArgumentType& argument_type : arguments()) {
    // The 'argument_type.type()' can be nullptr for templated arguments.
    if (argument_type.type() != nullptr &&
        !argument_type.type()->IsSupportedType(language_options)) {
      return true;
    }
  }
  return false;
}

void FunctionSignature::ComputeConcreteArgumentTypes() {
  // TODO: Do we really care if the result signature is concrete?
  is_concrete_ = ComputeIsConcrete();
  if (!HasConcreteArguments()) return;

  // Count number of concrete args, and find the range of repeateds.
  int first_repeated_idx = -1;
  int last_repeated_idx = -1;
  int num_concrete_args = 0;
  for (int idx = 0; idx < arguments_.size(); ++idx) {
    const FunctionArgumentType& arg = arguments_[idx];
    if (arg.repeated()) {
      if (first_repeated_idx == -1) first_repeated_idx = idx;
      last_repeated_idx = idx;
    }
    if (arg.num_occurrences() > 0) {
      num_concrete_args += arg.num_occurrences();
    }
  }

  concrete_arguments_.reserve(num_concrete_args);

  if (first_repeated_idx == -1) {
    // If we have no repeateds, just loop through and copy present args.
    for (int idx = 0; idx < arguments_.size(); ++idx) {
      const FunctionArgumentType& arg = arguments_[idx];
      if (arg.num_occurrences() == 1) {
        concrete_arguments_.push_back(arg);
      }
    }
  } else {
    // Add arguments that come before repeated arguments.
    for (int idx = 0; idx < first_repeated_idx; ++idx) {
      const FunctionArgumentType& arg = arguments_[idx];
      if (arg.num_occurrences() == 1) {
        concrete_arguments_.push_back(arg);
      }
    }

    // Add concrete repetitions of all repeated arguments.
    const int num_repeated_occurrences =
        arguments_[first_repeated_idx].num_occurrences();
    for (int c = 0; c < num_repeated_occurrences; ++c) {
      for (int idx = first_repeated_idx; idx <= last_repeated_idx; ++idx) {
        concrete_arguments_.push_back(arguments_[idx]);
      }
    }

    // Add any arguments that come after the repeated arguments.
    for (int idx = last_repeated_idx + 1; idx < arguments_.size(); ++idx) {
      const FunctionArgumentType& arg = arguments_[idx];
      if (arg.num_occurrences() == 1) {
        concrete_arguments_.push_back(arg);
      }
    }
  }
}

bool FunctionSignature::HasConcreteArguments() const {
  if (is_concrete_) {
    return true;
  }
  for (const FunctionArgumentType& argument : arguments_) {
    // Missing templated arguments may have unknown types in a concrete
    // signature if they are omitted in a function call.
    if (argument.num_occurrences() > 0 &&
        !argument.IsConcrete()) {
      return false;
    }
  }
  return true;
}

bool FunctionSignature::ComputeIsConcrete() const {
  if (!HasConcreteArguments()) {
    return false;
  }
  if (result_type().IsRelation()) {
    // This signature is for a TVF, so the return type is always a relation.
    // The signature is concrete if and only if all the arguments are concrete.
    // TODO: A relation argument or result_type indicates that any
    // relation can be used, and therefore it is not concrete.  Fix this.
    return true;
  } else {
    return result_type_.IsConcrete();
  }
}

bool FunctionSignature::CheckArgumentConstraints(
    const std::vector<InputArgumentType>& arguments) const {
  return options_.CheckFunctionSignatureConstraints(arguments);
}

std::string FunctionSignature::DebugString(const std::string& function_name,
                                           bool verbose) const {
  std::string result = absl::StrCat(function_name, "(");
  int first = true;
  for (const FunctionArgumentType& argument : arguments_) {
    absl::StrAppend(&result, (first ? "" : ", "),
                    argument.DebugString(verbose));
    first = false;
  }
  absl::StrAppend(&result, ") -> ", result_type_.DebugString(verbose));
  if (verbose) {
    const std::string deprecation_warnings_debug_string =
        DeprecationWarningsToDebugString(AdditionalDeprecationWarnings());
    if (!deprecation_warnings_debug_string.empty()) {
      absl::StrAppend(&result, " ", deprecation_warnings_debug_string);
    }
  }
  return result;
}

std::string FunctionSignature::SignaturesToString(
    const std::vector<FunctionSignature>& signatures, bool verbose,
    const std::string& prefix, const std::string& separator) {
  std::string out;
  for (const FunctionSignature& signature : signatures) {
    absl::StrAppend(&out, (out.empty() ? "" : separator), prefix,
                    signature.DebugString(/*function_name=*/"", verbose));
  }
  return out;
}

std::string FunctionSignature::GetSQLDeclaration(
    const std::vector<std::string>& argument_names,
    ProductMode product_mode) const {
  std::string out = "(";
  for (int i = 0; i < arguments_.size(); ++i) {
    if (i > 0) out += ", ";
    if (arguments_[i].options().procedure_argument_mode() !=
        FunctionEnums::NOT_SET) {
      absl::StrAppend(&out,
                      FunctionEnums::ProcedureArgumentMode_Name(
                          arguments_[i].options().procedure_argument_mode()),
                      " ");
    }
    if (argument_names.size() > i) {
      absl::StrAppend(&out, ToIdentifierLiteral(argument_names[i]), " ");
    }
    absl::StrAppend(&out, arguments_[i].GetSQLDeclaration(product_mode));
  }
  absl::StrAppend(&out, ")");
  if (!result_type_.IsVoid() &&
      result_type_.kind() != ARG_TYPE_ARBITRARY &&
      !(result_type_.IsRelation() &&
        !result_type_.options().has_relation_input_schema())) {
    absl::StrAppend(&out, " RETURNS ",
                    result_type_.GetSQLDeclaration(product_mode));
  }
  return out;
}

bool FunctionArgumentType::TemplatedKindIsRelated(SignatureArgumentKind kind)
    const {
  if (!IsTemplated()) {
    return false;
  }
  if (kind_ == kind) {
    return true;
  }
  if ((kind_ == ARG_ARRAY_TYPE_ANY_1 && kind == ARG_TYPE_ANY_1) ||
      (kind_ == ARG_ARRAY_TYPE_ANY_2 && kind == ARG_TYPE_ANY_2) ||
      (kind == ARG_ARRAY_TYPE_ANY_1 && kind_ == ARG_TYPE_ANY_1) ||
      (kind == ARG_ARRAY_TYPE_ANY_2 && kind_ == ARG_TYPE_ANY_2)) {
    return true;
  }
  return false;
}

absl::Status FunctionSignature::IsValid() const {
  if (result_type_.repeated() ||
      result_type_.optional()) {
    return MakeSqlError() << "Result type cannot be repeated or optional";
  }

  // The result type can be ARBITRARY for template functions that have not
  // fully resolved the signature yet.
  //
  // For other templated result types (such as ANY_TYPE_1, ANY_PROTO, etc.)
  // the result's templated kind must match a templated kind from an argument
  // since the result type will be determined based on an argument type.
  if (result_type_.IsTemplated() &&
      result_type_.kind() != ARG_TYPE_ARBITRARY &&
      !result_type_.IsRelation()) {
    bool result_type_matches_an_argument_type = false;
    for (const auto& arg : arguments_) {
      if (result_type_.TemplatedKindIsRelated(arg.kind())) {
        result_type_matches_an_argument_type = true;
        break;
      }
    }
    if (!result_type_matches_an_argument_type) {
      return MakeSqlError()
             << "Result type template must match an argument type template: "
             << DebugString();
    }
  }
  // Optional arguments must be at the end of the argument list, and repeated
  // arguments must be consecutive.  Arguments must themselves be valid.
  bool saw_optional = false;
  bool after_repeated_block = false;
  bool in_repeated_block = false;
  for (const FunctionArgumentType& arg : arguments_) {
    ZETASQL_RETURN_IF_ERROR(arg.IsValid());
    if (arg.IsVoid()) {
      return MakeSqlError() << "Arguments cannot have type VOID: "
                            << DebugString();
    }
    if (arg.optional()) {
      saw_optional = true;
    } else if (saw_optional) {
      return MakeSqlError()
             << "Optional arguments must be at the end of the argument list: "
             << DebugString();
    }
    if (arg.repeated()) {
      if (after_repeated_block) {
        return MakeSqlError() << "Repeated arguments must be consecutive: "
                              << DebugString();
      }
      in_repeated_block = true;
    } else if (in_repeated_block) {
      after_repeated_block = true;
      in_repeated_block = false;
    }
  }
  const int first_repeated = FirstRepeatedArgumentIndex();
  if (first_repeated >= 0) {
    const int last_repeated = LastRepeatedArgumentIndex();
    const int repeated_occurrences =
        arguments_[first_repeated].num_occurrences();
    for (int i = first_repeated + 1; i <= last_repeated; ++i) {
      if (arguments_[i].num_occurrences() != repeated_occurrences) {
        return MakeSqlError()
               << "Repeated arguments must have the same num_occurrences: "
               << DebugString();
      }
    }
    if (NumRepeatedArguments() <= NumOptionalArguments()) {
      return MakeSqlError()
             << "The number of repeated arguments (" << NumRepeatedArguments()
             << ") must be greater than the number of optional arguments ("
             << NumOptionalArguments() << ") for signature: " << DebugString();
    }
  }

  // Check if descriptor's table offset arguments point to valid table
  // arguments in the same TVF call.
  for (int i = 0; i < arguments_.size(); i++) {
    const FunctionArgumentType& argument_type = arguments_[i];
    if (argument_type.IsDescriptor() &&
        argument_type.options()
            .get_resolve_descriptor_names_table_offset()
            .has_value()) {
      int table_offset = argument_type.options()
                             .get_resolve_descriptor_names_table_offset()
                             .value();
      if (table_offset < 0 || table_offset >= arguments_.size() ||
          !arguments_[table_offset].IsRelation()) {
        return MakeSqlError()
               << "The table offset argument (" << table_offset
               << ") of descriptor at argument (" << i
               << ") should point to a valid table argument for signature: "
               << DebugString();
      }
    }
  }

  return absl::OkStatus();
}

absl::Status FunctionSignature::IsValidForFunction() const {
  // Arguments and result values may not have relation types. These are special
  // types reserved only for table-valued functions.
  // TODO: Add all other constraints required to make a signature
  // valid.
  for (const FunctionArgumentType& argument : arguments()) {
    ZETASQL_RET_CHECK(!argument.IsRelation())
        << "Relation arguments are only allowed in table-valued functions: "
        << DebugString();
  }
  ZETASQL_RET_CHECK(!result_type().IsRelation())
      << "Relation return types are only allowed in table-valued functions: "
      << DebugString();
  ZETASQL_RET_CHECK(!result_type().IsVoid())
      << "Function must have a return type: " << DebugString();
  return absl::OkStatus();
}

absl::Status FunctionSignature::IsValidForTableValuedFunction() const {
  // Optional and repeated arguments before relation arguments are not
  // supported yet since ResolveTVF() currently requires that relation
  // arguments in the signature map positionally to the function call's
  // arguments.
  // TODO: Support repeated relation arguments at the end of the
  // function signature only, then update the ZETASQL_RET_CHECK below.
  bool seen_non_required_args = false;
  for (const FunctionArgumentType& argument : arguments()) {
    if (argument.IsRelation()) {
      ZETASQL_RET_CHECK(!argument.repeated())
          << "Repeated relation argument is not supported: " << DebugString();
      ZETASQL_RET_CHECK(!seen_non_required_args)
          << "Relation arguments cannot follow repeated or optional arguments: "
          << DebugString();
      // If the relation argument has a required schema, make sure that the
      // column names are unique.
      if (argument.options().has_relation_input_schema()) {
        std::set<std::string, zetasql_base::StringCaseLess> column_names;
        for (const TVFRelation::Column& column :
             argument.options().relation_input_schema().columns()) {
          ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&column_names, column.name))
              << DebugString();
        }
      }
    }
    if (argument.options().has_relation_input_schema()) {
      ZETASQL_RET_CHECK(argument.IsRelation()) << DebugString();
    }
    if (!argument.required()) {
      seen_non_required_args = true;
    }
  }
  // The result type must be a relation type, since the table-valued function
  // returns a relation.
  ZETASQL_RET_CHECK(result_type().IsRelation())
      << "Table-valued functions must have relation return type: "
      << DebugString();
  return absl::OkStatus();
}

absl::Status FunctionSignature::IsValidForProcedure() const {
  for (const FunctionArgumentType& argument : arguments()) {
    ZETASQL_RET_CHECK(!argument.IsRelation())
        << "Relation arguments are only allowed in table-valued functions: "
        << DebugString();
  }
  ZETASQL_RET_CHECK(!result_type().IsRelation())
      << "Relation return types are only allowed in table-valued functions: "
      << DebugString();
  return absl::OkStatus();
}

int FunctionSignature::FirstRepeatedArgumentIndex() const {
  for (int idx = 0; idx < arguments_.size(); ++idx) {
    if (arguments_[idx].repeated()) {
      return idx;
    }
  }
  return -1;
}

int FunctionSignature::LastRepeatedArgumentIndex() const {
  for (int idx = arguments_.size() - 1; idx >= 0; --idx) {
    if (arguments_[idx].repeated()) {
      return idx;
    }
  }
  return -1;
}

int FunctionSignature::NumRequiredArguments() const {
  return arguments_.size() - NumRepeatedArguments() - NumOptionalArguments();
}

int FunctionSignature::ComputeNumRepeatedArguments() const {
  if (FirstRepeatedArgumentIndex() == -1) {
    return 0;
  }
  return LastRepeatedArgumentIndex() - FirstRepeatedArgumentIndex() + 1;
}

int FunctionSignature::ComputeNumOptionalArguments() const {
  int idx = arguments_.size();
  while (idx - 1 >= 0 && arguments_[idx - 1].optional()) {
    --idx;
  }
  return arguments_.size() - idx;
}

void FunctionSignature::SetConcreteResultType(const Type* type) {
  result_type_ = type;
  result_type_.set_num_occurrences(1);  // Make concrete.
  // Recompute <is_concrete_> since it now may have changed by setting a
  // concrete result type.
  is_concrete_ = ComputeIsConcrete();
}

}  // namespace zetasql
