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

#include "zetasql/public/function_signature.h"

#include <cstdint>
#include <memory>
#include <set>
#include <utility>

#include "google/protobuf/util/message_differencer.h"
#include "zetasql/common/errors.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type_deserializer.h"
#include "zetasql/resolved_ast/serialization.pb.h"
#include "zetasql/base/case.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "absl/types/optional.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Helper function that returns true if an argument of <kind_> can have a
// default value.
// Currently, returns true for normal expression typed kinds, and false for
// others (model, relation, descriptor, connection, void, etc).
bool CanHaveDefaultValue(SignatureArgumentKind kind) {
  switch (kind) {
    case ARG_TYPE_FIXED:
    case ARG_TYPE_ANY_1:
    case ARG_TYPE_ANY_2:
    case ARG_ARRAY_TYPE_ANY_1:
    case ARG_ARRAY_TYPE_ANY_2:
    case ARG_PROTO_MAP_ANY:
    case ARG_PROTO_MAP_KEY_ANY:
    case ARG_PROTO_MAP_VALUE_ANY:
    case ARG_PROTO_ANY:
    case ARG_STRUCT_ANY:
    case ARG_ENUM_ANY:
    case ARG_TYPE_ARBITRARY:
      return true;
    case ARG_TYPE_RELATION:
    case ARG_TYPE_VOID:
    case ARG_TYPE_MODEL:
    case ARG_TYPE_CONNECTION:
    case ARG_TYPE_DESCRIPTOR:
      return false;
    default:
      ZETASQL_DCHECK(false) << "Invalid signature argument kind: " << kind;
      return false;
  }
}

}  // namespace

FunctionArgumentTypeOptions::FunctionArgumentTypeOptions(
    const TVFRelation& relation_input_schema,
    bool extra_relation_input_columns_allowed)
    : relation_input_schema_(new TVFRelation(relation_input_schema)),
      extra_relation_input_columns_allowed_(
          extra_relation_input_columns_allowed) {}

absl::StatusOr<bool>
FunctionSignatureOptions::CheckFunctionSignatureConstraints(
    const FunctionSignature& concrete_signature,
    const std::vector<InputArgumentType>& arguments) const {
  if (constraints_ == nullptr) {
    return true;
  }
  ZETASQL_RET_CHECK(concrete_signature.IsConcrete())
      << "FunctionSignatureArgumentConstraintsCallback must be called with a "
         "concrete signature";
  return constraints_(concrete_signature, arguments);
}

absl::Status FunctionSignatureOptions::Deserialize(
    const FunctionSignatureOptionsProto& proto,
    std::unique_ptr<FunctionSignatureOptions>* result) {
  *result = absl::make_unique<FunctionSignatureOptions>();
  (*result)->set_is_deprecated(proto.is_deprecated());
  (*result)->set_additional_deprecation_warnings(
      proto.additional_deprecation_warning());
  for (const int each : proto.required_language_feature()) {
    (*result)->add_required_language_feature(LanguageFeature(each));
  }
  (*result)->set_is_aliased_signature(proto.is_aliased_signature());
  (*result)->set_propagates_collation(proto.propagates_collation());
  (*result)->set_uses_operation_collation(proto.uses_operation_collation());
  (*result)->set_rejects_collation(proto.rejects_collation());

  return absl::OkStatus();
}

void FunctionSignatureOptions::Serialize(
    FunctionSignatureOptionsProto* proto) const {
  proto->set_is_deprecated(is_deprecated());
  for (const FreestandingDeprecationWarning& warning :
       additional_deprecation_warnings()) {
    *proto->add_additional_deprecation_warning() = warning;
  }
  for (const LanguageFeature each : required_language_features_) {
    proto->add_required_language_feature(each);
  }
  if (is_aliased_signature()) {
    proto->set_is_aliased_signature(true);
  }
  if (!propagates_collation()) {
    proto->set_propagates_collation(false);
  }
  if (uses_operation_collation()) {
    proto->set_uses_operation_collation(true);
  }
  if (rejects_collation()) {
    proto->set_rejects_collation(true);
  }
}

const FunctionEnums::ArgumentCardinality FunctionArgumentType::REQUIRED;
const FunctionEnums::ArgumentCardinality FunctionArgumentType::REPEATED;
const FunctionEnums::ArgumentCardinality FunctionArgumentType::OPTIONAL;

// static
absl::Status FunctionArgumentTypeOptions::Deserialize(
    const FunctionArgumentTypeOptionsProto& options_proto,
    const TypeDeserializer& type_deserializer, SignatureArgumentKind arg_kind,
    const Type* arg_type, FunctionArgumentTypeOptions* options) {
  options->set_cardinality(options_proto.cardinality());
  options->set_must_be_constant(options_proto.must_be_constant());
  options->set_must_be_non_null(options_proto.must_be_non_null());
  options->set_is_not_aggregate(options_proto.is_not_aggregate());
  options->set_must_support_equality(options_proto.must_support_equality());
  options->set_must_support_ordering(options_proto.must_support_ordering());
  if (options_proto.has_procedure_argument_mode()) {
    options->set_procedure_argument_mode(
        options_proto.procedure_argument_mode());
  }
  if (options_proto.has_min_value()) {
    options->set_min_value(options_proto.min_value());
  }
  if (options_proto.has_max_value()) {
    options->set_max_value(options_proto.max_value());
  }
  if (options_proto.has_extra_relation_input_columns_allowed()) {
    options->set_extra_relation_input_columns_allowed(
        options_proto.extra_relation_input_columns_allowed());
  }
  if (options_proto.has_relation_input_schema()) {
    // TODO: propagate TypeDeserializer through TVFRelation::Deserialize.
    ZETASQL_ASSIGN_OR_RETURN(TVFRelation relation,
                     TVFRelation::Deserialize(
                         options_proto.relation_input_schema(),
                         std::vector<const google::protobuf::DescriptorPool*>(
                             type_deserializer.descriptor_pools().begin(),
                             type_deserializer.descriptor_pools().end()),
                         type_deserializer.type_factory()));
    *options = FunctionArgumentTypeOptions(
        relation, options->extra_relation_input_columns_allowed());
  }
  if (options_proto.has_argument_name()) {
    options->set_argument_name(options_proto.argument_name());
  }
  if (options_proto.has_argument_name_is_mandatory()) {
    options->set_argument_name_is_mandatory(
        options_proto.argument_name_is_mandatory());
  }
  ParseLocationRange location;
  if (options_proto.has_argument_name_parse_location()) {
    ZETASQL_ASSIGN_OR_RETURN(location,
                     ParseLocationRange::Create(
                         options_proto.argument_name_parse_location()));
    options->set_argument_name_parse_location(location);
  }
  if (options_proto.has_argument_type_parse_location()) {
    ZETASQL_ASSIGN_OR_RETURN(location,
                     ParseLocationRange::Create(
                         options_proto.argument_type_parse_location()));
    options->set_argument_type_parse_location(location);
  }
  if (options_proto.has_descriptor_resolution_table_offset()) {
    options->set_resolve_descriptor_names_table_offset(
        options_proto.descriptor_resolution_table_offset());
  }
  if (options_proto.has_default_value()) {
    if (!CanHaveDefaultValue(arg_kind)) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << FunctionArgumentType::SignatureArgumentKindToString(arg_kind)
             << " argument cannot have a default value";
    }
    const Type* default_value_type = arg_type;
    // For templated arguments, we use
    // `FunctionArgumentTypeOptionsProto.default_value_type` to help
    // deserializing the default value, while for fixed type arguments, we use
    // directly `type` which is from `FunctionArgumentTypeProto.type`. Only one
    // of the two types will be set.
    if (options_proto.has_default_value_type()) {
      ZETASQL_RET_CHECK_EQ(arg_type, nullptr);
      ZETASQL_ASSIGN_OR_RETURN(
          default_value_type,
          type_deserializer.Deserialize(options_proto.default_value_type()));
    }
    ZETASQL_RET_CHECK_NE(default_value_type, nullptr);
    ZETASQL_ASSIGN_OR_RETURN(Value value,
                     Value::Deserialize(options_proto.default_value(),
                                        default_value_type));
    options->set_default(std::move(value));
  }
  if (options_proto.has_argument_collation_mode()) {
    options->set_argument_collation_mode(
        options_proto.argument_collation_mode());
  }
  if (options_proto.has_uses_array_element_for_collation()) {
    options->set_uses_array_element_for_collation(
        options_proto.uses_array_element_for_collation());
  }
  return absl::OkStatus();
}

// static
absl::StatusOr<std::unique_ptr<FunctionArgumentType>>
FunctionArgumentType::Deserialize(const FunctionArgumentTypeProto& proto,
                                  const TypeDeserializer& type_deserializer) {
  const Type* type = nullptr;
  if (proto.kind() == ARG_TYPE_FIXED) {
    ZETASQL_ASSIGN_OR_RETURN(type, type_deserializer.Deserialize(proto.type()));
  }

  FunctionArgumentTypeOptions options;
  ZETASQL_RETURN_IF_ERROR(FunctionArgumentTypeOptions::Deserialize(
      proto.options(), type_deserializer, proto.kind(), type, &options));

  if (type != nullptr) {
    // <type> can not be nullptr when proto.kind() == ARG_TYPE_FIXED
    return absl::make_unique<FunctionArgumentType>(type, options,
                                                   proto.num_occurrences());
  }

  if (proto.kind() == ARG_TYPE_LAMBDA) {
    auto result = absl::make_unique<FunctionArgumentType>(ARG_TYPE_LAMBDA);
    std::vector<FunctionArgumentType> lambda_argument_types;
    for (const FunctionArgumentTypeProto& arg_proto :
         proto.lambda().argument()) {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<FunctionArgumentType> arg_type,
          FunctionArgumentType::Deserialize(arg_proto, type_deserializer));
      lambda_argument_types.push_back(*arg_type);
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<FunctionArgumentType> lambda_body_type,
                     FunctionArgumentType::Deserialize(proto.lambda().body(),
                                                       type_deserializer));
    (*result) = FunctionArgumentType::Lambda(std::move(lambda_argument_types),
                                             std::move(*lambda_body_type));
    return result;
  }

  return absl::make_unique<FunctionArgumentType>(proto.kind(), options,
                                                 proto.num_occurrences());
}

absl::Status FunctionArgumentTypeOptions::Serialize(
    const Type* arg_type, FunctionArgumentTypeOptionsProto* options_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  options_proto->set_cardinality(cardinality());
  if (procedure_argument_mode() != FunctionEnums::NOT_SET) {
    options_proto->set_procedure_argument_mode(procedure_argument_mode());
  }
  if (must_be_constant()) {
    options_proto->set_must_be_constant(must_be_constant());
  }
  if (must_be_non_null()) {
    options_proto->set_must_be_non_null(must_be_non_null());
  }
  if (is_not_aggregate()) {
    options_proto->set_is_not_aggregate(is_not_aggregate());
  }
  if (must_support_equality()) {
    options_proto->set_must_support_equality(must_support_equality());
  }
  if (must_support_ordering()) {
    options_proto->set_must_support_ordering(must_support_ordering());
  }
  if (has_min_value()) {
    options_proto->set_min_value(min_value());
  }
  if (has_max_value()) {
    options_proto->set_max_value(max_value());
  }
  if (get_resolve_descriptor_names_table_offset().has_value()) {
    options_proto->set_descriptor_resolution_table_offset(
        get_resolve_descriptor_names_table_offset().value());
  }
  if (get_default().has_value()) {
    const Value& default_value = get_default().value();
    ZETASQL_RETURN_IF_ERROR(
        default_value.Serialize(options_proto->mutable_default_value()));
    if (arg_type == nullptr) {
      ZETASQL_RETURN_IF_ERROR(
          default_value.type()->SerializeToProtoAndDistinctFileDescriptors(
              options_proto->mutable_default_value_type(),
              file_descriptor_set_map));
    }
  }
  options_proto->set_extra_relation_input_columns_allowed(
      extra_relation_input_columns_allowed());
  if (has_relation_input_schema()) {
    ZETASQL_RETURN_IF_ERROR(relation_input_schema().Serialize(
        file_descriptor_set_map,
        options_proto->mutable_relation_input_schema()));
  }
  if (has_argument_name()) {
    options_proto->set_argument_name(argument_name());
  }
  if (argument_name_is_mandatory()) {
    options_proto->set_argument_name_is_mandatory(true);
  }
  absl::optional<ParseLocationRange> parse_location_range =
      argument_name_parse_location();
  if (parse_location_range.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(*options_proto->mutable_argument_name_parse_location(),
                     parse_location_range.value().ToProto());
  }
  parse_location_range = argument_type_parse_location();
  if (parse_location_range.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(*options_proto->mutable_argument_type_parse_location(),
                     parse_location_range.value().ToProto());
  }
  if (argument_collation_mode() !=
      FunctionEnums::AFFECTS_OPERATION_AND_PROPAGATION) {
    options_proto->set_argument_collation_mode(argument_collation_mode());
  }
  if (uses_array_element_for_collation()) {
    options_proto->set_uses_array_element_for_collation(true);
  }
  return absl::OkStatus();
}

absl::Status FunctionArgumentType::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    FunctionArgumentTypeProto* proto) const {
  proto->set_kind(kind());
  proto->set_num_occurrences(num_occurrences());

  if (type() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(type()->SerializeToProtoAndDistinctFileDescriptors(
        proto->mutable_type(), file_descriptor_set_map));
  }

  ZETASQL_RETURN_IF_ERROR(options().Serialize(type(), proto->mutable_options(),
                                      file_descriptor_set_map));

  if (IsLambda()) {
    for (const FunctionArgumentType& arg_type : lambda().argument_types()) {
      ZETASQL_RETURN_IF_ERROR(arg_type.Serialize(
          file_descriptor_set_map, proto->mutable_lambda()->add_argument()));
    }
    ZETASQL_RETURN_IF_ERROR(lambda().body_type().Serialize(
        file_descriptor_set_map, proto->mutable_lambda()->mutable_body()));
  }

  return absl::OkStatus();
}

FunctionArgumentType FunctionArgumentType::Lambda(
    std::vector<FunctionArgumentType> lambda_argument_types,
    FunctionArgumentType lambda_body_type) {
  // For now, we don't have the use cases of non REQUIRED values.
  FunctionArgumentType arg_type = FunctionArgumentType(ARG_TYPE_LAMBDA);
  arg_type.lambda_ = std::make_shared<ArgumentTypeLambda>(
      std::move(lambda_argument_types), std::move(lambda_body_type));
  arg_type.num_occurrences_ = 1;
  arg_type.type_ = nullptr;
  return arg_type;
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
  if (default_.has_value()) {
    options.push_back(
        absl::StrCat("default_value: ", default_->ShortDebugString()));
  }
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
  if (default_.has_value()) {
    options.push_back("DEFAULT");
    options.push_back(default_->GetSQLLiteral(product_mode));
  }
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
    case ARG_PROTO_MAP_ANY:
      return "<map<K, V>>";
    case ARG_PROTO_MAP_KEY_ANY:
      return "<K>";
    case ARG_PROTO_MAP_VALUE_ANY:
      return "<V>";
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
    case ARG_TYPE_LAMBDA:
      return "ANY LAMBDA";
    case __SignatureArgumentKind__switch_must_have_a_default__:
      break;  // Handling this case is only allowed internally.
  }
  return "UNKNOWN_ARG_KIND";
}

std::shared_ptr<const FunctionArgumentTypeOptions>
FunctionArgumentType::SimpleOptions(ArgumentCardinality cardinality) {
  static auto* options =
      new std::array<std::shared_ptr<const FunctionArgumentTypeOptions>, 3>{
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
      num_occurrences_(num_occurrences),
      type_(type),
      options_(std::move(options)) {
  ZETASQL_DCHECK_EQ(kind == ARG_TYPE_FIXED, type != nullptr);
}

FunctionArgumentType::FunctionArgumentType(SignatureArgumentKind kind,
                                           ArgumentCardinality cardinality,
                                           int num_occurrences)
    : FunctionArgumentType(kind, /*type=*/nullptr, SimpleOptions(cardinality),
                           num_occurrences) {}

FunctionArgumentType::FunctionArgumentType(SignatureArgumentKind kind,
                                           FunctionArgumentTypeOptions options,
                                           int num_occurrences)
    : FunctionArgumentType(
          kind, /*type=*/nullptr,
          std::make_shared<FunctionArgumentTypeOptions>(std::move(options)),
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

FunctionArgumentType::FunctionArgumentType(const Type* type,
                                           FunctionArgumentTypeOptions options,
                                           int num_occurrences)
    : FunctionArgumentType(
          ARG_TYPE_FIXED, type,
          std::make_shared<FunctionArgumentTypeOptions>(std::move(options)),
          num_occurrences) {}

FunctionArgumentType::FunctionArgumentType(const Type* type,
                                           int num_occurrences)
    : FunctionArgumentType(ARG_TYPE_FIXED, type, SimpleOptions(),
                           num_occurrences) {}

bool FunctionArgumentType::IsConcrete() const {
  if (kind_ != ARG_TYPE_FIXED && kind_ != ARG_TYPE_RELATION &&
      kind_ != ARG_TYPE_MODEL && kind_ != ARG_TYPE_CONNECTION &&
      kind_ != ARG_TYPE_LAMBDA) {
    return false;
  }
  if (num_occurrences_ < 0) {
    return false;
  }

  // Lambda is concrete if all args and body are concrete.
  if (kind_ == ARG_TYPE_LAMBDA) {
    for (const auto& arg : lambda().argument_types()) {
      if (!arg.IsConcrete()) {
        return false;
      }
    }
    return lambda().body_type().IsConcrete();
  }
  return true;
}

bool FunctionArgumentType::IsTemplated() const {
  // It is templated if it is not a fixed scalar, it is not a fixed relation,
  // and it is not a void argument. It is also templated if it is a lambda that
  // has a templated argument or body.
  if (kind_ == ARG_TYPE_LAMBDA) {
    for (const FunctionArgumentType& arg_type : lambda().argument_types()) {
      if (arg_type.IsTemplated()) {
        return true;
      }
    }
    return lambda().body_type().IsTemplated();
  }
  return kind_ != ARG_TYPE_FIXED && !IsFixedRelation() && !IsVoid();
}

bool FunctionArgumentType::IsScalar() const {
  return kind_ == ARG_TYPE_FIXED || kind_ == ARG_TYPE_ANY_1 ||
         kind_ == ARG_TYPE_ANY_2 || kind_ == ARG_ARRAY_TYPE_ANY_1 ||
         kind_ == ARG_ARRAY_TYPE_ANY_2 || kind_ == ARG_PROTO_MAP_ANY ||
         kind_ == ARG_PROTO_MAP_KEY_ANY || kind_ == ARG_PROTO_MAP_VALUE_ANY ||
         kind_ == ARG_PROTO_ANY || kind_ == ARG_STRUCT_ANY ||
         kind_ == ARG_ENUM_ANY || kind_ == ARG_TYPE_ARBITRARY;
}

// Intentionally restrictive for known functional programming functions. If this
// is to be expanded in the future, make sure type inference part of signature
// matching works as intended.
static bool IsLambdaAllowedArgKind(const SignatureArgumentKind kind) {
  return kind == ARG_TYPE_FIXED || kind == ARG_TYPE_ANY_1 ||
         kind == ARG_TYPE_ANY_2;
}

absl::Status FunctionArgumentType::CheckLambdaArgType(
    const FunctionArgumentType& arg_type) {
  if (!IsLambdaAllowedArgKind(arg_type.kind())) {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Argument kind not supported by lambda: "
           << SignatureArgumentKindToString(arg_type.kind());
  }

  // Make sure the argument type options are just simple REQUIRED options.
  zetasql::Type::FileDescriptorSetMap arg_fdset_map;
  FunctionArgumentTypeOptionsProto arg_options_proto;
  ZETASQL_RETURN_IF_ERROR(arg_type.options().Serialize(
      /*arg_type=*/nullptr, &arg_options_proto, &arg_fdset_map));
  ZETASQL_RET_CHECK(arg_fdset_map.empty());

  FunctionArgumentTypeOptionsProto simple_options_proto;
  zetasql::Type::FileDescriptorSetMap simple_arg_fdset_map;
  ZETASQL_RETURN_IF_ERROR(SimpleOptions(REQUIRED)->Serialize(
      nullptr, &simple_options_proto, &simple_arg_fdset_map));
  ZETASQL_RET_CHECK(simple_arg_fdset_map.empty());

  ZETASQL_RET_CHECK(google::protobuf::util::MessageDifferencer::Equals(arg_options_proto,
                                                     simple_options_proto))
      << "Only REQUIRED simple options are supported by lambda";
  return absl::OkStatus();
}

absl::Status FunctionArgumentType::IsValid(ProductMode product_mode) const {
  switch (cardinality()) {
    case REPEATED:
      if (IsConcrete()) {
        if (num_occurrences_ < 0) {
          return MakeSqlError()
                 << "REPEATED concrete argument has " << num_occurrences_
                 << " occurrences but must have at least 0: " << DebugString();
        }
      }
      if (HasDefault()) {
        return MakeSqlError()
               << "Default value cannot be applied to a REPEATED argument: "
               << DebugString();
      }
      break;
    case OPTIONAL:
      if (IsConcrete()) {
        if (num_occurrences_ < 0 || num_occurrences_ > 1) {
          return MakeSqlError()
                 << "OPTIONAL concrete argument has " << num_occurrences_
                 << " occurrences but must have 0 or 1: " << DebugString();
        }
      }
      if (HasDefault()) {
        if (!CanHaveDefaultValue(kind())) {
          // Relation/Model/Connection/Descriptor arguments cannot have
          // default values.
          return MakeSqlError()
                 << SignatureArgumentKindToString(kind())
                 << " argument cannot have a default value: " << DebugString();
        }
        if (!GetDefault().value().is_valid()) {
          return MakeSqlError()
                 << "Default value must be valid: " << DebugString();
        }
        // Verify type match for fixed-typed arguments.
        if (type() != nullptr && !GetDefault().value().type()->Equals(type())) {
          return MakeSqlError()
                 << "Default value type does not match the argument type: "
                 << type()->ShortTypeName(product_mode) << " vs "
                 << GetDefault().value().type()->ShortTypeName(product_mode)
                 << "; " << DebugString();
        }
      }
      break;
    case REQUIRED:
      if (IsConcrete()) {
        if (num_occurrences_ != 1) {
          return MakeSqlError()
                 << "REQUIRED concrete argument has " << num_occurrences_
                 << " occurrences but must have exactly 1: " << DebugString();
        }
      }
      if (HasDefault()) {
        return MakeSqlError()
               << "Default value cannot be applied to a REQUIRED argument: "
               << DebugString();
      }
      break;
  }

  if (IsLambda()) {
    ZETASQL_RET_CHECK_EQ(cardinality(), REQUIRED);
    for (const auto& arg_type : lambda().argument_types()) {
      ZETASQL_RETURN_IF_ERROR(CheckLambdaArgType(arg_type));
    }
    ZETASQL_RETURN_IF_ERROR(CheckLambdaArgType(lambda().body_type()));
  }
  return absl::OkStatus();
}

std::string FunctionArgumentType::UserFacingName(
    ProductMode product_mode) const {
  if (IsLambda()) {
    // If we only return "LAMBDA", for signature not found error, the user would
    // get a list of two identical signature strings.
    std::string args = absl::StrJoin(
        lambda().argument_types(), ", ",
        [product_mode](std::string* out, const FunctionArgumentType& arg) {
          out->append(arg.UserFacingName(product_mode));
        });
    if (lambda().argument_types().size() == 1) {
      return absl::Substitute(
          "LAMBDA($0->$1)", args,
          lambda().body_type().UserFacingName(product_mode));
    }
    return absl::Substitute("LAMBDA(($0)->$1)", args,
                            lambda().body_type().UserFacingName(product_mode));
  }
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
      case ARG_PROTO_MAP_ANY:
        return "PROTO_MAP";
      case ARG_PROTO_MAP_KEY_ANY:
        return "PROTO_MAP_KEY";
      case ARG_PROTO_MAP_VALUE_ANY:
        return "PROTO_MAP_VALUE";
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
      case ARG_TYPE_LAMBDA:
        return "LAMBDA";
      case ARG_TYPE_FIXED:
      default:
        // We really should have had type() != nullptr in this case.
        ZETASQL_DCHECK(type() != nullptr) << DebugString();
        return "?";
    }
  } else {
    return type()->ShortTypeName(product_mode);
  }
}

std::string FunctionArgumentType::UserFacingNameWithCardinality(
    ProductMode product_mode) const {
  std::string arg_type_string = UserFacingName(product_mode);
  if (options().argument_name_is_mandatory()) {
    arg_type_string = absl::StrCat(argument_name(), " => ", arg_type_string);
  }
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
  std::string cardinality(repeated()   ? "repeated"
                          : optional() ? "optional"
                                       : "");
  std::string occurrences(IsConcrete() && !required()
                              ? absl::StrCat("(", num_occurrences_, ")")
                              : "");
  std::string result =
      absl::StrCat(cardinality, occurrences, required() ? "" : " ");
  if (IsLambda()) {
    std::string args = absl::StrJoin(
        lambda().argument_types(), ", ",
        [verbose](std::string* out, const FunctionArgumentType& arg) {
          out->append(arg.DebugString(verbose));
        });
    if (lambda().argument_types().size() == 1) {
      absl::SubstituteAndAppend(&result, "LAMBDA($0->$1)", args,
                                lambda().body_type().DebugString());
    } else {
      absl::SubstituteAndAppend(&result, "LAMBDA(($0)->$1)", args,
                                lambda().body_type().DebugString());
    }
  } else if (type_ != nullptr) {
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
  if (IsLambda()) {
    std::string args = absl::StrJoin(
        lambda().argument_types(), ", ",
        [product_mode](std::string* out, const FunctionArgumentType& arg) {
          out->append(arg.GetSQLDeclaration(product_mode));
        });
    if (lambda().argument_types().size() == 1) {
      return absl::Substitute(
          "LAMBDA($0->$1)", args,
          lambda().body_type().GetSQLDeclaration(product_mode));
    }
    return absl::Substitute(
        "LAMBDA(($0)->$1)", args,
        lambda().body_type().GetSQLDeclaration(product_mode));
  }
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

FunctionSignature::FunctionSignature(FunctionArgumentType result_type,
                                     FunctionArgumentTypeList arguments,
                                     void* context_ptr)
    : arguments_(std::move(arguments)),
      result_type_(std::move(result_type)),
      num_repeated_arguments_(ComputeNumRepeatedArguments()),
      num_optional_arguments_(ComputeNumOptionalArguments()),
      context_ptr_(context_ptr) {
  ZETASQL_DCHECK_OK(IsValid(ProductMode::PRODUCT_EXTERNAL));
  ComputeConcreteArgumentTypes();
}

FunctionSignature::FunctionSignature(FunctionArgumentType result_type,
                                     FunctionArgumentTypeList arguments,
                                     int64_t context_id)
    : FunctionSignature(std::move(result_type), std::move(arguments),
                        context_id, FunctionSignatureOptions()) {}

FunctionSignature::FunctionSignature(FunctionArgumentType result_type,
                                     FunctionArgumentTypeList arguments,
                                     int64_t context_id,
                                     FunctionSignatureOptions options)
    : arguments_(std::move(arguments)),
      result_type_(std::move(result_type)),
      num_repeated_arguments_(ComputeNumRepeatedArguments()),
      num_optional_arguments_(ComputeNumOptionalArguments()),
      context_id_(context_id),
      options_(std::move(options)) {
  ZETASQL_DCHECK_OK(IsValid(ProductMode::PRODUCT_EXTERNAL));
  ComputeConcreteArgumentTypes();
}

absl::Status FunctionSignature::Deserialize(
    const FunctionSignatureProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory,
    std::unique_ptr<FunctionSignature>* result) {
  ZETASQL_ASSIGN_OR_RETURN(*result,
                   Deserialize(proto, TypeDeserializer(factory, pools)));
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<FunctionSignature>>
FunctionSignature::Deserialize(const FunctionSignatureProto& proto,
                               const TypeDeserializer& type_deserializer) {
  FunctionArgumentTypeList arguments;
  for (const FunctionArgumentTypeProto& argument_proto : proto.argument()) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<FunctionArgumentType> argument,
        FunctionArgumentType::Deserialize(argument_proto, type_deserializer));
    arguments.push_back(*argument);
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<FunctionArgumentType> result_type,
                   FunctionArgumentType::Deserialize(proto.return_type(),
                                                     type_deserializer));

  std::unique_ptr<FunctionSignatureOptions> options;
  ZETASQL_RETURN_IF_ERROR(FunctionSignatureOptions::Deserialize(
      proto.options(), &options));

  return absl::make_unique<FunctionSignature>(*result_type, arguments,
                                              proto.context_id(), *options);
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

absl::StatusOr<bool> FunctionSignature::CheckArgumentConstraints(
    const std::vector<InputArgumentType>& arguments) const {
  return options_.CheckFunctionSignatureConstraints(*this, arguments);
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
    if (options_.rejects_collation()) {
      absl::StrAppend(&result, " rejects_collation=TRUE");
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

  if (IsLambda()) {
    for (const FunctionArgumentType& arg_type : lambda().argument_types()) {
      if (arg_type.TemplatedKindIsRelated(kind)) {
        return true;
      }
    }
    if (lambda().body_type().TemplatedKindIsRelated(kind)) {
      return true;
    }
    return false;
  }

  if ((kind_ == ARG_ARRAY_TYPE_ANY_1 && kind == ARG_TYPE_ANY_1) ||
      (kind_ == ARG_ARRAY_TYPE_ANY_2 && kind == ARG_TYPE_ANY_2) ||
      (kind == ARG_ARRAY_TYPE_ANY_1 && kind_ == ARG_TYPE_ANY_1) ||
      (kind == ARG_ARRAY_TYPE_ANY_2 && kind_ == ARG_TYPE_ANY_2) ||
      (kind == ARG_PROTO_MAP_ANY && kind_ == ARG_PROTO_MAP_KEY_ANY) ||
      (kind_ == ARG_PROTO_MAP_ANY && kind == ARG_PROTO_MAP_KEY_ANY) ||
      (kind == ARG_PROTO_MAP_ANY && kind_ == ARG_PROTO_MAP_VALUE_ANY) ||
      (kind_ == ARG_PROTO_MAP_ANY && kind == ARG_PROTO_MAP_VALUE_ANY)) {
    return true;
  }
  return false;
}

absl::Status FunctionSignature::IsValid(ProductMode product_mode) const {
  if (result_type_.repeated() || result_type_.optional()) {
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
      if (arg.TemplatedKindIsRelated(result_type_.kind())) {
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
  bool saw_default_value = false;
  bool after_repeated_block = false;
  bool in_repeated_block = false;
  absl::flat_hash_set<SignatureArgumentKind> templated_kind_used_by_lambda;
  for (int arg_index = 0; arg_index < arguments().size(); arg_index++) {
    const auto& arg = arguments()[arg_index];
    ZETASQL_RETURN_IF_ERROR(arg.IsValid(product_mode));
    if (arg.IsVoid()) {
      return MakeSqlError()
             << "Arguments cannot have type VOID: " << DebugString();
    }
    if (arg.optional()) {
      saw_optional = true;
      if (arg.HasDefault()) {
        saw_default_value = true;
      } else if (saw_default_value) {
        return MakeSqlError() << "Optional arguments with default values must "
                                 "be at the end of the argument list: "
                              << DebugString();
      }
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

    if (arg.IsLambda()) {
      // We require an argument of lambda type is related to a previous
      // argument. For example, the following function signature is not allowed:
      //   Func(LAMBDA(T1->BOOL), ARRAY(T1))
      // The concern is the above function requires two pass for readers and the
      // resolver of a function call to understand the call. All of the known
      // functions meets this requirement. Could be relaxed if the need arises.
      for (const auto& lambda_arg_type : arg.lambda().argument_types()) {
        bool has_tempalted_args = false;
        bool is_related_to_previous_function_arg = false;
        for (int j = 0; j < arg_index; j++) {
          if (!lambda_arg_type.IsTemplated()) {
            continue;
          }
          templated_kind_used_by_lambda.insert(lambda_arg_type.kind());
          has_tempalted_args = true;
          if (lambda_arg_type.TemplatedKindIsRelated(arguments()[j].kind())) {
            is_related_to_previous_function_arg = true;
          }
        }
        if (has_tempalted_args && !is_related_to_previous_function_arg) {
          return MakeSqlError()
                 << "Templated argument of lambda argument type must match an "
                    "argument type before the lambda argument. Function "
                    "signature: "
                 << DebugString();
        }
      }
    } else {
      if (templated_kind_used_by_lambda.contains(arg.kind())) {
        return MakeSqlError()
               << "Templated argument kind used by lambda argument cannot be "
                  "used by arguments to the right of the lambda using it. "
                  "Kind: "
               << FunctionArgumentType::SignatureArgumentKindToString(
                      arg.kind())
               << " at index: " << arg_index;
      }
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
  // Repeated arguments before relation arguments are not
  // supported yet since ResolveTVF() currently requires that relation
  // arguments in the signature map positionally to the function call's
  // arguments.
  // TODO: Support repeated relation arguments at the end of the
  // function signature only, then update the ZETASQL_RET_CHECK below.
  bool seen_repeated_args = false;
  for (const FunctionArgumentType& argument : arguments()) {
    if (argument.IsRelation()) {
      ZETASQL_RET_CHECK(!argument.repeated())
          << "Repeated relation argument is not supported: " << DebugString();
      ZETASQL_RET_CHECK(!seen_repeated_args)
          << "Relation arguments cannot follow repeated arguments: "
          << DebugString();
      // If the relation argument has a required schema, make sure that the
      // column names are unique.
      if (argument.options().has_relation_input_schema()) {
        std::set<std::string, zetasql_base::CaseLess> column_names;
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
    if (argument.repeated()) {
      seen_repeated_args = true;
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
