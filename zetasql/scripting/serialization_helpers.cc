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

#include "zetasql/scripting/serialization_helpers.h"

#include "absl/status/statusor.h"

namespace zetasql {

absl::Status SerializeVariableProto(
    const VariableMap& variables, const VariableTypeParametersMap& type_params,
    google::protobuf::RepeatedPtrField<VariableProto>* variables_proto) {
  for (const auto& pair : variables) {
    const IdString name = pair.first;
    const Value& value = pair.second;
    VariableProto* variable_proto = variables_proto->Add();
    variable_proto->set_name(name.ToString());
    ZETASQL_RETURN_IF_ERROR(value.Serialize(variable_proto->mutable_value()));
    // TODO: Use SerializeToProtoAndFileDescriptors to serialize a
    // type with a deduplicated collection of FileDescriptorProtos.
    ZETASQL_RETURN_IF_ERROR(value.type()->SerializeToSelfContainedProto(
        variable_proto->mutable_type()));

    auto it = type_params.find(name);
    if (it != type_params.end()) {
      ZETASQL_RETURN_IF_ERROR(
          it->second.Serialize(variable_proto->mutable_type_params()));
    }
  }
  return absl::OkStatus();
}

absl::Status DeserializeVariableProto(
    const google::protobuf::RepeatedPtrField<VariableProto>& variables_proto,
    VariableMap* variables, VariableTypeParametersMap* variable_type_params,
    google::protobuf::DescriptorPool* descriptor_pool, IdStringPool* id_string_pool,
    TypeFactory* type_factory) {
  for (const VariableProto& variable_proto : variables_proto) {
    IdString var_name = id_string_pool->Make(variable_proto.name());
    ZETASQL_RET_CHECK(!zetasql_base::ContainsKey(*variables, var_name))
        << "Duplicate variable " << var_name.ToStringView();
    const Type* type;
    ZETASQL_RETURN_IF_ERROR(type_factory->DeserializeFromSelfContainedProto(
        variable_proto.type(), descriptor_pool, &type));
    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value, zetasql::Value::Deserialize(
                                                 variable_proto.value(), type));
    zetasql_base::InsertOrDie(variables, var_name, value);

    if (variable_proto.has_type_params()) {
      ZETASQL_ASSIGN_OR_RETURN(
          TypeParameters type_params,
          TypeParameters::Deserialize(variable_proto.type_params()));
      zetasql_base::InsertOrDie(variable_type_params, var_name, type_params);
    }
  }
  return absl::OkStatus();
}

// static
absl::StatusOr<std::unique_ptr<ProcedureDefinition>>
DeserializeProcedureDefinitionProto(
    const ScriptExecutorStateProto::ProcedureDefinition& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory) {
  std::unique_ptr<FunctionSignature> function_signature;
  ZETASQL_RETURN_IF_ERROR(FunctionSignature::Deserialize(proto.signature(), pools,
                                                 factory, &function_signature));
  if (proto.is_dynamic_sql()) {
    return absl::make_unique<ProcedureDefinition>(*function_signature,
                                                  proto.body());
  } else {
    std::vector<std::string> argument_name_list(
        proto.argument_name_list().begin(), proto.argument_name_list().end());
    return absl::make_unique<ProcedureDefinition>(
        proto.name(), *function_signature, std::move(argument_name_list),
        proto.body());
  }
}

absl::Status SerializeProcedureDefinitionProto(
    const ProcedureDefinition& procedure_definition,
    ScriptExecutorStateProto::ProcedureDefinition* proto,
    FileDescriptorSetMap* file_descriptor_set_map) {
  proto->set_name(procedure_definition.name());
  ZETASQL_RETURN_IF_ERROR(procedure_definition.signature().Serialize(
      file_descriptor_set_map, proto->mutable_signature()));
  *proto->mutable_argument_name_list() = {
      procedure_definition.argument_name_list().begin(),
      procedure_definition.argument_name_list().end()};
  proto->set_body(procedure_definition.body());
  proto->set_is_dynamic_sql(procedure_definition.is_dynamic_sql());
  return absl::OkStatus();
}

absl::Status SerializeParametersProto(
    const absl::optional<absl::variant<ParameterValueList, ParameterValueMap>>&
        parameters,
    ParametersProto* parameters_proto) {
  if (!parameters) {
    parameters_proto->set_mode(ParametersProto::NONE);
  } else if (absl::holds_alternative<ParameterValueMap>(*parameters)) {
    parameters_proto->set_mode(ParametersProto::NAMED);
    for (const auto& [name, value] :
         absl::get<ParameterValueMap>(*parameters)) {
      VariableProto* variable_proto =
          parameters_proto->mutable_variables()->Add();
      variable_proto->set_name(name);
      ZETASQL_RETURN_IF_ERROR(value.Serialize(variable_proto->mutable_value()));
      ZETASQL_RETURN_IF_ERROR(value.type()->SerializeToSelfContainedProto(
          variable_proto->mutable_type()));
    }
  } else {
    parameters_proto->set_mode(ParametersProto::POSITIONAL);
    for (const Value& value : absl::get<ParameterValueList>(*parameters)) {
      VariableProto* variable_proto =
          parameters_proto->mutable_variables()->Add();
      ZETASQL_RETURN_IF_ERROR(value.Serialize(variable_proto->mutable_value()));
      ZETASQL_RETURN_IF_ERROR(value.type()->SerializeToSelfContainedProto(
          variable_proto->mutable_type()));
    }
  }
  return absl::OkStatus();
}

absl::Status DeserializeParametersProto(
    const ParametersProto& parameters_proto,
    absl::optional<absl::variant<ParameterValueList, ParameterValueMap>>*
        parameters,
    google::protobuf::DescriptorPool* descriptor_pool, IdStringPool* id_string_pool,
    TypeFactory* type_factory) {
  if (parameters_proto.mode() == ParametersProto::NONE) {
    *parameters = {};
  } else if (parameters_proto.mode() == ParametersProto::NAMED) {
    ParameterValueMap map;
    for (const VariableProto& variable_proto : parameters_proto.variables()) {
      ZETASQL_RET_CHECK(!zetasql_base::ContainsKey(map, variable_proto.name()))
          << "Duplicate variable " << variable_proto.name();
      const Type* type;
      ZETASQL_RETURN_IF_ERROR(type_factory->DeserializeFromSelfContainedProto(
          variable_proto.type(), descriptor_pool, &type));
      ZETASQL_ASSIGN_OR_RETURN(
          zetasql::Value value,
          zetasql::Value::Deserialize(variable_proto.value(), type));
      zetasql_base::InsertOrDie(&map, variable_proto.name(), value);
    }
    *parameters = map;
  } else {
    ParameterValueList list;
    for (const VariableProto& variable_proto : parameters_proto.variables()) {
      const Type* type;
      ZETASQL_RETURN_IF_ERROR(type_factory->DeserializeFromSelfContainedProto(
          variable_proto.type(), descriptor_pool, &type));
      ZETASQL_ASSIGN_OR_RETURN(
          zetasql::Value value,
          zetasql::Value::Deserialize(variable_proto.value(), type));
      list.push_back(value);
    }
    *parameters = list;
  }
  return absl::OkStatus();
}

}  // namespace zetasql
