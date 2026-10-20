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

#include "googlesql/public/annotation/vector_encoding.h"

#include <optional>
#include <string>

#include "googlesql/common/errors.h"
#include "googlesql/public/annotation/default_annotation_spec.h"
#include "googlesql/public/builtin_function.pb.h"
#include "googlesql/public/constant.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/parse_location.h"
#include "googlesql/public/proto/vector_encoding_id.pb.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/simple_value.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_parameters.h"
#include "googlesql/public/types/vector_type_util.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace googlesql {

namespace {

constexpr absl::string_view kDefaultVectorEncoding = "FLOAT32";

absl::StatusOr<googlesql::VectorEncodingId::Id> ParseAndValidateVectorEncoding(
    absl::string_view encoding_str) {
  std::string upper_encoding(encoding_str);
  absl::AsciiStrToUpper(&upper_encoding);
  googlesql::VectorEncodingId::Id encoding_enum;
  if (!googlesql::VectorEncodingId_Id_Parse(upper_encoding, &encoding_enum) ||
      encoding_enum == googlesql::VectorEncodingId::UNKNOWN_VECTOR_ENCODING) {
    return MakeSqlError() << R"(Unrecognized VECTOR encoding: ")"
                          << encoding_str << R"(")";
  }
  return encoding_enum;
}

// The vector encoding annotation can be deduced from a constant literal or a
// constant. Constant expressions like `CONCAT('FLOAT', '32')` are not
// supported.
absl::StatusOr<std::optional<std::string>> GetConstantOrLiteralString(
    const ResolvedExpr* expr) {
  if (expr->Is<ResolvedLiteral>()) {
    const Value& val = expr->GetAs<ResolvedLiteral>()->value();
    GOOGLESQL_RET_CHECK(val.type()->IsString());
    if (!val.is_null()) {
      return val.string_value();
    }
  } else if (expr->Is<ResolvedConstant>()) {
    const Constant* constant = expr->GetAs<ResolvedConstant>()->constant();
    // If the constant does not have a value at analysis time, we cannot deduce
    // the vector encoding and return std::nullopt to signal to the analyzer.
    if (constant->HasValue()) {
      GOOGLESQL_ASSIGN_OR_RETURN(Value val, constant->GetValue());
      GOOGLESQL_RET_CHECK(val.type()->IsString());
      if (!val.is_null()) {
        return val.string_value();
      }
    }
  }
  return std::nullopt;
}

absl::StatusOr<std::optional<std::string>> GetVectorEncodingFromAnnotationMap(
    const AnnotationMap* annotation_map) {
  if (annotation_map == nullptr) {
    return std::nullopt;
  }
  const SimpleValue* in_encoding =
      annotation_map->GetAnnotation(VectorEncodingAnnotation::GetId());
  if (in_encoding == nullptr) {
    return std::nullopt;
  }
  GOOGLESQL_RET_CHECK(in_encoding->IsValid());
  GOOGLESQL_RET_CHECK(in_encoding->has_string_value());
  return in_encoding->string_value();
}

// Applies kUnconstrained ("UNCONSTRAINED") to any unannotated VECTOR slots in
// the annotation map.
absl::Status ApplyUnconstrainedVectorEncoding(const Type* type,
                                              AnnotationMap* annotation_map) {
  if (annotation_map == nullptr) {
    return absl::OkStatus();
  }
  if (IsVectorType(type)) {
    GOOGLESQL_RET_CHECK(type->ComponentTypes().empty());
    if (!annotation_map->Has<VectorEncodingAnnotation>()) {
      annotation_map->SetAnnotation<VectorEncodingAnnotation>(
          SimpleValue::String(
              std::string(VectorEncodingAnnotation::kUnconstrained)));
    }
    return absl::OkStatus();
  }

  TypeListView component_types = type->ComponentTypes();
  if (component_types.empty()) {
    return absl::OkStatus();
  }

  GOOGLESQL_RET_CHECK(annotation_map->IsStructMap());
  StructAnnotationMap* struct_annotation_map = annotation_map->AsStructMap();
  GOOGLESQL_RET_CHECK_EQ(struct_annotation_map->num_fields(), component_types.size());
  for (int i = 0; i < component_types.size(); ++i) {
    GOOGLESQL_RETURN_IF_ERROR(ApplyUnconstrainedVectorEncoding(
        component_types[i], struct_annotation_map->mutable_field(i)));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<std::optional<std::string>>
VectorEncodingAnnotation::GetEffectiveEncoding(
    const AnnotationMap* annotation_map) {
  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<std::string> encoding,
                   GetVectorEncodingFromAnnotationMap(annotation_map));
  if (!encoding.has_value() || *encoding == kUnconstrained) {
    return std::nullopt;
  }
  return encoding;
}

absl::Status VectorEncodingAnnotation::CheckAndPropagateForParameter(
    const ResolvedParameter& parameter, AnnotationMap* result_annotation_map) {
  GOOGLESQL_RET_CHECK(result_annotation_map != nullptr);

  GOOGLESQL_RETURN_IF_ERROR(DefaultAnnotationSpec::CheckAndPropagateForParameter(
      parameter, result_annotation_map));
  return ApplyUnconstrainedVectorEncoding(parameter.type(),
                                          result_annotation_map);
}

absl::Status VectorEncodingAnnotation::CheckAndPropagateForCast(
    const ResolvedCast& cast, AnnotationMap* result_annotation_map) {
  GOOGLESQL_RET_CHECK(result_annotation_map != nullptr);

  return PropagateFromTypeParameters(
      cast.type(), cast.type_modifiers().type_parameters(),
      cast.expr()->type_annotation_map(), *result_annotation_map,
      cast.return_null_on_error(), cast.GetParseLocationRangeOrNULL());
}

absl::Status VectorEncodingAnnotation::PropagateFromTypeParameters(
    const Type* target_type, const TypeParameters& target_type_params,
    const AnnotationMap* input_map, AnnotationMap& result_annotation_map,
    bool return_null_on_error, const ParseLocationRange* error_location) {
  TypeListView component_types = target_type->ComponentTypes();
  // If the target type is not a composite type, then we are propagating a
  // base type.
  if (component_types.empty()) {
    if (!IsVectorType(target_type)) {
      return absl::OkStatus();
    }
    GOOGLESQL_RET_CHECK_EQ(target_type_params.num_children(), 0);
    GOOGLESQL_RET_CHECK(target_type_params.IsEmpty() ||
              target_type_params.IsVectorTypeParameters());

    std::optional<std::string> target_encoding;
    if (target_type_params.IsVectorTypeParameters()) {
      const VectorTypeParametersProto* vector_params =
          target_type_params.vector_type_parameters();
      if (vector_params != nullptr && vector_params->has_encoding() &&
          vector_params->encoding() !=
              googlesql::VectorEncodingId::UNKNOWN_VECTOR_ENCODING) {
        target_encoding =
            googlesql::VectorEncodingId_Id_Name(vector_params->encoding());
      }
    }

    GOOGLESQL_ASSIGN_OR_RETURN(std::optional<std::string> input_encoding,
                     GetVectorEncodingFromAnnotationMap(input_map));

    // If the target type parameter already has a value, we use it since it
    // takes precedence. Otherwise, we simply pass on the input value.
    if (target_encoding.has_value()) {
      result_annotation_map.SetAnnotation<VectorEncodingAnnotation>(
          SimpleValue::String(*target_encoding));
      return absl::OkStatus();
    }
    if (input_encoding.has_value()) {
      result_annotation_map.SetAnnotation<VectorEncodingAnnotation>(
          SimpleValue::String(*input_encoding));
    }
    return absl::OkStatus();
  }

  // If the target type is a composite type, then we need to propagate the
  // annotations to all the children.
  GOOGLESQL_RET_CHECK(result_annotation_map.IsStructMap());
  GOOGLESQL_RET_CHECK_EQ(result_annotation_map.AsStructMap()->num_fields(),
               component_types.size());

  if (target_type_params.num_children() > 0) {
    GOOGLESQL_RET_CHECK_EQ(target_type_params.num_children(), component_types.size());
  }

  for (int i = 0; i < component_types.size(); ++i) {
    const AnnotationMap* input_child = nullptr;
    if (input_map != nullptr && input_map->IsStructMap() &&
        i < input_map->AsStructMap()->num_fields()) {
      input_child = input_map->AsStructMap()->field(i);
    }

    GOOGLESQL_RETURN_IF_ERROR(PropagateFromTypeParameters(
        component_types[i],
        target_type_params.num_children() > 0 ? target_type_params.child(i)
                                              : TypeParameters(),
        input_child, *result_annotation_map.AsStructMap()->mutable_field(i),
        return_null_on_error, error_location));
  }
  return absl::OkStatus();
}

absl::Status VectorEncodingAnnotation::CheckAndPropagateForFunctionCallBase(
    const ResolvedFunctionCallBase& function_call,
    AnnotationMap* result_annotation_map) {
  if (result_annotation_map == nullptr) {
    return absl::OkStatus();
  }

  if (function_call.function()->IsGoogleSQLBuiltin(FN_ENCODE_VECTOR)) {
    std::optional<std::string> target_encoding;

    // In GoogleSQL ResolvedFunctionCall, arguments are positionally guaranteed
    // to match the function signature, regardless of whether the user used
    // named arguments in the query. Index 0 is always the input array. Index 1
    // (if present) is the target dims. Index 2 (if present) is the target
    // encoding.
    constexpr int kEncodingArgIdx = 2;
    const FunctionSignature& signature = function_call.signature();
    GOOGLESQL_RET_CHECK(signature.IsConcrete());
    GOOGLESQL_RET_CHECK_EQ(signature.NumConcreteArguments(),
                 function_call.argument_list_size());
    GOOGLESQL_RET_CHECK_GT(function_call.argument_list_size(), 0);
    if (function_call.argument_list_size() > kEncodingArgIdx) {
      // Verify against the signature that the argument at `kEncodingArgIdx` is
      // the encoding argument, which is the only STRING argument of
      // `encode_vector`.
      GOOGLESQL_RET_CHECK(signature.ConcreteArgumentType(kEncodingArgIdx)->IsString())
          << "Unexpected type for argument " << kEncodingArgIdx
          << " of encode_vector: "
          << signature.ConcreteArgumentType(kEncodingArgIdx)->DebugString();
      GOOGLESQL_ASSIGN_OR_RETURN(target_encoding,
                       GetConstantOrLiteralString(
                           function_call.argument_list(kEncodingArgIdx)));
      if (target_encoding.has_value()) {
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::VectorEncodingId::Id encoding_enum,
                         ParseAndValidateVectorEncoding(*target_encoding));
        result_annotation_map->SetAnnotation<VectorEncodingAnnotation>(
            SimpleValue::String(std::string(
                googlesql::VectorEncodingId_Id_Name(encoding_enum))));
      } else {
        result_annotation_map->SetAnnotation<VectorEncodingAnnotation>(
            SimpleValue::String(std::string(kUnconstrained)));
      }
      return absl::OkStatus();
    }

    // Default encoding is FLOAT32 if not specified.
    result_annotation_map->SetAnnotation<VectorEncodingAnnotation>(
        SimpleValue::String(std::string(kDefaultVectorEncoding)));
    return absl::OkStatus();
  }

  return DefaultAnnotationSpec::CheckAndPropagateForFunctionCallBase(
      function_call, result_annotation_map);
}

absl::Status VectorEncodingAnnotation::ScalarMergeIfCompatible(
    const AnnotationMap* in, AnnotationMap& out) const {
  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<std::string> in_encoding,
                   GetVectorEncodingFromAnnotationMap(in));
  // If `in` is unannotated (nullptr, e.g. untyped null / empty container),
  // it acts as identity and does not modify `out`.
  if (!in_encoding.has_value()) {
    return absl::OkStatus();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<std::string> out_encoding,
                   GetVectorEncodingFromAnnotationMap(&out));
  // If `out` was unannotated, adopt `in`.
  if (!out_encoding.has_value()) {
    out.SetAnnotation<VectorEncodingAnnotation>(
        SimpleValue::String(*in_encoding));
    return absl::OkStatus();
  }

  // Both have values. If encodings differ (e.g. FLOAT32 vs INT8), or if either
  // is kUnconstrained ("UNCONSTRAINED" vs "FLOAT32"), transition to
  // kUnconstrained.
  // Note: if both are already kUnconstrained, out remains kUnconstrained.
  if (*in_encoding != *out_encoding) {
    out.SetAnnotation<VectorEncodingAnnotation>(
        SimpleValue::String(std::string(kUnconstrained)));
  }

  return absl::OkStatus();
}

}  // namespace googlesql
