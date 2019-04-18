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

#include "zetasql/public/function.h"

#include <ctype.h>
#include <algorithm>
#include <map>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/public/language_options.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/strip.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

const FunctionEnums::WindowOrderSupport FunctionOptions::ORDER_UNSUPPORTED;
const FunctionEnums::WindowOrderSupport FunctionOptions::ORDER_OPTIONAL;
const FunctionEnums::WindowOrderSupport FunctionOptions::ORDER_REQUIRED;

zetasql_base::Status FunctionOptions::Deserialize(
    const FunctionOptionsProto& proto,
    std::unique_ptr<FunctionOptions>* result) {
  std::unique_ptr<FunctionOptions> options;
  if (proto.supports_over_clause()) {
    options = absl::make_unique<FunctionOptions>(
        proto.window_ordering_support(), proto.supports_window_framing());
  } else {
    ZETASQL_RET_CHECK(!proto.supports_window_framing());
    ZETASQL_RET_CHECK_EQ(proto.window_ordering_support(), ORDER_UNSUPPORTED);
    options = absl::make_unique<FunctionOptions>();
  }

  options->set_arguments_are_coercible(proto.arguments_are_coercible());
  options->set_is_deprecated(proto.is_deprecated());
  options->set_alias_name(proto.alias_name());
  options->set_sql_name(proto.sql_name());
  options->set_allow_external_usage(proto.allow_external_usage());
  options->set_volatility(proto.volatility());
  options->set_supports_order_by(proto.supports_order_by());
  options->set_supports_safe_error_mode(proto.supports_safe_error_mode());
  for (const int each : proto.required_language_feature()) {
    options->add_required_language_feature(LanguageFeature(each));
  }
  options->set_supports_limit(proto.supports_limit());
  options->set_supports_null_handling_modifier(
      proto.supports_null_handling_modifier());
  options->set_supports_having_modifier(proto.supports_having_modifier());
  options->set_uses_upper_case_sql_name(proto.uses_upper_case_sql_name());

  *result = std::move(options);
  return ::zetasql_base::OkStatus();
}

void FunctionOptions::Serialize(FunctionOptionsProto* proto) const {
  proto->Clear();
  proto->set_supports_over_clause(supports_over_clause);
  proto->set_window_ordering_support(window_ordering_support);
  proto->set_supports_window_framing(supports_window_framing);
  proto->set_arguments_are_coercible(arguments_are_coercible);
  proto->set_is_deprecated(is_deprecated);
  proto->set_alias_name(alias_name);
  proto->set_sql_name(sql_name);
  proto->set_allow_external_usage(allow_external_usage);
  proto->set_volatility(volatility);
  proto->set_supports_order_by(supports_order_by);
  proto->set_supports_safe_error_mode(supports_safe_error_mode);
  proto->set_supports_having_modifier(supports_having_modifier);
  proto->set_uses_upper_case_sql_name(uses_upper_case_sql_name);

  for (const LanguageFeature each : required_language_features) {
    proto->add_required_language_feature(each);
  }
  proto->set_supports_limit(supports_limit);
  proto->set_supports_null_handling_modifier(supports_null_handling_modifier);
}

FunctionOptions& FunctionOptions::set_evaluator(
    const FunctionEvaluator& function_evaluator) {
  set_evaluator_factory(
      [function_evaluator](const FunctionSignature&) {
        return function_evaluator;
      });
  return *this;
}

bool FunctionOptions::check_all_required_features_are_enabled(
    const std::set<LanguageFeature>& enabled_features) const {
  return std::includes(enabled_features.begin(), enabled_features.end(),
                       required_language_features.begin(),
                       required_language_features.end());
}

const char Function::kZetaSQLFunctionGroupName[] = "ZetaSQL";

const FunctionEnums::Mode Function::SCALAR;
const FunctionEnums::Mode Function::AGGREGATE;
const FunctionEnums::Mode Function::ANALYTIC;

Function::Function(const std::string& name, const std::string& group, Mode mode,
                   const FunctionOptions& function_options)
    : group_(group), mode_(mode), function_options_(function_options) {
  function_name_path_.push_back(name);
  ZETASQL_CHECK_OK(CheckWindowSupportOptions());
}

Function::Function(const std::string& name, const std::string& group, Mode mode,
                   const std::vector<FunctionSignature>& function_signatures,
                   const FunctionOptions& function_options)
    : group_(group), mode_(mode), function_options_(function_options) {
  function_name_path_.push_back(name);
  function_signatures_ = function_signatures;
  ZETASQL_CHECK_OK(CheckWindowSupportOptions());
  for (const FunctionSignature& signature : function_signatures) {
    ZETASQL_CHECK_OK(signature.IsValidForFunction())
        << signature.DebugString(FullName());
  }
}

Function::Function(
    const std::vector<std::string>& name_path, const std::string& group, Mode mode,
    const std::vector<FunctionSignature>& function_signatures,
    const FunctionOptions& function_options)
    : function_name_path_(name_path), group_(group), mode_(mode),
      function_options_(function_options) {
  function_signatures_ = function_signatures;
  ZETASQL_CHECK_OK(CheckWindowSupportOptions());
  for (const FunctionSignature& signature : function_signatures) {
    ZETASQL_CHECK_OK(signature.IsValidForFunction())
        << signature.DebugString(FullName());
  }
}

// A FunctionDeserializer for functions by group name. Case-sensitive. Thread
// safe after module initializers.
static std::map<std::string, Function::FunctionDeserializer>*
FunctionDeserializers() {
  static auto* function_deserializers =
      new std::map<std::string, Function::FunctionDeserializer>;
  return function_deserializers;
}

zetasql_base::Status Function::Deserialize(
    const FunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory,
    std::unique_ptr<Function>* result) {
  // First check if there is a custom deserializer for this function group.
  // If so, invoke it early and return instead.
  if (proto.has_group()) {
    FunctionDeserializer* custom_deserializer =
        zetasql_base::FindOrNull(*FunctionDeserializers(), proto.group());
    if (custom_deserializer != nullptr) {
      return (*custom_deserializer)(proto, pools, factory, result);
    }
  }

  std::vector<std::string> name_path;
  for (const std::string& name : proto.name_path()) {
    name_path.push_back(name);
  }

  std::vector<FunctionSignature> function_signatures;
  for (const auto& signature_proto : proto.signature()) {
    std::unique_ptr<FunctionSignature> signature;
    ZETASQL_RETURN_IF_ERROR(FunctionSignature::Deserialize(
        signature_proto, pools, factory, &signature));
    function_signatures.push_back(*signature);
  }

  std::unique_ptr<FunctionOptions> options;
  ZETASQL_RETURN_IF_ERROR(FunctionOptions::Deserialize(proto.options(), &options));

  *result = absl::make_unique<Function>(name_path, proto.group(), proto.mode(),
                                        function_signatures, *options);

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Function::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    FunctionProto* proto, bool omit_signatures) const {
  for (const std::string& name : this->FunctionNamePath()) {
    proto->add_name_path(name);
  }

  if (!omit_signatures) {
    for (const zetasql::FunctionSignature& signature : this->signatures()) {
      ZETASQL_RETURN_IF_ERROR(signature.Serialize(
          file_descriptor_set_map, proto->add_signature()));
    }
  }

  proto->set_mode(mode());
  proto->set_group(GetGroup());
  function_options().Serialize(proto->mutable_options());

  return ::zetasql_base::OkStatus();
}

// static
void Function::RegisterDeserializer(const std::string& group_name,
                                    FunctionDeserializer deserializer) {
  // CHECK validated -- This is used at initialization time only.
  CHECK(zetasql_base::InsertIfNotPresent(FunctionDeserializers(), group_name,
                                deserializer));
}

const std::string Function::FullName(bool include_group) const {
  return absl::StrCat((include_group ? absl::StrCat(group_, ":") : ""),
                      absl::StrJoin(function_name_path_, "."));
}

const std::string Function::SQLName() const {
  std::string name;
  if (!function_options_.sql_name.empty()) {
    name = function_options_.sql_name;
  } else if (absl::StartsWith(Name(), "$")) {
    // The name starts with '$' so it is an internal function name.  Strip off
    // the leading '$', convert all '_' to ' ', and upper case it.
    name = absl::StrReplaceAll(Name().substr(1), {{"_", " "}});
  } else if (IsZetaSQLBuiltin()) {
    name = FullName(/*include_group=*/ false);
  } else {
    name = FullName();
  }
  if (function_options_.uses_upper_case_sql_name) {
    absl::AsciiStrToUpper(&name);
  }
  return name;
}

const std::string Function::QualifiedSQLName(bool capitalize_qualifier) const {
  std::string qualifier;
  switch (mode_) {
    case Function::AGGREGATE:
      qualifier = "aggregate ";
      break;
    case Function::ANALYTIC:
      qualifier = "analytic ";
      break;
    default:
      break;
  }
  if (is_operator()) {
    absl::StrAppend(&qualifier, "operator ");
  } else {
    absl::StrAppend(&qualifier, "function ");
  }
  if (!qualifier.empty() && capitalize_qualifier) {
    qualifier[0] = toupper(qualifier[0]);
  }
  return absl::StrCat(qualifier, SQLName());
}

int Function::NumSignatures() const {
  return signatures().size();
}

const std::vector<FunctionSignature>& Function::signatures() const {
  return function_signatures_;
}

void Function::ResetSignatures(
    const std::vector<FunctionSignature>& signatures) {
  function_signatures_ = signatures;
  for (const FunctionSignature& signature : signatures) {
    ZETASQL_CHECK_OK(signature.IsValidForFunction())
        << signature.DebugString(FullName());
  }
}

void Function::AddSignature(const FunctionSignature& signature) {
  function_signatures_.push_back(signature);
  ZETASQL_CHECK_OK(signature.IsValidForFunction()) << signature.DebugString(FullName());
}

zetasql_base::Status Function::AddSignature(const TypeKind result_kind,
                                    const std::vector<TypeKind>& input_kinds,
                                    void* context,
                                    TypeFactory* factory) {
  if (!Type::IsSimpleType(result_kind)) {
    return MakeSqlError()
           << "Result TypeKinds should be simple type kinds, but found: "
           << Type::TypeKindToString(result_kind, PRODUCT_INTERNAL);
  }

  FunctionArgumentTypeList arguments;
  for (const TypeKind input_kind : input_kinds) {
    if (!Type::IsSimpleType(input_kind)) {
      return MakeSqlError()
             << "Input TypeKinds should be simple type kinds, but found: "
             << Type::TypeKindToString(input_kind, PRODUCT_INTERNAL);
    }
    arguments.push_back(
        FunctionArgumentType(factory->MakeSimpleType(input_kind)));
  }

  AddSignature(FunctionSignature(
      FunctionArgumentType(factory->MakeSimpleType(result_kind)),
      arguments, context));
  return ::zetasql_base::OkStatus();
}

Function* Function::AddSignatureOrDie(
    const TypeKind result_kind, const std::vector<TypeKind>& input_kinds,
    void* context, TypeFactory* factory) {
  ZETASQL_CHECK_OK(AddSignature(result_kind, input_kinds, context, factory));
  return this;
}

const FunctionSignature* Function::GetSignature(int idx) const {
  if (idx < 0 || idx >= NumSignatures()) {
    return nullptr;
  }
  return &(function_signatures_[idx]);
}

std::string Function::DebugString(bool verbose) const {
  if (verbose) {
    return absl::StrCat(
        FullName(),
        (alias_name().empty() ? "" : absl::StrCat("|", alias_name())),
        (function_signatures_.empty() ? "" : "\n"),
        FunctionSignature::SignaturesToString(function_signatures_));
  }
  return FullName();
}

std::string Function::GetSQL(const std::vector<std::string>& inputs) const {
  if (GetSQLCallback() != nullptr) {
    return GetSQLCallback()(inputs);
  }
  std::string name = FullName(/*include_group=*/ false);
  if (function_options_.uses_upper_case_sql_name) {
    absl::AsciiStrToUpper(&name);
  }
  return absl::StrCat(name, "(", absl::StrJoin(inputs, ", "), ")");
}

zetasql_base::Status Function::CheckArgumentConstraints(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options,
    const ArgumentConstraintsCallback& constraints_callback) {
  if (constraints_callback == nullptr) {
    return ::zetasql_base::OkStatus();
  }
  return constraints_callback(arguments, language_options);
}

// static
const std::string Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments,
    ProductMode product_mode) {
  return absl::StrCat(
      "No matching signature for ", qualified_function_name,
      (arguments.empty() ? " with no arguments"
                         : absl::StrCat(" for argument types: ",
                                        InputArgumentType::ArgumentsToString(
                                            arguments, product_mode))));
}

const std::string Function::GetNoMatchingFunctionSignatureErrorMessage(
    const std::vector<InputArgumentType>& arguments,
    ProductMode product_mode) const {
  if (GetNoMatchingSignatureCallback() != nullptr) {
    return GetNoMatchingSignatureCallback()(QualifiedSQLName(), arguments,
                                            product_mode);
  }
  return GetGenericNoMatchingFunctionSignatureErrorMessage(QualifiedSQLName(),
                                                           arguments,
                                                           product_mode);
}

// TODO: When we use this to make error messages for signatures that
// take templated args like ANY, the error messages aren't very good.  Fix
// this.
const std::string Function::GetSupportedSignaturesUserFacingText(
    const LanguageOptions& language_options) const {
  if (GetSupportedSignaturesCallback() != nullptr) {
    return GetSupportedSignaturesCallback()(language_options, *this);
  }
  std::string supported_signatures;
  for (const FunctionSignature& signature : signatures()) {
    // Ignore deprecated signatures, and signatures that include
    // unsupported data types.
    if (signature.IsDeprecated() ||
        signature.HasUnsupportedType(language_options)) {
      continue;
    }
    if (!supported_signatures.empty()) {
      absl::StrAppend(&supported_signatures, "; ");
    }
    std::vector<std::string> argument_texts;
    for (const FunctionArgumentType& argument : signature.arguments()) {
      argument_texts.push_back(argument.UserFacingNameWithCardinality(
          language_options.product_mode()));
    }
    absl::StrAppend(&supported_signatures, GetSQL(argument_texts));
  }
  return supported_signatures;
}

const ArgumentConstraintsCallback& Function::PreResolutionConstraints() const {
  return function_options_.pre_resolution_constraint;
}

const ArgumentConstraintsCallback& Function::PostResolutionConstraints() const {
  return function_options_.post_resolution_constraint;
}

const ComputeResultTypeCallback& Function::GetComputeResultTypeCallback()
    const {
  return function_options_.compute_result_type_callback;
}

const FunctionGetSQLCallback& Function::GetSQLCallback() const {
  return function_options_.get_sql_callback;
}

const NoMatchingSignatureCallback& Function::GetNoMatchingSignatureCallback()
    const {
  return function_options_.no_matching_signature_callback;
}

const SupportedSignaturesCallback& Function::GetSupportedSignaturesCallback()
    const {
  return function_options_.supported_signatures_callback;
}

const BadArgumentErrorPrefixCallback&
Function::GetBadArgumentErrorPrefixCallback() const {
  return function_options_.bad_argument_error_prefix_callback;
}

FunctionEvaluatorFactory Function::GetFunctionEvaluatorFactory() const {
  return function_options_.function_evaluator_factory;
}

zetasql_base::Status Function::CheckWindowSupportOptions() const {
  if (IsScalar() && SupportsOverClause()) {
    return MakeSqlError() << "Scalar functions cannot support OVER clause";
  }
  if (IsAnalytic() && !SupportsOverClause()) {
    return MakeSqlError() << "Analytic functions must support OVER clause";
  }
  return ::zetasql_base::OkStatus();
}

bool Function::SupportsOverClause() const {
  return function_options_.supports_over_clause;
}

bool Function::SupportsWindowOrdering() const {
  return function_options_.window_ordering_support ==
             FunctionOptions::ORDER_REQUIRED ||
         function_options_.window_ordering_support ==
             FunctionOptions::ORDER_OPTIONAL;
}

bool Function::RequiresWindowOrdering() const {
  return function_options_.window_ordering_support ==
             FunctionOptions::ORDER_REQUIRED;
}

bool Function::SupportsWindowFraming() const {
  return function_options_.supports_window_framing;
}

bool Function::SupportsOrderingArguments() const {
  return function_options_.supports_order_by;
}

bool Function::SupportsLimitArguments() const {
  return function_options_.supports_limit;
}

bool Function::SupportsNullHandlingModifier() const {
  return function_options_.supports_null_handling_modifier;
}

bool Function::SupportsSafeErrorMode() const {
  return function_options_.supports_safe_error_mode;
}

bool Function::SupportsHavingModifier() const {
  return function_options_.supports_having_modifier;
}

}  // namespace zetasql
