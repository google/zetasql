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

#ifndef ZETASQL_COMMON_BUILTIN_FUNCTION_INTERNAL_H_
#define ZETASQL_COMMON_BUILTIN_FUNCTION_INTERNAL_H_

#include <stddef.h>

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "zetasql/proto/options.pb.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Wrapper around FunctionSignature that allocates on the heap to reduce the
// impact on stack size while still using the same initializer list.
class FunctionSignatureOnHeap {
 public:
  FunctionSignatureOnHeap(FunctionArgumentType result_type,
                          FunctionArgumentTypeList arguments, int64_t context_id)
      : signature_(new FunctionSignature(std::move(result_type),
                                         std::move(arguments), context_id)) {}

  FunctionSignatureOnHeap(FunctionArgumentType result_type,
                          FunctionArgumentTypeList arguments, int64_t context_id,
                          FunctionSignatureOptions options)
      : signature_(new FunctionSignature(std::move(result_type),
                                         std::move(arguments), context_id,
                                         std::move(options))) {}

  const FunctionSignature& Get() const { return *signature_; }

 private:
  std::shared_ptr<FunctionSignature> signature_;
};

// A proxy object representing a simple FunctionArgumentType object, one that
// can be constructed very easily and cheaply, but lacking the full
// expressiveness of an actual FunctionArgumentType.
// If you need to specify a FunctionArgumentTypeOptions, you can't use the
// 'Simple' methods.
struct FunctionArgumentTypeProxy {
  FunctionArgumentTypeProxy(
      SignatureArgumentKind kind,
      FunctionArgumentType::ArgumentCardinality cardinality)
      : kind(kind), cardinality(cardinality) {}
  FunctionArgumentTypeProxy(
      SignatureArgumentKind kind, const Type* type,
      FunctionArgumentType::ArgumentCardinality cardinality)
      : kind(kind), cardinality(cardinality) {}
  // NOLINTNEXTLINE: runtime/explicit
  FunctionArgumentTypeProxy(SignatureArgumentKind kind) : kind(kind) {}

  FunctionArgumentTypeProxy(
      const Type* type, FunctionArgumentType::ArgumentCardinality cardinality)
      : type(type), cardinality(cardinality) {}

  FunctionArgumentTypeProxy(const Type* type)  // NOLINT: runtime/explicit
      : type(type) {}

  const Type* type = nullptr;
  SignatureArgumentKind kind = ARG_TYPE_FIXED;
  FunctionArgumentType::ArgumentCardinality cardinality =
      FunctionEnums::REQUIRED;

  operator FunctionArgumentType() const {  // NOLINT: runtime/explicit
    if (type == nullptr) {
      return FunctionArgumentType(kind, cardinality);
    } else {
      return FunctionArgumentType(type, cardinality);
    }
  }
};

// A proxy object representing a simple FunctionSignature object, one that
// can be constructed very easily and cheaply, but lacking the full
// expressiveness of an actual FunctionSignature.
// If you need to specify a FunctionSignatureOptions, you can't use the 'Simple'
// methods.
struct FunctionSignatureProxy {
  FunctionArgumentTypeProxy result_type;
  std::initializer_list<FunctionArgumentTypeProxy> arguments;
  FunctionSignatureId context_id;

  // Implicit conversion to a FunctionSignature object.
  operator FunctionSignature() const {  // NOLINT: runtime/explicit
    std::vector<FunctionArgumentType> argument_vec(arguments.begin(),
                                                   arguments.end());
    return FunctionSignature(result_type, std::move(argument_vec), context_id);
  }

  // Implicit conversion to a FunctionSignatureOnHeap. This is occasionally
  // useful when you want to _mostly_ use FunctionSignatureProxy, but must
  // fallback to FunctionSignatureOnHeap for a few signatures.
  operator FunctionSignatureOnHeap() const {  // NOLINT: runtime/explicit
    std::vector<FunctionArgumentType> argument_vec(arguments.begin(),
                                                   arguments.end());
    return FunctionSignatureOnHeap(result_type, std::move(argument_vec),
                                   context_id);
  }
};

using FunctionIdToNameMap =
    absl::flat_hash_map<FunctionSignatureId, std::string>;
using NameToFunctionMap = std::map<std::string, std::unique_ptr<Function>>;

bool ArgumentsAreComparable(const std::vector<InputArgumentType>& arguments,
                            const LanguageOptions& language_options,
                            int* bad_argument_idx);

// Checks whether all arguments are/are not arrays depending on the value of the
// 'is_array' flag.
bool ArgumentsArrayType(const std::vector<InputArgumentType>& arguments,
                        bool is_array, int* bad_argument_idx);

// The argument <display_name> in the below ...FunctionSQL represents the
// operator and should include space if inputs and operator needs to be space
// separated for pretty printing.
// TODO: Consider removing this callback, since Function now knows
// whether it is operator, and has correct sql_name to print.
std::string InfixFunctionSQL(const std::string& display_name,
                             const std::vector<std::string>& inputs);

std::string PreUnaryFunctionSQL(const std::string& display_name,
                                const std::vector<std::string>& inputs);

std::string PostUnaryFunctionSQL(const std::string& display_name,
                                 const std::vector<std::string>& inputs);

std::string DateAddOrSubFunctionSQL(const std::string& display_name,
                                    const std::vector<std::string>& inputs);

std::string CountStarFunctionSQL(const std::vector<std::string>& inputs);

std::string AnonCountStarFunctionSQL(const std::vector<std::string>& inputs);

std::string SupportedSignaturesForAnonCountStarFunction(
    const std::string& unused_function_name,
    const LanguageOptions& language_options, const Function& function);

std::string BetweenFunctionSQL(const std::vector<std::string>& inputs);

std::string InListFunctionSQL(const std::vector<std::string>& inputs);

std::string LikeAnyFunctionSQL(const std::vector<std::string>& inputs);

std::string LikeAllFunctionSQL(const std::vector<std::string>& inputs);

std::string CaseWithValueFunctionSQL(const std::vector<std::string>& inputs);

std::string CaseNoValueFunctionSQL(const std::vector<std::string>& inputs);

std::string InArrayFunctionSQL(const std::vector<std::string>& inputs);

std::string LikeAnyArrayFunctionSQL(const std::vector<std::string>& inputs);

std::string LikeAllArrayFunctionSQL(const std::vector<std::string>& inputs);

std::string ArrayAtOffsetFunctionSQL(const std::vector<std::string>& inputs);

std::string ArrayAtOrdinalFunctionSQL(const std::vector<std::string>& inputs);

std::string SafeArrayAtOffsetFunctionSQL(
    const std::vector<std::string>& inputs);

std::string SubscriptFunctionSQL(const std::vector<std::string>& inputs);
std::string SubscriptWithKeyFunctionSQL(const std::vector<std::string>& inputs);
std::string SubscriptWithOffsetFunctionSQL(
    const std::vector<std::string>& inputs);
std::string SubscriptWithOrdinalFunctionSQL(
    const std::vector<std::string>& inputs);

std::string SafeArrayAtOrdinalFunctionSQL(
    const std::vector<std::string>& inputs);

std::string ProtoMapAtKeySQL(const std::vector<std::string>& inputs);

std::string SafeProtoMapAtKeySQL(const std::vector<std::string>& inputs);

std::string GenerateDateTimestampArrayFunctionSQL(
    const std::string& function_name, const std::vector<std::string>& inputs);

// For MakeArray we explicitly prepend the array type to the function sql, thus
// array-type-name is expected to be passed as the first element of inputs.
std::string MakeArrayFunctionSQL(const std::vector<std::string>& inputs);

std::string ExtractFunctionSQL(const std::vector<std::string>& inputs);

std::string ExtractDateOrTimeFunctionSQL(
    const std::string& date_part, const std::vector<std::string>& inputs);

bool ArgumentIsStringLiteral(const InputArgumentType& argument);

absl::Status CheckBitwiseOperatorArgumentsHaveSameType(
    const std::string& operator_string,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckBitwiseOperatorFirstArgumentIsIntegerOrBytes(
    const std::string& operator_string,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckDateDatetimeTimeTimestampTruncArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckLastDayArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckExtractPreResolutionArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckExtractPostResolutionArguments(
    const zetasql::FunctionSignature& signature,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckDateDatetimeTimestampAddSubArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckDateDatetimeTimeTimestampDiffArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckDatetimeAddSubDiffArguments(
    const std::string& function_name,
    const FunctionSignature& /*signature*/,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

// This function returns a ZetaSQL ProtoType for
// google::protobuf::EnumValueDescriptorProto, which is either from the <catalog> (if this
// ProtoType is present) or newly created in the <type_factory>.
absl::StatusOr<const Type*> GetOrMakeEnumValueDescriptorType(
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle,
    const FunctionSignature& /*signature*/,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options);

absl::Status CheckTimeAddSubArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckGenerateDateArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckGenerateTimestampArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckJsonArguments(const std::vector<InputArgumentType>& arguments,
                                const LanguageOptions& options);

absl::Status CheckFormatPostResolutionArguments(
    const FunctionSignature& /*signature*/,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckIsSupportedKeyType(
    absl::string_view function_name,
    const std::set<std::string>& supported_key_types,
    int key_type_argument_index,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

const std::set<std::string>& GetSupportedKeyTypes();

const std::set<std::string>& GetSupportedRawKeyTypes();

bool IsStringLiteralComparedToBytes(const InputArgumentType& lhs_arg,
                                    const InputArgumentType& rhs_arg);

std::string NoMatchingSignatureForCaseNoValueFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string NoMatchingSignatureForInFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string NoMatchingSignatureForInArrayFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string NoMatchingSignatureForLikeExprFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string NoMatchingSignatureForLikeExprArrayFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string NoMatchingSignatureForComparisonOperator(
    const std::string& operator_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string NoMatchingSignatureForFunctionUsingInterval(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode,
    int index_of_interval_argument);

std::string NoMatchingSignatureForDateOrTimeAddOrSubFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string NoMatchingSignatureForGenerateDateOrTimestampArrayFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

// Supports 'ArgumentType' of either InputArgumentType or FunctionArgumentType.
//
// Example return values:
//   DATE_TIME_PART FROM TIMESTAMP
//   DATE FROM TIMESTAMP
//   TIME FROM TIMESTAMP
//   DATETIME FROM TIMESTAMP
//   DATE_TIME_PART FROM TIMESTAMP AT TIME ZONE STRING
//   DATETIME FROM TIMESTAMP [AT TIME ZONE STRING]
//
// 'include_bracket' indicates whether or not the 'AT TIME ZONE' argument
// is enclosed in brackets to indicate that the clause is optional.
// The input 'arguments' must be a valid signature for EXTRACT.
//
// If 'explicit_datepart_name' is non-empty, then the signature must not
// have a date part argument.  Otherwise, the signature must have a date
// part argument.
//
// For $extract, the date part argument is present in 'arguments', and
// 'explicit_datepart_name' is empty.
//
// For $extract_date, $extract_time, and $extract_datetime, the date part
// argument is *not* present in 'arguments', and 'explicit_datepart_name'
// is non-empty.
template <class ArgumentType>
std::string GetExtractFunctionSignatureString(
    const std::string& explicit_datepart_name,
    const std::vector<ArgumentType>& arguments, ProductMode product_mode,
    bool include_bracket);

std::string NoMatchingSignatureForExtractFunction(
    const std::string& explicit_datepart_name,
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string ExtractSupportedSignatures(
    const std::string& explicit_datepart_name,
    const LanguageOptions& language_options, const Function& function);

std::string NoMatchingSignatureForSubscript(
    absl::string_view offset_or_ordinal, absl::string_view operator_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode);

std::string EmptySupportedSignatures(const LanguageOptions& language_options,
                                     const Function& function);

absl::Status CheckArgumentsSupportEquality(
    const std::string& comparison_name,
    const FunctionSignature& /*signature*/,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckArgumentsSupportGrouping(
    const std::string& comparison_name, const FunctionSignature& signature,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckArgumentsSupportComparison(
    const std::string& comparison_name,
    const FunctionSignature& /*signature*/,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckMinMaxArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckGreatestLeastArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckFirstArgumentSupportsEquality(
    const std::string& comparison_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckArrayAggArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckArrayConcatArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckArrayIsDistinctArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckInArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckLikeExprArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckRangeBucketArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

std::string AnonCountStarBadArgumentErrorPrefix(const FunctionSignature&,
                                                int idx);

// Construct FunctionOptions for aggregate functions with default settings for
// an OVER clause.
FunctionOptions DefaultAggregateFunctionOptions();

// Returns true if an arithmetic operation has a floating point type as its
// input.
bool HasFloatingPointArgument(const FunctionSignature& matched_signature,
                              const std::vector<InputArgumentType>& arguments);

// Returns true if at least one input argument has NUMERIC type.
bool HasNumericTypeArgument(const FunctionSignature& matched_signature,
                            const std::vector<InputArgumentType>& arguments);

// Returns true if all input arguments have NUMERIC or BIGNUMERIC type,
// including the case without input arguments.
bool AllArgumentsHaveNumericOrBigNumericType(
    const FunctionSignature& matched_signature,
    const std::vector<InputArgumentType>& arguments);

// Returns true if there is at least one input argument and the last argument
// has NUMERIC type or BIGNUMERIC type.
bool LastArgumentHasNumericOrBigNumericType(
    const FunctionSignature& matched_signature,
    const std::vector<InputArgumentType>& arguments);

// Returns true if at least one input argument has BIGNUMERIC type.
bool HasBigNumericTypeArgument(
    const FunctionSignature& matched_signature,
    const std::vector<InputArgumentType>& arguments);

// Returns true if at least one input argument has INTERVAL type.
bool HasIntervalTypeArgument(
    const FunctionSignature& matched_signature,
    const std::vector<InputArgumentType>& arguments);

// Returns true if FN_CONCAT_STRING function can coerce argument of given type
// to STRING.
bool CanStringConcatCoerceFrom(const zetasql::Type* arg_type);

// Compute the result type for TOP_COUNT and TOP_SUM.
// The output type is
//   ARRAY<
//     STRUCT<`value` <arguments[0].type()>,
//            `<field2_name>` <arguments[1].type()> > >
absl::StatusOr<const Type*> ComputeResultTypeForTopStruct(
    const std::string& field2_name, Catalog* catalog, TypeFactory* type_factory,
    CycleDetector* cycle_detector,
    const FunctionSignature& /*signature*/,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options);

// Compute the result type for ST_NEAREST_NEIGHBORS.
// The output type is
//   ARRAY<
//     STRUCT<`neighbor` <arguments[0].type>,
//            `distance` Double> >
absl::StatusOr<const Type*> ComputeResultTypeForNearestNeighborsStruct(
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle_detector,
    const FunctionSignature& /*signature*/,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options);

void InsertCreatedFunction(NameToFunctionMap* functions,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           Function* function_in);

void InsertFunction(NameToFunctionMap* functions,
                    const ZetaSQLBuiltinFunctionOptions& options,
                    absl::string_view name, Function::Mode mode,
                    const std::vector<FunctionSignatureOnHeap>& signatures,
                    FunctionOptions function_options);

// Note: This function is intentionally overloaded to prevent a default
// FunctionOptions object to be allocated on the callers stack.
void InsertFunction(NameToFunctionMap* functions,
                    const ZetaSQLBuiltinFunctionOptions& options,
                    absl::string_view name, Function::Mode mode,
                    const std::vector<FunctionSignatureOnHeap>& signatures);

void InsertSimpleFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, absl::string_view name,
    Function::Mode mode,
    std::initializer_list<FunctionSignatureProxy> signatures,
    const FunctionOptions& function_options);

// Note: This function is intentionally overloaded to prevent a default
// FunctionOptions object to be allocated on the callers stack.
void InsertSimpleFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, absl::string_view name,
    Function::Mode mode,
    std::initializer_list<FunctionSignatureProxy> signatures);

void InsertNamespaceFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, absl::string_view space,
    absl::string_view name, Function::Mode mode,
    const std::vector<FunctionSignatureOnHeap>& signatures);

// Note: This function is intentionally overloaded to prevent a default
// FunctionOptions object to be allocated on the callers stack.
void InsertNamespaceFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, absl::string_view space,
    absl::string_view name, Function::Mode mode,
    const std::vector<FunctionSignatureOnHeap>& signatures,
    FunctionOptions function_options);

void InsertSimpleNamespaceFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, absl::string_view space,
    absl::string_view name, Function::Mode mode,
    std::initializer_list<FunctionSignatureProxy> signatures);

// Note: This function is intentionally overloaded to prevent a default
// FunctionOptions object to be allocated on the callers stack.
void InsertSimpleNamespaceFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, absl::string_view space,
    absl::string_view name, Function::Mode mode,
    std::initializer_list<FunctionSignatureProxy> signatures,
    FunctionOptions function_options);

void GetDatetimeExtractFunctions(TypeFactory* type_factory,
                                 const ZetaSQLBuiltinFunctionOptions& options,
                                 NameToFunctionMap* functions);

void GetDatetimeConversionFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions);

void GetTimeAndDatetimeConstructionAndConversionFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions);

void GetDatetimeCurrentFunctions(TypeFactory* type_factory,
                                 const ZetaSQLBuiltinFunctionOptions& options,
                                 NameToFunctionMap* functions);

void GetDatetimeAddSubFunctions(TypeFactory* type_factory,
                                const ZetaSQLBuiltinFunctionOptions& options,
                                NameToFunctionMap* functions);

void GetDatetimeDiffTruncLastFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions);

void GetDatetimeFormatFunctions(TypeFactory* type_factory,
                                const ZetaSQLBuiltinFunctionOptions& options,
                                NameToFunctionMap* functions);

void GetDatetimeFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions);

void GetIntervalFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions);

void GetArithmeticFunctions(TypeFactory* type_factory,
                            const ZetaSQLBuiltinFunctionOptions& options,
                            NameToFunctionMap* functions);

void GetBitwiseFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions);

void GetAggregateFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions);

void GetApproxFunctions(TypeFactory* type_factory,
                        const ZetaSQLBuiltinFunctionOptions& options,
                        NameToFunctionMap* functions);

void GetStatisticalFunctions(TypeFactory* type_factory,
                             const ZetaSQLBuiltinFunctionOptions& options,
                             NameToFunctionMap* functions);

void GetAnalyticFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions);

void GetBooleanFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions);
void GetLogicFunctions(TypeFactory* type_factory,
                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions);

void GetStringFunctions(TypeFactory* type_factory,
                        const ZetaSQLBuiltinFunctionOptions& options,
                        NameToFunctionMap* functions);

void GetRegexFunctions(TypeFactory* type_factory,
                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions);

void GetProto3ConversionFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions);

void GetMiscellaneousFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions);

void GetSubscriptFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions);

void GetJSONFunctions(TypeFactory* type_factory,
                      const ZetaSQLBuiltinFunctionOptions& options,
                      NameToFunctionMap* functions);

void GetNumericFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions);

void GetTrigonometricFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions);

void GetMathFunctions(TypeFactory* type_factory,
                      const ZetaSQLBuiltinFunctionOptions& options,
                      NameToFunctionMap* functions);

void GetNetFunctions(TypeFactory* type_factory,
                     const ZetaSQLBuiltinFunctionOptions& options,
                     NameToFunctionMap* functions);

void GetHllCountFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions);

void GetKllQuantilesFunctions(TypeFactory* type_factory,
                              const ZetaSQLBuiltinFunctionOptions& options,
                              NameToFunctionMap* functions);

void GetHashingFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions);

void GetEncryptionFunctions(TypeFactory* type_factory,
                            const ZetaSQLBuiltinFunctionOptions& options,
                            NameToFunctionMap* functions);

void GetGeographyFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions);

void GetAnonFunctions(TypeFactory* type_factory,
                      const ZetaSQLBuiltinFunctionOptions& options,
                      NameToFunctionMap* functions);

void GetTypeOfFunction(TypeFactory* type_factory,
                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions);

void GetFilterFieldsFunction(TypeFactory* type_factory,
                             const ZetaSQLBuiltinFunctionOptions& options,
                             NameToFunctionMap* functions);
}  // namespace zetasql

#endif  // ZETASQL_COMMON_BUILTIN_FUNCTION_INTERNAL_H_
