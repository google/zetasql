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

#ifndef ZETASQL_COMMON_BUILTIN_FUNCTION_INTERNAL_H_
#define ZETASQL_COMMON_BUILTIN_FUNCTION_INTERNAL_H_

#include <stddef.h>

#include <map>
#include <string>

#include "zetasql/proto/options.pb.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/type.h"
#include "absl/container/flat_hash_set.h"

namespace zetasql {

// Wrapper around FunctionSignature that allocates on the heap to reduce the
// impact on stack size while still using the same initializer list.
class FunctionSignatureOnHeap {
 public:
  FunctionSignatureOnHeap(const FunctionArgumentType& result_type,
                          const FunctionArgumentTypeList& arguments,
                          int64_t context_id)
      : signature_(new FunctionSignature(result_type, arguments, context_id)) {}

  FunctionSignatureOnHeap(const FunctionArgumentType& result_type,
                          const FunctionArgumentTypeList& arguments,
                          int64_t context_id,
                          const FunctionSignatureOptions& options)
      : signature_(new FunctionSignature(result_type, arguments, context_id,
                                         options)) {}

  const FunctionSignature& Get() const { return *signature_; }

 private:
  std::shared_ptr<FunctionSignature> signature_;
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

std::string BetweenFunctionSQL(const std::vector<std::string>& inputs);

std::string InListFunctionSQL(const std::vector<std::string>& inputs);

std::string CaseWithValueFunctionSQL(const std::vector<std::string>& inputs);

std::string CaseNoValueFunctionSQL(const std::vector<std::string>& inputs);

std::string InArrayFunctionSQL(const std::vector<std::string>& inputs);

std::string ArrayAtOffsetFunctionSQL(const std::vector<std::string>& inputs);

std::string ArrayAtOrdinalFunctionSQL(const std::vector<std::string>& inputs);

std::string SafeArrayAtOffsetFunctionSQL(
    const std::vector<std::string>& inputs);

std::string SafeArrayAtOrdinalFunctionSQL(
    const std::vector<std::string>& inputs);

std::string GenerateDateTimestampArrayFunctionSQL(
    const std::string& function_name, const std::vector<std::string>& inputs);

// For MakeArray we explicitly prepend the array type to the function sql, thus
// array-type-name is expected to be passed as the first element of inputs.
std::string MakeArrayFunctionSQL(const std::vector<std::string>& inputs);

std::string ExtractFunctionSQL(const std::vector<std::string>& inputs);

std::string ExtractDateOrTimeFunctionSQL(
    const std::string& date_part, const std::vector<std::string>& inputs);

bool ArgumentIsStringLiteral(const InputArgumentType& argument);

absl::Status CheckDateDiffArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckBitwiseOperatorArgumentsHaveSameType(
    const std::string& operator_string,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckBitwiseOperatorFirstArgumentIsIntegerOrBytes(
    const std::string& operator_string,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckDateTruncArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckTimeTruncArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckExtractPreResolutionArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckExtractPostResolutionArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckDateAddDateSubArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckDatetimeAddSubDiffArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

// This function returns a ZetaSQL ProtoType for
// google::protobuf::EnumValueDescriptorProto, which is either from the <catalog> (if this
// ProtoType is present) or newly created in the <type_factory>.
zetasql_base::StatusOr<const Type*> GetOrMakeEnumValueDescriptorType(
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options);

absl::Status CheckTimestampAddTimestampSubArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckTimestampDiffArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckTimestampTruncArguments(
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

std::string EmptySupportedSignatures(const LanguageOptions& language_options,
                                     const Function& function);

absl::Status CheckArgumentsSupportEquality(
    const std::string& comparison_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckArgumentsSupportComparison(
    const std::string& comparison_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckMinMaxGreatestLeastArguments(
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

absl::Status CheckInArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

absl::Status CheckRangeBucketArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options);

// Returns true if an arithmetic operation has a floating point type as its
// input.
bool HasFloatingPointArgument(const std::vector<InputArgumentType>& arguments);

// Returns true if an arithmetic operation has a numeric type as its
// input.
bool HasNumericTypeArgument(const std::vector<InputArgumentType>& arguments);

// Returns true if an arithmetic operation has a bignumeric type as its
// input.
bool HasBigNumericTypeArgument(const std::vector<InputArgumentType>& arguments);

// Returns true if FN_CONCAT_STRING function can coerce argument of given type
// to STRING.
bool CanStringConcatCoerceFrom(const zetasql::Type* arg_type);

// Compute the result type for TOP_COUNT and TOP_SUM.
// The output type is
//   ARRAY<
//     STRUCT<`value` <arguments[0].type()>,
//            `<field2_name>` <arguments[1].type()> > >
zetasql_base::StatusOr<const Type*> ComputeResultTypeForTopStruct(
    const std::string& field2_name, Catalog* catalog, TypeFactory* type_factory,
    CycleDetector* cycle_detector,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options);

void InsertCreatedFunction(NameToFunctionMap* functions,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           Function* function_in);

void InsertFunctionImpl(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options,
    const std::vector<std::string>& name, Function::Mode mode,
    const std::vector<FunctionSignatureOnHeap>& signatures_on_heap,
    const FunctionOptions& function_options);

void InsertFunction(NameToFunctionMap* functions,
                    const ZetaSQLBuiltinFunctionOptions& options,
                    const std::string& name, Function::Mode mode,
                    const std::vector<FunctionSignatureOnHeap>& signatures,
                    const FunctionOptions& function_options);

// Note: This function is intentionally overloaded to prevent a default
// FunctionOptions object to be allocated on the callers stack.
void InsertFunction(NameToFunctionMap* functions,
                    const ZetaSQLBuiltinFunctionOptions& options,
                    const std::string& name, Function::Mode mode,
                    const std::vector<FunctionSignatureOnHeap>& signatures);

void InsertNamespaceFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, const std::string& space,
    const std::string& name, Function::Mode mode,
    const std::vector<FunctionSignatureOnHeap>& signatures,
    const FunctionOptions& function_options = FunctionOptions());
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

void GetDatetimeDiffTruncFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions);

void GetDatetimeFormatFunctions(TypeFactory* type_factory,
                                const ZetaSQLBuiltinFunctionOptions& options,
                                NameToFunctionMap* functions);

void GetDatetimeFunctions(TypeFactory* type_factory,
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

}  // namespace zetasql

#endif  // ZETASQL_COMMON_BUILTIN_FUNCTION_INTERNAL_H_
