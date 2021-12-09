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

#ifndef ZETASQL_COMPLIANCE_FUNCTIONS_TESTLIB_H_
#define ZETASQL_COMPLIANCE_FUNCTIONS_TESTLIB_H_

#include <vector>

#include "zetasql/testing/test_function.h"

namespace zetasql {

std::vector<QueryParamsWithResult> GetFunctionTestsAdd();
std::vector<QueryParamsWithResult> GetFunctionTestsSubtract();
std::vector<QueryParamsWithResult> GetFunctionTestsMultiply();
std::vector<QueryParamsWithResult> GetFunctionTestsDivide();
std::vector<QueryParamsWithResult> GetFunctionTestsSafeAdd();
std::vector<QueryParamsWithResult> GetFunctionTestsSafeSubtract();
std::vector<QueryParamsWithResult> GetFunctionTestsSafeMultiply();
std::vector<QueryParamsWithResult> GetFunctionTestsSafeDivide();
std::vector<QueryParamsWithResult> GetFunctionTestsSafeNegate();
std::vector<QueryParamsWithResult> GetFunctionTestsDiv();
std::vector<QueryParamsWithResult> GetFunctionTestsModulo();
std::vector<QueryParamsWithResult> GetFunctionTestsUnaryMinus();

std::vector<QueryParamsWithResult> GetFunctionTestsCoercedAdd();
std::vector<QueryParamsWithResult> GetFunctionTestsCoercedSubtract();
std::vector<QueryParamsWithResult> GetFunctionTestsCoercedMultiply();
std::vector<QueryParamsWithResult> GetFunctionTestsCoercedDivide();
std::vector<QueryParamsWithResult> GetFunctionTestsCoercedDiv();
std::vector<QueryParamsWithResult> GetFunctionTestsCoercedModulo();

std::vector<QueryParamsWithResult> GetFunctionTestsArrayConcatOperator();
std::vector<QueryParamsWithResult> GetFunctionTestsStringConcatOperator();

// TODO: Remove 'include_nano_timestamp' by always generating all
// compliance tests, possibly with features set in QueryParamsWithResult. We
// should avoid adding new bools.
std::vector<QueryParamsWithResult> GetFunctionTestsEqual(
    bool include_nano_timestamp);
std::vector<QueryParamsWithResult> GetFunctionTestsNotEqual(
    bool include_nano_timestamp);
std::vector<QueryParamsWithResult> GetFunctionTestsGreater(
    bool include_nano_timestamp);
std::vector<QueryParamsWithResult> GetFunctionTestsGreaterOrEqual(
    bool include_nano_timestamp);
std::vector<QueryParamsWithResult> GetFunctionTestsLess(
    bool include_nano_timestamp);
std::vector<QueryParamsWithResult> GetFunctionTestsLessOrEqual(
    bool include_nano_timestamp);

// Get all IN tests.
std::vector<QueryParamsWithResult> GetFunctionTestsIn();
// Get IN tests that include NULL in the input set
std::vector<QueryParamsWithResult> GetFunctionTestsInWithNulls();
// Get IN tests that do not include NULL in the input set
std::vector<QueryParamsWithResult> GetFunctionTestsInWithoutNulls();

std::vector<QueryParamsWithResult> GetFunctionTestsStructIn();
std::vector<QueryParamsWithResult> GetFunctionTestsIsNull();

std::vector<QueryParamsWithResult> GetFunctionTestsNot();
std::vector<QueryParamsWithResult> GetFunctionTestsAnd();
std::vector<QueryParamsWithResult> GetFunctionTestsOr();

std::vector<QueryParamsWithResult> GetFunctionTestsCast();  // all CAST tests

std::vector<QueryParamsWithResult>
GetFunctionTestsCastBool();  // boolean casts only

std::vector<QueryParamsWithResult>
GetFunctionTestsCastComplex();  // complex types

std::vector<QueryParamsWithResult> GetFunctionTestsCastDateTime();
std::vector<QueryParamsWithResult> GetFunctionTestsCastInterval();

std::vector<QueryParamsWithResult>
GetFunctionTestsCastNumeric();  // numeric only

std::vector<QueryParamsWithResult>
GetFunctionTestsCastString();  // string/bytes

// Casts between strings and numeric types
std::vector<QueryParamsWithResult> GetFunctionTestsCastNumericString();

// All SAFE_CAST tests.
std::vector<QueryParamsWithResult> GetFunctionTestsSafeCast();

// Casts between different array types, where arrays include/exclude NULL
// elements.  If <arrays_with_nulls> then all generated test cases will
// include arrays with NULL elements, otherwise no generated test case
// will include an array with NULL elements.
std::vector<QueryParamsWithResult>
GetFunctionTestsCastBetweenDifferentArrayTypes(bool arrays_with_nulls);

std::vector<QueryParamsWithResult> GetFunctionTestsBitwiseNot();
std::vector<QueryParamsWithResult> GetFunctionTestsBitwiseOr();
std::vector<QueryParamsWithResult> GetFunctionTestsBitwiseXor();
std::vector<QueryParamsWithResult> GetFunctionTestsBitwiseAnd();
std::vector<QueryParamsWithResult> GetFunctionTestsBitwiseLeftShift();
std::vector<QueryParamsWithResult> GetFunctionTestsBitwiseRightShift();
std::vector<QueryParamsWithResult> GetFunctionTestsBitCount();

std::vector<QueryParamsWithResult> GetFunctionTestsAtOffset();
std::vector<QueryParamsWithResult> GetFunctionTestsSafeAtOffset();

std::vector<QueryParamsWithResult> GetFunctionTestsIf();
std::vector<QueryParamsWithResult> GetFunctionTestsIfNull();
std::vector<QueryParamsWithResult> GetFunctionTestsNullIf();
std::vector<QueryParamsWithResult> GetFunctionTestsCoalesce();

// TODO: Remove 'include_nano_timestamp' by always generating all
// compliance tests, possibly with features set in QueryParamsWithResult. We
// should avoid adding new bools.
std::vector<QueryParamsWithResult> GetFunctionTestsGreatest(
    bool include_nano_timestamp);
std::vector<QueryParamsWithResult> GetFunctionTestsLeast(
    bool include_nano_timestamp);

std::vector<QueryParamsWithResult> GetFunctionTestsLike();

std::vector<FunctionTestCall> GetFunctionTestsDateTime();
// Include all date/time functions with standard function call syntax here.
std::vector<FunctionTestCall> GetFunctionTestsDateTimeStandardFunctionCalls();
std::vector<FunctionTestCall> GetFunctionTestsDateAndTimestampDiff();
std::vector<FunctionTestCall> GetFunctionTestsDateFromTimestamp();
std::vector<FunctionTestCall> GetFunctionTestsDateFromUnixDate();
std::vector<FunctionTestCall> GetFunctionTestsDateAdd();
std::vector<FunctionTestCall> GetFunctionTestsDateSub();
std::vector<FunctionTestCall> GetFunctionTestsDateAddSub();
std::vector<FunctionTestCall> GetFunctionTestsDateTrunc();
std::vector<FunctionTestCall> GetFunctionTestsDatetimeAddSub();
std::vector<FunctionTestCall> GetFunctionTestsDatetimeDiff();
std::vector<FunctionTestCall> GetFunctionTestsDatetimeTrunc();
std::vector<FunctionTestCall> GetFunctionTestsLastDay();
std::vector<FunctionTestCall> GetFunctionTestsTimeAddSub();
std::vector<FunctionTestCall> GetFunctionTestsTimeDiff();
std::vector<FunctionTestCall> GetFunctionTestsTimeTrunc();
std::vector<FunctionTestCall> GetFunctionTestsTimestampAdd();
std::vector<FunctionTestCall> GetFunctionTestsTimestampSub();
std::vector<FunctionTestCall> GetFunctionTestsTimestampAddSub();
std::vector<FunctionTestCall> GetFunctionTestsTimestampTrunc();
std::vector<FunctionTestCall> GetFunctionTestsExtractFrom();
std::vector<FunctionTestCall> GetFunctionTestsFormatDateTimestamp();
std::vector<FunctionTestCall> GetFunctionTestsFormatDatetime();
std::vector<FunctionTestCall> GetFunctionTestsFormatTime();
std::vector<FunctionTestCall> GetFunctionTestsParseDateTimestamp();
std::vector<FunctionTestCall> GetFunctionTestsCastStringToDateTimestamp();
std::vector<FunctionTestCall> GetFunctionTestsTimestampConversion();
std::vector<FunctionTestCall> GetFunctionTestsTimestampFromDate();

std::vector<FunctionTestCall> GetFunctionTestsDateConstruction();
std::vector<FunctionTestCall> GetFunctionTestsTimeConstruction();
std::vector<FunctionTestCall> GetFunctionTestsDatetimeConstruction();

std::vector<FunctionTestCall> GetFunctionTestsConvertDatetimeToTimestamp();
std::vector<FunctionTestCall> GetFunctionTestsConvertTimestampToTime();
std::vector<FunctionTestCall> GetFunctionTestsConvertTimestampToDatetime();

// Tests for CAST formatting of time types.
std::vector<FunctionTestCall> GetFunctionTestsCastFormatDateTimestamp();

std::vector<FunctionTestCall> GetFunctionTestsIntervalConstructor();
std::vector<FunctionTestCall> GetFunctionTestsIntervalComparisons();
std::vector<QueryParamsWithResult> GetFunctionTestsIntervalUnaryMinus();
std::vector<QueryParamsWithResult> GetDateTimestampIntervalSubtractions();
std::vector<QueryParamsWithResult> GetDatetimeTimeIntervalSubtractions();
std::vector<QueryParamsWithResult> GetDatetimeAddSubIntervalBase();
std::vector<QueryParamsWithResult> GetDatetimeAddSubInterval();
std::vector<QueryParamsWithResult> GetTimestampAddSubIntervalBase();
std::vector<QueryParamsWithResult> GetTimestampAddSubInterval();
std::vector<QueryParamsWithResult> GetFunctionTestsIntervalAdd();
std::vector<QueryParamsWithResult> GetFunctionTestsIntervalSub();
std::vector<QueryParamsWithResult> GetFunctionTestsIntervalMultiply();
std::vector<QueryParamsWithResult> GetFunctionTestsIntervalDivide();
std::vector<QueryParamsWithResult> GetFunctionTestsExtractInterval();
std::vector<FunctionTestCall> GetFunctionTestsJustifyInterval();

std::vector<FunctionTestCall> GetFunctionTestsFromProto();
std::vector<QueryParamsWithResult> GetFunctionTestsFromProto3TimeOfDay();

std::vector<FunctionTestCall> GetFunctionTestsToProto();
std::vector<QueryParamsWithResult> GetFunctionTestsToProto3TimeOfDay();

std::vector<FunctionTestCall> GetFunctionTestsMath();
std::vector<FunctionTestCall> GetFunctionTestsRounding();
std::vector<FunctionTestCall> GetFunctionTestsTrigonometric();

std::vector<FunctionTestCall> GetFunctionTestsAscii();
std::vector<FunctionTestCall> GetFunctionTestsUnicode();
std::vector<FunctionTestCall> GetFunctionTestsChr();
std::vector<FunctionTestCall> GetFunctionTestsOctetLength();
std::vector<FunctionTestCall> GetFunctionTestsSubstring();
std::vector<FunctionTestCall> GetFunctionTestsString();
std::vector<FunctionTestCall> GetFunctionTestsInstr1();
std::vector<FunctionTestCall> GetFunctionTestsInstr2();
std::vector<FunctionTestCall> GetFunctionTestsInstr3();
std::vector<FunctionTestCall> GetFunctionTestsInstrNoCollator();
std::vector<FunctionTestCall> GetFunctionTestsStringWithCollator();
std::vector<FunctionTestCall> GetFunctionTestsSoundex();
std::vector<FunctionTestCall> GetFunctionTestsTranslate();
std::vector<FunctionTestCall> GetFunctionTestsInitCap();
std::vector<FunctionTestCall> GetFunctionTestsRegexp();
std::vector<FunctionTestCall> GetFunctionTestsRegexp2(bool include_feature_set);
std::vector<FunctionTestCall> GetFunctionTestsRegexpInstr();
std::vector<FunctionTestCall> GetFunctionTestsFormat();
std::vector<FunctionTestCall> GetFunctionTestsArray();
std::vector<FunctionTestCall> GetFunctionTestsNormalize();
std::vector<FunctionTestCall> GetFunctionTestsBase2();
std::vector<FunctionTestCall> GetFunctionTestsBase8();
std::vector<FunctionTestCall> GetFunctionTestsBase32();
std::vector<FunctionTestCall> GetFunctionTestsBase64();
std::vector<FunctionTestCall> GetFunctionTestsHex();
std::vector<FunctionTestCall> GetFunctionTestsCodePoints();
std::vector<FunctionTestCall> GetFunctionTestsPadding();
std::vector<FunctionTestCall> GetFunctionTestsRepeat();
std::vector<FunctionTestCall> GetFunctionTestsReverse();
std::vector<FunctionTestCall> GetFunctionTestsParseNumeric();

std::vector<FunctionTestCall> GetFunctionTestsNet();

std::vector<FunctionTestCall> GetFunctionTestsBitCast();

std::vector<FunctionTestCall> GetFunctionTestsGenerateArray();
std::vector<FunctionTestCall> GetFunctionTestsGenerateDateArray();
std::vector<FunctionTestCall> GetFunctionTestsGenerateTimestampArray();

std::vector<FunctionTestCall> GetFunctionTestsRangeBucket();

// Engines should prefer GetFunctionTest{String,Native}Json{Query,Value} over
// GetFunctionTests{String,Native}JsonExtract{,Scalar}. The former group
// contains the functions defined in the SQL2016 standard.
//
// TODO: Remove 'include_nano_timestamp' by always generating all
// compliance tests, possibly with features set in QueryParamsWithResult. We
// should avoid adding new bools.
std::vector<FunctionTestCall> GetFunctionTestsStringJsonQuery();
std::vector<FunctionTestCall> GetFunctionTestsStringJsonExtract();
std::vector<FunctionTestCall> GetFunctionTestsStringJsonValue();
std::vector<FunctionTestCall> GetFunctionTestsStringJsonExtractScalar();
std::vector<FunctionTestCall> GetFunctionTestsStringJsonExtractArray();
std::vector<FunctionTestCall> GetFunctionTestsStringJsonExtractStringArray();
std::vector<FunctionTestCall> GetFunctionTestsStringJsonQueryArray();
std::vector<FunctionTestCall> GetFunctionTestsStringJsonValueArray();

std::vector<FunctionTestCall> GetFunctionTestsNativeJsonQuery();
std::vector<FunctionTestCall> GetFunctionTestsNativeJsonExtract();
std::vector<FunctionTestCall> GetFunctionTestsNativeJsonValue();
std::vector<FunctionTestCall> GetFunctionTestsNativeJsonExtractScalar();
std::vector<FunctionTestCall> GetFunctionTestsNativeJsonExtractArray();
std::vector<FunctionTestCall> GetFunctionTestsNativeJsonExtractStringArray();
std::vector<FunctionTestCall> GetFunctionTestsNativeJsonQueryArray();
std::vector<FunctionTestCall> GetFunctionTestsNativeJsonValueArray();
std::vector<FunctionTestCall> GetFunctionTestsToJsonString(
    bool include_nano_timestamp);
std::vector<FunctionTestCall> GetFunctionTestsToJson(
    bool include_nano_timestamp);
std::vector<QueryParamsWithResult> GetFunctionTestsJsonIsNull();
std::vector<FunctionTestCall> GetFunctionTestsParseJson();
std::vector<FunctionTestCall> GetFunctionTestsConvertJson();
std::vector<FunctionTestCall> GetFunctionTestsConvertJsonIncompatibleTypes();

std::vector<FunctionTestCall> GetFunctionTestsHash();
std::vector<FunctionTestCall> GetFunctionTestsFarmFingerprint();

std::vector<FunctionTestCall> GetFunctionTestsError();

std::vector<FunctionTestCall> GetFunctionTestsBytesStringConversion();

}  // namespace zetasql

#endif  // ZETASQL_COMPLIANCE_FUNCTIONS_TESTLIB_H_
