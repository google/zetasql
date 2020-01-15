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

#include "zetasql/public/functions/parse_date_time.h"

#include <time.h>

#include <functional>
#include <set>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace functions {
namespace {

using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

// Test parsing a timestamp from a string.  The <expected_ result> must be in
// a form that ConvertStringToTimestamp() will work on it (for example,
// the canonical form '2015-01-31 12:34:56.999999+0').  If <expected_string>
// is empty then an error is expected and we validate that parsing the string
// fails.  This is a helper for TestParseSecondsSinceEpoch().
static void TestParseStringToTimestamp(const std::string& format,
                                       const std::string& timestamp_string,
                                       const std::string& default_time_zone,
                                       const std::string& expected_result) {
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  if (expected_result.empty()) {
    // An error is expected.
    zetasql_base::Status status = ParseStringToTimestamp(
        format, timestamp_string, default_time_zone, &timestamp);
    std::string result_timestamp_string;
    if (status.ok()) {
      EXPECT_TRUE(IsValidTimestamp(timestamp, kMicroseconds));
      ZETASQL_EXPECT_OK(ConvertTimestampToStringWithoutTruncation(
          timestamp, kMicroseconds, "UTC", &result_timestamp_string));
    }
    EXPECT_FALSE(status.ok())
        << "format: " << format << "\ntimestamp_string: '" << timestamp_string
        << "'"
        << "\nresult timestamp: " << timestamp
        << "\nresult timestamp string: " << result_timestamp_string;
    return;
  }
  int64_t expected;
  ZETASQL_EXPECT_OK(ConvertStringToTimestamp(expected_result,
                                     default_time_zone, kMicroseconds,
                                     &expected));
  ZETASQL_EXPECT_OK(ParseStringToTimestamp(format, timestamp_string,
                                   default_time_zone, &timestamp))
      << "format: " << format
      << "\ntimestamp: " << timestamp_string;
  std::string converted_timestamp_string;
  ZETASQL_EXPECT_OK(ConvertTimestampToStringWithoutTruncation(
      timestamp, kMicroseconds, default_time_zone,
      &converted_timestamp_string));
  EXPECT_EQ(expected, timestamp)
      << "format: " << format
      << "\ninput_timestamp: " << timestamp_string
      << "\nconverted_timestamp:  " << converted_timestamp_string
      << "\nexpected:  " << expected_result;
}

static void TestParseSecondsSinceEpoch(int64_t timestamp_seconds) {
  std::string seconds_string;
  absl::StrAppend(&seconds_string, timestamp_seconds);

  std::string expected_timestamp_string;
  ZETASQL_EXPECT_OK(ConvertTimestampToStringWithoutTruncation(
      timestamp_seconds, kSeconds, "UTC", &expected_timestamp_string));

  TestParseStringToTimestamp(
      "%s", seconds_string, "UTC", expected_timestamp_string);
}

static void TestParseTimestamp(const FunctionTestCall& test) {
  // Ignore tests that do not have the time zone explicitly specified since
  // the function library requires a timezone.  Also ignore test cases
  // with NULL value inputs.  The date/time function library is only
  // implemented for non-NULL values.
  if (test.params.params().size() != 3 || test.params.param(0).is_null() ||
      test.params.param(1).is_null() || test.params.param(2).is_null()) {
    return;
  }

  const Value& format_param = test.params.param(0);
  const Value& timestamp_string_param = test.params.param(1);
  const Value& timezone_param = test.params.param(2);
  int64_t result_timestamp;
  absl::Time base_time_result;

  zetasql_base::Status status = ParseStringToTimestamp(
      format_param.string_value(), timestamp_string_param.string_value(),
      timezone_param.string_value(), &result_timestamp);

  zetasql_base::Status base_time_status = ParseStringToTimestamp(
      format_param.string_value(), timestamp_string_param.string_value(),
      timezone_param.string_value(), &base_time_result);

  std::string test_string = absl::Substitute(
      absl::StrCat(test.function_name, "($0, $1, $2)"),
      format_param.DebugString(), timestamp_string_param.DebugString(),
      timezone_param.DebugString());

  const QueryParamsWithResult::Result& micros_test_result = zetasql_base::FindOrDie(
      test.params.results(), QueryParamsWithResult::kEmptyFeatureSet);
  if (micros_test_result.status.ok()) {
    ZETASQL_EXPECT_OK(status) << test_string;
    if (status.ok()) {
      EXPECT_EQ(TYPE_TIMESTAMP, micros_test_result.result.type_kind())
          << test_string;
      EXPECT_EQ(micros_test_result.result.ToTime(),
                absl::FromUnixMicros(result_timestamp))
          << test_string << "\nexpected: "
          << absl::FormatTime(
                 absl::FromUnixMicros(micros_test_result.result.ToUnixMicros()))
          << "\nactual: "
          << absl::FormatTime(absl::FromUnixMicros(result_timestamp));
    }
  } else {
    EXPECT_FALSE(status.ok()) << test_string << "\nstatus: "
                              << micros_test_result.status;
  }

  const std::set<LanguageFeature> feature_set{FEATURE_TIMESTAMP_NANOS};
  const QueryParamsWithResult::Result& nanos_test_result =
      zetasql_base::FindOrDie(test.params.results(), feature_set);

  if (nanos_test_result.status.ok()) {
    ZETASQL_EXPECT_OK(base_time_status) << test_string;
    EXPECT_EQ(TYPE_TIMESTAMP, nanos_test_result.result.type_kind())
        << test_string;
    EXPECT_EQ(nanos_test_result.result.ToTime(), base_time_result)
        << test_string << ": "
        << absl::FormatTime(nanos_test_result.result.ToTime()) << " vs "
        << absl::FormatTime(base_time_result);
  } else {
    EXPECT_FALSE(base_time_status.ok()) << test_string << "\nstatus: "
                                        << nanos_test_result.status;
  }
}

static void TestParseDate(const FunctionTestCall& test) {
  // Ignore test cases with NULL value inputs. The date/time function library
  // is only implemented for non-NULL values.
  if (test.params.params().size() != 2 ||
      test.params.param(0).is_null() || test.params.param(1).is_null()) {
    return;
  }

  const Value& format_param = test.params.param(0);
  const Value& date_string_param = test.params.param(1);
  int32_t result_date;

  zetasql_base::Status status = ParseStringToDate(
      format_param.string_value(), date_string_param.string_value(),
      &result_date);
  std::string test_string = absl::Substitute(
      absl::StrCat(test.function_name, "($0, $1)"), format_param.DebugString(),
      date_string_param.DebugString());

  if (test.params.status().ok()) {
    ZETASQL_EXPECT_OK(status) << test_string;
    EXPECT_EQ(TYPE_DATE, test.params.result().type_kind()) << test_string;
    EXPECT_EQ(test.params.result().date_value(), result_date)
        << test_string;
  } else {
    EXPECT_FALSE(status.ok()) << test_string;
  }
}

// Use the supplied <result_validator> to validate that the <actual_status> and
// <actual_output_string_value> matches the expected <result>.
static void ValidateResult(
    const QueryParamsWithResult::Result* result,
    const zetasql_base::Status& actual_status,
    const std::string& actual_output_string_value,
    const std::function<bool(const Value& expected_result,
                             const std::string& actual_string_value)>&
        result_validator) {
  CHECK(result != nullptr);
  const zetasql_base::Status& expected_status = result->status;
  const Value& expected_result = result->result;

  if (expected_status.ok()) {
    ZETASQL_ASSERT_OK(actual_status);
    EXPECT_TRUE(result_validator(expected_result, actual_output_string_value))
        << "Expected result string: " << expected_result.DebugString()
        << "\nActual result string: " << actual_output_string_value;
  } else {
    EXPECT_FALSE(actual_status.ok());
  }
}

// TODO: This a modified copy of a very similar functions defined in
// date_time_util_test.cc, in which there are multiple similar functions to test
// other civil time related functions. We should extract this function into a
// shared library and avoid repeatedly define them.
static void TestCivilTimeFunction(
    const FunctionTestCall& testcase,
    const std::function<bool(const FunctionTestCall& testcase)>&
        should_skip_test_case,
    const std::function<zetasql_base::Status(const FunctionTestCall& testcase,
                                     std::string* output_string_value)>&
        function_to_test_for_micro,
    const std::function<zetasql_base::Status(const FunctionTestCall& testcase,
                                     std::string* output_string_value)>&
        function_to_test_for_nano,
    const std::function<bool(const Value& expected_result,
                             const std::string& actual_string_value)>&
        result_validator) {
  if (should_skip_test_case(testcase)) {
    return;
  }

  // Validate micro result
  static const QueryParamsWithResult::FeatureSet civil_time_feature_set(
      {FEATURE_V_1_2_CIVIL_TIME});
  std::string actual_micro_string_value;
  zetasql_base::Status actual_micro_status =
      function_to_test_for_micro(testcase, &actual_micro_string_value);
  const QueryParamsWithResult::Result* expected_micro_result =
      zetasql_base::FindOrNull(testcase.params.results(), civil_time_feature_set);
  ValidateResult(expected_micro_result, actual_micro_status,
                 actual_micro_string_value, result_validator);

  // Validate nano result
  static const QueryParamsWithResult::FeatureSet
      civil_time_and_nano_feature_set(
          {FEATURE_V_1_2_CIVIL_TIME, FEATURE_TIMESTAMP_NANOS});
  std::string actual_nano_string_value;
  zetasql_base::Status actual_nano_status =
      function_to_test_for_nano(testcase, &actual_nano_string_value);
  const QueryParamsWithResult::Result* expected_nano_result = zetasql_base::FindOrNull(
      testcase.params.results(), civil_time_and_nano_feature_set);
  ValidateResult(expected_nano_result, actual_nano_status,
                 actual_nano_string_value, result_validator);
}

static void TestParseTime(const FunctionTestCall& testcase) {
  auto ShouldSkipTestCase = [](const FunctionTestCall& testcase) {
    DCHECK_EQ(testcase.params.num_params(), 2);
    // Ignore test cases with NULL value inputs.  The date/time function
    // library is only implemented for non-NULL values.
    if (testcase.params.param(0).is_null() ||
        testcase.params.param(1).is_null()) {
      return true;
    }
    return false;
  };
  auto GetParseTimeFunc = [](TimestampScale scale) {
    return [scale](const FunctionTestCall& testcase,
                   std::string* output_string) -> ::zetasql_base::Status {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(ParseStringToTime(testcase.params.param(0).string_value(),
                                        testcase.params.param(1).string_value(),
                                        scale, &time));
      *output_string = time.DebugString();
      return ::zetasql_base::OkStatus();
    };
  };
  auto ParseTimeResultValidator = [](const Value& expected_result,
                                     const std::string& actual_string) {
    return expected_result.type_kind() == TYPE_TIME &&
           expected_result.DebugString() == actual_string;
  };
  TestCivilTimeFunction(
      testcase, ShouldSkipTestCase, GetParseTimeFunc(kMicroseconds),
      GetParseTimeFunc(kNanoseconds), ParseTimeResultValidator);
}

static void TestParseDatetime(const FunctionTestCall& testcase) {
  auto ShouldSkipTestCase = [](const FunctionTestCall& testcase) {
    DCHECK_EQ(testcase.params.num_params(), 2);
    // Ignore test cases with NULL value inputs.  The date/time function
    // library is only implemented for non-NULL values.
    if (testcase.params.param(0).is_null() ||
        testcase.params.param(1).is_null()) {
      return true;
    }
    return false;
  };
  auto GetParseDatetimeFunc = [](TimestampScale scale) {
    return [scale](const FunctionTestCall& testcase,
                   std::string* output_string) -> ::zetasql_base::Status {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(ParseStringToDatetime(
          testcase.params.param(0).string_value(),
          testcase.params.param(1).string_value(), scale, &datetime));
      *output_string = datetime.DebugString();
      return ::zetasql_base::OkStatus();
    };
  };
  auto ParseDatetimeResultValidator = [](const Value& expected_result,
                                         const std::string& actual_string) {
    return expected_result.type_kind() == TYPE_DATETIME &&
           expected_result.DebugString() == actual_string;
  };
  TestCivilTimeFunction(
      testcase, ShouldSkipTestCase, GetParseDatetimeFunc(kMicroseconds),
      GetParseDatetimeFunc(kNanoseconds), ParseDatetimeResultValidator);
}

static void ParseDateTimeFunctionTest(const FunctionTestCall& test) {
  if (test.function_name == "parse_timestamp") {
    TestParseTimestamp(test);
  } else if (test.function_name == "parse_date") {
    TestParseDate(test);
  } else if (test.function_name == "parse_datetime") {
    TestParseDatetime(test);
  } else if (test.function_name == "parse_time") {
    TestParseTime(test);
  } else {
    ASSERT_FALSE(true) << "Test cases do not support function: "
                       << test.function_name;
  }
}

class ParseDateTimeTestWithParam
  : public ::testing::TestWithParam<FunctionTestCall> {};

TEST_P(ParseDateTimeTestWithParam, ParseStringToDateTimestampTests) {
  const FunctionTestCall& test = GetParam();
  ParseDateTimeFunctionTest(test);
}

// These tests are populated in zetasql/compliance/functions_testlib.cc.
INSTANTIATE_TEST_SUITE_P(
    ParseDateTimestampTests, ParseDateTimeTestWithParam,
    testing::ValuesIn(GetFunctionTestsParseDateTimestamp()));

TEST(StringToTimestampTests, ParseTimestampSecondsSinceEpochTests) {
  // These tests validate results by comparing the parsed string result vs.
  // converting that int64_t seconds to timestamp using
  // ConvertTimestampToStringWithoutTruncation().
  TestParseSecondsSinceEpoch(types::kTimestampMin / 1000000);
  TestParseSecondsSinceEpoch(-31536000000);
  TestParseSecondsSinceEpoch(-3153600000);
  TestParseSecondsSinceEpoch(-315360000);
  TestParseSecondsSinceEpoch(-3600);
  TestParseSecondsSinceEpoch(-60);
  TestParseSecondsSinceEpoch(0);
  TestParseSecondsSinceEpoch(60);
  TestParseSecondsSinceEpoch(3600);
  TestParseSecondsSinceEpoch(315360000);
  TestParseSecondsSinceEpoch(3153600000);
  TestParseSecondsSinceEpoch(31536000000);
  TestParseSecondsSinceEpoch(252288000000);
  TestParseSecondsSinceEpoch(types::kTimestampMax / 1000000);
}

TEST(StringToTimestampTests, StrptimeTests) {
  // A few tests of strptime() showing behavior with duplicate format
  // elements.  When there is element overlap, the last one wins.
  char* strptime_result;
  std::string format_string("%Y %Y");
  std::string timestamp_string("2006 2005");
  struct tm tm = { 0 };
  strptime_result = strptime(timestamp_string.c_str(), format_string.c_str(),
                             &tm);
  EXPECT_EQ(105, tm.tm_year);

  format_string = "%D";  // shorthand for %m/%d/%y (with two digit year)
  timestamp_string = "03/04/05";
  tm = { 0 };
  strptime_result = strptime(timestamp_string.c_str(), format_string.c_str(),
                             &tm);

  EXPECT_EQ(105, tm.tm_year) << "year: " << tm.tm_year
                             << ", month: " << tm.tm_mon
                             << ", day: " << tm.tm_mday;
  EXPECT_EQ(2, tm.tm_mon);
  EXPECT_EQ(4, tm.tm_mday);

  format_string = "%Y-%m-%d %D %m";
  timestamp_string = "2000-02-03 04/05/06 08";
  tm = { 0 };
  strptime_result = strptime(timestamp_string.c_str(), format_string.c_str(),
                             &tm);

  EXPECT_EQ(106, tm.tm_year) << "year: " << tm.tm_year
                             << ", month: " << tm.tm_mon
                             << ", day: " << tm.tm_mday;
  EXPECT_EQ(7, tm.tm_mon);
  EXPECT_EQ(5, tm.tm_mday);


  // The '%Ey' element is not working in strptime so we have to roll our own.
  format_string = "%Ey";  // two digit year
  timestamp_string = "07";
  tm = { 0 };
  strptime_result = strptime(timestamp_string.c_str(), format_string.c_str(),
                             &tm);
  EXPECT_THAT(strptime_result, testing::IsNull());

  // The '%Ou' element is not working in strptime so we have to roll our own.
  format_string = "%Ou";  // day of week
  timestamp_string = "7";
  tm = { 0 };
  strptime_result = strptime(timestamp_string.c_str(), format_string.c_str(),
                             &tm);
  EXPECT_THAT(strptime_result, testing::IsNull());
}

TEST(StringToTimestampTests, NonNullTerminatedStringViewTests) {
  // Tests for string_views that are not null-terminated.

  // Base case, the format and timestamp strings are both null-terminated.
  std::string good_format_string("%M");
  std::string good_timestamp_string("26");

  TimeValue time;
  ZETASQL_EXPECT_OK(ParseStringToTime(good_format_string, good_timestamp_string,
                              kMicroseconds, &time));
  EXPECT_EQ(time.Packed64TimeMicros(),
            TimeValue::FromHMSAndMicros(0, 26, 0, 0).Packed64TimeMicros());

  absl::string_view bad_format_string("%Mabc", 2);
  absl::string_view bad_timestamp_string("26xyz", 2);

  // Only the format string is not null-terminated.
  ZETASQL_EXPECT_OK(ParseStringToTime(bad_format_string, good_timestamp_string,
                              kMicroseconds, &time));
  EXPECT_EQ(time.Packed64TimeMicros(),
            TimeValue::FromHMSAndMicros(0, 26, 0, 0).Packed64TimeMicros());

  // Only the timestamp string is not null-terminated.
  ZETASQL_EXPECT_OK(ParseStringToTime(good_format_string, bad_timestamp_string,
                              kMicroseconds, &time));
  EXPECT_EQ(time.Packed64TimeMicros(),
            TimeValue::FromHMSAndMicros(0, 26, 0, 0).Packed64TimeMicros());

  // The format and timestamp strings are both not null-terminated..
  ZETASQL_EXPECT_OK(ParseStringToTime(bad_format_string, bad_timestamp_string,
                              kMicroseconds, &time));
  EXPECT_EQ(time.Packed64TimeMicros(),
            TimeValue::FromHMSAndMicros(0, 26, 0, 0).Packed64TimeMicros());

  // Related to b/124474887
  absl::string_view bad_format_string_2("%m%d%Y", 6);
  absl::string_view bad_timestamp_string_2("122320189", 8);

  DatetimeValue datetime;
  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_2, bad_timestamp_string_2,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 12, 23, 0, 0, 0, 0).Packed64DatetimeMicros());

  // In this test case, "123" is out of 'bad_timestamp_string_3' with length 23.
  // The actual 'bad_timestamp_string_3' is "2018-09-26 23:42:13.350"
  // But because 'fmt' is ended with "%E*S", if we don't stop at
  // bad_timestamp_string_3[23], there will be an out-of-range error.
  absl::string_view bad_format_string_3("%Y-%m-%d %H:%M:%E*S", 19);
  absl::string_view bad_timestamp_string_3("2018-09-26 23:42:13.350123", 23);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_3, bad_timestamp_string_3,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 9, 26, 23, 42, 13, 350000).Packed64DatetimeMicros());

  absl::string_view bad_format_string_4("%Y-%m-%d %H:%M:%E3S", 19);
  absl::string_view bad_timestamp_string_4("2018-09-26 23:42:13.350123", 21);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_4, bad_timestamp_string_4,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 9, 26, 23, 42, 13, 300000).Packed64DatetimeMicros());

  absl::string_view bad_format_string_5("%Y-%m-%d %H:%M:%E3S", 19);
  absl::string_view bad_timestamp_string_5("2018-09-26 23:42:13.3", 21);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_5, bad_timestamp_string_5,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 9, 26, 23, 42, 13, 300000).Packed64DatetimeMicros());

  absl::string_view bad_format_string_6("%Y-%m-%d %H:%M:%E3S", 19);
  absl::string_view bad_timestamp_string_6("2018-09-26 23:42:13.", 19);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_6, bad_timestamp_string_6,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 9, 26, 23, 42, 13, 000000).Packed64DatetimeMicros());

  absl::string_view bad_format_string_7("%Y-%m-%d %H:%M:%E3S", 19);
  absl::string_view bad_timestamp_string_7("2018-09-26 23:42:13", 19);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_7, bad_timestamp_string_7,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 9, 26, 23, 42, 13, 000000).Packed64DatetimeMicros());

  absl::string_view bad_format_string_8("%Y-%m-%d %H:%M:%S%", 17);
  absl::string_view bad_timestamp_string_8("   2018-09-26 23:42:13", 22);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_8, bad_timestamp_string_8,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 9, 26, 23, 42, 13, 000000).Packed64DatetimeMicros());

  absl::string_view bad_format_string_9("%EY%", 3);
  absl::string_view bad_timestamp_string_9("2018-", 4);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_9, bad_timestamp_string_9,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(
                2018, 1, 1, 0, 0, 0, 0).Packed64DatetimeMicros());

  std::string timestamp_string_10("Monday Feb 28 12:34:56 2006");
  char char_array_10[27];
  for (int i = 0; i < 27; i++) {
    char_array_10[i] = timestamp_string_10[i];
  }
  absl::string_view bad_format_string_10("%a %b %e %H:%M:%S %Y", 20);
  absl::string_view bad_timestamp_string_10(char_array_10, 27);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_10, bad_timestamp_string_10,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(2006, 2, 28, 12, 34, 56, 0)
                .Packed64DatetimeMicros());

  std::string format_string_11("%a %b %e %H:%M:%S %Y");
  char char_array_11[20];
  for (int i = 0; i < 20; i++) {
    char_array_11[i] = format_string_11[i];
  }
  absl::string_view bad_format_string_11(char_array_11, 20);
  absl::string_view bad_timestamp_string_11("Thu July 23 12:00:00 20155", 25);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_11, bad_timestamp_string_11,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(2015, 7, 23, 12, 00, 00, 0)
                .Packed64DatetimeMicros());

  std::string timestamp_string_12("1991-01-01 02:00:00.000000+01");
  char char_array_12[29];
  for (int i = 0; i < 29; i++) {
    char_array_12[i] = timestamp_string_12[i];
  }
  absl::string_view bad_format_string_12("%Y-%m-%d %H:%M:%E6S%Ez", 22);
  absl::string_view bad_timestamp_string_12(char_array_12, 29);

  absl::Time result_timestamp;
  ZETASQL_EXPECT_OK(ParseStringToTimestamp(
      bad_format_string_12, bad_timestamp_string_12, "UTC", &result_timestamp));
  ZETASQL_EXPECT_OK(ConvertTimestampToDatetime(result_timestamp, "UTC", &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(1991, 01, 01, 01, 00, 00, 0)
                .Packed64DatetimeMicros());

  std::string timestamp_string_13(
      " \x0c\n\r\t\x0b\x32\x30\x30\x30\x0b\t\r\n\x0c ");
  char char_array_13[16];
  for (int i = 0; i < 16; i++) {
    char_array_13[i] = timestamp_string_13[i];
  }
  absl::string_view bad_format_string_13("%t%Y%t", 6);
  absl::string_view bad_timestamp_string_13(char_array_13, 16);

  ZETASQL_EXPECT_OK(ParseStringToDatetime(bad_format_string_13, bad_timestamp_string_13,
                                  kMicroseconds, &datetime));
  EXPECT_EQ(datetime.Packed64DatetimeMicros(),
            DatetimeValue::FromYMDHMSAndMicros(2000, 1, 1, 00, 00, 00, 0)
                .Packed64DatetimeMicros());

  char char_array_14[1];
  char_array_14[0] = '-';
  absl::string_view bad_format_string_14("%Y", 2);
  absl::string_view bad_timestamp_string_14(char_array_14, 1);

  EXPECT_THAT(
      ParseStringToDatetime(bad_format_string_14, bad_timestamp_string_14,
                            kMicroseconds, &datetime),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               HasSubstr("Failed to parse input string")));
}

TEST(StringToTimestampTests, NullTerminatedStringViewTests) {
  // The last byte of the string_view can be a null byte.
  absl::string_view good_format_string("%M", 2);
  absl::string_view good_format_string_2("%M\0", 3);
  absl::string_view good_timestamp_string("26xyz", 2);
  absl::string_view good_timestamp_string_2("26\0xyz", 3);
  std::vector<absl::string_view> good_format_strings =
      {good_format_string, good_format_string_2};
  std::vector<absl::string_view> good_timestamp_strings =
      {good_timestamp_string, good_timestamp_string_2};

  TimeValue time;
  for (const absl::string_view& format_string : good_format_strings) {
    for (const absl::string_view& timestamp_string : good_timestamp_strings) {
      ZETASQL_EXPECT_OK(ParseStringToTime(format_string, timestamp_string,
                                  kMicroseconds, &time))
          << "\nformat_string: '" << format_string << "'"
          << "\ntimestamp_string: '" << timestamp_string << "'";
      EXPECT_EQ(time.Packed64TimeMicros(),
                TimeValue::FromHMSAndMicros(0, 26, 0, 0).Packed64TimeMicros());
    }
  }

  absl::string_view good_format_string_3("%M\0xy%Hzx", 8);
  absl::string_view good_timestamp_string_3("26\0xy13zx", 8);
  ZETASQL_EXPECT_OK(ParseStringToTime(good_format_string_3, good_timestamp_string_3,
                              kMicroseconds, &time));
  EXPECT_EQ(time.Packed64TimeMicros(),
            TimeValue::FromHMSAndMicros(13, 26, 0, 0).Packed64TimeMicros());
}

TEST(StringToTimestampTests, EmptyStringViewTests) {
  absl::string_view whitespace_format_string("     \n     ", 11);
  absl::string_view empty_format_string("", 0);
  absl::string_view null_format_string(nullptr, 0);
  std::vector<absl::string_view> format_strings =
      {whitespace_format_string, empty_format_string, null_format_string};

  absl::string_view whitespace_timestamp_string(" \n ", 3);
  absl::string_view empty_timestamp_string("", 0);
  absl::string_view null_timestamp_string(nullptr, 0);
  std::vector<absl::string_view> timestamp_strings =
      {whitespace_timestamp_string, empty_timestamp_string,
       null_timestamp_string};

  TimeValue time;
  for (const absl::string_view& format_string : format_strings) {
    for (const absl::string_view& timestamp_string : timestamp_strings) {
      ZETASQL_EXPECT_OK(ParseStringToTime(format_string, timestamp_string,
                                  kMicroseconds, &time))
          << "\nformat_string("
          << (format_string.data() != nullptr ?
              absl::StrCat("'", format_string, "'") :
              "nullptr")
          << ", " << format_string.length() << "): '"
          << format_string << "'"
          << "\ntimestamp_string: '"
          << (timestamp_string.data() != nullptr ?
              absl::StrCat("'", timestamp_string, "'") :
              "nullptr")
          << ", " << timestamp_string.length() << "): '"
          << timestamp_string << "'";
      EXPECT_EQ(time.Packed64TimeMicros(),
                TimeValue::FromHMSAndMicros(0, 0, 0, 0).Packed64TimeMicros());
    }
  }

  absl::string_view nonempty_format_string("a");
  absl::string_view nonempty_timestamp_string("b");

  // Matching an empty format string to non-empty timestamp string (or vice
  // versa) should never succeed.
  for (const absl::string_view& format_string : format_strings) {
    EXPECT_THAT(ParseStringToTime(format_string, nonempty_timestamp_string,
                                  kMicroseconds, &time),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         HasSubstr("Failed to parse input string")));
  }
  for (const absl::string_view& timestamp_string : timestamp_strings) {
    EXPECT_THAT(ParseStringToTime(nonempty_format_string, timestamp_string,
                                  kMicroseconds, &time),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         HasSubstr("Failed to parse input string")));
  }
}

TEST(StringToTimestampTests, LeadingAndTrailingWhitespaceTests) {
  // %n and %t both consume arbitrary whitespace
  absl::string_view format0("");
  absl::string_view format1(" ");
  absl::string_view format2("    ");
  absl::string_view format3("%n");
  absl::string_view format4(" %n");
  absl::string_view format5(" %n ");
  absl::string_view format6("%n ");
  absl::string_view format7("%t");
  absl::string_view format8(" %t");
  absl::string_view format9(" %t ");
  absl::string_view format10("%t ");
  absl::string_view format11(" %n   %t %n   ");
  std::vector<absl::string_view> formats =
      {format0, format1, format2, format3, format4, format5, format6, format7,
       format8, format9, format10, format11};

  absl::string_view time1("");
  absl::string_view time2(" ");
  absl::string_view time3("\n");
  absl::string_view time4(" \t");
  absl::string_view time5("\v ");
  absl::string_view time6(" \f ");
  absl::string_view time7("   \r   ");
  absl::string_view time8(" \f \t\v\n \r  ");
  std::vector<absl::string_view> times =
      {time1, time2, time3, time4, time5, time6, time7, time8};

  TimeValue time_value;
  for (const absl::string_view& format : formats) {
    for (const absl::string_view& time : times) {
      ZETASQL_EXPECT_OK(ParseStringToTime(format, time, kMicroseconds, &time_value));
      EXPECT_EQ(time_value.Packed64TimeMicros(),
                TimeValue::FromHMSAndMicros(0, 0, 0, 0).Packed64TimeMicros());
    }
  }

  // One additional test for an empty timestamp string, where the format
  // string ends in '%' (which is invalid).
  for (const absl::string_view& time : times) {
    EXPECT_THAT(
        ParseStringToTime("  %", time, kMicroseconds, &time_value),
        StatusIs(zetasql_base::OUT_OF_RANGE,
                 HasSubstr("Format string cannot end with a single '%'")));
  }

  // Tests with a significant leading format element, and trailing whitespace.
  format0 = absl::string_view("%M");
  format1 = absl::string_view("%M ");
  format2 = absl::string_view("%M    ");
  format3 = absl::string_view("%M%n");
  format4 = absl::string_view("%M %n");
  format5 = absl::string_view("%M %n ");
  format6 = absl::string_view("%M%n ");
  format7 = absl::string_view("%M%t");
  format8 = absl::string_view("%M %t");
  format9 = absl::string_view("%M %t ");
  format10 = absl::string_view("%M%t ");
  format11 = absl::string_view("%M %n   %t %n   ");
  formats = {format0, format1, format2, format3, format4, format5, format6,
             format7, format8, format9, format10, format11};

  time1 = absl::string_view("26");
  time2 = absl::string_view("26 ");
  time3 = absl::string_view("26\n");
  time4 = absl::string_view("26 \t");
  time5 = absl::string_view("26\v ");
  time6 = absl::string_view("26 \f ");
  time7 = absl::string_view("26 \r ");
  time8 = absl::string_view("26 \t\v\n\f\r ");
  times = {time1, time2, time3, time4, time5, time6, time7, time8};


  for (const absl::string_view& format : formats) {
    for (const absl::string_view& time : times) {
      ZETASQL_EXPECT_OK(ParseStringToTime(format, time, kMicroseconds, &time_value));
      EXPECT_EQ(time_value.Packed64TimeMicros(),
                TimeValue::FromHMSAndMicros(0, 26, 0, 0).Packed64TimeMicros());
    }
  }

  // Tests with leading whitespace and trailing whitespace and a significant
  // format element in the middle.
  format0 = absl::string_view("%M");
  format1 = absl::string_view(" %M ");
  format2 = absl::string_view("   %M   ");
  format3 = absl::string_view("%n%M%n");
  format4 = absl::string_view(" %n%M %n");
  format5 = absl::string_view(" %n %M %n ");
  format6 = absl::string_view("%n %M%n ");
  format7 = absl::string_view("%t%M%t");
  format8 = absl::string_view(" %t%M %t");
  format9 = absl::string_view(" %t %M %t ");
  format10 = absl::string_view("%t %M%t ");
  format11 = absl::string_view(" %n   %t %n   %M %n   %t %n   ");
  formats = {format0, format1, format2, format3, format4, format5, format6,
             format7, format8, format9, format10, format11};

  time1 = absl::string_view("26");
  time2 = absl::string_view(" 26 ");
  time3 = absl::string_view("\n26\n");
  time4 = absl::string_view(" \t26 \t");
  time5 = absl::string_view("\v 26\v ");
  time6 = absl::string_view(" \f 26 \f ");
  time7 = absl::string_view("\r26\r");
  time8 = absl::string_view("\n \r \v\t\f26 \t\n\v\f\r   ");
  times = {time1, time2, time3, time4, time5, time6, time7, time8};

  for (const absl::string_view& format : formats) {
    for (const absl::string_view& time : times) {
      ZETASQL_EXPECT_OK(ParseStringToTime(format, time, kMicroseconds, &time_value));
      EXPECT_EQ(time_value.Packed64TimeMicros(),
                TimeValue::FromHMSAndMicros(0, 26, 0, 0).Packed64TimeMicros());
    }
  }
}

TEST(StringToTimestampTests, IntermediateWhitespaceTests) {
  // %n and %t both consume arbitrary whitespace
  absl::string_view format1("%M %H");
  absl::string_view format2("%M%n%H");
  absl::string_view format3("%M%t%H");
  absl::string_view format4("%M %t %n %t  %n   %H");
  std::vector<absl::string_view> formats =
      {format1, format2, format3, format4};

  absl::string_view time1("2611");
  absl::string_view time2("26   11");
  absl::string_view time3("26\n11");
  absl::string_view time4("26 \t11");
  absl::string_view time5("26\v 11");
  absl::string_view time6("26 \f 11");
  absl::string_view time7("26   \r   11");
  absl::string_view time8("26 \f   \t\v\n \r  11");
  std::vector<absl::string_view> times =
      {time1, time2, time3, time4, time5, time6, time7, time8};

  TimeValue time_value;
  for (const absl::string_view& format : formats) {
    for (const absl::string_view& time : times) {
      ZETASQL_EXPECT_OK(ParseStringToTime(format, time, kMicroseconds, &time_value))
          << "format: " << format
          << "\ntime: " << time;
      EXPECT_EQ(time_value.Packed64TimeMicros(),
                TimeValue::FromHMSAndMicros(11, 26, 0, 0).Packed64TimeMicros());
    }
  }
}

TEST(StringToTimestampTests,
     SecondsWithMoreThanOneDigitOfFractionalPrecisionTests) {
  // Only %E0S to %E9S is supported (0-9 subseconds digits).
  TimeValue time_value;
  EXPECT_THAT(ParseStringToTime("%E10S", "1", kMicroseconds, &time_value),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       HasSubstr("Failed to parse input string")));
}

}  // namespace
}  // namespace functions
}  // namespace zetasql
