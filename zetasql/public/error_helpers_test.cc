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

#include "zetasql/public/error_helpers.h"

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/enum_utils.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

using ::zetasql::testing::EqualsProto;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

TEST(ErrorHelpersTest, FormatErrorLocation) {
  ErrorLocation location;
  EXPECT_EQ("1:1", FormatErrorLocation(location));

  location.set_line(4);
  location.set_column(15);
  EXPECT_EQ("4:15", FormatErrorLocation(location));

  location.set_filename("filename");
  EXPECT_EQ("filename:4:15", FormatErrorLocation(location));

  location.set_line(0);
  location.set_column(-4);
  EXPECT_EQ("filename:0:-4", FormatErrorLocation(location));
}

TEST(ErrorHelpersTest, FormatError) {
  // Dummy query for use in these tests. The important part is that the query
  // has the offsets referenced in the ParseLocationPoints.
  const std::string dummy_query =
      "1234567890123456789_1\n"
      "1234567890123456789_2\n"
      "1234567890123456789_3\n"
      "1234567890123456789_4\n";

  const absl::Status ok;
  EXPECT_EQ("OK", FormatError(ok));
  const absl::Status status1 = MakeSqlError() << "Message1";
  EXPECT_EQ("generic::invalid_argument: Message1",
            internal::StatusToString(status1));
  EXPECT_EQ("Message1", FormatError(status1));

  absl::Status status2 =
      MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(74)) << "Message2";
  status2 = ConvertInternalErrorLocationToExternal(status2, dummy_query);
  EXPECT_EQ(
      "generic::invalid_argument: Message2 "
      "[zetasql.ErrorLocation] { line: 4 column: 9 }",
      internal::StatusToString(status2));
  EXPECT_EQ("Message2 [at 4:9]", FormatError(status2));

  // Error with code other than INVALID_ARGUMENT.
  const absl::Status status3 = ::zetasql_base::UnknownErrorBuilder() << "Message3";
  EXPECT_EQ("generic::unknown: Message3", internal::StatusToString(status3));
  EXPECT_EQ("generic::unknown: Message3", FormatError(status3));

  // Error with a zetasql payload but the wrong code.
  absl::Status status4 =
      ::zetasql_base::UnknownErrorBuilder().Attach(
          ParseLocationPoint::FromByteOffset(1).ToInternalErrorLocation())
      << "Message4";
  status4 = ConvertInternalErrorLocationToExternal(status4, dummy_query);
  EXPECT_EQ(
      "generic::unknown: Message4 "
      "[zetasql.ErrorLocation] { line: 1 column: 2 }",
      internal::StatusToString(status4));
  EXPECT_EQ(internal::StatusToString(status4), FormatError(status4));

  zetasql_test__::TestStatusPayload extra_extension;
  extra_extension.set_value("abc");

  // Error with a non-ErrorLocation payload.
  absl::Status status5 = ::zetasql_base::UnknownErrorBuilder().Attach(extra_extension)
                         << "Message5";
  EXPECT_EQ(
      "generic::unknown: Message5 "
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }",
      internal::StatusToString(status5));
  EXPECT_EQ(internal::StatusToString(status5), FormatError(status5));

  // Error with both ErrorLocation and non-ErrorLocation payloads.
  // The ErrorLocation gets stripped by FormatError, but the other payload
  // still prints.
  absl::Status status6 =
      MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(43))
          .Attach(extra_extension)
      << "Message6";
  status6 = ConvertInternalErrorLocationToExternal(status6, dummy_query);
  EXPECT_THAT(
      internal::StatusToString(status6),
      AllOf(
          HasSubstr("generic::invalid_argument: Message6"),
          HasSubstr("[zetasql.ErrorLocation] { line: 2 column: 22 }"),
          HasSubstr("[zetasql_test__.TestStatusPayload] { value: \"abc\" }")));
  EXPECT_EQ(
      "Message6 [at 2:22] "
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }",
      FormatError(status6));
}

TEST(ErrorHelpersTest, ErrorLocationHelpers) {
  ErrorLocation location;

  absl::Status status1 = MakeSqlError() << "Message1";
  EXPECT_EQ("generic::invalid_argument: Message1",
            internal::StatusToString(status1));
  EXPECT_FALSE(HasErrorLocation(status1));
  EXPECT_FALSE(GetErrorLocation(status1, &location));
  ClearErrorLocation(&status1);
  EXPECT_EQ("generic::invalid_argument: Message1",
            internal::StatusToString(status1));

  absl::Status status2 =
      ::zetasql_base::UnknownErrorBuilder().Attach(
          ParseLocationPoint::FromByteOffset(1).ToInternalErrorLocation())
      << "Message2";
  status2 = ConvertInternalErrorLocationToExternal(status2, "123\n456");
  EXPECT_EQ(
      "generic::unknown: Message2 "
      "[zetasql.ErrorLocation] { line: 1 column: 2 }",
      internal::StatusToString(status2));
  EXPECT_TRUE(HasErrorLocation(status2));
  EXPECT_TRUE(GetErrorLocation(status2, &location));
  EXPECT_THAT(location, EqualsProto("line: 1 column: 2"));
  ClearErrorLocation(&status2);
  EXPECT_EQ("generic::unknown: Message2", internal::StatusToString(status2));
  // No payload, not an empty payload.
  EXPECT_FALSE(internal::HasPayload(status2));
  EXPECT_FALSE(HasErrorLocation(status2));
  ClearErrorLocation(&status2);

  zetasql_test__::TestStatusPayload extra_extension;

  extra_extension.set_value("abc");

  // For a status with both an ErrorLocation and another payload, Clear
  // will just remove the ErrorLocation.
  absl::Status status3 =
      MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(11))
          .Attach(extra_extension)
      << "Message3";
  status3 = ConvertInternalErrorLocationToExternal(status3, "123\n456\n78901");
  std::string status3_str = internal::StatusToString(status3);

  EXPECT_THAT(
      internal::StatusToString(status3),
      AllOf(
          HasSubstr("generic::invalid_argument: Message3"),
          HasSubstr("[zetasql.ErrorLocation] { line: 3 column: 4 }"),
          HasSubstr("[zetasql_test__.TestStatusPayload] { value: \"abc\" }")));

  EXPECT_TRUE(HasErrorLocation(status3));
  EXPECT_TRUE(GetErrorLocation(status3, &location));
  EXPECT_THAT(location, EqualsProto("line: 3 column: 4"));
  ClearErrorLocation(&status3);
  EXPECT_EQ(
      "generic::invalid_argument: Message3 "
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }",
      internal::StatusToString(status3));
  EXPECT_FALSE(HasErrorLocation(status3));
  ClearErrorLocation(&status3);
}

static void TestGetCaret(const std::string& query,
                         const ErrorLocation& location,
                         const std::string& expected_output) {
  EXPECT_EQ(expected_output, GetErrorStringWithCaret(query, location));
}

static ErrorLocation MakeErrorLocation(int line, int column) {
  ZETASQL_DCHECK_GE(line, 1) << "with line = " << line << ", column = " << column;
  ZETASQL_DCHECK_GE(column, 1) << "with line = " << line << ", column = " << column;

  ErrorLocation location;
  location.set_line(line);
  location.set_column(column);
  return location;
}

TEST(ErrorHelpersTest, GetErrorStringWithCaret) {
  const std::string str1 =
      "abc\n"
      "def\n"
      "ghi";
  TestGetCaret(str1, MakeErrorLocation(1, 1),
               "abc\n"
               "^");
  TestGetCaret(str1, MakeErrorLocation(1, 3),
               "abc\n"
               "  ^");
  TestGetCaret(str1, MakeErrorLocation(2, 2),
               "def\n"
               " ^");
  TestGetCaret(str1, MakeErrorLocation(2, 4),  // One off end of line is okay.
               "def\n"
               "   ^");
  TestGetCaret(str1, MakeErrorLocation(3, 3),
               "ghi\n"
               "  ^");
  TestGetCaret(str1, MakeErrorLocation(3, 4),  // One off end of string is okay.
               "ghi\n"
               "   ^");

  // Test with tabs.  Tabs are expanded to eight spaces.
  TestGetCaret("abc\ndef\tghi\njkl", MakeErrorLocation(2, 10),
               "def     ghi\n"
               "         ^");
  TestGetCaret("1234567\txxx", MakeErrorLocation(1, 10),
               "1234567 xxx\n"
               "         ^");
  TestGetCaret("12345678\txxx", MakeErrorLocation(1, 18),
               "12345678        xxx\n"
               "                 ^");
  TestGetCaret("\t\txxx\tyyy", MakeErrorLocation(1, 18),
               "                xxx     yyy\n"
               "                 ^");

  // Test with \r in the string.
  TestGetCaret("ab\rcd\ref", MakeErrorLocation(2, 1),
               "cd\n"
               "^");
  // Test with \r\n in the string.
  TestGetCaret("ab\r\ncd\r\nef", MakeErrorLocation(2, 1),
               "cd\n"
               "^");
  // Test with \n\r in the string.  This does not count as a single newline,
  // so it ends up counting as two.
  TestGetCaret("ab\n\rcd\n\ref", MakeErrorLocation(2, 1),
               "\n"
               "^");
  TestGetCaret("ab\n\rcd\n\ref", MakeErrorLocation(3, 1),
               "cd\n"
               "^");

  // Out of bounds locations will hit DCHECKs.
  EXPECT_DEBUG_DEATH(
      GetErrorStringWithCaret(str1, MakeErrorLocation(0, 1)),
      "Check failed: line >= 1");
  EXPECT_DEBUG_DEATH(
      GetErrorStringWithCaret(str1, MakeErrorLocation(1, 0)),
      "Check failed: column >= 1");
  EXPECT_DEBUG_DEATH(GetErrorStringWithCaret(str1, MakeErrorLocation(4, 1)),
                     "Query had .* lines but line .* was requested");
  EXPECT_DEBUG_DEATH(
      GetErrorStringWithCaret(str1, MakeErrorLocation(1, 5)),
      "Check failed: location.column.. <= truncated_input->size");

  // Now try some where we have to prune a long line.
  // Construct a line with multiple words, where each word starts with
  // its character position (e.g. "45_45678" at column 45).
  std::string line;
  for (int block_size = 5; line.size() < 160; block_size += 3) {
    const std::string block_background = "12345678901234567890";
    std::string block = absl::StrCat(line.size() + 1, "_");
    absl::StrAppend(&block, block_background.substr(block.size()));
    absl::StrAppend(&line, block.substr(0, block_size - 1), " ");
    block_size += 3;
  }
  ZETASQL_LOG(INFO) << "Made line '" << line << "' with length " << line.length();
  const int kMaxWidth = 56;
  std::vector<std::string> outputs;
  for (int i = 1; i < line.length() - 1; i += 7) {
    outputs.push_back(
        GetErrorStringWithCaret(line, MakeErrorLocation(1, i), kMaxWidth));
    ZETASQL_LOG(INFO) << "Error at column " << i << "\n" << outputs.back();

    // Should be less than max_width chars before the newline.
    EXPECT_LE(outputs.back().find('\n'), kMaxWidth);
  }
  outputs.push_back(
      GetErrorStringWithCaret(line,
                              MakeErrorLocation(1, line.length() - 1),
                              kMaxWidth));
  outputs.push_back(
      GetErrorStringWithCaret(line,
                              MakeErrorLocation(1, line.length()), kMaxWidth));

  // These are example outputs from GetErrorStringWithCaret at various
  // positions.  These look like substring of the full string of
  // length <= kMaxWidth that try to start at word boundary.
  // Some don't find an acceptable word boundary and just point at the middle.
  EXPECT_THAT(outputs, ElementsAreArray({
      "1_34 6_34567890 17_4567890123456 34_45678901234567890...\n"
      "^",
      "1_34 6_34567890 17_4567890123456 34_45678901234567890...\n"
      "       ^",
      "1_34 6_34567890 17_4567890123456 34_45678901234567890...\n"
      "              ^",
      "1_34 6_34567890 17_4567890123456 34_45678901234567890...\n"
      "                     ^",
      "1_34 6_34567890 17_4567890123456 34_45678901234567890...\n"
      "                            ^",
      "1_34 6_34567890 17_4567890123456 34_45678901234567890...\n"
      "                                   ^",
      "...17_4567890123456 34_45678901234567890 55_456789012...\n"
      "                             ^",
      "...17_4567890123456 34_45678901234567890 55_456789012...\n"
      "                                    ^",
      "...34_45678901234567890 55_45678901234567890 76_45678...\n"
      "                          ^",
      "...34_45678901234567890 55_45678901234567890 76_45678...\n"
      "                                 ^",
      "...01234567890 55_45678901234567890 76_45678901234567...\n"
      "                               ^",
      "...55_45678901234567890 76_45678901234567890 97_45678...\n"
      "                          ^",
      "...55_45678901234567890 76_45678901234567890 97_45678...\n"
      "                                 ^",
      "...01234567890 76_45678901234567890 97_45678901234567...\n"
      "                               ^",
      "...76_45678901234567890 97_45678901234567890 118_5678...\n"
      "                          ^",
      "...76_45678901234567890 97_45678901234567890 118_5678...\n"
      "                                 ^",
      "...01234567890 97_45678901234567890 118_5678901234567...\n"
      "                               ^",
      "...97_45678901234567890 118_5678901234567890 139_5678...\n"
      "                          ^",
      "...97_45678901234567890 118_5678901234567890 139_5678...\n"
      "                                 ^",
      "...01234567890 118_5678901234567890 139_5678901234567...\n"
      "                               ^",
      "...118_5678901234567890 139_5678901234567890 160_5678...\n"
      "                          ^",
      "...118_5678901234567890 139_5678901234567890 160_5678...\n"
      "                                 ^",
      "...01234567890 139_5678901234567890 160_5678901234567...\n"
      "                               ^",
      "...139_5678901234567890 160_5678901234567890 \n"
      "                          ^",
      "...139_5678901234567890 160_5678901234567890 \n"
      "                                 ^",
      "...01234567890 160_5678901234567890 \n"
      "                               ^",
      "...160_5678901234567890 \n"
      "                      ^",
      "...160_5678901234567890 \n"
      "                       ^"
  }));

  // Here's one where we don't have any word boundaries, so we just
  // point at the middle.
  EXPECT_EQ(
      "...901234567890123456789012...\n"
      "                  ^",
      GetErrorStringWithCaret(
          "01234567890123456789012345678901234567890123456789"
          "01234567890123456789012345678901234567890123456789",
          MakeErrorLocation(1, 35), 30 /* max_width */));

  // Here's one where we avoid slicing a UTF8 codepoint when we do max width
  // truncation. In this example we choose to print a 29 byte error string
  // rather than slicing the 2-byte UTF8 codepoint in half.
  EXPECT_EQ("01234567890123456789012345...\n^",
            GetErrorStringWithCaret("01234567890123456789012345"
                                    "\xc3\xb0"
                                    "67890",
                                    MakeErrorLocation(1, 1),
                                    /*max_width_in=*/30));
  // Repeat the above test but this time give enough room for the whole
  // codepoint.
  EXPECT_EQ("01234567890123456789012345\xc3\xb0...\n^",
            GetErrorStringWithCaret("01234567890123456789012345"
                                    "\xc3\xb0"
                                    "67890",
                                    MakeErrorLocation(1, 1),
                                    /*max_width_in=*/31));
}

TEST(ErrorHelpersTest, GetErrorStringWithCaret_WeirdData) {
  // 30 characters of garbage data.
  std::string test_string1 =
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80";
  EXPECT_EQ(30, test_string1.size());
  EXPECT_EQ(
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\n^",
      GetErrorStringWithCaret(test_string1, MakeErrorLocation(1, 1),
                              /*max_width_in=*/31));

  // 32 characters of garbage data.
  std::string test_string2 =
      "\xfe\xff\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80";
  EXPECT_EQ(32, test_string2.size());
  EXPECT_EQ(
      "\xfe\xff\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80"
      "...\n^",
      GetErrorStringWithCaret(test_string2, MakeErrorLocation(1, 1),
                              /*max_width_in=*/31));
  // 30 characters of garbage data, then half a code point.
  std::string test_string3 =
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\xc3\xb0";
  EXPECT_EQ(test_string3.size(), 32);
  EXPECT_EQ(
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80"
      "...\n^",
      GetErrorStringWithCaret(test_string3, MakeErrorLocation(1, 1),
                              /*max_width_in=*/31));

  // 30 characters of garbage data, then half a code point, then lots more
  // garbage.
  std::string test_string4 =
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\xc3\xb0"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80";
  EXPECT_EQ(test_string4.size(), 42);
  EXPECT_EQ(
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"
      "\x80\x80\x80\x80\x80\x80\x80\x80"
      "...\n^",
      GetErrorStringWithCaret(test_string3, MakeErrorLocation(1, 1),
                              /*max_width_in=*/31));
}

struct UpdateErrorFromPayloadTestCase {
 public:
  UpdateErrorFromPayloadTestCase(
      const std::string& query_in, const absl::Status& status_in,
      const std::map<ErrorMessageMode, std::string>& expected_results_in)
      : query(query_in),
        status(status_in),
        expected_results(expected_results_in) {}
  ~UpdateErrorFromPayloadTestCase() {}

  std::string query;
  absl::Status status;
  std::map<ErrorMessageMode, std::string> expected_results;
};

// Helper method that runs a list of test cases.  Each test case indicates
// a source query string, a Status, and a map of <ErrorMessageMode, string>.
// Each entry in the map identifies the expected result of calling
// MaybeUpdateErrorFromPayload() for the given mode, followed by FormatError()
// to get a string for the updated Status.
static void RunTests(
    const std::vector<UpdateErrorFromPayloadTestCase> test_cases) {
  for (const UpdateErrorFromPayloadTestCase& test_case : test_cases) {
    for (const auto& map_entry : test_case.expected_results) {
      const ErrorMessageMode mode = map_entry.first;
      const std::string& expected_error_string = map_entry.second;
      absl::Status adjusted_status =
          MaybeUpdateErrorFromPayload(mode, test_case.query, test_case.status);
      const std::string test_string = absl::StrCat(
          "mode: ", ErrorMessageMode_Name(mode),
          "\ninput status: ", internal::StatusToString(test_case.status),
          "\nexpected_error_string: ", expected_error_string,
          "\ninternal::StatusToString(nadjusted_status): ",
          internal::StatusToString(adjusted_status),
          "\nFormatError(adjusted_status): ", FormatError(adjusted_status));

      // The adjusted status must not have an InternalErrorLocation under
      // any mode.
      EXPECT_FALSE(
          internal::HasPayloadWithType<InternalErrorLocation>(adjusted_status))
          << test_string;
      // The adjusted status should match the expected status string.
      EXPECT_EQ(FormatError(adjusted_status), expected_error_string)
          << test_string;

      if (!internal::HasPayload(test_case.status)) {
        EXPECT_FALSE(internal::HasPayload(adjusted_status)) << test_string;
      } else {
        // If the original status has a payload and the mode is
        // ERROR_MESSAGE_WITH_PAYLOAD, so the adjusted status will
        // have also have similar payloads.
        if (mode == ERROR_MESSAGE_WITH_PAYLOAD) {
          ASSERT_TRUE(internal::HasPayload(adjusted_status));
          EXPECT_EQ(
              internal::HasPayloadWithType<ErrorLocation>(test_case.status),
              internal::HasPayloadWithType<ErrorLocation>(adjusted_status))
              << test_string;
        } else {
          // The original status had an ErrorLocation payload and/or another
          // payload.
          if (!internal::HasPayload(adjusted_status)) {
            continue;
          }
          // The mode is not ERROR_MESSAGE_WITH_PAYLOAD, so the adjusted
          // status must not have an ErrorLocation.
          EXPECT_FALSE(
              internal::HasPayloadWithType<ErrorLocation>(adjusted_status));
        }
      }
    }
  }
}

// Tests for MaybeUpdateErrorFromPayload, for an ErrorLocation payload
TEST(ErrorHelpersTest, UpdateErrorFromErrorLocationPayloadTests) {
  // Dummy query for use in these tests. The important bit is that the query
  // has the offsets referenced in the ParseLocationPoints.
  const std::string dummy_query =
      "1234567890123456789_1\n"
      "1234567890123456789_2\n"
      "1234567890123456789_3\n"
      "1234567890123456789_4\n";

  const absl::Status ok;
  for (const ErrorMessageMode mode :
           zetasql_base::EnumerateEnumValues<ErrorMessageMode>()) {
    ZETASQL_EXPECT_OK(MaybeUpdateErrorFromPayload(
        static_cast<ErrorMessageMode>(mode), dummy_query, ok));
  }
  std::vector<UpdateErrorFromPayloadTestCase> test_cases;

  // Basic error status, with no ErrorLocation.
  // MaybeUpdateErrorFromPayload() has no effect.
  const absl::Status status1 = MakeSqlError() << "Message1";
  const std::string expected_string1 = "Message1";
  EXPECT_EQ(expected_string1, FormatError(status1));
  std::map<ErrorMessageMode, std::string> expected_result;
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_string1);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_string1);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_string1);
  test_cases.emplace_back(dummy_query, status1, expected_result);

  // Status with InternalErrorLocation is not allowed in this API, resulting
  // in ZETASQL_RET_CHECK.
  absl::Status status2 =
      MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(74)) << "Message2";
  EXPECT_EQ("Message2 [zetasql.InternalErrorLocation] { byte_offset: 74 }",
            FormatError(status2));
  // InternalErrorLocations are not allowed, resulting in RET_CHECK
  // regardless of <mode>.
  for (const ErrorMessageMode mode :
           zetasql_base::EnumerateEnumValues<ErrorMessageMode>()) {
    EXPECT_THAT(
        MaybeUpdateErrorFromPayload(mode, dummy_query, status2),
        StatusIs(absl::StatusCode::kInternal,
                 HasSubstr("Status must not have InternalErrorLocation")));
  }

  // Convert the status InternalErrorLocation to ErrorLocation for this test.
  status2 = ConvertInternalErrorLocationToExternal(status2, dummy_query);
  EXPECT_EQ("Message2 [at 4:9]",
            FormatError(status2));
  const std::string expected_string2 = "Message2 [at 4:9]";
  const std::string expected_caret_string2 =
      "Message2 [at 4:9]\n1234567890123456789_4\n        ^";
  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_string2);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_string2);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string2);
  test_cases.emplace_back(dummy_query, status2, expected_result);

  // Error with a zetasql payload but different/invalid code.
  // MaybeUpdateErrorFromPayload() doesn't consider the
  // error code.
  absl::Status status3 =
      ::zetasql_base::UnknownErrorBuilder().Attach(
          ParseLocationPoint::FromByteOffset(3).ToInternalErrorLocation())
      << "Message3";
  EXPECT_EQ("generic::unknown: Message3 "
            "[zetasql.InternalErrorLocation] { byte_offset: 3 }",
            FormatError(status3));
  status3 = ConvertInternalErrorLocationToExternal(status3, dummy_query);
  EXPECT_EQ("generic::unknown: Message3 [zetasql.ErrorLocation] "
            "{ line: 1 column: 4 }",
            FormatError(status3));

  // The byte_offset is 0-based and the column is 1-based, so byte_offset 3
  // is line 1, column 4.
  const std::string expected_payload_string3 =
      "generic::unknown: Message3 [zetasql.ErrorLocation] "
      "{ line: 1 column: 4 }";
  const std::string expected_oneline_string3 =
      "generic::unknown: Message3 [at 1:4]";
  const std::string expected_caret_string3 =
      "generic::unknown: Message3 [at 1:4]\n1234567890123456789_1\n   ^";
  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string3);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string3);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string3);
  test_cases.emplace_back(dummy_query, status3, expected_result);

  // Error with a non-ErrorLocation payload.  Since no location,
  // MaybeUpdateErrorFromPayload() has no affect.
  zetasql_test__::TestStatusPayload extra_extension;
  extra_extension.set_value("abc");
  const absl::Status status4 =
      ::zetasql_base::UnknownErrorBuilder().Attach(extra_extension) << "Message4";
  const std::string expected_string4 = FormatError(status4);
  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_string4);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_string4);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_string4);
  test_cases.emplace_back(dummy_query, status4, expected_result);

  // Status with both an ErrorLocation and another payload.
  absl::Status status5 =
      MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(43))
          .Attach(extra_extension)
      << "Message5";
  EXPECT_THAT(
      FormatError(status5),
      AllOf(
          HasSubstr("Message5"),
          HasSubstr("[zetasql.InternalErrorLocation] { byte_offset: 43 }"),
          HasSubstr("[zetasql_test__.TestStatusPayload] { value: \"abc\" }")));

  status5 = ConvertInternalErrorLocationToExternal(status5, dummy_query);
  EXPECT_EQ(
      "Message5 [at 2:22] "
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }",
      FormatError(status5));

  const std::string expected_payload_string5 =
      "Message5 [at 2:22] [zetasql_test__.TestStatusPayload] "
      "{ value: \"abc\" }";
  const std::string expected_oneline_string5 =
      "Message5 [at 2:22] [zetasql_test__.TestStatusPayload] "
      "{ value: \"abc\" }";
  const std::string expected_caret_string5 =
      "Message5 [at 2:22]\n1234567890123456789_2\n                     ^\n"
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }";
  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string5);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string5);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string5);
  test_cases.emplace_back(dummy_query, status5, expected_result);

  // A status that already has an external error location.
  // MaybeUpdateErrorFromPayload() does not update the location.
  absl::Status status6 =
      MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(43))
          .Attach(extra_extension)
      << "Message6";
  status6 = ConvertInternalErrorLocationToExternal(status6, dummy_query);
  const std::string expected_payload_string6 =
      "Message6 [at 2:22] [zetasql_test__.TestStatusPayload] "
      "{ value: \"abc\" }";
  const std::string expected_oneline_string6 =
      "Message6 [at 2:22] [zetasql_test__.TestStatusPayload] "
      "{ value: \"abc\" }";
  const std::string expected_caret_string6 =
      "Message6 [at 2:22]\n1234567890123456789_2\n                     ^\n"
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }";
  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string6);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string6);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string6);
  test_cases.emplace_back(dummy_query, status6, expected_result);

  RunTests(test_cases);
}

TEST(ErrorHelpersTest, BasicErrorSourcePayloadTests) {
  // Basic tests for a Status that does not include an ErrorSource payload.
  ErrorLocation location;
  location.set_filename("location_file");
  location.set_line(2);
  location.set_column(2);

  const std::string source_error_message = "source_error_message";
  const std::string source_caret_string = "caret string 2\n      ^";
  ErrorLocation source_error_location;
  source_error_location.set_filename("source_file");
  source_error_location.set_line(1);
  source_error_location.set_column(7);
  const std::string source_location_string = "[at source_file:1:7]";

  // Basic tests for a Status that includes an ErrorSource payload.
  ErrorSource error_source;
  error_source.set_error_message(source_error_message);
  error_source.set_error_message_caret_string(source_caret_string);
  *error_source.mutable_error_location() = source_error_location;
  *location.add_error_source() = error_source;
  absl::Status status = MakeSqlError().Attach(location) << "Message2";

  error_source.Clear();
  EXPECT_FALSE(error_source.has_error_message());
  EXPECT_FALSE(error_source.has_error_message_caret_string());
  EXPECT_FALSE(error_source.has_error_location());

  ErrorLocation error_location;
  EXPECT_TRUE(HasErrorLocation(status));
  EXPECT_TRUE(GetErrorLocation(status, &error_location));

  ASSERT_EQ(1, error_location.error_source_size());
  error_source = error_location.error_source(0);

  ASSERT_TRUE(error_source.has_error_message());
  EXPECT_EQ(error_source.error_message(), source_error_message);
  ASSERT_TRUE(error_source.has_error_message_caret_string());
  EXPECT_EQ(error_source.error_message_caret_string(), source_caret_string);
  ASSERT_TRUE(error_source.has_error_location());
  EXPECT_EQ(FormatErrorLocation(error_source.error_location()),
            FormatErrorLocation(source_error_location));
}

// Tests for MaybeUpdateErrorFromPayload, for an ErrorSource payload
TEST(ErrorHelpersTest, UpdateErrorFromErrorSourcePayloadTests) {
  const std::string dummy_query =
      "1234567890123456789_1\n"
      "1234567890123456789_2\n"
      "1234567890123456789_3\n"
      "1234567890123456789_4\n";
  const std::string source_dummy_query = "abcdefghijklmnopqrs";

  std::vector<UpdateErrorFromPayloadTestCase> test_cases;

  // Basic error status, with no ErrorSource.
  // MaybeUpdateErrorFromPayload() has no effect.
  absl::Status status = MakeSqlError() << "Message1";
  std::string expected_oneline_string = "Message1";
  // These tests use FormatError() to compare the expected string to
  // the updated status.  If the mode is ERROR_MESSAGE_MODE_WITH_PAYLOAD,
  // the result of FormatError() for a Status with payload is the
  // same as FormatError() on a Status updated for ERROR_MESSAGE_MODE_ONE_LINE
  // mode.
  std::string expected_payload_string = expected_oneline_string;
  std::string expected_caret_string = "Message1";
  EXPECT_EQ(expected_payload_string, FormatError(status));

  std::map<ErrorMessageMode, std::string> expected_result;
  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  // Basic error status with ErrorLocation, but no ErrorSource in ErrorLocation
  ErrorLocation location;
  location.set_filename("location_file");
  location.set_line(2);
  location.set_column(2);
  status = MakeSqlError().Attach(location) << "Message1b";
  expected_oneline_string = "Message1b [at location_file:2:2]";
  expected_payload_string = expected_oneline_string;
  expected_caret_string =
      "Message1b [at location_file:2:2]\n"
      "1234567890123456789_2\n"
      " ^";
  EXPECT_EQ(expected_oneline_string, FormatError(status));

  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  // Status with simple ErrorSource and only <error_message>.
  ErrorSource error_source;
  error_source.set_error_message("error_source_message");
  location.clear_error_source();
  *location.add_error_source() = error_source;
  status = MakeSqlError().Attach(location) << "Message2";
  expected_oneline_string =
      "Message2 [at location_file:2:2]; error_source_message";
  expected_payload_string = expected_oneline_string;
  expected_caret_string =
      "Message2 [at location_file:2:2]\n"
      "1234567890123456789_2\n"
      " ^\n"
      "error_source_message";
  EXPECT_EQ(expected_oneline_string, FormatError(status));

  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  // Status with simple ErrorSource and only <error_message_caret_string>.
  error_source.Clear();
  error_source.set_error_message_caret_string("caret_string");
  location.clear_error_source();
  *location.add_error_source() = error_source;
  status = MakeSqlError().Attach(location) << "Message3";
  expected_oneline_string = "Message3 [at location_file:2:2]";
  expected_payload_string = expected_oneline_string;
  expected_caret_string =
      "Message3 [at location_file:2:2]\n"
      "1234567890123456789_2\n"
      " ^\n"
      "caret_string";
  EXPECT_EQ(expected_oneline_string, FormatError(status));

  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  // Status with simple ErrorSource and only <error_location>.  Normally, an
  // <error_message> and <error_message_caret_string> would also be present.
  // he <error_location> is not included if there is no error message, since
  // since it only makes sense in the context of an error message.
  error_source.Clear();
  ErrorLocation error_location;
  error_location.set_line(1);
  error_location.set_column(7);
  *error_source.mutable_error_location() = error_location;
  location.clear_error_source();
  *location.add_error_source() = error_source;
  status = MakeSqlError().Attach(location) << "Message4";
  expected_oneline_string = "Message4 [at location_file:2:2]";
  expected_payload_string = expected_oneline_string;
  expected_caret_string =
      "Message4 [at location_file:2:2]\n"
      "1234567890123456789_2\n"
      " ^";
  EXPECT_EQ(expected_oneline_string, FormatError(status));

  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  // Status with simple ErrorSource, with <error_message_caret_string> and
  // <error_location>.  For ONE_LINE mode, <error_message_caret_string> and
  // <error_location> are ignored.  For CARET mode, the <error_location>
  // is ignored.
  error_location.set_filename("error_filename");
  error_location.set_line(1);
  error_location.set_column(7);
  *error_source.mutable_error_location() = error_location;
  error_source.set_error_message_caret_string("abcdefghijklmnopqrs\n      ^");
  location.clear_error_source();
  *location.add_error_source() = error_source;
  status = MakeSqlError().Attach(location) << "Message5";
  expected_oneline_string = "Message5 [at location_file:2:2]";
  expected_payload_string = expected_oneline_string;
  expected_caret_string =
      "Message5 [at location_file:2:2]\n"
      "1234567890123456789_2\n"
      " ^\n"
      "abcdefghijklmnopqrs\n"
      "      ^";
  EXPECT_EQ(expected_oneline_string, FormatError(status));

  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  // This is the normal/expected case, where the Status has a simple
  // ErrorSource with all of <error_message>, <error_message_with_caret_string>,
  // and <error_location>.
  //
  // 1) ERROR_MESSAGE_WITH_PAYLOAD - The error payload is left alone on the
  //    status.
  // 2) ERROR_MESSAGE_ONE_LINE - For a status with an ErrorLocation payload,
  //    the error location gets appended to the error message, i.e.,
  //    "<error message> [at 1:4]".  The location is relative to the
  //    query/statement string that provided for resolution.  Nested errors
  //    are appended with their own error location.
  // 3) ERROR_MESSAGE_MULTI_LINE_WITH_CARET - The nested error message is
  //    provided along with the source statement and caret line indicating
  //    the error location.
  error_source.set_error_message("Nested message 6");
  location.clear_error_source();
  *location.add_error_source() = error_source;
  status = MakeSqlError().Attach(location) << "Message6";
  expected_oneline_string =
      "Message6 [at location_file:2:2]; "
      "Nested message 6 [at error_filename:1:7]";
  expected_payload_string = expected_oneline_string;
  expected_caret_string =
      "Message6 [at location_file:2:2]\n"
      "1234567890123456789_2\n"
      " ^\n"
      "Nested message 6 [at error_filename:1:7]\n"
      "abcdefghijklmnopqrs\n"
      "      ^";
  EXPECT_EQ(expected_oneline_string, FormatError(status));

  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  // Status with an ErrorLocation with an ErrorSource, and another payload.
  error_source.set_error_message("Nested message 7");
  zetasql_test__::TestStatusPayload extra_extension;
  extra_extension.set_value("abc");
  location.clear_error_source();
  *location.add_error_source() = error_source;
  status = MakeSqlError().Attach(location).Attach(extra_extension)
      << "Message7";

  expected_oneline_string =
      "Message7 [at location_file:2:2]; "
      "Nested message 7 [at error_filename:1:7] "
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }";

  expected_payload_string = expected_oneline_string;

  expected_caret_string = R"(Message7 [at location_file:2:2]
1234567890123456789_2
 ^
Nested message 7 [at error_filename:1:7]
abcdefghijklmnopqrs
      ^
[zetasql_test__.TestStatusPayload] { value: "abc" })";

  EXPECT_EQ(expected_oneline_string, FormatError(status));

  expected_result.clear();
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  RunTests(test_cases);
}

// Tests for MaybeUpdateErrorFromPayload, for a nested ErrorSource payload
TEST(ErrorHelpersTest, UpdateErrorFromNestedErrorSourcePayloadTests) {
  const std::string dummy_query =
      "1234567890123456789_1\n"
      "1234567890123456789_2\n"
      "1234567890123456789_3\n"
      "1234567890123456789_4\n";
  const std::string caret_string = "abcdefghijklmnopqrs\n  ^";
  const std::string nested_caret_string = "123456\n    ^";

  std::vector<UpdateErrorFromPayloadTestCase> test_cases;

  ErrorSource error_source;
  ErrorLocation error_location;
  error_location.set_filename("filename");
  error_location.set_line(1);
  error_location.set_column(3);
  ErrorSource nested_error_source;
  ErrorLocation nested_error_location;
  nested_error_location.set_filename("nested_filename");
  nested_error_location.set_line(1);
  nested_error_location.set_column(5);

  // Status with an ErrorSource, an ErrorLocation, and another payload.
  nested_error_source.set_error_message("nested_source_error_message");
  nested_error_source.set_error_message_caret_string(nested_caret_string);
  *nested_error_source.mutable_error_location() = nested_error_location;

  error_source.set_error_message("source_error_message");
  error_source.set_error_message_caret_string(caret_string);
  *error_source.mutable_error_location() = error_location;

  InternalErrorLocation location;
  location.set_byte_offset(43);
  // Nested errors are first in the ErrorSource repeated list.
  *location.add_error_source() = nested_error_source;
  *location.add_error_source() = error_source;

  zetasql_test__::TestStatusPayload extra_extension;
  extra_extension.set_value("abc");

  absl::Status status =
      MakeSqlError().Attach(location).Attach(extra_extension)
      << "ErrorMessage";
  std::string error_str = FormatError(status);
  EXPECT_THAT(
      FormatError(status),
      AllOf(
          HasSubstr("ErrorMessage"),
          HasSubstr("[zetasql_test__.TestStatusPayload] { value: \"abc\" }"),
          HasSubstr("[zetasql.InternalErrorLocation] { "
                    "byte_offset: 43 "
                    "error_source { "
                    "error_message: \"nested_source_error_message\" "
                    "error_message_caret_string: \"123456\\n    ^\" "
                    "error_location { "
                    "line: 1 column: 5 filename: \"nested_filename\" } } "
                    "error_source { "
                    "error_message: \"source_error_message\" "
                    "error_message_caret_string: \"abcdefghijklmnopqrs\\n  ^\" "
                    "error_location { "
                    "line: 1 column: 3 filename: \"filename\" } } }")));
  status = ConvertInternalErrorLocationToExternal(status, dummy_query);

  const std::string expected_oneline_string =
      "ErrorMessage [at 2:22]; "
      "source_error_message [at filename:1:3]; "
      "nested_source_error_message [at nested_filename:1:5] "
      "[zetasql_test__.TestStatusPayload] { value: \"abc\" }";

  const std::string expected_payload_string = expected_oneline_string;

  const std::string expected_caret_string = R"(ErrorMessage [at 2:22]
1234567890123456789_2
                     ^
source_error_message [at filename:1:3]
abcdefghijklmnopqrs
  ^
nested_source_error_message [at nested_filename:1:5]
123456
    ^
[zetasql_test__.TestStatusPayload] { value: "abc" })";

  EXPECT_EQ(expected_oneline_string, FormatError(status));

  std::map<ErrorMessageMode, std::string> expected_result;
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_WITH_PAYLOAD,
                          expected_payload_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_ONE_LINE,
                          expected_oneline_string);
  zetasql_base::InsertIfNotPresent(&expected_result, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
                          expected_caret_string);
  test_cases.emplace_back(dummy_query, status, expected_result);

  RunTests(test_cases);
}

TEST(ErrorHelpersTest, UpdateErrorLocationPayloadWithFilenameIfNotPresentTests)
{
  const std::string dummy_query =
      "1234567890123456789_1\n"
      "1234567890123456789_2\n"
      "1234567890123456789_3\n"
      "1234567890123456789_4\n";
  const std::string caret_string = "abcdefghijklmnopqrs\n  ^";
  const std::string nested_caret_string = "123456\n    ^";

  ErrorLocation error_location;
  error_location.set_filename("filename");
  error_location.set_line(1);
  error_location.set_column(3);

  absl::Status status = MakeSqlError().Attach(error_location) << "ErrorMessage";
  absl::Status updated_status =
      UpdateErrorLocationPayloadWithFilenameIfNotPresent(status,
                                                         "new_filename");
  EXPECT_TRUE(internal::HasPayloadWithType<ErrorLocation>(updated_status));

  ErrorLocation updated_status_location =
      internal::GetPayload<ErrorLocation>(updated_status);

  EXPECT_EQ(error_location.filename(), updated_status_location.filename());

  error_location.clear_filename();
  error_location.set_line(2);
  error_location.set_column(4);

  status = MakeSqlError().Attach(error_location) << "ErrorMessage";
  updated_status =
      UpdateErrorLocationPayloadWithFilenameIfNotPresent(status,
                                                         "new_filename");
  EXPECT_TRUE(internal::HasPayloadWithType<ErrorLocation>(updated_status));

  updated_status_location = internal::GetPayload<ErrorLocation>(updated_status);
  EXPECT_EQ("new_filename", updated_status_location.filename());
}

}  // namespace zetasql
