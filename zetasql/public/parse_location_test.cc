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

#include "zetasql/public/parse_location.h"

#include <memory>
#include <string>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/status.h"

namespace zetasql {

using testing::Eq;
using testing::Not;
using zetasql_base::testing::IsOk;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

TEST(ParseLocationPoint, FromByteOffset) {
  ParseLocationPoint location;
  location = ParseLocationPoint::FromByteOffset(7);
  EXPECT_EQ("7", location.GetString());
}

TEST(ParseLocationPoint, FromByteOffsetWithFilename) {
  ParseLocationPoint location;
  location = ParseLocationPoint::FromByteOffset("filename", 7);
  EXPECT_EQ("filename:7", location.GetString());
}

TEST(ParseLocationRange, SetStartEndBytOffset) {
  ParseLocationRange location;
  location.set_start(ParseLocationPoint::FromByteOffset(7));
  location.set_end(ParseLocationPoint::FromByteOffset(9));
  EXPECT_EQ("7-9", location.GetString());
}

TEST(ParseLocationRange, SetStartEndBytOffsetWithFilename) {
  ParseLocationRange location;
  location.set_start(ParseLocationPoint::FromByteOffset("filename1", 7));
  location.set_end(ParseLocationPoint::FromByteOffset("filename2", 9));
  EXPECT_EQ("filename1:7-filename2:9", location.GetString());
}

// Test cases. Each test case adds a character to the input, and lists the
// expected line and column for that character. For characters that have no
// line and column number, {-1, -1} is used. The last character is not included
// in the input, but it is in this array to define the line and column number
// for the one-past-the-end position.
struct CharacterAndLineAndColumn {
  char character;
  struct LineAndColumn {
    int line;
    int column;
  } line_and_column;
  int round_trip_offset;
};

const std::vector<CharacterAndLineAndColumn> characters_and_line_column_pairs =
    {
        {'a', {1, 1}, 0},
        {'b', {1, 2}, 1},
        {'c', {1, 3}, 2},
        {'\n', {1, 4}, 3},
        {'d', {2, 1}, 4},
        {'e', {2, 2}, 5},
        {'\r', {2, 3}, 6},
        {'\n', {-1, -1}, 6},  // \r\n counts as a single character
        {'\n', {3, 1}, 8},
        {'g', {4, 1}, 9},
        {'h', {4, 2}, 10},
        {'\t', {4, 3}, 11},
        {'i', {4, 9}, 12},  // Tab expands to the next multiple of 8 bytes.
        {'\t', {4, 10}, 13},
        {'\t', {4, 17}, 14},  // Tab starting at multiple of 8 bytes.
        {'j', {4, 25}, 15},
        {'\r', {4, 26}, 16},
        {'k', {5, 1}, 17},
        {'\xc2', {5, 2}, 18},  // Multibyte characters map to just one column.
        {'\xb2', {5, 2}, 18},
        {'\t', {5, 3}, 20},
        {'l', {5, 9}, 21},  // This is correct even with multibyte!
        {'m', {5, 10}, 22},
        {'n', {5, 11}, 23},
        {'o', {5, 12}, 24},
        {'\xc2', {5, 13}, 25},  // Multibyte characters map to just one column
        {'\xb2', {5, 13}, 25},
        {'p', {5, 14}, 27},
        {'q', {5, 15}, 28},
        {'\t', {5, 16}, 29},
        {'r', {5, 17}, 30},
        {'\n', {5, 18}, 31},
        {'\r', {6, 1}, 32},  // \n\r does not count as a single newline.
        {'\xc2', {7, 1}, 33},
        {'\n', {-1, -1}, 34},  // Error because of invalid utf-8
        {'s', {8, 1}, 35},
        {'t', {8, 2}, 36},
        {'\xc2', {8, 3}, 37},
        {'u', {-1, -1}, 38},   // Error because of invalid utf-8
        {'\n', {-1, -1}, 39},  // Error because of invalid utf-8
        {' ', {9, 1}, 40},     // This byte is NOT included in the output.
};

// Returns the concatenated characters from 'characters_and_line_column_pairs';
static std::string GetConcatenatedCharacters() {
  std::string characters;
  for (const auto& test_case : characters_and_line_column_pairs) {
    characters += test_case.character;
  }
  // The last character is not included because it defines a line and column
  // for the one-past-the-end position.
  characters.pop_back();
  return characters;
}

TEST(ParseLocationTranslator, GetLineText) {
  const std::string test_input = GetConcatenatedCharacters();
  ParseLocationTranslator translator(test_input);

  EXPECT_THAT(translator.GetLineText(1), IsOkAndHolds("abc"));
  EXPECT_THAT(translator.GetLineText(2), IsOkAndHolds("de"));
  EXPECT_THAT(translator.GetLineText(3), IsOkAndHolds(""));
  EXPECT_THAT(translator.GetLineText(4), IsOkAndHolds("gh\ti\t\tj"));
  EXPECT_THAT(translator.GetLineText(5),
              IsOkAndHolds("k\xc2\xb2\tlmno\xc2\xb2pq\tr"));
  EXPECT_THAT(translator.GetLineText(6), IsOkAndHolds(""));
}

TEST(ParseLocationTranslator,
     GetByteOffsetFromLineAndColumnWithOutOfBoundsLineNumbers) {
  const std::string test_input = GetConcatenatedCharacters();
  ParseLocationTranslator translator(test_input);

  // Test 0 and negative values for line numbers.
  for (int line = -2; line <= 0; ++line) {
    int col = 1;
    SCOPED_TRACE(absl::StrCat("Testing line ", line, ", column ", col));
    EXPECT_THAT(translator.GetByteOffsetFromLineAndColumn(line, col),
                StatusIs(zetasql_base::StatusCode::kInternal));
  }

  // Test line numbers beyond the end of the input.
  int last_line = characters_and_line_column_pairs.back().line_and_column.line;
  for (int i = 1; i <= 3; ++i) {
    int line = last_line + i;
    int col = 1;
    SCOPED_TRACE(absl::StrCat("Testing line ", line, ", column ", col));
    EXPECT_THAT(translator.GetByteOffsetFromLineAndColumn(line, col),
                StatusIs(zetasql_base::StatusCode::kInternal));
  }
}

TEST(ParseLocationTranslator, EmptyInput) {
  ParseLocationTranslator translator("");
  EXPECT_THAT(translator.GetLineAndColumnAfterTabExpansion(
                  ParseLocationPoint::FromByteOffset(0)),
              IsOkAndHolds(std::make_pair(1, 1)));
  EXPECT_THAT(translator.GetByteOffsetFromLineAndColumn(1, 1),
              IsOkAndHolds(0));
  EXPECT_THAT(translator.GetLineText(1),
              IsOkAndHolds(""));
}

TEST(ParseLocationTranslator, ExpandTabs) {
  // Test with tabs. Tabs are expanded to eight spaces without paying attention
  // to multibyte characters.
  EXPECT_EQ("def     ghi", ParseLocationTranslator::ExpandTabs("def\tghi"));
  EXPECT_EQ("1234567 xxx", ParseLocationTranslator::ExpandTabs("1234567\txxx"));
  EXPECT_EQ("12345678        xxx",
            ParseLocationTranslator::ExpandTabs("12345678\txxx"));
  // This is wrong because \xc2\xb2 is a single multi-byte character. But it
  // matches what JavaCC does for calculating column values, and that's what we
  // have to match.
  EXPECT_EQ("123456\xc2\xb2        xxx",
            ParseLocationTranslator::ExpandTabs("123456\xc2\xb2\txxx"));
}

TEST(ParseLocationTranslator, GetLineTextWithOutOfBoundsLineNumbers) {
  const std::string str =
      "abc\n"
      "def\n"
      "ghi";
  ParseLocationTranslator translator(str);

  // Out of bounds locations will hit DCHECKs and DLOG(FATAL)s.
  EXPECT_THAT(translator.GetLineText(0), StatusIs(zetasql_base::INTERNAL));
  EXPECT_THAT(translator.GetLineText(4), StatusIs(zetasql_base::INTERNAL));
}

TEST(ParseLocationPointTest, BasicTests) {
  ParseLocationPoint parse_location_point_1;

  EXPECT_EQ("", parse_location_point_1.filename());
  EXPECT_EQ("INVALID", parse_location_point_1.GetString());
  EXPECT_EQ(-1, parse_location_point_1.GetByteOffset());

  // Invalid byte offset.
  parse_location_point_1 = ParseLocationPoint::FromByteOffset(-5);
  EXPECT_EQ("", parse_location_point_1.filename());
  EXPECT_EQ("INVALID", parse_location_point_1.GetString());
  EXPECT_EQ(-5, parse_location_point_1.GetByteOffset());

  // A 0 byte offset is the first valid byte offset.
  parse_location_point_1 = ParseLocationPoint::FromByteOffset(0);
  EXPECT_EQ("", parse_location_point_1.filename());
  EXPECT_EQ("0", parse_location_point_1.GetString());
  EXPECT_EQ(0, parse_location_point_1.GetByteOffset());

  parse_location_point_1 = ParseLocationPoint::FromByteOffset(5);
  EXPECT_EQ("", parse_location_point_1.filename());
  EXPECT_EQ("5", parse_location_point_1.GetString());
  EXPECT_EQ(5, parse_location_point_1.GetByteOffset());

  // Tests with filename.
  parse_location_point_1 = ParseLocationPoint::FromByteOffset("file1", -5);
  EXPECT_EQ("file1", parse_location_point_1.filename());
  EXPECT_EQ("INVALID", parse_location_point_1.GetString());
  EXPECT_EQ(-5, parse_location_point_1.GetByteOffset());

  parse_location_point_1 = ParseLocationPoint::FromByteOffset("file2", 5);
  EXPECT_EQ("file2", parse_location_point_1.filename());
  EXPECT_EQ("file2:5", parse_location_point_1.GetString());
  EXPECT_EQ(5, parse_location_point_1.GetByteOffset());

  InternalErrorLocation error_location =
      parse_location_point_1.ToInternalErrorLocation();

  EXPECT_TRUE(error_location.has_filename());
  EXPECT_EQ("file2", error_location.filename());
  EXPECT_TRUE(error_location.has_byte_offset());
  EXPECT_EQ(5, error_location.byte_offset());

  // Round trip test, ParseLocationPoint to InternalErrorLocation and back.
  ParseLocationPoint parse_location_point_2 =
      ParseLocationPoint::FromInternalErrorLocation(error_location);
  EXPECT_EQ(parse_location_point_1, parse_location_point_2);

  // Compare points with different filename, same offset.
  parse_location_point_1 = ParseLocationPoint::FromByteOffset("file1", 5);
  parse_location_point_2 = ParseLocationPoint::FromByteOffset("file2", 5);
  EXPECT_NE(parse_location_point_1, parse_location_point_2);

  // Compare points with same filename, different offset.
  parse_location_point_1 = ParseLocationPoint::FromByteOffset("file1", 5);
  parse_location_point_2 = ParseLocationPoint::FromByteOffset("file1", 6);
  EXPECT_NE(parse_location_point_1, parse_location_point_2);
}

TEST(ParseLocationRangeTest, BasicTests) {
  ParseLocationRange parse_location_range_1;
  EXPECT_EQ("INVALID-INVALID", parse_location_range_1.GetString());

  parse_location_range_1.set_start(
      ParseLocationPoint::FromByteOffset("file1", 5));
  EXPECT_EQ("file1:5-INVALID", parse_location_range_1.GetString());

  parse_location_range_1.set_end(
      ParseLocationPoint::FromByteOffset("file1", 10));
  EXPECT_EQ("file1:5-10", parse_location_range_1.GetString());

  // The start and end of the range has different filenames
  parse_location_range_1.set_end(
      ParseLocationPoint::FromByteOffset("file2", 10));
  EXPECT_EQ("file1:5-file2:10", parse_location_range_1.GetString());

  EXPECT_EQ(parse_location_range_1, parse_location_range_1);

  ParseLocationRange parse_location_range_2;
  parse_location_range_2.set_start(
      ParseLocationPoint::FromByteOffset("file1", 5));
  parse_location_range_2.set_end(
      ParseLocationPoint::FromByteOffset("file2", 10));

  EXPECT_EQ(parse_location_range_1, parse_location_range_2);
}

TEST(ParseLocationRangeTest, SerializationTest) {
  ParseLocationRange parse_location_range;
  parse_location_range.set_start(
      ParseLocationPoint::FromByteOffset("file1", 7));
  parse_location_range.set_end(ParseLocationPoint::FromByteOffset("file1", 9));

  ZETASQL_ASSERT_OK_AND_ASSIGN(ParseLocationRangeProto proto,
                       parse_location_range.ToProto());

  EXPECT_THAT(proto.filename(), "file1");
  EXPECT_THAT(proto.start(), Eq(7));
  EXPECT_THAT(proto.end(), Eq(9));
}

// If start and end parse location have different filenames then the
// serialization will fail.
TEST(ParseLocationRangeTest, SerializationFailsIfStartAndEndFilenameNotSame) {
  ParseLocationRange parse_location_range;
  parse_location_range.set_start(
      ParseLocationPoint::FromByteOffset("file1", 9));
  parse_location_range.set_end(ParseLocationPoint::FromByteOffset("file2", 12));

  ASSERT_THAT(parse_location_range.ToProto(), Not(IsOk()));
}

// Given a ParseLocationRangeProto with start and end byte offsets populated we
// can deserialize it to ParseLocationRange object.
TEST(ParseLocationRangeTest, DeserializationTest) {
  ParseLocationRangeProto proto;

  proto.set_start(13);
  proto.set_end(17);
  proto.set_filename("anyfile");

  auto parse_location_range_or = ParseLocationRange::Create(proto);
  ZETASQL_ASSERT_OK(parse_location_range_or);
  ParseLocationRange parse_location_range =
      parse_location_range_or.ValueOrDie();

  EXPECT_EQ(parse_location_range.start().filename(),
            parse_location_range.end().filename());
  EXPECT_THAT(parse_location_range.start().GetByteOffset(), Eq(13));
  EXPECT_THAT(parse_location_range.end().GetByteOffset(), Eq(17));
}

// Given a ParseLocationRangeProto with no start byte offsets populated we can't
// deserialize it to ParseLocationRange object.
TEST(ParseLocationRangeTest, DeserializationFailsIfStartLocationNotPresent) {
  ParseLocationRangeProto proto;
  proto.set_end(17);

  auto parse_location_range_or = ParseLocationRange::Create(proto);
  EXPECT_THAT(parse_location_range_or.status(), Not(IsOk()));
}

// Given a ParseLocationRangeProto with no end byte offsets populated we can't
// deserialize it to ParseLocationRange object.
TEST(ParseLocationRangeTest, DeserializationFailsIfEndLocationNotPresent) {
  ParseLocationRangeProto proto;
  proto.set_start(17);

  auto parse_location_range_or = ParseLocationRange::Create(proto);
  EXPECT_THAT(parse_location_range_or.status(), Not(IsOk()));
}

// Given a ParseLocationRangeProto with no filename populated we can
// deserialize it to ParseLocationRange object.
TEST(ParseLocationRangeTest, DeserializationWorksIfFilenameNotPresent) {
  ParseLocationRangeProto proto;
  proto.set_start(17);
  proto.set_end(19);

  auto parse_location_range_or = ParseLocationRange::Create(proto);
  ZETASQL_ASSERT_OK(parse_location_range_or.status());

  ParseLocationRange parse_location_range =
      parse_location_range_or.ValueOrDie();
  EXPECT_TRUE(parse_location_range.start().filename().empty());
  EXPECT_TRUE(parse_location_range.end().filename().empty());

  EXPECT_THAT(parse_location_range.start().GetByteOffset(), Eq(17));
  EXPECT_THAT(parse_location_range.end().GetByteOffset(), Eq(19));
}

}  // namespace zetasql
