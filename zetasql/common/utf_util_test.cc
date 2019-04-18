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

#include "zetasql/common/utf_util.h"

#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql {

static void TestWellFormedString(absl::string_view str) {
  EXPECT_EQ(SpanWellFormedUTF8(str), str.length());
  EXPECT_TRUE(IsWellFormedUTF8(str));
  EXPECT_EQ(CoerceToWellFormedUTF8(str), str);
}

TEST(UtfUtilTest, WellFormedUTF8) {
  TestWellFormedString("");
  TestWellFormedString("\n");
  TestWellFormedString("\uFFFD");  // REPLACEMENT CHARACTER
  TestWellFormedString("abc");
  TestWellFormedString("abc\u000BBabc");
  TestWellFormedString("\xc2\xbf");
  TestWellFormedString("\xe8\xb0\xb7\xe6\xad\x8c");
}

void TestCoerce(std::string str, std::string expected) {
  if (str == expected) {
    // Sanity check.
    EXPECT_TRUE(IsWellFormedUTF8(str));
  }
  EXPECT_EQ(CoerceToWellFormedUTF8(str), expected) << "str: " << str;
}

TEST(UtfUtilTest, CoerceToWellFormedUTF8) {
}

TEST(UtfUtilTest, PrettyTruncateUTF8) {
  // 10 ascii characters
  std::string str1 = "1234567890";
  EXPECT_EQ(10, str1.size());

  // No truncation.
  EXPECT_EQ(PrettyTruncateUTF8(str1, 11), str1);
  EXPECT_EQ(PrettyTruncateUTF8(str1, 10), str1);

  // Truncate, add ...
  EXPECT_EQ(PrettyTruncateUTF8(str1, 9), "123456...");
  EXPECT_EQ(PrettyTruncateUTF8(str1, 4), "1...");

  // Corner case, drop the ..., pure truncate.
  EXPECT_EQ(PrettyTruncateUTF8(str1, 3), "123");
  EXPECT_EQ(PrettyTruncateUTF8(str1, 2), "12");

  // Be error tolerant, worse case, return empty std::string.
  EXPECT_EQ(PrettyTruncateUTF8(str1, 0), "");
  EXPECT_EQ(PrettyTruncateUTF8(str1, -4500), "");

  // 10 characters of utf garbage data.
  // Note, this gets exactly the same as ascii.
  std::string str2 = "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80";
  EXPECT_EQ(10, str2.size());

  // No truncation.
  EXPECT_EQ(PrettyTruncateUTF8(str2, 11), str2);
  EXPECT_EQ(PrettyTruncateUTF8(str2, 10), str2);

  // Truncate, add ...
  EXPECT_EQ(PrettyTruncateUTF8(str2, 9), "\x80\x80\x80\x80\x80\x80...");
  EXPECT_EQ(PrettyTruncateUTF8(str2, 4), "\x80...");

  // Corner case, drop the ..., pure truncate.
  EXPECT_EQ(PrettyTruncateUTF8(str2, 3), "\x80\x80\x80");
  EXPECT_EQ(PrettyTruncateUTF8(str2, 2), "\x80\x80");

  // Be error tolerant, worse case, return empty std::string.
  EXPECT_EQ(PrettyTruncateUTF8(str2, 0), "");
  EXPECT_EQ(PrettyTruncateUTF8(str2, -4500), "");
}
}  // namespace zetasql
