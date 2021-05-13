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

#include "zetasql/parser/flex_istream.h"

#include <cstdint>
#include <sstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace zetasql {
namespace parser {
namespace {

using ::testing::ElementsAre;

auto &kEofSentinelInput = StringStreamBufWithSentinel::kEofSentinelInput;

// Reads sequences of characters from `input` using
// StringStreamWithSentinel. `read_sizes` is a list of numbers of chars to
// read.
std::vector<std::string> ReadFromStream(absl::string_view input,
                                        absl::Span<const size_t> read_sizes) {
  StringStreamWithSentinel s(input);
  std::vector<std::string> result;
  for (size_t n : read_sizes) {
    std::vector<char> stream(n);
    s.read(stream.data(), static_cast<std::streamsize>(n));
    std::string str = std::string(stream.begin(), stream.end());
    str.resize(s.gcount());
    result.push_back(std::move(str));
    if (s.eof()) {
      return result;
    }
  }
  return result;
}

TEST(StringStreamWithSentinel, Read) {
  absl::string_view input = "abcdef";
  EXPECT_THAT(ReadFromStream(input, {3, 2, 1, 1}),
              ElementsAre("abc", "de", "f", kEofSentinelInput));
  // Read N+10 chars.
  EXPECT_THAT(ReadFromStream(input, {16}),
              ElementsAre(absl::StrCat("abcdef", kEofSentinelInput)));
  // Read N chars and then 1 char.
  EXPECT_THAT(ReadFromStream(input, {input.size(), 1}),
              ElementsAre("abcdef", kEofSentinelInput));
  // Read N-1 chars and then 2 chars.
  EXPECT_THAT(ReadFromStream(input, {input.size() - 1, 2}),
              ElementsAre("abcde", absl::StrCat("f", kEofSentinelInput)));
  // Read N chars and then 10 chars.
  EXPECT_THAT(ReadFromStream(input, {input.size(), 10}),
              ElementsAre("abcdef", absl::StrCat(kEofSentinelInput)));
  // Read 2, N-3 and then 2 chars.
  EXPECT_THAT(ReadFromStream(input, {2, input.size() - 3, 2}),
              ElementsAre("ab", "cde", absl::StrCat("f", kEofSentinelInput)));
  // Read 2, N chars.
  EXPECT_THAT(ReadFromStream(input, {2, input.size()}),
              ElementsAre("ab", absl::StrCat("cdef", kEofSentinelInput)));
}

TEST(StringStreamWithSentinel, GetAndPeek) {
  absl::string_view str = "abc";
  StringStreamWithSentinel s(str);

  {
    char c[3] = {'\0', '\0'};
    s.get(c, 3);
    EXPECT_EQ(absl::string_view(c, 2), "ab\0");
    EXPECT_FALSE(s.eof());
  }

  {
    char c = s.peek();
    EXPECT_EQ(c, 'c');
    EXPECT_FALSE(s.eof());
  }

  {
    char c = s.get();
    EXPECT_EQ(c, 'c');
    EXPECT_FALSE(s.eof());
  }

  {
    char c = s.peek();
    EXPECT_EQ(c, '\n');
    EXPECT_FALSE(s.eof());
  }

  {
    char c = s.get();
    EXPECT_EQ(c, '\n');
    EXPECT_FALSE(s.eof());
  }

  {
    s.get();
    EXPECT_TRUE(s.eof());
  }
}

TEST(StringStreamWithSentinel, SeekAndPeek) {
  absl::string_view str = "abcdefg";
  StringStreamWithSentinel s(str);
  s.seekg(2);
  EXPECT_EQ(s.peek(), 'c');
  s.seekg(1);
  EXPECT_EQ(s.peek(), 'b');
  s.seekg(6);
  EXPECT_EQ(s.peek(), 'g');
  s.seekg(7);
  EXPECT_EQ(s.peek(), '\n');
  s.seekg(8);
  EXPECT_EQ(s.peek(), -1);
}

TEST(StringStreamWithSentinel, Unget) {
  absl::string_view str = "abcd";
  StringStreamWithSentinel s(str);

  {
    char c[4] = {'\0', '\0', '\0'};
    s.read(c, 4);
    EXPECT_EQ(absl::string_view(c, 4), "abcd");
    EXPECT_FALSE(s.eof());
  }

  {
    char c = s.peek();
    EXPECT_EQ(c, '\n');
    EXPECT_FALSE(s.eof());
  }

  {
    s.unget();
    s.unget();
    char c = s.get();
    EXPECT_EQ(c, 'c');
    EXPECT_FALSE(s.eof());
  }

  {
    char c[6] = {'\0', '\0', '\0', '\0', '\0', '\0'};
    s.read(c, 6);
    EXPECT_EQ(absl::string_view(c, 2), "d\n");
    EXPECT_TRUE(s.eof());
  }
}

}  // namespace
}  // namespace parser
}  // namespace zetasql
