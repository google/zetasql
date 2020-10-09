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

#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/compliance/functions_testlib_common.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"
#include "absl/status/status.h"

namespace zetasql {
namespace {
constexpr absl::StatusCode OUT_OF_RANGE = absl::StatusCode::kOutOfRange;
}  // namespace

std::vector<QueryParamsWithResult> GetFunctionTestsLike() {
  // NOTE: We are not compliance testing invalid utf8 on the lhs of LIKE.
  // The language doesn't have a rule for whether an error should happen here.

  // All one-byte chars that aren't LIKE control chars and are valid in utf8.
  std::string all_chars;
  for (int i = 1; i < 128; ++i) {
    if (i != '%' && i != '_' && i != '\\') {
      all_chars += static_cast<char>(i);
    }
  }
  // High ascii one-byte chars valid in bytes but not strings.
  std::string all_bytes;
  for (int i = 128; i < 256; ++i) {
    all_bytes += static_cast<char>(i);
  }

  const std::string a0c = std::string("a\0c", 3);
  ZETASQL_CHECK_EQ(a0c.size(), 3);

  // Tests for value LIKE value -> bool that work for string or bytes.
  std::vector<QueryParamsWithResult> common_tests = {
    {{"", ""}, True()},
    {{"abc", "abc"}, True()},
    {{"abc", "ABC"}, False()},
    {{"ABC", "abc"}, False()},
    {{"ABc", "aBC"}, False()},
    {{"ABc", NullString()}, NullBool()},
    {{NullString(), "abc"}, NullBool()},
    {{NullString(), NullString()}, NullBool()},

    {{"abc", ""}, False()},
    {{"", "abc"}, False()},
    {{"abc", "ab"}, False()},
    {{"ab", "abc"}, False()},

    // % as a suffix.
    {{"abc", "abc%"}, True()},
    {{"abcd", "abc%"}, True()},
    {{"abcde", "abc%"}, True()},
    {{"ab", "abc%"}, False()},
    {{"acc", "abc%"}, False()},
    {{"xabc", "abc%"}, False()},
    {{"bc", "abc%"}, False()},

    // % as a prefix.
    {{"abc", "%abc"}, True()},
    {{"xabc", "%abc"}, True()},
    {{"xxabc", "%abc"}, True()},
    {{"bc", "%abc"}, False()},
    {{"acc", "%abc"}, False()},
    {{"abcx", "%abc"}, False()},
    {{"ab", "%abc"}, False()},

    // % in the middle.
    {{"abc", "a%c"}, True()},
    {{"ac", "a%c"}, True()},
    {{"axxxxxxc", "a%c"}, True()},
    {{"a%c", "a%c"}, True()},
    {{"a%%c", "a%c"}, True()},
    {{"acx", "a%c"}, False()},
    {{"xac", "a%c"}, False()},

    {{"abcdef", "ab%cd%ef"}, True()},
    {{"abxxxxcdyyyef", "ab%cd%ef"}, True()},
    {{"abxxxxcyyyef", "ab%cd%ef"}, False()},

    // _
    {{"abc", "_bc"}, True()},
    {{"abc", "a_c"}, True()},
    {{"ac", "a_c"}, False()},
    {{"abbc", "a_c"}, False()},
    {{"abc", "ab_"}, True()},
    {{"abc", "___"}, True()},
    {{"abcd", "___"}, False()},
    {{"aXcXeX", "a_c_e_"}, True()},

    // _ and %
    {{"", "__%"}, False()},
    {{"a", "__%"}, False()},
    {{"ab", "__%"}, True()},
    {{"abc", "__%"}, True()},
    {{"abcd", "__%"}, True()},
    {{"", "_%_"}, False()},
    {{"a", "_%_"}, False()},
    {{"ab", "_%_"}, True()},
    {{"abc", "_%_"}, True()},
    {{"abcd", "_%_"}, True()},
    {{"", "%__"}, False()},
    {{"a", "%__"}, False()},
    {{"ab", "%__"}, True()},
    {{"abc", "%__"}, True()},
    {{"abcd", "%__"}, True()},

    // Escaping \%.
    {{"abc", "ab\\%"}, False()},
    {{"ab%", "ab\\%"}, True()},
    {{"ab\\%", "ab\\%"}, False()},
    {{"ab\\", "ab\\%"}, False()},
    {{"ab\\x", "ab\\%"}, False()},
    {{"ab\\xx", "ab\\%"}, False()},

    // Escaping \_.
    {{"ab", "ab\\_"}, False()},
    {{"ab_", "ab\\_"}, True()},
    {{"abc", "ab\\_"}, False()},
    {{"ab\\x", "ab\\_"}, False()},
    {{"ab\\_", "ab\\_"}, False()},

    // Escaping \\.
    {{"a\\c", "a\\\\c"}, True()},
    {{"a\\\\c", "a\\\\c"}, False()},
    {{"abc", "a\\\\c"}, False()},
    {{"abc", "a\\\\%c"}, False()},
    {{"a\\c", "a\\\\%c"}, True()},
    {{"a\\xxxc", "a\\\\%c"}, True()},

    // Invalid escaping - odd numbers of trailing escapes.
    {{"", "\\"}, False(), OUT_OF_RANGE},
    {{"\\", "\\\\"}, True()},
    {{"", "\\\\\\"}, False(), OUT_OF_RANGE},

    // Escaping other characters is allowed.
    {{"a", "\\a"}, True()},
    {{"b", "\\a"}, False()},
    {{"\\a", "\\a"}, False()},

    // Regex chars not leaking through.  (Tested more with all_chars.)
    {{"abc", "a.c"}, False()},
    {{"a.c", "a.c"}, True()},
    {{"abc", "a.*"}, False()},
    {{"a.*", "a.*"}, True()},

    // \0 char doesn't truncate or cause problems.
    {{a0c, "a"}, False()},
    {{"a", a0c}, False()},
    {{a0c, "abc"}, False()},
    {{"abc", a0c}, False()},
    {{a0c, a0c}, True()},

    {{all_chars, all_chars}, True()},
  };

  // This 3-char string has a 3-byte utf8 character in it.  It is "a?b".
  const std::string utf8_string =
      "a"
      "\xe8\xb0\xb7"
      "b";
  ZETASQL_LOG(INFO) << "utf8_string: " << utf8_string;

  // Tests that work on STRING but not BYTES.
  std::vector<QueryParamsWithResult> string_tests = {
      {{utf8_string, utf8_string}, True()},

      // Invalid utf8 character in the pattern.
      {{"", "\xC2"}, False(), OUT_OF_RANGE},

      // The utf8 character counts as one character.
      {{utf8_string, "a%b"}, True()},
      {{utf8_string, "a_b"}, True()},
      {{utf8_string, "a_%b"}, True()},
      {{utf8_string, "a__b"}, False()},
      {{utf8_string, "a__%b"}, False()},
      {{utf8_string, "a___b"}, False()},

      // Splitting the utf8 character in the pattern will not work.
      // We should get an error.
      {{utf8_string, absl::StrCat(utf8_string.substr(0, 2), "%")},
       False(),
       OUT_OF_RANGE},
      {{utf8_string, absl::StrCat(utf8_string.substr(0, 3))},
       False(),
       OUT_OF_RANGE},
      {{utf8_string, absl::StrCat(utf8_string.substr(0, 3), "_")},
       False(),
       OUT_OF_RANGE},
      {{utf8_string, absl::StrCat(utf8_string.substr(0, 3), "__")},
       False(),
       OUT_OF_RANGE},
  };

  // Tests that work on BYTES but not STRING.
  std::vector<QueryParamsWithResult> bytes_tests = {
      {{Bytes(all_bytes), Bytes(all_bytes)}, True()},

      // The utf8 character counts as three bytes.
      {{Bytes(utf8_string), Bytes("a%b")}, True()},
      {{Bytes(utf8_string), Bytes("a_b")}, False()},
      {{Bytes(utf8_string), Bytes("a__b")}, False()},
      {{Bytes(utf8_string), Bytes("a___b")}, True()},
      {{Bytes(utf8_string), Bytes("a____b")}, False()},

      // Splitting the utf8 character will work fine.
      {{Bytes(utf8_string), Bytes(absl::StrCat(utf8_string.substr(0, 2), "%"))},
       True()},
      {{Bytes(utf8_string), Bytes(absl::StrCat(utf8_string.substr(0, 3), "%"))},
       True()},
      {{Bytes(utf8_string),
        Bytes(absl::StrCat(utf8_string.substr(0, 3), "%b"))},
       True()},
      {{Bytes(utf8_string),
        Bytes(absl::StrCat(utf8_string.substr(0, 3), "__"))},
       True()},
      {{Bytes(utf8_string),
        Bytes(absl::StrCat(utf8_string.substr(0, 3), "___"))},
       False()},
      {{Bytes(utf8_string), Bytes(absl::StrCat(utf8_string.substr(0, 2), "_",
                                               utf8_string.substr(3)))},
       True()},
  };

  // Starting with all possible non-UTF8 chars (except 0, %, _), if we change
  // any one char on either side, LIKE returns false.  That's too many tests,
  // so we do them in three batches, stripping every third character.
  const int kNumBatches = 3;
  for (int batch = 0; batch < kNumBatches; ++batch) {
    std::string modified_chars = all_chars;
    for (int i = batch; i < all_chars.size(); i += kNumBatches) {
      if (all_chars[i] == 'A') continue;
      modified_chars[i] = 'A';
    }
    common_tests.push_back(QueryParamsWithResult(
        {all_chars, modified_chars}, False()));
    common_tests.push_back(QueryParamsWithResult(
        {modified_chars, all_chars}, False()));
  }

  // Same thing for bytes with all high chars that aren't valid in strings.
  for (int batch = 0; batch < kNumBatches; ++batch) {
    std::string modified_bytes = all_bytes;
    for (int i = batch; i < all_bytes.size(); i += kNumBatches) {
      modified_bytes[i] = 'A';
    }
    bytes_tests.push_back(QueryParamsWithResult(
        {Bytes(all_bytes), Bytes(modified_bytes)}, False()));
    bytes_tests.push_back(QueryParamsWithResult(
        {Bytes(modified_bytes), Bytes(all_bytes)}, False()));
  }

  std::vector<QueryParamsWithResult> all_tests;
  all_tests.insert(all_tests.end(), common_tests.begin(), common_tests.end());
  all_tests.insert(all_tests.end(), string_tests.begin(), string_tests.end());
  all_tests.insert(all_tests.end(), bytes_tests.begin(), bytes_tests.end());

  // Add the BYTES version of all tests in common_tests.
  for (const QueryParamsWithResult& test : common_tests) {
    all_tests.push_back(QueryParamsWithResult(
        {StringToBytes(test.param(0)), StringToBytes(test.param(1))},
        test.result(), test.status()));
  }

  return all_tests;
}

}  // namespace zetasql
