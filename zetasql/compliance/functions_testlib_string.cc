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

#include <string>
#include <utility>
#include <vector>

#include "zetasql/compliance/functions_testlib_common.h"
#include "zetasql/public/functions/normalize_mode.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/using_test_value.cc"  // NOLINT
#include <cstdint>
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace {
constexpr absl::StatusCode INVALID_ARGUMENT =
    absl::StatusCode::kInvalidArgument;
constexpr absl::StatusCode OUT_OF_RANGE = absl::StatusCode::kOutOfRange;
}  // namespace

std::vector<FunctionTestCall> GetStringConcatTests() {
  return {
      // concat(string...) -> string
      {"concat", {NullString()}, NullString()},
      {"concat", {NullString(), NullString()}, NullString()},
      {"concat", {"a", NullString()}, NullString()},
      {"concat", {NullString(), "b"}, NullString()},
      {"concat", {"a", "b"}, "ab"},
      {"concat", {"", ""}, ""},
      {"concat", {"", NullString()}, NullString()},
      {"concat", {"", "A"}, "A"},
      {"concat", {"A", ""}, "A"},
      {"concat", {"abc", "def", "xyz"}, "abcdefxyz"},

      // concat(bytes...) -> bytes
      {"concat", {NullBytes()}, NullBytes()},
      {"concat", {NullBytes(), NullBytes()}, NullBytes()},
      {"concat", {Bytes("a"), NullBytes()}, NullBytes()},
      {"concat", {NullBytes(), Bytes("b")}, NullBytes()},
      {"concat", {Bytes("a"), Bytes("b")}, Bytes("ab")},
      {"concat", {Bytes(""), Bytes("")}, Bytes("")},
      {"concat", {Bytes(""), NullBytes()}, NullBytes()},
      {"concat", {Bytes(""), Bytes("A")}, Bytes("A")},
      {"concat", {Bytes("A"), Bytes("")}, Bytes("A")},
      {"concat", {Bytes("abc"), Bytes("def"), Bytes("xyz")},
       Bytes("abcdefxyz")},
  };
}

std::vector<FunctionTestCall> GetFunctionTestsAscii() {
  std::vector<FunctionTestCall> results = {
    // ASCII(string) -> int64_t
    {"ascii", {NullString()}, NullInt64()},
    {"ascii", {""}, 0ll},
    {"ascii", {" A"}, 32ll},
    {"ascii", {"a"}, 97ll},
    {"ascii", {"abcd"}, 97ll},
    {"ascii", {"nЖЩФ"}, 110ll},
    {"ascii", {"\x41"}, 65ll},
    {"ascii", {"\?"}, 63ll},
    {"ascii", {"\t"}, 9ll},
    {"ascii", {"\uFFFF"}, NullInt64(), OUT_OF_RANGE},
    {"ascii", {"ЖЩФ"}, NullInt64(), OUT_OF_RANGE},

    // ASCII(bytes) -> int64_t
    {"ascii", {NullBytes()}, NullInt64()},
    {"ascii", {Bytes("")}, 0ll},
    {"ascii", {Bytes(" A")}, 32ll},
    {"ascii", {Bytes("a")}, 97ll},
    {"ascii", {Bytes("abcd")}, 97ll},
    {"ascii", {Bytes("nbca\0\1cde")}, 110ll},
    {"ascii", {Bytes("\x41")}, 65ll},
    {"ascii", {Bytes("\?")}, 63ll},
    {"ascii", {Bytes("\t")}, 9ll},
    {"ascii", {Bytes("\uFFFF")}, 239ll},
    {"ascii", {Bytes("ЖЩФ")}, 208ll},
  };
  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsUnicode() {
  std::vector<FunctionTestCall> results = {
    // unicode(string) -> int64_t
    {"unicode", {NullString()}, NullInt64()},
    {"unicode", {""}, 0ll},
    {"unicode", {" A"}, 32ll},
    {"unicode", {"a"}, 97ll},
    {"unicode", {"abcd"}, 97ll},
    {"unicode", {"nЖЩФ"}, 110ll},
    {"unicode", {"\x41"}, 65ll},
    {"unicode", {"?"}, 63ll},
    {"unicode", {"\t"}, 9ll},
    {"unicode", {"\uE000"}, 0xE000ll},
    {"unicode", {"\uFFFF"}, 0xFFFFll},
    {"unicode", {"\U0010FFFE"}, 0x10FFFEll},
    {"unicode", {"\U0010FFFF"}, 0x10FFFFll},
    {"unicode", {"жщф"}, 1078ll},
  };
  { // construct a string
    std::string valid_codepoint_string;
    valid_codepoint_string.push_back(0x11);
    valid_codepoint_string.push_back('\xFF');
    valid_codepoint_string.push_back('\xFF');

    results.push_back({"unicode", {String(valid_codepoint_string)}, 17ll});
  }
  // Error cases.
  // The C++ compiler rejects Unicode literals in strings that aren't valid
  // codepoints, so we have to construct them "manually".
  {
    std::string invalid_codepoint_string;
    // The first character is an invalid codepoint.
    invalid_codepoint_string.push_back(0xD8);
    // invalid_codepoint_string.push_back('\xFF');

    results.push_back(
        {"unicode", {String(invalid_codepoint_string)}, NullInt64(),
         absl::OutOfRangeError("First char of input is not a structurally "
                               "valid UTF-8 character: '\\xd8'")});
  }
  {
    std::string invalid_codepoint_string;
    // The first character is an invalid codepoint.
    invalid_codepoint_string.push_back(0xDF);
    invalid_codepoint_string.push_back('\xFF');

    results.push_back(
        {"unicode", {String(invalid_codepoint_string)}, NullInt64(),
         absl::OutOfRangeError("First char of input is not a structurally "
                               "valid UTF-8 character: '\\xdf'")});
  }
  {
    std::string invalid_codepoint_string;
    // Invalid three-byte codepoint (above the valid range).
    invalid_codepoint_string.push_back(0xFF);
    invalid_codepoint_string.push_back('\xFF');
    invalid_codepoint_string.push_back('\xFF');
    invalid_codepoint_string.push_back('\xFF');

    results.push_back(
        {"unicode", {String(invalid_codepoint_string)}, NullInt64(),
         absl::OutOfRangeError("First char of input is not a structurally "
                               "valid UTF-8 character: '\\xff'")});
  }
  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsChr() {
  std::vector<FunctionTestCall> results = {
    {"chr", {NullInt64()}, NullString()},
    {"chr", {32ll}, String(" ")},
    {"chr", {97ll}, String("a")},
    {"chr", {66ll}, String("B")},
    {"chr", {0ll}, String("\0")},
    {"chr", {1ll}, String("\1")},
    {"chr", {1078ll}, String("ж")},
    {"chr", {1076ll}, String("д")},
    {"chr", {0xD7FEll}, String("\uD7FE")},
    {"chr", {0xE000ll}, String("\uE000")},
    {"chr", {0x10FFFFll}, String("\U0010FFFF")},
    {"chr", {-1ll}, NullString(),
     absl::OutOfRangeError("Invalid codepoint -1")},
    {"chr", {-100ll}, NullString(),
     absl::OutOfRangeError("Invalid codepoint -100")},
    {"chr", {0xD800ll}, NullString(),
     absl::OutOfRangeError("Invalid codepoint 55296")},
    {"chr", {0x11FFFFll}, NullString(),
     absl::OutOfRangeError("Invalid codepoint 1179647")},
    {"chr", {0x1000000000000ll}, NullString(),
     absl::OutOfRangeError("Invalid codepoint 281474976710656")},
  };
  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsSubstr() {
  std::vector<FunctionTestCall> results = {
      // substr(string, pos) -> string
      {"substr", {NullString(), 0ll}, NullString()},
      {"substr", {NullString(), NullInt64()}, NullString()},
      {"substr", {"", NullInt64()}, NullString()},
      {"substr", {"", 0ll}, ""},
      {"substr", {"", 1ll}, ""},
      {"substr", {"", -1ll}, ""},
      {"substr", {"abc", 1ll}, "abc"},
      {"substr", {"abc", 3ll}, "c"},
      {"substr", {"abc", 4ll}, ""},
      {"substr", {"abc", 0ll}, "abc"},
      {"substr", {"abc", -2ll}, "bc"},
      {"substr", {"abc", -5ll}, "abc"},
      {"substr", {"abc", int64min}, "abc"},
      {"substr", {"abc", int64max}, ""},
      {"substr", {"ЩФБШ", 0ll}, "ЩФБШ"},
      {"substr", {"ЩФБШ", 1ll}, "ЩФБШ"},
      {"substr", {"ЩФБШ", 3ll}, "БШ"},
      {"substr", {"ЩФБШ", 5ll}, ""},
      {"substr", {"ЩФБШ", -2ll}, "БШ"},
      {"substr", {"ЩФБШ", -4ll}, "ЩФБШ"},
      {"substr", {"ЩФБШ", -5ll}, "ЩФБШ"},
      {"substr", {"ЩФБШ", int64min}, "ЩФБШ"},
      {"substr", {"ЩФБШ", int64max}, ""},

      // substr(string, pos, length) -> string
      {"substr", {NullString(), 0ll, 1ll}, NullString()},
      {"substr", {NullString(), 0ll, 0ll}, NullString()},
      {"substr", {NullString(), 0ll, -1ll}, NullString()},
      {"substr", {NullString(), 0ll, NullInt64()}, NullString()},
      {"substr", {NullString(), 1ll, 1ll}, NullString()},
      {"substr", {NullString(), 1ll, 0ll}, NullString()},
      {"substr", {NullString(), 1ll, -1ll}, NullString()},
      {"substr", {NullString(), 1ll, NullInt64()}, NullString()},
      {"substr", {NullString(), -1ll, 1ll}, NullString()},
      {"substr", {NullString(), -1ll, 0ll}, NullString()},
      {"substr", {NullString(), -1ll, -1ll}, NullString()},
      {"substr", {NullString(), -1ll, NullInt64()}, NullString()},
      {"substr", {NullString(), NullInt64(), 1ll}, NullString()},
      {"substr", {NullString(), NullInt64(), 0ll}, NullString()},
      {"substr", {NullString(), NullInt64(), -1ll}, NullString()},
      {"substr", {NullString(), NullInt64(), NullInt64()}, NullString()},
      {"substr", {"", NullInt64(), 1ll}, NullString()},
      {"substr", {"", NullInt64(), 0ll}, NullString()},
      {"substr", {"", NullInt64(), -1ll}, NullString()},
      {"substr", {"", NullInt64(), NullInt64()}, NullString()},
      {"substr", {"", 0ll, 1ll}, ""},
      {"substr", {"", 0ll, 0ll}, ""},
      {"substr", {"", 0ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"", 0ll, NullInt64()}, NullString()},
      {"substr", {"", 1ll, 1ll}, ""},
      {"substr", {"", 1ll, 0ll}, ""},
      {"substr", {"", 1ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"", 1ll, NullInt64()}, NullString()},
      {"substr", {"", -1ll, 1ll}, ""},
      {"substr", {"", -1ll, 0ll}, ""},
      {"substr", {"", -1ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"", -1ll, NullInt64()}, NullString()},
      {"substr", {"abc", 0ll, 0ll}, ""},
      {"substr", {"abc", 0ll, 1ll}, "a"},
      {"substr", {"abc", 0ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"abc", 0ll, NullInt64()}, NullString()},
      {"substr", {"abc", -1ll, 0ll}, ""},
      {"substr", {"abc", -1ll, 1ll}, "c"},
      {"substr", {"abc", -1ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"abc", -1ll, NullInt64()}, NullString()},
      {"substr", {"abc", NullInt64(), 0ll}, NullString()},
      {"substr", {"abc", NullInt64(), 1ll}, NullString()},
      {"substr", {"abc", NullInt64(), -1ll}, NullString()},
      {"substr", {"abc", NullInt64(), NullInt64()}, NullString()},
      {"substr", {"abc", 1ll, 2ll}, "ab"},
      {"substr", {"abc", 3ll, 1ll}, "c"},
      {"substr", {"abc", 3ll, 0ll}, ""},
      {"substr", {"abc", 2ll, 1ll}, "b"},
      {"substr", {"abc", 2ll, NullInt64()}, NullString()},
      {"substr", {"abc", 4ll, 5ll}, ""},
      {"substr", {"abc", 1ll, int64max}, "abc"},
      {"substr", {"abc", 3ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"abc", -2ll, 1ll}, "b"},
      {"substr", {"abc", -5ll, 2ll}, "ab"},
      {"substr", {"abc", -5ll, 0ll}, ""},
      {"substr", {"abc", -5ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"abc", int64min, 1ll}, "a"},
      {"substr", {"abc", int64max, int64max}, ""},
      {"substr", {"abc", 1ll, -1ll}, NullString(), OUT_OF_RANGE},
      {"substr", {"abc", 1ll, int64min}, NullString(), OUT_OF_RANGE},
      {"substr", {"ЩФБШ", 0ll, 2ll}, "ЩФ"},
      {"substr", {"ЩФБШ", 0ll, 5ll}, "ЩФБШ"},
      {"substr", {"ЩФБШ", 2ll, 0ll}, ""},
      {"substr", {"ЩФБШ", 2ll, 1ll}, "Ф"},
      {"substr", {"ЩФБШ", 2ll, 5ll}, "ФБШ"},
      {"substr", {"ЩФБШ", 3ll, 2ll}, "БШ"},
      {"substr", {"ЩФБШ", 5ll, 2ll}, ""},
      {"substr", {"ЩФБШ", -2ll, 3ll}, "БШ"},
      {"substr", {"ЩФБШ", -2ll, 0ll}, ""},
      {"substr", {"ЩФБШ", -2ll, 1ll}, "Б"},
      {"substr", {"ЩФБШ", -4ll, 5ll}, "ЩФБШ"},
      {"substr", {"ЩФБШ", -3ll, 2ll}, "ФБ"},
      {"substr", {"ЩФБШ", -5ll, 3ll}, "ЩФБ"},
      {"substr", {"ЩФБШ", int64min, 1ll}, "Щ"},
      {"substr", {"ЩФБШ", int64max, 1ll}, ""},

      // substr(bytes, pos) -> bytes
      {"substr", {NullBytes(), 1ll}, NullBytes()},
      {"substr", {NullBytes(), 0ll}, NullBytes()},
      {"substr", {NullBytes(), -1ll}, NullBytes()},
      {"substr", {Bytes(""), 0ll}, Bytes("")},
      {"substr", {Bytes(""), 1ll}, Bytes("")},
      {"substr", {Bytes(""), -1ll}, Bytes("")},
      {"substr", {Bytes(""), NullInt64()}, NullBytes()},
      {"substr", {NullBytes(), NullInt64()}, NullBytes()},
      {"substr", {Bytes("abc"), 1ll}, Bytes("abc")},
      {"substr", {Bytes("abc"), 3ll}, Bytes("c")},
      {"substr", {Bytes("abc"), 4ll}, Bytes("")},
      {"substr", {Bytes("abc"), 0ll}, Bytes("abc")},
      {"substr", {Bytes("abc"), -2ll}, Bytes("bc")},
      {"substr", {Bytes("abc"), -5ll}, Bytes("abc")},
      {"substr", {Bytes("abc"), int64min}, Bytes("abc")},
      {"substr", {Bytes("abc"), int64max}, Bytes("")},
      {"substr", {Bytes("abc"), NullInt64()}, NullBytes()},
      {"substr", {Bytes("abc\0xyz"), 4ll}, Bytes("\0xyz")},
      {"substr", {Bytes("\xff\x80\xa0"), 2ll}, Bytes("\x80\xa0")},

      // substr(bytes, pos, length) -> bytes
      {"substr", {NullBytes(), NullInt64(), 1ll}, NullBytes()},
      {"substr", {NullBytes(), NullInt64(), 0ll}, NullBytes()},
      {"substr", {NullBytes(), NullInt64(), -1ll}, NullBytes()},
      {"substr", {NullBytes(), NullInt64(), NullInt64()}, NullBytes()},
      {"substr", {NullBytes(), 0ll, 1ll}, NullBytes()},
      {"substr", {NullBytes(), 0ll, 0ll}, NullBytes()},
      {"substr", {NullBytes(), 0ll, -1ll}, NullBytes()},
      {"substr", {NullBytes(), 0ll, NullInt64()}, NullBytes()},
      {"substr", {NullBytes(), 1ll, 1ll}, NullBytes()},
      {"substr", {NullBytes(), 1ll, 0ll}, NullBytes()},
      {"substr", {NullBytes(), 1ll, -1ll}, NullBytes()},
      {"substr", {NullBytes(), 1ll, NullInt64()}, NullBytes()},
      {"substr", {NullBytes(), -1ll, 1ll}, NullBytes()},
      {"substr", {NullBytes(), -1ll, 0ll}, NullBytes()},
      {"substr", {NullBytes(), -1ll, -1ll}, NullBytes()},
      {"substr", {NullBytes(), -1ll, NullInt64()}, NullBytes()},
      {"substr", {Bytes(""), NullInt64(), 1ll}, NullBytes()},
      {"substr", {Bytes(""), NullInt64(), 0ll}, NullBytes()},
      {"substr", {Bytes(""), NullInt64(), -1ll}, NullBytes()},
      {"substr", {Bytes(""), NullInt64(), NullInt64()}, NullBytes()},
      {"substr", {Bytes(""), 0ll, 1ll}, Bytes("")},
      {"substr", {Bytes(""), 0ll, 0ll}, Bytes("")},
      {"substr", {Bytes(""), 0ll, -1ll}, NullBytes(), OUT_OF_RANGE},
      {"substr", {Bytes(""), 0ll, NullInt64()}, NullBytes()},
      {"substr", {Bytes(""), 1ll, 1ll}, Bytes("")},
      {"substr", {Bytes(""), 1ll, 0ll}, Bytes("")},
      {"substr", {Bytes(""), 1ll, -1ll}, NullBytes(), OUT_OF_RANGE},
      {"substr", {Bytes(""), 1ll, NullInt64()}, NullBytes()},
      {"substr", {Bytes(""), -1ll, 1ll}, Bytes("")},
      {"substr", {Bytes(""), -1ll, 0ll}, Bytes("")},
      {"substr", {Bytes(""), -1ll, -1ll}, NullBytes(), OUT_OF_RANGE},
      {"substr", {Bytes(""), -1ll, NullInt64()}, NullBytes()},
      {"substr", {Bytes("abc"), 1ll, 2ll}, Bytes("ab")},
      {"substr", {Bytes("abc"), 3ll, 1ll}, Bytes("c")},
      {"substr", {Bytes("abc"), 3ll, 0ll}, Bytes("")},
      {"substr", {Bytes("abc"), 2ll, 1ll}, Bytes("b")},
      {"substr", {Bytes("abc"), 4ll, 5ll}, Bytes("")},
      {"substr", {Bytes("abc"), 1ll, int64max}, Bytes("abc")},
      {"substr", {Bytes("abc"), -2ll, 1ll}, Bytes("b")},
      {"substr", {Bytes("abc"), -5ll, 2ll}, Bytes("ab")},
      {"substr", {Bytes("abc"), -5ll, 0ll}, Bytes("")},
      {"substr", {Bytes("abc"), -5ll, -1ll}, NullBytes(), OUT_OF_RANGE},
      {"substr", {Bytes("abc"), int64min, 1ll}, Bytes("a")},
      {"substr", {Bytes("abc"), int64max, int64max}, Bytes("")},
      {"substr", {Bytes("abc"), NullInt64(), 1ll}, NullBytes()},
      {"substr", {Bytes("abc"), NullInt64(), 0ll}, NullBytes()},
      {"substr", {Bytes("abc"), NullInt64(), -1ll}, NullBytes()},
      {"substr", {Bytes("abc"), NullInt64(), NullInt64()}, NullBytes()},
      {"substr", {Bytes("abc"), 1ll, -1ll}, NullBytes(), OUT_OF_RANGE},
      {"substr", {Bytes("abc"), 1ll, int64min}, NullBytes(), OUT_OF_RANGE},
      {"substr", {Bytes("\xff\x80\xa0"), 2ll, 1ll}, Bytes("\x80")}};
  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsSubstring() {
  std::vector<FunctionTestCall> substr_tests = GetFunctionTestsSubstr();
  // Run test for SUBSTR for its alias SUBSTRING.
  std::vector<FunctionTestCall> substring_tests;
  for (auto& expected_result : substr_tests) {
    if (expected_result.function_name == "substr") {
      substring_tests.emplace_back(expected_result);
      substring_tests.back().function_name = "substring";
    }
  }
  return substring_tests;
}

std::vector<FunctionTestCall> GetFunctionTestsString() {
  absl::string_view all_bytes_str(
      "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12"
      "\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\" #$ %"
      "&\'()*+,-./"
      "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`"
      "abcdefghijklmnopqrstuvwxyz{|}~"
      "\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90"
      "\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2"
      "\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4"
      "\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6"
      "\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8"
      "\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea"
      "\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc"
      "\xfd\xfe\xff",
      256);
  absl::string_view all_bytes_uppercase(
      "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12"
      "\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\" #$ %"
      "&\'()*+,-./"
      "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}~"
      "\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90"
      "\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2"
      "\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4"
      "\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6"
      "\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8"
      "\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea"
      "\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc"
      "\xfd\xfe\xff",
      256);
  absl::string_view all_bytes_lowercase(
      "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12"
      "\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\" #$ %"
      "&\'()*+,-./"
      "0123456789:;<=>?@abcdefghijklmnopqrstuvwxyz[\\]^_`"
      "abcdefghijklmnopqrstuvwxyz{|}~"
      "\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90"
      "\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2"
      "\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4"
      "\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6"
      "\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8"
      "\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea"
      "\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc"
      "\xfd\xfe\xff",
      256);

  std::vector<FunctionTestCall> results = {
      // strpos(string, string) -> int64_t
      {"strpos", {NullString(), ""}, NullInt64()},
      {"strpos", {NullString(), "x"}, NullInt64()},
      {"strpos", {"", NullString()}, NullInt64()},
      {"strpos", {NullString(), NullString()}, NullInt64()},
      {"strpos", {"", "x"}, 0ll},
      {"strpos", {"x", ""}, 1ll},
      {"strpos", {"x", NullString()}, NullInt64()},
      {"strpos", {"", ""}, 1ll},
      {"strpos", {"xxx", "x"}, 1ll},
      {"strpos", {"abcdef", "cd"}, 3ll},
      {"strpos", {"abcdefabcdef", "de"}, 4ll},
      {"strpos", {"abcdefabcdef", "xz"}, 0ll},
      {"strpos", {"\0abcedf", "abc"}, 2ll},
      {"strpos", {"abca\0b\0c\0", "a\0b\0c"}, 4ll},
      {"strpos", {"zгдl", "дl"}, 3ll},

      // strpos(bytes, bytes) -> int64_t
      {"strpos", {NullBytes(), Bytes("")}, NullInt64()},
      {"strpos", {NullBytes(), Bytes("x")}, NullInt64()},
      {"strpos", {NullBytes(), NullBytes()}, NullInt64()},
      {"strpos", {Bytes(""), NullBytes()}, NullInt64()},
      {"strpos", {Bytes(""), Bytes("x")}, 0ll},
      {"strpos", {Bytes(""), Bytes("")}, 1ll},
      {"strpos", {Bytes("x"), Bytes("")}, 1ll},
      {"strpos", {Bytes("x"), NullBytes()}, NullInt64()},
      {"strpos", {Bytes("xxx"), Bytes("x")}, 1ll},
      {"strpos", {Bytes("abcdef"), Bytes("cd")}, 3ll},
      {"strpos", {Bytes("abcdefabcdef"), Bytes("de")}, 4ll},
      {"strpos", {Bytes("abcdefabcdef"), Bytes("xz")}, 0ll},
      {"strpos", {Bytes("\0abcedf"), Bytes("abc")}, 2ll},
      {"strpos", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c")}, 4ll},
      {"strpos", {Bytes("zгдl"), Bytes("дl")}, 4ll},

      // starts_with(string, string) -> bool
      {"starts_with", {NullString(), ""}, NullBool()},
      {"starts_with", {NullString(), "a"}, NullBool()},
      {"starts_with", {"", NullString()}, NullBool()},
      {"starts_with", {"starts_with", NullString()}, NullBool()},
      {"starts_with", {NullString(), NullString()}, NullBool()},
      {"starts_with", {"", ""}, true},
      {"starts_with", {"", "a"}, false},
      {"starts_with", {"starts_with", "starts"}, true},
      {"starts_with", {"starts_with", "starts_with"}, true},
      {"starts_with", {"starts_with", "ends"}, false},
      {"starts_with", {"starts_with", "with"}, false},
      {"starts_with", {"starts_with", "starts_with_extra"}, false},
      {"starts_with", {"starts_with", ""}, true},
      {"starts_with", {"абвгд", "абв"}, true},
      {"starts_with", {"абв", "абвг"}, false},

      // starts_with(bytes, bytes) -> bool
      {"starts_with", {NullBytes(), Bytes("")}, NullBool()},
      {"starts_with", {Bytes(""), NullBytes()}, NullBool()},
      {"starts_with", {Bytes("starts_with"), NullBytes()}, NullBool()},
      {"starts_with", {NullBytes(), NullBytes()}, NullBool()},
      {"starts_with", {Bytes(""), Bytes("")}, true},
      {"starts_with", {Bytes(""), Bytes("a")}, false},
      {"starts_with", {Bytes("starts_with"), Bytes("starts")}, true},
      {"starts_with", {Bytes("starts_with"), Bytes("starts_with")}, true},
      {"starts_with",
       {Bytes("starts_with"), Bytes("starts_with_extra")},
       false},
      {"starts_with", {Bytes("starts_with"), Bytes("ends")}, false},
      {"starts_with", {Bytes("starts_with"), Bytes("with")}, false},
      {"starts_with", {Bytes("starts_with"), Bytes("")}, true},

      // ends_with(string, string) -> bool
      {"ends_with", {NullString(), ""}, NullBool()},
      {"ends_with", {"", NullString()}, NullBool()},
      {"ends_with", {"ends_with", NullString()}, NullBool()},
      {"ends_with", {NullString(), NullString()}, NullBool()},
      {"ends_with", {"ends_with", "with"}, true},
      {"ends_with", {"ends_with", "ends_with"}, true},
      {"ends_with", {"ends_with", "extra_ends_with"}, false},
      {"ends_with", {"ends_with", "ends"}, false},
      {"ends_with", {"ends_with", "with"}, true},
      {"ends_with", {"ends_with", ""}, true},
      {"ends_with", {"абвгд", "вгд"}, true},
      {"ends_with", {"бв", "aбв"}, false},

      // ends_with(bytes, bytes) -> bool
      {"ends_with", {NullBytes(), Bytes("")}, NullBool()},
      {"ends_with", {Bytes("ends_with"), NullBytes()}, NullBool()},
      {"ends_with", {Bytes(""), NullBytes()}, NullBool()},
      {"ends_with", {NullBytes(), NullBytes()}, NullBool()},
      {"ends_with", {Bytes("ends_with"), Bytes("with")}, true},
      {"ends_with", {Bytes("ends_with"), Bytes("ends_with")}, true},
      {"ends_with", {Bytes("ends_with"), Bytes("extra_ends_with")}, false},
      {"ends_with", {Bytes("ends_with"), Bytes("ends")}, false},
      {"ends_with", {Bytes("ends_with"), Bytes("with")}, true},
      {"ends_with", {Bytes("ends_with"), Bytes("")}, true},

      // length(string) -> int64_t
      {"length", {NullString()}, NullInt64()},
      {"length", {""}, 0ll},
      {"length", {"abcde"}, 5ll},
      {"length", {"абвгд"}, 5ll},
      {"length", {"\0\0"}, 2ll},

      // length(bytes) -> int64_t
      {"length", {NullBytes()}, NullInt64()},
      {"length", {Bytes("")}, 0ll},
      {"length", {Bytes("abcde")}, 5ll},
      {"length", {Bytes("абвгд")}, 10ll},
      {"length", {Bytes("\0\0")}, 2ll},

      // byte_length(string) -> int64_t
      {"byte_length", {NullString()}, NullInt64()},
      {"byte_length", {""}, 0ll},
      {"byte_length", {"abcde"}, 5ll},
      {"byte_length", {"абвгд"}, 10ll},
      {"byte_length", {"\0\0"}, 2ll},

      // byte_length(bytes) -> int64_t
      {"byte_length", {NullBytes()}, NullInt64()},
      {"byte_length", {Bytes("")}, 0ll},
      {"byte_length", {Bytes("abcde")}, 5ll},
      {"byte_length", {Bytes("абвгд")}, 10ll},
      {"byte_length", {Bytes("\0\0")}, 2ll},

      // char_length(string) -> int64_t
      {"char_length", {NullString()}, NullInt64()},
      {"character_length", {""}, 0ll},
      {"char_length", {"abcde"}, 5ll},
      {"character_length", {"абвгд"}, 5ll},
      {"char_length", {"\0\0"}, 2ll},

      {"char_length", {Bytes("abcde")}, NullInt64(), INVALID_ARGUMENT},

      // trim(bytes, bytes) -> bytes
      {"trim", {NullBytes(), Bytes("")}, NullBytes()},
      {"trim", {NullBytes(), Bytes("a")}, NullBytes()},
      {"trim", {Bytes(""), NullBytes()}, NullBytes()},
      {"trim", {Bytes("a"), NullBytes()}, NullBytes()},
      {"trim", {NullBytes(), NullBytes()}, NullBytes()},
      {"trim", {Bytes(""), Bytes("")}, Bytes("")},
      {"trim", {Bytes(""), Bytes("a")}, Bytes("")},
      {"trim", {Bytes("  abc  "), Bytes("")}, Bytes("  abc  ")},
      {"trim", {Bytes("  abc  "), Bytes(" ")}, Bytes("abc")},
      {"trim", {Bytes("  a b c  "), Bytes(" ")}, Bytes("a b c")},
      {"trim", {Bytes("  abc  "), Bytes(" cba")}, Bytes("")},
      {"trim", {Bytes("  abc  "), Bytes(" edcba")}, Bytes("")},
      {"trim", {Bytes("a \0xyza \0"), Bytes("\0a ")}, Bytes("xyz")},
      {"trim",
       {Bytes("\xFF\x80\x81xyz\x81\x80\xFF"), Bytes("\x80\xFF")},
       Bytes("\x81xyz\x81")},

      // ltrim(bytes, bytes) -> bytes
      {"ltrim", {NullBytes(), Bytes("")}, NullBytes()},
      {"ltrim", {NullBytes(), Bytes("a")}, NullBytes()},
      {"ltrim", {Bytes(""), NullBytes()}, NullBytes()},
      {"ltrim", {Bytes("a"), NullBytes()}, NullBytes()},
      {"ltrim", {NullBytes(), NullBytes()}, NullBytes()},
      {"ltrim", {Bytes(""), Bytes("")}, Bytes("")},
      {"ltrim", {Bytes(""), Bytes("a")}, Bytes("")},
      {"ltrim", {Bytes("  abc  "), Bytes("")}, Bytes("  abc  ")},
      {"ltrim", {Bytes("  abc  "), Bytes(" ")}, Bytes("abc  ")},
      {"ltrim", {Bytes("  a b c  "), Bytes(" ")}, Bytes("a b c  ")},
      {"ltrim", {Bytes("  abc  "), Bytes(" cba")}, Bytes("")},
      {"ltrim", {Bytes("  abc  "), Bytes(" edcba")}, Bytes("")},
      {"ltrim", {Bytes("a \0xyza \0"), Bytes("\0a ")}, Bytes("xyza \0")},
      {"ltrim",
       {Bytes("\xFF\x80\x81xyz\x81\x80\xFF"), Bytes("\x80\xFF")},
       Bytes("\x81xyz\x81\x80\xFF")},

      // rtrim(bytes, bytes) -> bytes
      {"rtrim", {NullBytes(), Bytes("")}, NullBytes()},
      {"rtrim", {NullBytes(), Bytes("a")}, NullBytes()},
      {"rtrim", {Bytes(""), NullBytes()}, NullBytes()},
      {"rtrim", {Bytes("a"), NullBytes()}, NullBytes()},
      {"rtrim", {NullBytes(), NullBytes()}, NullBytes()},
      {"rtrim", {Bytes(""), Bytes("")}, Bytes("")},
      {"rtrim", {Bytes(""), Bytes("a")}, Bytes("")},
      {"rtrim", {Bytes("  abc  "), Bytes("")}, Bytes("  abc  ")},
      {"rtrim", {Bytes("  abc  "), Bytes(" ")}, Bytes("  abc")},
      {"rtrim", {Bytes("  a b c  "), Bytes(" ")}, Bytes("  a b c")},
      {"rtrim", {Bytes("  abc  "), Bytes(" cba")}, Bytes("")},
      {"rtrim", {Bytes("  abc  "), Bytes(" edcba")}, Bytes("")},
      {"rtrim", {Bytes("a \0xyza \0"), Bytes("\0a ")}, Bytes("a \0xyz")},
      {"rtrim",
       {Bytes("\xFF\x80\x81xyz\x81\x80\xFF"), Bytes("\x80\xFF")},
       Bytes("\xFF\x80\x81xyz\x81")},

      // trim(string) -> string
      {"trim", {NullString()}, NullString()},
      {"trim", {""}, ""},
      {"trim", {"   "}, ""},
      {"trim", {"   abc   "}, "abc"},
      {"trim", {"   a b c   "}, "a b c"},
      {"trim", {"   ыфщ   "}, "ыфщ"},
      // The following string encodes all 25 space characters defined by
      // Unicode.
      {"trim",
       {"\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000"
        "\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A"
        "\u2028\u2029\u202F\u205Fabc\u0009\u000A\u000B\u000C\u000D\u0020"
        "\u0085\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006"
        "\u2007\u2008\u2009\u200A\u2028\u2029\u202F\u205F"},
       "abc"},

      // ltrim(string) -> string
      {"ltrim", {NullString()}, NullString()},
      {"ltrim", {""}, ""},
      {"ltrim", {"   "}, ""},
      {"ltrim", {"   abc   "}, "abc   "},
      {"ltrim", {"   a b c   "}, "a b c   "},
      {"ltrim", {"   ыфщ   "}, "ыфщ   "},
      {"ltrim",
       {"\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000"
        "\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A"
        "\u2028\u2029\u202F\u205Fabc"},
       "abc"},

      // rtrim(string) -> string
      {"rtrim", {NullString()}, NullString()},
      {"rtrim", {""}, ""},
      {"rtrim", {"   "}, ""},
      {"rtrim", {"   abc   "}, "   abc"},
      {"rtrim", {"   a b c   "}, "   a b c"},
      {"rtrim", {"   ыфщ   "}, "   ыфщ"},
      {"rtrim",
       {"abc\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000"
        "\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A"
        "\u2028\u2029\u202F\u205F"},
       "abc"},

      // trim(string, string) -> string
      {"trim", {NullString(), ""}, NullString()},
      {"trim", {NullString(), "a"}, NullString()},
      {"trim", {"", NullString()}, NullString()},
      {"trim", {"a", NullString()}, NullString()},
      {"trim", {NullString(), NullString()}, NullString()},
      {"trim", {"", ""}, ""},
      {"trim", {"", "a"}, ""},
      {"trim", {"  abc  ", ""}, "  abc  "},
      {"trim", {"", " "}, ""},
      {"trim", {"   ", " "}, ""},
      {"trim", {"\0   \0", "\0"}, "   "},
      {"trim", {"   abc   ", " "}, "abc"},
      {"trim", {"   abc   ", " cba"}, ""},
      {"trim", {"   a b c   ", " cba"}, ""},
      {"trim", {"   ыфщ   ", " "}, "ыфщ"},
      {"trim", {"   ыфabcщ   ", " ыфщ"}, "abc"},
      // The explicitly specified REPLACEMENT CHARACTER is allowed as a trim
      // pattern and should be handled as any other unicode character.
      {"trim", {"a\ufffdb\ufffdxyz\ufffdb\ufffda", "ab\ufffd"}, "xyz"},

      // ltrim(string, string) -> string
      {"ltrim", {NullString(), ""}, NullString()},
      {"ltrim", {"", " "}, ""},
      {"ltrim", {"", ""}, ""},
      {"ltrim", {"  abc  ", ""}, "  abc  "},
      {"ltrim", {"\0   \0", "\0"}, "   \0"},
      {"ltrim", {"   ", " "}, ""},
      {"ltrim", {"   abc   ", " "}, "abc   "},
      {"ltrim", {"   abc   ", " cba"}, ""},
      {"ltrim", {"   a b c   ", " cba"}, ""},
      {"ltrim", {"   ыфщ   ", " "}, "ыфщ   "},
      {"ltrim", {"   ыфabcщ   ", " ыфщ"}, "abcщ   "},
      // The explicitly specified REPLACEMENT CHARACTER is allowed as a trim
      // input
      // and should be handled as any other unicode character.
      {"ltrim",
       {"a\ufffdb\ufffdxyz\ufffdb\ufffda", "ab\ufffd"},
       "xyz\ufffdb\ufffda"},

      // rtrim(string, string) -> string
      {"rtrim", {NullString(), ""}, NullString()},
      {"rtrim", {NullString(), "a"}, NullString()},
      {"rtrim", {"", NullString()}, NullString()},
      {"rtrim", {"a", NullString()}, NullString()},
      {"rtrim", {NullString(), NullString()}, NullString()},
      {"rtrim", {"", ""}, ""},
      {"rtrim", {"", " "}, ""},
      {"rtrim", {"  abc  ", ""}, "  abc  "},
      {"rtrim", {"\0   \0", "\0"}, "\0   "},
      {"rtrim", {"   ", " "}, ""},
      {"rtrim", {"   abc   ", " "}, "   abc"},
      {"rtrim", {"   abc   ", " cba"}, ""},
      {"rtrim", {"   a b c   ", " cba"}, ""},
      {"rtrim", {"   ыфщ   ", " "}, "   ыфщ"},
      {"rtrim", {"   ыфabcщ   ", " ыфщ"}, "   ыфabc"},
      // The explicitly specified REPLACEMENT CHARACTER is allowed as a trim
      // input
      // and should be handled as any other unicode character.
      {"rtrim",
       {"a\ufffdb\ufffdxyz\ufffdb\ufffda", "ab\ufffd"},
       "a\ufffdb\ufffdxyz"},

      // left(string, length) -> string
      {"left", {NullString(), 1ll}, NullString()},
      {"left", {NullString(), 0ll}, NullString()},
      {"left", {NullString(), -1ll}, NullString()},
      {"left", {NullString(), NullInt64()}, NullString()},
      {"left", {"", 1ll}, ""},
      {"left", {"", 0ll}, ""},
      {"left", {"", -1ll}, NullString(), OUT_OF_RANGE},
      {"left", {"", NullInt64()}, NullString()},
      {"left", {"abc", 0ll}, ""},
      {"left", {"abc", 1ll}, "a"},
      {"left", {"abc", -1ll}, NullString(), OUT_OF_RANGE},
      {"left", {"abc", NullInt64()}, NullString()},
      {"left", {"abc", 2ll}, "ab"},
      {"left", {"abc", 3ll}, "abc"},
      {"left", {"abc", int64max}, "abc"},
      {"left", {"abc", -5ll}, NullString(), OUT_OF_RANGE},
      {"left", {"abc", int64min}, NullString(), OUT_OF_RANGE},
      {"left", {"ЩФБШ", NullInt64()}, NullString()},
      {"left", {"ЩФБШ", 0ll}, ""},
      {"left", {"ЩФБШ", 2ll}, "ЩФ"},
      {"left", {"ЩФБШ", 5ll}, "ЩФБШ"},
      {"left", {"ЩФБШ", int64max}, "ЩФБШ"},

      // left(bytes, length) -> bytes
      {"left", {NullBytes(), 1ll}, NullBytes()},
      {"left", {NullBytes(), 0ll}, NullBytes()},
      {"left", {NullBytes(), -1ll}, NullBytes()},
      {"left", {NullBytes(), NullInt64()}, NullBytes()},
      {"left", {Bytes(""), 1ll}, Bytes("")},
      {"left", {Bytes(""), 0ll}, Bytes("")},
      {"left", {Bytes(""), -1ll}, NullBytes(), OUT_OF_RANGE},
      {"left", {Bytes(""), NullInt64()}, NullBytes()},
      {"left", {Bytes("abc"), 0ll}, Bytes("")},
      {"left", {Bytes("abc"), 1ll}, Bytes("a")},
      {"left", {Bytes("abc"), -1ll}, NullBytes(), OUT_OF_RANGE},
      {"left", {Bytes("abc"), NullInt64()}, NullBytes()},
      {"left", {Bytes("abc"), 2ll}, Bytes("ab")},
      {"left", {Bytes("abc"), 3ll}, Bytes("abc")},
      {"left", {Bytes("abc"), int64max}, Bytes("abc")},
      {"left", {Bytes("abc"), -5ll}, NullBytes(), OUT_OF_RANGE},
      {"left", {Bytes("abc"), int64min}, NullBytes(), OUT_OF_RANGE},
      {"left", {Bytes("\xff\x80\xa0"), 2ll}, Bytes("\xff\x80")},

      // right(string, length) -> string
      {"right", {NullString(), 1ll}, NullString()},
      {"right", {NullString(), 0ll}, NullString()},
      {"right", {NullString(), -1ll}, NullString()},
      {"right", {NullString(), NullInt64()}, NullString()},
      {"right", {"", 1ll}, ""},
      {"right", {"", 0ll}, ""},
      {"right", {"", -1ll}, NullString(), OUT_OF_RANGE},
      {"right", {"", NullInt64()}, NullString()},
      {"right", {"abc", 0ll}, ""},
      {"right", {"abc", 1ll}, "c"},
      {"right", {"abc", -1ll}, NullString(), OUT_OF_RANGE},
      {"right", {"abc", NullInt64()}, NullString()},
      {"right", {"abc", 2ll}, "bc"},
      {"right", {"abc", 3ll}, "abc"},
      {"right", {"abc", int64max}, "abc"},
      {"right", {"abc", -5ll}, NullString(), OUT_OF_RANGE},
      {"right", {"abc", int64min}, NullString(), OUT_OF_RANGE},
      {"right", {"ЩФБШ", NullInt64()}, NullString()},
      {"right", {"ЩФБШ", 0ll}, ""},
      {"right", {"ЩФБШ", 2ll}, "БШ"},
      {"right", {"ЩФБШ", 5ll}, "ЩФБШ"},
      {"right", {"ЩФБШ", int64max}, "ЩФБШ"},

      // right(bytes, length) -> bytes
      {"right", {NullBytes(), 1ll}, NullBytes()},
      {"right", {NullBytes(), 0ll}, NullBytes()},
      {"right", {NullBytes(), -1ll}, NullBytes()},
      {"right", {NullBytes(), NullInt64()}, NullBytes()},
      {"right", {Bytes(""), 1ll}, Bytes("")},
      {"right", {Bytes(""), 0ll}, Bytes("")},
      {"right", {Bytes(""), -1ll}, NullBytes(), OUT_OF_RANGE},
      {"right", {Bytes(""), NullInt64()}, NullBytes()},
      {"right", {Bytes("abc"), 0ll}, Bytes("")},
      {"right", {Bytes("abc"), 1ll}, Bytes("c")},
      {"right", {Bytes("abc"), -1ll}, NullBytes(), OUT_OF_RANGE},
      {"right", {Bytes("abc"), NullInt64()}, NullBytes()},
      {"right", {Bytes("abc"), 2ll}, Bytes("bc")},
      {"right", {Bytes("abc"), 3ll}, Bytes("abc")},
      {"right", {Bytes("abc"), int64max}, Bytes("abc")},
      {"right", {Bytes("abc"), -5ll}, NullBytes(), OUT_OF_RANGE},
      {"right", {Bytes("abc"), int64min}, NullBytes(), OUT_OF_RANGE},
      {"right", {Bytes("\xff\x80\xa0"), 2ll}, Bytes("\x80\xa0")},

      // upper(string) -> string
      {"upper", {NullString()}, NullString()},
      {"upper", {""}, ""},
      {"upper", {"ß"}, "SS"},  // Returns a longer string.
      // ⱥ -> Ⱥ (from 3 bytes to 2 bytes)
      {"upper", {"ⱥ"}, "Ⱥ"},  // Returns a shorter string.
      {"upper", {"abcABCжщфЖЩФ"}, "ABCABCЖЩФЖЩФ"},
      // 'é' has two Unicode representations: composed and decomposed. The
      // encodings for these representations are 'C3 A9' and '65 CC 81',
      // respectively. Similarly, the encodings for 'É' are 'C3 89' and
      // '45 CC 81', respectively.
      // UPPER() on STRING works on both composed and decomposed forms,
      // i.e. upper("réSumÉ") == "RÉSUMÉ".
      {"upper", {"re\xCC\x81SumE\xCC\x81"}, "RE\xCC\x81SUME\xCC\x81"},
      {"upper", {"r\xc3\xa9Sum\xC3\x89"}, "R\xc3\x89SUM\xc3\x89"},
      {"upper",
       {all_bytes_str.substr(0, 128)},
       all_bytes_uppercase.substr(0, 128)},

      // lower(string) -> string
      {"lower", {NullString()}, NullString()},
      {"lower", {""}, ""},
      // Ⱥ -> ⱥ (from 2 bytes to 3 bytes)
      {"lower", {"Ⱥ"}, "ⱥ"},  // Returns a longer string.

      {"lower", {"abcABCжщфЖЩФ"}, "abcabcжщфжщф"},
      // LOWER() on STRING works on both composed and decomposed forms,
      // i.e. lower("réSumÉ") == "résumé".
      {"lower", {"re\xCC\x81SumE\xCC\x81"}, "re\xCC\x81sume\xCC\x81"},
      {"lower", {"r\xc3\xa9SuM\xC3\x89"}, "r\xc3\xa9sum\xc3\xa9"},
      {"lower",
       {all_bytes_str.substr(0, 128)},
       all_bytes_lowercase.substr(0, 128)},

      // upper(bytes) -> bytes
      {"upper", {NullBytes()}, NullBytes()},
      {"upper", {Bytes("")}, Bytes("")},
      {"upper", {Bytes("abcABCжщфЖЩФ")}, Bytes("ABCABCжщфЖЩФ")},
      {"upper", {Bytes(all_bytes_str)}, Bytes(all_bytes_uppercase)},
      // The composed forms for 'é' and 'É' stay unchanged if passed into
      // UPPER() on BYTES. The decomposed forms become upper case.
      {"upper",
       {Bytes("re\xCC\x81SumE\xCC\x81")},
       Bytes("RE\xCC\x81SUME\xCC\x81")},
      {"upper", {Bytes("r\xc3\xa9Sum\xC3\x89")}, Bytes("R\xc3\xa9SUM\xc3\x89")},

      // lower(bytes) -> bytes
      {"lower", {NullBytes()}, NullBytes()},
      {"lower", {Bytes("")}, Bytes("")},
      {"lower", {Bytes("abcABCжщфЖЩФ")}, Bytes("abcabcжщфЖЩФ")},
      {"lower", {Bytes(all_bytes_str)}, Bytes(all_bytes_lowercase)},
      // The composed forms for 'é' and 'É' stay unchanged if passed into
      // LOWER() on BYTES. The decomposed forms become upper case.
      {"lower",
       {Bytes("re\xCC\x81SumE\xCC\x81")},
       Bytes("re\xCC\x81sume\xCC\x81")},
      {"lower", {Bytes("r\xc3\xa9SuM\xC3\x89")}, Bytes("r\xc3\xa9sum\xc3\x89")},

      // replace(string, string, string) -> string
      {"replace", {NullString(), "", ""}, NullString()},
      {"replace", {NullString(), NullString(), ""}, NullString()},
      {"replace", {NullString(), "", NullString()}, NullString()},
      {"replace", {NullString(), NullString(), NullString()}, NullString()},
      {"replace", {"", NullString(), ""}, NullString()},
      {"replace", {"", "", NullString()}, NullString()},
      {"replace", {"", NullString(), NullString()}, NullString()},
      {"replace", {"", "", ""}, ""},
      {"replace", {"", "a", ""}, ""},
      {"replace", {"", "", "a"}, ""},
      {"replace", {"abc", "", "xyz"}, "abc"},
      {"replace", {"abc", "", ""}, "abc"},
      {"replace", {"abc", "b", "xyz"}, "axyzc"},
      {"replace", {"abc", "b", ""}, "ac"},
      {"replace", {"abcabc", "bc", "xyz"}, "axyzaxyz"},
      {"replace", {"abc", "abc", "xyz"}, "xyz"},
      {"replace", {"abc", "abcde", "xyz"}, "abc"},
      {"replace", {"banana", "ana", "xyz"}, "bxyzna"},
      {"replace", {"banana", "ana", ""}, "bna"},
      {"replace", {"banana", "a", "z"}, "bznznz"},
      {"replace", {"щцфшцф", "ф", "ы"}, "щцышцы"},

      // replace(bytes, bytes, bytes) -> bytes
      {"replace", {NullBytes(), Bytes(""), Bytes("")}, NullBytes()},
      {"replace", {NullBytes(), Bytes(""), NullBytes()}, NullBytes()},
      {"replace", {NullBytes(), NullBytes(), Bytes("")}, NullBytes()},
      {"replace", {NullBytes(), NullBytes(), NullBytes()}, NullBytes()},
      {"replace", {Bytes(""), NullBytes(), Bytes("")}, NullBytes()},
      {"replace", {Bytes(""), Bytes(""), NullBytes()}, NullBytes()},
      {"replace", {Bytes(""), NullBytes(), NullBytes()}, NullBytes()},
      {"replace", {Bytes(""), Bytes(""), Bytes("")}, Bytes("")},
      {"replace", {Bytes(""), Bytes("a"), Bytes("")}, Bytes("")},
      {"replace", {Bytes(""), Bytes(""), Bytes("a")}, Bytes("")},
      {"replace", {Bytes("abc"), Bytes(""), Bytes("xyz")}, Bytes("abc")},
      {"replace", {Bytes("abc"), Bytes(""), Bytes("")}, Bytes("abc")},
      {"replace", {Bytes("abc"), Bytes("b"), Bytes("xyz")}, Bytes("axyzc")},
      {"replace", {Bytes("abc"), Bytes("b"), Bytes("")}, Bytes("ac")},
      {"replace",
       {Bytes("abcabc"), Bytes("bc"), Bytes("xyz")},
       Bytes("axyzaxyz")},
      {"replace", {Bytes("abc"), Bytes("abc"), Bytes("xyz")}, Bytes("xyz")},
      {"replace",
       {Bytes("banana"), Bytes("ana"), Bytes("xyz")},
       Bytes("bxyzna")},
      {"replace", {Bytes("banana"), Bytes("a"), Bytes("z")}, Bytes("bznznz")},
      {"replace", {Bytes("banana"), Bytes("ana"), Bytes("")}, Bytes("bna")},

      // safe_convert_bytes_to_string(bytes) -> string
      // Valid UTF-8 string remains unchanged.
      {"safe_convert_bytes_to_string", {NullBytes()}, NullString()},
      {"safe_convert_bytes_to_string", {Bytes("")}, String("")},
      {"safe_convert_bytes_to_string", {Bytes("abcABC")}, "abcABC"},
      {"safe_convert_bytes_to_string", {Bytes("abcABCжщфЖЩФ")}, "abcABCжщфЖЩФ"},
      {"safe_convert_bytes_to_string", {Bytes("абвгд")}, "абвгд"},
      {"safe_convert_bytes_to_string", {Bytes("₡")}, "₡"},
      {"safe_convert_bytes_to_string", {Bytes("𐌼")}, "𐌼"},
      {"safe_convert_bytes_to_string",
       {Bytes("𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡")},
       "𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡𐌼₡"},

  // Invalid UTF-8 characters are replaced with U+FFFD.
#define U_FFFD "\xef\xbf\xbd"
      // Invalid 1 octet.
      {"safe_convert_bytes_to_string", {Bytes("\xc2")}, U_FFFD},
      {"safe_convert_bytes_to_string", {Bytes("\xd0")}, U_FFFD},
      // Invalid 2 octet sequence.
      {"safe_convert_bytes_to_string", {Bytes("\xc3\x28")}, U_FFFD "("},
      {"safe_convert_bytes_to_string", {Bytes("\xa0\xa1")}, U_FFFD U_FFFD},
      // Invalid 3 octet sequence.
      {"safe_convert_bytes_to_string",
       {Bytes("\xe2\x28\xa1")},
       U_FFFD "(" U_FFFD},
      {"safe_convert_bytes_to_string",
       {Bytes("\xe2\x82\x28")},
       U_FFFD U_FFFD "("},
      // Valid 3 octet sequence followed by invalid continuation bytes.
      {"safe_convert_bytes_to_string",
       {Bytes("\xe0\xa4\x88\xe2\x28")},
       "ई" U_FFFD "("},
      // Invalid 4 octet sequence.
      {"safe_convert_bytes_to_string",
       {Bytes("\xf0\x28\x8c\xbc")},
       U_FFFD "(" U_FFFD U_FFFD},
      {"safe_convert_bytes_to_string",
       {Bytes("\xf0\x90\x28\xbc")},
       U_FFFD U_FFFD "(" U_FFFD},
      {"safe_convert_bytes_to_string",
       {Bytes("\xf0\x28\x8c\x28")},
       U_FFFD "(" U_FFFD "("},
      // Valid 5, 6 octet sequence but not Unicode (UTF-8 was restricted by
      // RFC 3629 to end at U+10FFFF).
      {"safe_convert_bytes_to_string",
       {Bytes("\xf8\xa1\xa1\xa1\xa1")},
       U_FFFD U_FFFD U_FFFD U_FFFD U_FFFD},
      {"safe_convert_bytes_to_string",
       {Bytes("\xfc\xa1\xa1\xa1\xa1\xa1")},
       U_FFFD U_FFFD U_FFFD U_FFFD U_FFFD U_FFFD},
      // Mix of valid and invalid chars.
      {"safe_convert_bytes_to_string",
       {Bytes("𐌼₡\xc2𐌼₡\xa0\xa1𐌼₡\xf0\x28\x8c\x28𐌼₡")},
       "𐌼₡" U_FFFD "𐌼₡" U_FFFD U_FFFD "𐌼₡" U_FFFD "(" U_FFFD "(𐌼₡"},
#undef U_FFFD
  };

  std::vector<FunctionTestCall> tests_substr = GetFunctionTestsSubstr();
  results.insert(results.end(), std::make_move_iterator(tests_substr.begin()),
                 std::make_move_iterator(tests_substr.end()));
  std::vector<FunctionTestCall> tests_concat = GetStringConcatTests();
  results.insert(results.end(), std::make_move_iterator(tests_concat.begin()),
                 std::make_move_iterator(tests_concat.end()));
  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsInstr() {
  std::vector<FunctionTestCall> results = {
      // instr(string, string) -> int64_t
      {"instr", {NullString(), ""}, NullInt64()},
      {"instr", {NullString(), "x"}, NullInt64()},
      {"instr", {"", NullString()}, NullInt64()},
      {"instr", {NullString(), NullString()}, NullInt64()},
      {"instr", {"", "x"}, 0ll},
      {"instr", {"x", ""}, 1ll},
      {"instr", {"x", NullString()}, NullInt64()},
      // Similar to STRPOS("", "") = 1
      {"instr", {"", ""}, 1ll},
      {"instr", {"xxx", "x"}, 1ll},
      {"instr", {"abcdef", "cd"}, 3ll},
      {"instr", {"abcdefabcdef", "de"}, 4ll},
      {"instr", {"abcdefabcdef", "xz"}, 0ll},
      {"instr", {"\0abcedf", "abc"}, 2ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c"}, 4ll},
      {"instr", {"zгдl", "дl"}, 3ll},

      // instr(bytes, bytes) -> int64_t
      {"instr", {NullBytes(), Bytes("")}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x")}, NullInt64()},
      {"instr", {Bytes(""), NullBytes()}, NullInt64()},
      {"instr", {NullBytes(), NullBytes()}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x")}, 0ll},
      {"instr", {Bytes("x"), Bytes("")}, 1ll},
      {"instr", {Bytes("x"), NullBytes()}, NullInt64()},
      {"instr", {Bytes(""), Bytes("")}, 1ll},
      {"instr", {Bytes("xxx"), Bytes("x")}, 1ll},
      {"instr", {Bytes("abcdef"), Bytes("cd")}, 3ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("de")}, 4ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz")}, 0ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc")}, 2ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c")}, 4ll},
      {"instr", {Bytes("zгдl"), Bytes("дl")}, 4ll},

      // instr(string, string, int64_t) -> int64_t
      {"instr", {NullString(), "", 1ll}, NullInt64()},
      {"instr", {NullString(), "x", 1ll}, NullInt64()},
      {"instr", {"", NullString(), 1ll}, NullInt64()},
      {"instr", {NullString(), NullString(), 1ll}, NullInt64()},
      {"instr", {"", "x", NullInt64()}, NullInt64()},
      {"instr", {"", "", NullInt64()}, NullInt64()},
      {"instr", {NullString(), "x", NullInt64()}, NullInt64()},
      {"instr", {NullString(), "", NullInt64()}, NullInt64()},
      {"instr", {"", NullString(), NullInt64()}, NullInt64()},
      {"instr", {"x", NullString(), NullInt64()}, NullInt64()},
      {"instr", {NullString(), NullString(), NullInt64()}, NullInt64()},
      {"instr", {"", "x", 0ll}, NullInt64(), OUT_OF_RANGE},
      {"instr", {"", "x", 1ll}, 0ll},
      {"instr", {"", "x", 2ll}, 0ll},
      {"instr", {"", "", 1ll}, 1ll},
      {"instr", {"", "", 2ll}, 0ll},
      {"instr", {"x", "", 1ll}, 1ll},
      // Since INSTR("", "") = 1 (position after the string's end), then
      // INSTR("x", "", 2) = 2 is valid.
      {"instr", {"x", "", 2ll}, 2ll},
      {"instr", {"x", "", 3ll}, 0ll},
      {"instr", {"xx", "", 1ll}, 1ll},
      {"instr", {"xx", "", 2ll}, 2ll},
      // Similarly, INSTR("xx", "", 3) = 3.
      {"instr", {"xx", "", 3ll}, 3ll},
      {"instr", {"xx", "", 4ll}, 0ll},
      {"instr", {"zгl", "", 1ll}, 1ll},
      {"instr", {"zгl", "", 2ll}, 2ll},
      {"instr", {"zгl", "", 3ll}, 3ll},
      {"instr", {"zгl", "", 4ll}, 4ll},
      {"instr", {"zгl", "", 5ll}, 0ll},
      {"instr", {"xxx", "x", 1ll}, 1ll},
      {"instr", {"xxx", "x", 2ll}, 2ll},
      {"instr", {"xxx", "x", 3ll}, 3ll},
      {"instr", {"xxx", "x", 4ll}, 0ll},
      {"instr", {"xxx", "x", int64max}, 0ll},
      {"instr", {"abcdef", "cd", 1ll}, 3ll},
      {"instr", {"abcdef", "cd", 3ll}, 3ll},
      {"instr", {"abcdef", "cd", 4ll}, 0ll},
      {"instr", {"abcdef", "cd", 8ll}, 0ll},
      {"instr", {"abcdef", "cd", int64max}, 0ll},
      {"instr", {"abcdefabcdef", "xz", 1ll}, 0ll},
      {"instr", {"abcdefabcdef", "xz", 30ll}, 0ll},
      {"instr", {"\0abcedf", "abc", 1ll}, 2ll},
      {"instr", {"\0abcedf", "abc", 2ll}, 2ll},
      {"instr", {"\0abcedf", "abc", 3ll}, 0ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", 3ll}, 4ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", 4ll}, 4ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", 5ll}, 0ll},
      {"instr", {"zгдl", "дl", 1ll}, 3ll},
      {"instr", {"zгдl", "дl", 3ll}, 3ll},
      {"instr", {"zгдl", "дl", 4ll}, 0ll},

      {"instr", {NullString(), "", -1ll}, NullInt64()},
      {"instr", {NullString(), "x", -1ll}, NullInt64()},
      {"instr", {"", NullString(), -1ll}, NullInt64()},
      {"instr", {NullString(), NullString(), -1ll}, NullInt64()},
      {"instr", {"", "x", -1ll}, 0ll},
      {"instr", {"", "x", -2ll}, 0ll},
      // When counting from the end, the first occurrence of "" is after the
      // string's end.
      {"instr", {"x", "", -1ll}, 2ll},
      {"instr", {"x", "", -2ll}, 1ll},
      {"instr", {"x", "", -3ll}, 0ll},
      {"instr", {"xx", "", -1ll}, 3ll},
      {"instr", {"xx", "", -2ll}, 2ll},
      {"instr", {"xx", "", -3ll}, 1ll},
      {"instr", {"xx", "", -4ll}, 0ll},
      {"instr", {"zгl", "", -1ll}, 4ll},
      {"instr", {"zгl", "", -2ll}, 3ll},
      {"instr", {"zгl", "", -3ll}, 2ll},
      {"instr", {"zгl", "", -4ll}, 1ll},
      {"instr", {"zгl", "", -5ll}, 0ll},
      {"instr", {"", "", -1ll}, 1ll},
      {"instr", {"", "", -2ll}, 0ll},
      {"instr", {"xxx", "x", -1ll}, 3ll},
      {"instr", {"xxx", "x", -2ll}, 2ll},
      {"instr", {"xxx", "x", -3ll}, 1ll},
      {"instr", {"xxx", "x", -4ll}, 0ll},
      {"instr", {"xxx", "x", int64min}, 0ll},
      {"instr", {"abcdef", "cd", -1ll}, 3ll},
      {"instr", {"abcdef", "cd", -3ll}, 3ll},
      {"instr", {"abcdef", "cd", -4ll}, 3ll},
      {"instr", {"abcdef", "cd", -5ll}, 0ll},
      {"instr", {"abcdef", "cd", int64min}, 0ll},
      {"instr", {"abcdefabcdef", "xz", -1ll}, 0ll},
      {"instr", {"abcdefabcdef", "xz", -30ll}, 0ll},
      {"instr", {"\0abcedf", "abc", -1ll}, 2ll},
      {"instr", {"\0abcedf", "abc", -5ll}, 2ll},
      {"instr", {"\0abcedf", "abc", -7ll}, 0ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -1ll}, 4ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -2ll}, 4ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -6ll}, 4ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -7ll}, 0ll},
      {"instr", {"zгдlг", "дl", -1ll}, 3ll},
      {"instr", {"zгдlг", "дl", -2ll}, 3ll},
      {"instr", {"zгдlг", "дl", -3ll}, 3ll},
      {"instr", {"zгдlг", "дl", -4ll}, 0ll},

      // instr(bytes, bytes, int64_t) -> int64_t
      {"instr", {NullBytes(), Bytes(""), 1ll}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x"), 1ll}, NullInt64()},
      {"instr", {Bytes(""), NullBytes(), 1ll}, NullInt64()},
      {"instr", {NullBytes(), NullBytes(), 1ll}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), NullInt64()}, NullInt64()},
      {"instr", {Bytes(""), Bytes(""), NullInt64()}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x"), NullInt64()}, NullInt64()},
      {"instr", {NullBytes(), Bytes(""), NullInt64()}, NullInt64()},
      {"instr", {Bytes(""), NullBytes(), NullInt64()}, NullInt64()},
      {"instr", {Bytes("x"), NullBytes(), NullInt64()}, NullInt64()},
      {"instr", {NullBytes(), NullBytes(), NullInt64()}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), 0ll}, NullInt64(), OUT_OF_RANGE},
      {"instr", {Bytes(""), Bytes("x"), 1ll}, 0ll},
      {"instr", {Bytes(""), Bytes("x"), 2ll}, 0ll},
      {"instr", {Bytes("x"), Bytes(""), 1ll}, 1ll},
      {"instr", {Bytes("x"), Bytes(""), 2ll}, 2ll},
      {"instr", {Bytes("x"), Bytes(""), 3ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), 1ll}, 1ll},
      {"instr", {Bytes("xx"), Bytes(""), 2ll}, 2ll},
      {"instr", {Bytes("xx"), Bytes(""), 3ll}, 3ll},
      {"instr", {Bytes("xx"), Bytes(""), 4ll}, 0ll},
      {"instr", {Bytes("zгl"), Bytes(""), 1ll}, 1ll},
      {"instr", {Bytes("zгl"), Bytes(""), 2ll}, 2ll},
      {"instr", {Bytes("zгl"), Bytes(""), 3ll}, 3ll},
      {"instr", {Bytes("zгl"), Bytes(""), 4ll}, 4ll},
      {"instr", {Bytes("zгl"), Bytes(""), 5ll}, 5ll},
      {"instr", {Bytes("zгl"), Bytes(""), 6ll}, 0ll},
      {"instr", {Bytes(""), Bytes(""), 1ll}, 1ll},
      {"instr", {Bytes(""), Bytes(""), 2ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 1ll}, 1ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 2ll}, 2ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 3ll}, 3ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 4ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), int64max}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 1ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 3ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 4ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 8ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), int64max}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), 1ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), 30ll}, 0ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), 1ll}, 2ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), 2ll}, 2ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), 3ll}, 0ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), 3ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), 4ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), 5ll}, 0ll},
      {"instr", {Bytes("zгдl"), Bytes("дl"), 1ll}, 4ll},
      {"instr", {Bytes("zгдl"), Bytes("дl"), 4ll}, 4ll},
      {"instr", {Bytes("zгдl"), Bytes("дl"), 5ll}, 0ll},

      {"instr", {NullBytes(), Bytes(""), -1ll}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x"), -1ll}, NullInt64()},
      {"instr", {Bytes(""), NullBytes(), -1ll}, NullInt64()},
      {"instr", {NullBytes(), NullBytes(), -1ll}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), -1ll}, 0ll},
      {"instr", {Bytes(""), Bytes("x"), -2ll}, 0ll},
      {"instr", {Bytes("x"), Bytes(""), -1ll}, 2ll},
      {"instr", {Bytes("x"), Bytes(""), -2ll}, 1ll},
      {"instr", {Bytes("x"), Bytes(""), -3ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), -1ll}, 3ll},
      {"instr", {Bytes("xx"), Bytes(""), -2ll}, 2ll},
      {"instr", {Bytes("xx"), Bytes(""), -3ll}, 1ll},
      {"instr", {Bytes("xx"), Bytes(""), -4ll}, 0ll},
      {"instr", {Bytes("zгl"), Bytes(""), -1ll}, 5ll},
      {"instr", {Bytes("zгl"), Bytes(""), -2ll}, 4ll},
      {"instr", {Bytes("zгl"), Bytes(""), -3ll}, 3ll},
      {"instr", {Bytes("zгl"), Bytes(""), -4ll}, 2ll},
      {"instr", {Bytes("zгl"), Bytes(""), -5ll}, 1ll},
      {"instr", {Bytes("zгl"), Bytes(""), -6ll}, 0ll},
      {"instr", {Bytes(""), Bytes(""), -1ll}, 1ll},
      {"instr", {Bytes(""), Bytes(""), -2ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -1ll}, 3ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -2ll}, 2ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -3ll}, 1ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -4ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), int64min}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -1ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -3ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -4ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -5ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), int64min}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), -1ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), -30ll}, 0ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), -1ll}, 2ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), -4ll}, 2ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), -5ll}, 2ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), -6ll}, 2ll},
      {"instr", {Bytes("\0abcedf"), Bytes("abc"), -7ll}, 0ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -1ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -2ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -3ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -5ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -6ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -7ll}, 0ll},
      {"instr", {Bytes("zгдlг"), Bytes("дl"), -1ll}, 4ll},
      {"instr", {Bytes("zгдlг"), Bytes("дl"), -2ll}, 4ll},
      {"instr", {Bytes("zгдlг"), Bytes("дl"), -3ll}, 4ll},
      {"instr", {Bytes("zгдlг"), Bytes("дl"), -4ll}, 4ll},
      {"instr", {Bytes("zгдlг"), Bytes("дl"), -5ll}, 4ll},
      {"instr", {Bytes("zгдlг"), Bytes("дl"), -6ll}, 0ll},

      // instr(string, string, int64_t, int64_t) -> int64_t
      {"instr", {NullString(), "", 1ll, 1ll}, NullInt64()},
      {"instr", {NullString(), "x", 1ll, 1ll}, NullInt64()},
      {"instr", {"", NullString(), 1ll, 1ll}, NullInt64()},
      {"instr", {NullString(), NullString(), 1ll, 1ll}, NullInt64()},
      {"instr", {"", "x", NullInt64(), 1ll}, NullInt64()},
      {"instr", {"", "", NullInt64(), 1ll}, NullInt64()},
      {"instr", {NullString(), "x", NullInt64(), 1ll}, NullInt64()},
      {"instr", {NullString(), "", NullInt64(), 1ll}, NullInt64()},
      {"instr", {"", NullString(), NullInt64(), 1ll}, NullInt64()},
      {"instr", {"x", NullString(), NullInt64(), 1ll}, NullInt64()},
      {"instr", {NullString(), NullString(), NullInt64(), 1ll}, NullInt64()},
      {"instr", {"", "x", 0ll, 1ll}, NullInt64(), OUT_OF_RANGE},
      {"instr", {NullString(), "", 1ll, NullInt64()}, NullInt64()},
      {"instr", {NullString(), "x", 1ll, NullInt64()}, NullInt64()},
      {"instr", {"", NullString(), 1ll, NullInt64()}, NullInt64()},
      {"instr", {NullString(), NullString(), 1ll, NullInt64()}, NullInt64()},
      {"instr", {"", "x", NullInt64(), NullInt64()}, NullInt64()},
      {"instr", {"", "", NullInt64(), NullInt64()}, NullInt64()},
      {"instr", {NullString(), "x", NullInt64(), NullInt64()}, NullInt64()},
      {"instr", {NullString(), "", NullInt64(), NullInt64()}, NullInt64()},
      {"instr", {"", NullString(), NullInt64(), NullInt64()}, NullInt64()},
      {"instr", {"x", NullString(), NullInt64(), NullInt64()}, NullInt64()},
      {"instr",
       {NullString(), NullString(), NullInt64(), NullInt64()},
       NullInt64()},
      {"instr", {"x", "x", 1ll, NullInt64()}, NullInt64()},
      {"instr", {"", "x", 1ll, 0ll}, NullInt64(), OUT_OF_RANGE},
      {"instr", {"", "x", 1ll, 1ll}, 0ll},
      {"instr", {"", "x", 1ll, 2ll}, 0ll},
      {"instr", {"x", "", 1ll, 1ll}, 1ll},
      {"instr", {"x", "", 1ll, 2ll}, 2ll},
      {"instr", {"x", "", 1ll, 3ll}, 0ll},
      {"instr", {"x", "", 2ll, 1ll}, 2ll},
      {"instr", {"x", "", 2ll, 2ll}, 0ll},
      {"instr", {"xx", "", 1ll, 1ll}, 1ll},
      {"instr", {"xx", "", 1ll, 2ll}, 2ll},
      {"instr", {"xx", "", 1ll, 3ll}, 3ll},
      {"instr", {"xx", "", 1ll, 4ll}, 0ll},
      {"instr", {"xx", "", 2ll, 1ll}, 2ll},
      {"instr", {"xx", "", 2ll, 2ll}, 3ll},
      {"instr", {"xx", "", 2ll, 3ll}, 0ll},
      {"instr", {"xx", "", 3ll, 1ll}, 3ll},
      {"instr", {"xx", "", 3ll, 2ll}, 0ll},
      {"instr", {"zгl", "", 1ll, 1ll}, 1ll},
      {"instr", {"zгl", "", 1ll, 2ll}, 2ll},
      {"instr", {"zгl", "", 1ll, 3ll}, 3ll},
      {"instr", {"zгl", "", 1ll, 4ll}, 4ll},
      {"instr", {"zгl", "", 1ll, 5ll}, 0ll},
      {"instr", {"", "", 1ll, 1ll}, 1ll},
      {"instr", {"", "", 1ll, 2ll}, 0ll},
      {"instr", {"xxx", "x", 1ll, 1ll}, 1ll},
      {"instr", {"xxx", "x", 1ll, 2ll}, 2ll},
      {"instr", {"xxx", "x", 1ll, 3ll}, 3ll},
      {"instr", {"xxx", "x", 1ll, 4ll}, 0ll},
      {"instr", {"xxx", "x", 2ll, 1ll}, 2ll},
      {"instr", {"xxx", "x", 2ll, 2ll}, 3ll},
      {"instr", {"xxx", "x", 2ll, 3ll}, 0ll},
      {"instr", {"xxx", "x", 3ll, 1ll}, 3ll},
      {"instr", {"xxx", "x", 3ll, 2ll}, 0ll},
      {"instr", {"xxx", "x", 1ll, int64max}, 0ll},
      {"instr", {"xxx", "x", 2ll, int64max}, 0ll},
      {"instr", {"abcdef", "cd", 1ll, 1ll}, 3ll},
      {"instr", {"abcdef", "cd", 1ll, 2ll}, 0ll},
      {"instr", {"abcdef", "cd", 3ll, 1ll}, 3ll},
      {"instr", {"abcdef", "cd", 3ll, 2ll}, 0ll},
      {"instr", {"abcdef", "cd", 3ll, 5ll}, 0ll},
      {"instr", {"abcdef", "cd", int64max, int64max}, 0ll},
      {"instr", {"abcdefabcdef", "xz", 1ll, 1ll}, 0ll},
      {"instr", {"abcdefabcdef", "xz", 1ll, 5ll}, 0ll},
      {"instr", {"abcdefabcdef", "cd", 1ll, 1ll}, 3ll},
      {"instr", {"abcdefabcdef", "cd", 1ll, 2ll}, 9ll},
      {"instr", {"abcdefabcdef", "cd", 4ll, 1ll}, 9ll},
      {"instr", {"abcdefabcdef", "cd", 4ll, 2ll}, 0ll},
      {"instr", {"aaabbaaa", "aa", 1ll, 1ll}, 1ll},
      {"instr", {"aaabbaaa", "aa", 1ll, 2ll}, 2ll},
      {"instr", {"aaabbaaa", "aa", 1ll, 3ll}, 6ll},
      {"instr", {"aaabbaaa", "aa", 1ll, 4ll}, 7ll},
      {"instr", {"aaabbaaa", "aa", 1ll, 5ll}, 0ll},
      {"instr", {"aaabbaaa", "aa", 2ll, 1ll}, 2ll},
      {"instr", {"aaabbaaa", "aa", 2ll, 2ll}, 6ll},
      {"instr", {"aaabbaaa", "aa", 7ll, 1ll}, 7ll},
      {"instr", {"aaabbaaa", "aa", 7ll, 2ll}, 0ll},
      {"instr", {"\0abce\0dabcf", "abc", 1ll, 1ll}, 2ll},
      {"instr", {"\0abce\0dabcf", "abc", 1ll, 2ll}, 8ll},
      {"instr", {"\0abce\0dabcf", "abc", 2ll, 1ll}, 2ll},
      {"instr", {"\0abce\0dabcf", "abc", 2ll, 2ll}, 8ll},
      {"instr", {"\0abce\0dabcf", "abc", 5ll, 1ll}, 8ll},
      {"instr", {"\0abce\0dabcf", "abc", 5ll, 2ll}, 0ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", 3ll, 1ll}, 4ll},
      {"instr", {"zгдlгдl", "дl", 1ll, 1ll}, 3ll},
      {"instr", {"zгдlгдl", "дl", 1ll, 2ll}, 6ll},
      {"instr", {"zгдlгдl", "дl", 1ll, 3ll}, 0ll},
      {"instr", {"zгдlгдl", "дl", 4ll, 1ll}, 6ll},
      {"instr", {"zгдlгдl", "дl", 4ll, 2ll}, 0ll},

      {"instr", {NullString(), "", -1ll, 1ll}, NullInt64()},
      {"instr", {NullString(), "x", -1ll, 1ll}, NullInt64()},
      {"instr", {"", NullString(), -1ll, 1ll}, NullInt64()},
      {"instr", {NullString(), NullString(), -1ll, 1ll}, NullInt64()},
      {"instr", {"", "x", -1ll, 1ll}, 0ll},
      {"instr", {"", "x", -1ll, 2ll}, 0ll},
      {"instr", {"", "x", -2ll, 1ll}, 0ll},
      {"instr", {"", "x", -2ll, 2ll}, 0ll},
      {"instr", {"x", "", -1ll, 1ll}, 2ll},
      {"instr", {"x", "", -1ll, 2ll}, 1ll},
      {"instr", {"x", "", -2ll, 1ll}, 1ll},
      {"instr", {"x", "", -2ll, 2ll}, 0ll},
      {"instr", {"xx", "", -1ll, 1ll}, 3ll},
      {"instr", {"xx", "", -1ll, 2ll}, 2ll},
      {"instr", {"xx", "", -1ll, 3ll}, 1ll},
      {"instr", {"xx", "", -1ll, 4ll}, 0ll},
      {"instr", {"xx", "", -2ll, 1ll}, 2ll},
      {"instr", {"xx", "", -2ll, 2ll}, 1ll},
      {"instr", {"xx", "", -2ll, 3ll}, 0ll},
      {"instr", {"xx", "", -3ll, 1ll}, 1ll},
      {"instr", {"xx", "", -3ll, 2ll}, 0ll},
      {"instr", {"", "", -1ll, 1ll}, 1ll},
      {"instr", {"", "", -1ll, 2ll}, 0ll},
      {"instr", {"xxx", "x", -1ll, 1ll}, 3ll},
      {"instr", {"xxx", "x", -1ll, 2ll}, 2ll},
      {"instr", {"xxx", "x", -1ll, 3ll}, 1ll},
      {"instr", {"xxx", "x", -1ll, 4ll}, 0ll},
      {"instr", {"xxx", "x", -2ll, 1ll}, 2ll},
      {"instr", {"xxx", "x", -2ll, 2ll}, 1ll},
      {"instr", {"xxx", "x", -2ll, 3ll}, 0ll},
      {"instr", {"xxx", "x", -3ll, 1ll}, 1ll},
      {"instr", {"xxx", "x", -3ll, 2ll}, 0ll},
      {"instr", {"xxx", "x", -4ll, 1ll}, 0ll},
      {"instr", {"xxx", "x", int64min, 1ll}, 0ll},
      {"instr", {"abcdef", "cd", -1ll, 1ll}, 3ll},
      {"instr", {"abcdef", "cd", -1ll, 2ll}, 0ll},
      {"instr", {"abcdef", "cd", -3ll, 1ll}, 3ll},
      {"instr", {"abcdef", "cd", -3ll, 2ll}, 0ll},
      {"instr", {"abcdef", "cd", -4ll, 1ll}, 3ll},
      {"instr", {"abcdef", "cd", -4ll, 2ll}, 0ll},
      {"instr", {"abcdef", "cd", -5ll, 1ll}, 0ll},
      {"instr", {"abcdef", "cd", -5ll, 5ll}, 0ll},
      {"instr", {"abcdefabcdef", "xz", -1ll, 1ll}, 0ll},
      {"instr", {"abcdefabcdef", "xz", -1ll, 2ll}, 0ll},
      {"instr", {"abcdefabcdef", "xz", -30ll, 1ll}, 0ll},
      {"instr", {"abcdefabcdef", "xz", -30ll, 2ll}, 0ll},
      {"instr", {"abcdefabcdef", "cd", -1ll, 1ll}, 9ll},
      {"instr", {"abcdefabcdef", "cd", -1ll, 2ll}, 3ll},
      {"instr", {"abcdefabcdef", "cd", -4ll, 1ll}, 9ll},
      {"instr", {"abcdefabcdef", "cd", -4ll, 2ll}, 3ll},
      {"instr", {"abcdefabcdef", "cd", -5ll, 1ll}, 3ll},
      {"instr", {"abcdefabcdef", "cd", -5ll, 2ll}, 0ll},
      {"instr", {"aaabbaaa", "aa", -1ll, 1ll}, 7ll},
      {"instr", {"aaabbaaa", "aa", -1ll, 2ll}, 6ll},
      {"instr", {"aaabbaaa", "aa", -1ll, 3ll}, 2ll},
      {"instr", {"aaabbaaa", "aa", -1ll, 4ll}, 1ll},
      {"instr", {"aaabbaaa", "aa", -1ll, 5ll}, 0ll},
      {"instr", {"aaabbaaa", "aa", -2ll, 1ll}, 7ll},
      {"instr", {"aaabbaaa", "aa", -2ll, 2ll}, 6ll},
      {"instr", {"aaabbaaa", "aa", -7ll, 1ll}, 2ll},
      {"instr", {"aaabbaaa", "aa", -7ll, 2ll}, 1ll},
      {"instr", {"aaabbaaa", "aa", -8ll, 1ll}, 1ll},
      {"instr", {"aaabbaaa", "aa", -8ll, 2ll}, 0ll},
      {"instr", {"\0abce\0dabcf", "abc", -1ll, 1ll}, 8ll},
      {"instr", {"\0abce\0dabcf", "abc", -1ll, 2ll}, 2ll},
      {"instr", {"\0abce\0dabcf", "abc", -3ll, 1ll}, 8ll},
      {"instr", {"\0abce\0dabcf", "abc", -3ll, 2ll}, 2ll},
      {"instr", {"\0abce\0dabcf", "abc", -5ll, 1ll}, 2ll},
      {"instr", {"\0abce\0dabcf", "abc", -5ll, 2ll}, 0ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -1ll, 1ll}, 4ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -1ll, 2ll}, 0ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -6ll, 1ll}, 4ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -6ll, 2ll}, 0ll},
      {"instr", {"abca\0b\0c\0", "a\0b\0c", -7ll, 1ll}, 0ll},
      {"instr", {"zгдlгдl", "дl", -1ll, 1ll}, 6ll},
      {"instr", {"zгдlгдl", "дl", -1ll, 2ll}, 3ll},
      {"instr", {"zгдlгдl", "дl", -1ll, 3ll}, 0ll},
      {"instr", {"zгдlгдl", "дl", -2ll, 1ll}, 6ll},
      {"instr", {"zгдlгдl", "дl", -2ll, 2ll}, 3ll},
      {"instr", {"zгдlгдl", "дl", -3ll, 1ll}, 3ll},
      {"instr", {"zгдlгдl", "дl", -3ll, 2ll}, 0ll},
      {"instr", {"zгдlгдl", "дl", -5ll, 1ll}, 3ll},

      // instr(bytes, bytes, int64_t, int64_t) -> int64_t
      {"instr", {NullBytes(), Bytes(""), 1ll, 1ll}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x"), 1ll, 1ll}, NullInt64()},
      {"instr", {Bytes(""), NullBytes(), 1ll, 1ll}, NullInt64()},
      {"instr", {NullBytes(), NullBytes(), 1ll, 1ll}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), NullInt64(), 1ll}, NullInt64()},
      {"instr", {Bytes(""), Bytes(""), NullInt64(), 1ll}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x"), NullInt64(), 1ll}, NullInt64()},
      {"instr", {NullBytes(), Bytes(""), NullInt64(), 1ll}, NullInt64()},
      {"instr", {Bytes(""), NullBytes(), NullInt64(), 1ll}, NullInt64()},
      {"instr", {Bytes("x"), NullBytes(), NullInt64(), 1ll}, NullInt64()},
      {"instr", {NullBytes(), NullBytes(), NullInt64(), 1ll}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), 0ll, 1ll}, NullInt64(), OUT_OF_RANGE},
      {"instr", {NullBytes(), Bytes(""), 1ll, NullInt64()}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x"), 1ll, NullInt64()}, NullInt64()},
      {"instr", {Bytes(""), NullBytes(), 1ll, NullInt64()}, NullInt64()},
      {"instr", {NullBytes(), NullBytes(), 1ll, NullInt64()}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), NullInt64(), NullInt64()}, NullInt64()},
      {"instr", {Bytes(""), Bytes(""), NullInt64(), NullInt64()}, NullInt64()},
      {"instr",
       {NullBytes(), Bytes("x"), NullInt64(), NullInt64()},
       NullInt64()},
      {"instr",
       {NullBytes(), Bytes(""), NullInt64(), NullInt64()},
       NullInt64()},
      {"instr",
       {Bytes(""), NullBytes(), NullInt64(), NullInt64()},
       NullInt64()},
      {"instr",
       {Bytes("x"), NullBytes(), NullInt64(), NullInt64()},
       NullInt64()},
      {"instr",
       {NullBytes(), NullBytes(), NullInt64(), NullInt64()},
       NullInt64()},
      {"instr", {Bytes("x"), Bytes("x"), 1ll, NullInt64()}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), 1ll, 0ll}, NullInt64(), OUT_OF_RANGE},
      {"instr", {Bytes(""), Bytes("x"), 1ll, 1ll}, 0ll},
      {"instr", {Bytes(""), Bytes("x"), 1ll, 2ll}, 0ll},
      {"instr", {Bytes("x"), Bytes(""), 1ll, 1ll}, 1ll},
      {"instr", {Bytes("x"), Bytes(""), 1ll, 2ll}, 2ll},
      {"instr", {Bytes("x"), Bytes(""), 1ll, 3ll}, 0ll},
      {"instr", {Bytes("x"), Bytes(""), 2ll, 1ll}, 2ll},
      {"instr", {Bytes("x"), Bytes(""), 2ll, 2ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), 1ll, 1ll}, 1ll},
      {"instr", {Bytes("xx"), Bytes(""), 1ll, 2ll}, 2ll},
      {"instr", {Bytes("xx"), Bytes(""), 1ll, 3ll}, 3ll},
      {"instr", {Bytes("xx"), Bytes(""), 1ll, 4ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), 2ll, 1ll}, 2ll},
      {"instr", {Bytes("xx"), Bytes(""), 2ll, 2ll}, 3ll},
      {"instr", {Bytes("xx"), Bytes(""), 2ll, 3ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), 3ll, 1ll}, 3ll},
      {"instr", {Bytes("xx"), Bytes(""), 3ll, 2ll}, 0ll},
      {"instr", {Bytes("zгl"), Bytes(""), 1ll, 1ll}, 1ll},
      {"instr", {Bytes("zгl"), Bytes(""), 1ll, 2ll}, 2ll},
      {"instr", {Bytes("zгl"), Bytes(""), 1ll, 3ll}, 3ll},
      {"instr", {Bytes("zгl"), Bytes(""), 1ll, 4ll}, 4ll},
      {"instr", {Bytes("zгl"), Bytes(""), 1ll, 5ll}, 5ll},
      {"instr", {Bytes("zгl"), Bytes(""), 1ll, 6ll}, 0ll},
      {"instr", {Bytes(""), Bytes(""), 1ll, 1ll}, 1ll},
      {"instr", {Bytes(""), Bytes(""), 1ll, 2ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 1ll, 1ll}, 1ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 1ll, 2ll}, 2ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 1ll, 3ll}, 3ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 1ll, 4ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 2ll, 1ll}, 2ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 2ll, 2ll}, 3ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 2ll, 3ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 3ll, 1ll}, 3ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 3ll, 2ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 1ll, int64max}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), 2ll, int64max}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 1ll, 1ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 1ll, 2ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 3ll, 1ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 3ll, 2ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), 3ll, 5ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), int64max, int64max}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), 1ll, 1ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), 1ll, 5ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), 1ll, 1ll}, 3ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), 1ll, 2ll}, 9ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), 4ll, 1ll}, 9ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), 4ll, 2ll}, 0ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 1ll, 1ll}, 1ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 1ll, 2ll}, 2ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 1ll, 3ll}, 6ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 1ll, 4ll}, 7ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 1ll, 5ll}, 0ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 2ll, 1ll}, 2ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 2ll, 2ll}, 6ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 7ll, 1ll}, 7ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), 7ll, 2ll}, 0ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), 1ll, 1ll}, 2ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), 1ll, 2ll}, 8ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), 2ll, 1ll}, 2ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), 2ll, 2ll}, 8ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), 5ll, 1ll}, 8ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), 5ll, 2ll}, 0ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), 3ll, 1ll}, 4ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), 1ll, 1ll}, 4ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), 1ll, 2ll}, 9ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), 1ll, 3ll}, 0ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), 5ll, 1ll}, 9ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), 5ll, 2ll}, 0ll},

      {"instr", {NullBytes(), Bytes(""), -1ll, 1ll}, NullInt64()},
      {"instr", {NullBytes(), Bytes("x"), -1ll, 1ll}, NullInt64()},
      {"instr", {Bytes(""), NullBytes(), -1ll, 1ll}, NullInt64()},
      {"instr", {NullBytes(), NullBytes(), -1ll, 1ll}, NullInt64()},
      {"instr", {Bytes(""), Bytes("x"), -1ll, 1ll}, 0ll},
      {"instr", {Bytes(""), Bytes("x"), -1ll, 2ll}, 0ll},
      {"instr", {Bytes(""), Bytes("x"), -2ll, 1ll}, 0ll},
      {"instr", {Bytes(""), Bytes("x"), -2ll, 2ll}, 0ll},
      {"instr", {Bytes("x"), Bytes(""), -1ll, 1ll}, 2ll},
      {"instr", {Bytes("x"), Bytes(""), -1ll, 2ll}, 1ll},
      {"instr", {Bytes("x"), Bytes(""), -2ll, 1ll}, 1ll},
      {"instr", {Bytes("x"), Bytes(""), -2ll, 2ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), -1ll, 1ll}, 3ll},
      {"instr", {Bytes("xx"), Bytes(""), -1ll, 2ll}, 2ll},
      {"instr", {Bytes("xx"), Bytes(""), -1ll, 3ll}, 1ll},
      {"instr", {Bytes("xx"), Bytes(""), -1ll, 4ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), -2ll, 1ll}, 2ll},
      {"instr", {Bytes("xx"), Bytes(""), -2ll, 2ll}, 1ll},
      {"instr", {Bytes("xx"), Bytes(""), -2ll, 3ll}, 0ll},
      {"instr", {Bytes("xx"), Bytes(""), -3ll, 1ll}, 1ll},
      {"instr", {Bytes("xx"), Bytes(""), -3ll, 2ll}, 0ll},
      {"instr", {Bytes(""), Bytes(""), -1ll, 1ll}, 1ll},
      {"instr", {Bytes(""), Bytes(""), -1ll, 2ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -1ll, 1ll}, 3ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -1ll, 2ll}, 2ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -1ll, 3ll}, 1ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -1ll, 4ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -2ll, 1ll}, 2ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -2ll, 2ll}, 1ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -2ll, 3ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -3ll, 1ll}, 1ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -3ll, 2ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), -4ll, 1ll}, 0ll},
      {"instr", {Bytes("xxx"), Bytes("x"), int64min, 1ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -1ll, 1ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -1ll, 2ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -3ll, 1ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -3ll, 2ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -4ll, 1ll}, 3ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -4ll, 2ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -5ll, 1ll}, 0ll},
      {"instr", {Bytes("abcdef"), Bytes("cd"), -5ll, 5ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), -1ll, 1ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), -1ll, 2ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), -30ll, 1ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("xz"), -30ll, 2ll}, 0ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), -1ll, 1ll}, 9ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), -1ll, 2ll}, 3ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), -4ll, 1ll}, 9ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), -4ll, 2ll}, 3ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), -5ll, 1ll}, 3ll},
      {"instr", {Bytes("abcdefabcdef"), Bytes("cd"), -5ll, 2ll}, 0ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -1ll, 1ll}, 7ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -1ll, 2ll}, 6ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -1ll, 3ll}, 2ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -1ll, 4ll}, 1ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -1ll, 5ll}, 0ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -2ll, 1ll}, 7ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -2ll, 2ll}, 6ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -7ll, 1ll}, 2ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -7ll, 2ll}, 1ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -8ll, 1ll}, 1ll},
      {"instr", {Bytes("aaabbaaa"), Bytes("aa"), -8ll, 2ll}, 0ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -1ll, 1ll}, 8ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -1ll, 2ll}, 2ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -2ll, 1ll}, 8ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -2ll, 2ll}, 2ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -3ll, 1ll}, 8ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -3ll, 2ll}, 2ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -5ll, 1ll}, 2ll},
      {"instr", {Bytes("\0abce\0dabcf"), Bytes("abc"), -5ll, 2ll}, 0ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -1ll, 1ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -1ll, 2ll}, 0ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -2ll, 1ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -2ll, 2ll}, 0ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -4ll, 1ll}, 4ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -4ll, 2ll}, 0ll},
      {"instr", {Bytes("abca\0b\0c\0"), Bytes("a\0b\0c"), -7ll, 1ll}, 0ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -1ll, 1ll}, 9ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -1ll, 2ll}, 4ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -1ll, 3ll}, 0ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -2ll, 1ll}, 9ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -3ll, 1ll}, 9ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -4ll, 1ll}, 4ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -6ll, 1ll}, 4ll},
      {"instr", {Bytes("zгдlгдl"), Bytes("дl"), -6ll, 2ll}, 0ll}};

  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsSoundex() {
  std::vector<FunctionTestCall> results = {
      // soundex(string) -> string
      {"soundex", {NullString()}, NullString()},
      {"soundex", {""}, ""},
      // Invalid characters are skipped.
      {"soundex", {" "}, ""},
      {"soundex", {"  \t \n 78912!(@#&   "}, ""},
      {"soundex", {"A"}, "A000"},
      {"soundex", {"Aa"}, "A000"},
      {"soundex", {"AaAaa a123aA"}, "A000"},
      // Test that all alpha character maps to the correct value.
      {"soundex", {"AEIOUYHWaeiouyhw"}, "A000"},
      {"soundex", {"ABFPVbfpv"}, "A100"},
      {"soundex", {"ACGJKQSXZcgjkqsxz"}, "A200"},
      {"soundex", {"ADTdt"}, "A300"},
      {"soundex", {"ALl"}, "A400"},
      {"soundex", {"AMNmn"}, "A500"},
      {"soundex", {"ARr"}, "A600"},
      // H, W are considered vowels and are skipped.
      {"soundex", {"AAWIOUYHWB"}, "A100"},
      {"soundex", {"KhcABl"}, "K140"},
      // Second c is skipped because it has the same code as K.
      {"soundex", {"KcABl"}, "K140"},
      // c is skipped because the precedent non-skipped code is the same (s).
      {"soundex", {"Ashcraft"}, "A261"},
      {"soundex", {"Robert"}, "R163"},
      {"soundex", {"Rupert"}, "R163"},
      {"soundex", {"I LOVE YOU"}, "I410"},
      {"soundex", {"I LOVE YOU TOO"}, "I413"},
      {"soundex", {"ACDL"}, "A234"},
      {"soundex", {"3  ACDL"}, "A234"},
      {"soundex", {"ACDL56+56/-13"}, "A234"},
      {"soundex", {"AC#$!D193  L"}, "A234"},
      // Multi-byte UTF8 characters are skipped. The second l is skipped because
      // the previous valid character is already l.
      {"soundex", {"Aгдaгдlдl"}, "A400"}};

  // Test every character to make sure all non-alpha character is invalid.
  for (int c = 0; c < 256; ++c) {
    std::string single_char = std::string(1, static_cast<char>(c));
    results.push_back(
        {"soundex",
         {single_char},
         absl::ascii_isalpha(c) ? absl::StrCat(single_char, "000") : ""});
  }

  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsTranslate() {
  constexpr size_t kMaxOutputSize = (1 << 20);  // 1MB

  const std::string small_size_text(kMaxOutputSize / 2 + 1, 'a');
  const std::string exact_size_text(kMaxOutputSize, 'a');
  const std::string large_size_text(kMaxOutputSize + 1, 'a');

  std::vector<FunctionTestCall> results = {
      // translate(string, string, string) -> string
      {"translate", {NullString(), NullString(), NullString()}, NullString()},
      {"translate", {NullString(), NullString(), ""}, NullString()},
      {"translate", {NullString(), "", NullString()}, NullString()},
      {"translate", {NullString(), "", ""}, NullString()},
      {"translate", {"", NullString(), NullString()}, NullString()},
      {"translate", {"", NullString(), ""}, NullString()},
      {"translate", {"", "", NullString()}, NullString()},

      {"translate", {"", "", ""}, ""},
      {"translate", {"abcde", "", ""}, "abcde"},
      // Repeated characters in source characters are not allowed (even if they
      // map to the same target character).
      {"translate", {"abcde", "aba", "xyz"}, NullString(), OUT_OF_RANGE},
      {"translate", {"abcde", "aba", "xyx"}, NullString(), OUT_OF_RANGE},
      // Source characters without a corresponding target character are removed
      // from the input.
      {"translate", {"abcde", "ad", "x"}, "xbce"},
      {"translate", {"abcdebadbad", "ad", "x"}, "xbcebxbx"},
      // Extra characters in target characters are ignored.
      {"translate", {"abcde", "ab", "xyz123xy"}, "xycde"},
      // Characters are only modified once.
      {"translate", {"ababbce", "ab", "ba"}, "babaace"},
      {"translate", {"abcde", "abd", "xyz"}, "xycze"},
      {"translate", {"abcdebde", "abd", "xyz"}, "xyczeyze"},
      {"translate", {"abcde", "abd", "xдz"}, "xдcze"},
      {"translate", {"abcdebde", "abde", "xдz"}, "xдczдz"},
      {"translate", {"abcдd\nebde", "ab\ndeд", "12ф4\t456"}, "12c44ф\t24\t"},

      // Testing output size errors.
      {"translate", {small_size_text, "a", "a"}, small_size_text},
      // Input size is under limit, but TRANSLATE will make it over limit.
      // ¢ is a 2-byte character.
      {"translate",
       {small_size_text, "a", "¢"},
       NullString(),
       absl::OutOfRangeError(
           "Output of TRANSLATE exceeds max allowed output size of 1MB")},
      {"translate", {exact_size_text, "a", "a"}, exact_size_text},
      {"translate",
       {large_size_text, "", ""},
       NullString(),
       absl::OutOfRangeError(
           "Output of TRANSLATE exceeds max allowed output size of 1MB")},

      // Testing duplicate character
      {"translate",
       {"unused", "aa", "bc"},
       NullString(),
       absl::OutOfRangeError(
           "Duplicate character \"a\" in TRANSLATE source characters")},
      // Error even if the duplicate character doesn't have a corresponding
      // target character.
      {"translate",
       {"unused", "aa", "bc"},
       NullString(),
       absl::OutOfRangeError(
           "Duplicate character \"a\" in TRANSLATE source characters")},
      // Make sure characters are correctly escaped.
      {"translate",
       {"unused", "ab\nd\n", "vwxyz"},
       NullString(),
       absl::OutOfRangeError(
           "Duplicate character \"\\n\" in TRANSLATE source characters")},
      {"translate",
       {"unused", "ab'd'", "vwxyz"},
       NullString(),
       absl::OutOfRangeError(
           "Duplicate character \"'\" in TRANSLATE source characters")},
      {"translate",
       {"unused", "ab\"d\"", "vwxyz"},
       NullString(),
       absl::OutOfRangeError(
           "Duplicate character '\"' in TRANSLATE source characters")},

      // translate(bytes, bytes, bytes) -> bytes
      {"translate", {NullBytes(), NullBytes(), NullBytes()}, NullBytes()},
      {"translate", {NullBytes(), NullBytes(), Bytes("")}, NullBytes()},
      {"translate", {NullBytes(), Bytes(""), NullBytes()}, NullBytes()},
      {"translate", {NullBytes(), Bytes(""), Bytes("")}, NullBytes()},
      {"translate", {Bytes(""), NullBytes(), NullBytes()}, NullBytes()},
      {"translate", {Bytes(""), NullBytes(), Bytes("")}, NullBytes()},
      {"translate", {Bytes(""), Bytes(""), NullBytes()}, NullBytes()},

      {"translate", {Bytes(""), Bytes(""), Bytes("")}, Bytes("")},
      {"translate", {Bytes("abcde"), Bytes(""), Bytes("")}, Bytes("abcde")},
      // Repeated characters in source characters are not allowed (even if they
      // map to the same target character).
      {"translate",
       {Bytes("abcde"), Bytes("aba"), Bytes("xyz")},
       NullBytes(),
       OUT_OF_RANGE},
      {"translate",
       {Bytes("abcde"), Bytes("aba"), Bytes("xyx")},
       NullBytes(),
       OUT_OF_RANGE},
      // Source characters without a corresponding target character are removed
      // from the input.
      {"translate", {Bytes("abcde"), Bytes("ad"), Bytes("x")}, Bytes("xbce")},
      {"translate",
       {Bytes("abcdebadbad"), Bytes("ad"), Bytes("x")},
       Bytes("xbcebxbx")},
      // Extra characters in target characters are ignored.
      {"translate",
       {Bytes("abcde"), Bytes("ab"), Bytes("xyz123xy")},
       Bytes("xycde")},
      // Characters are only modified once.
      {"translate",
       {Bytes("ababbce"), Bytes("ab"), Bytes("ba")},
       Bytes("babaace")},
      {"translate",
       {Bytes("abcde"), Bytes("abd"), Bytes("xyz")},
       Bytes("xycze")},
      {"translate",
       {Bytes("abcdebde"), Bytes("abd"), Bytes("xyz")},
       Bytes("xyczeyze")},
      // д's UTF8 encoding is 0xD0 0xB4.
      {"translate",
       {Bytes("abmdgb"), Bytes("abd"), Bytes("xд")},
       Bytes("x\xD0m\xB4g\xD0")},
      {"translate",
       {Bytes("\x15\x01\xFFz"), Bytes("abc\x15\xFFxyz"), Bytes("\xAAxyz\xFA")},
       Bytes("z\x01\xFA")},

      // Testing output size errors.
      {"translate",
       {Bytes(small_size_text), Bytes("a"), Bytes("a")},
       Bytes(small_size_text)},
      {"translate",
       {Bytes(exact_size_text), Bytes("a"), Bytes("a")},
       Bytes(exact_size_text)},
      {"translate",
       {Bytes(large_size_text), Bytes(""), Bytes("")},
       NullBytes(),
       absl::OutOfRangeError(
           "Output of TRANSLATE exceeds max allowed output size of 1MB")},
      // Testing duplicate byte
      {"translate",
       {Bytes("unused"), Bytes("abcda"), Bytes("vwxyz")},
       NullBytes(),
       absl::OutOfRangeError("Duplicate byte 0x61 in TRANSLATE source bytes")},
      {"translate",
       {Bytes("unused"), Bytes("\x01\x01"), Bytes("yz")},
       NullBytes(),
       absl::OutOfRangeError("Duplicate byte 0x01 in TRANSLATE source bytes")},
  };

  return results;
}

std::vector<FunctionTestCall> GetFunctionTestsInitCap() {
  return {
      // initcap(string) -> string
      {"initcap", {NullString()}, NullString()},
      {"initcap", {""}, ""},
      {"initcap", {" "}, " "},
      {"initcap", {" a 1"}, " A 1"},
      {"initcap", {" a \tb"}, " A \tB"},
      {"initcap", {" a\tb\nc d"}, " A\tB\nC D"},
      {"initcap", {"aB-C ?DEF []h? j"}, "Ab-C ?Def []H? J"},
      {"initcap", {"ταινιών"}, "Ταινιών"},
      {"initcap", {"abcABC ж--щ фФ"}, "Abcabc Ж--Щ Фф"},
      {"initcap", {"\x61\xa7\x65\x71"}, NullString(), OUT_OF_RANGE},

      // initcap(string, string) -> string
      {"initcap", {NullString(), NullString()}, NullString()},
      {"initcap", {NullString(), ""}, NullString()},
      {"initcap", {"", NullString()}, NullString()},
      {"initcap", {"a \tb\n\tc d\ne", " \t"}, "A \tB\n\tC D\ne"},
      {"initcap", {"aBc -d", " -"}, "Abc -D"},
      {"initcap", {"a b", "ab"}, "a b"},
      {"initcap", {"cbac", "aabb"}, "CbaC"},
      {"initcap", {"Aa BC\nd", "ab\n"}, "Aa bc\nD"},
      {"initcap", {"aAbBc", "abAB"}, "aAbBC"},
      {"initcap", {"HERE-iS-a string", "a -"}, "Here-Is-a String"},
      {"initcap", {"τΑΑ τιν ιτιών", "τ"}, "τΑα τΙν ιτΙών"},
      {"initcap", {" 123 4a 5A b", " 1 234"}, " 123 4A 5a B"},
      {"initcap", {"ABCD", ""}, "Abcd"},
      {"initcap", {"\x61\xa7\x65\x71", " "}, NullString(), OUT_OF_RANGE},
      {"initcap", {"", "\x61\xa7\x65\x71"}, NullString(), OUT_OF_RANGE},
  };
}

std::vector<QueryParamsWithResult> GetFunctionTestsStringConcatOperator() {
  std::vector<QueryParamsWithResult> results;
  // string || string -> string
  // bytes || bytes -> bytes
  // Test the test cases of concat function.
  for (const FunctionTestCall& function_call : GetStringConcatTests()) {
    if (function_call.params.params().size() == 2) {
      results.push_back(function_call.params);
    }
  }
  return results;
}

// Defines the test cases for string normalization functions.
// The second argument represents the expected results under each
// normalize mode following exact same order as {NFC, NFKC, NFD, NFKD}.
static std::vector<NormalizeTestCase> GetNormalizeTestCases() {
  return {
    // normalize(string [, mode]) -> string
    {NullString(), {NullString(), NullString(), NullString(), NullString()}},
    {"", {"", "", "", ""}},
    {"abcABC", {"abcABC", "abcABC", "abcABC", "abcABC"}},
    {"abcABCжщфЖЩФ", {"abcABCжщфЖЩФ", "abcABCжщфЖЩФ", "abcABCжщфЖЩФ",
        "abcABCжщфЖЩФ"}},
    {"Ḋ", {"Ḋ", "Ḋ", "D\u0307", "D\u0307"}},
    {"D\u0307", {"Ḋ", "Ḋ", "D\u0307", "D\u0307"}},
    {"Google™", {"Google™", "GoogleTM", "Google™", "GoogleTM"}},
    {"龟", {"龟", "\u9F9F", "龟", "\u9F9F"}},
    {"10¹²³", {"10¹²³", "10123", "10¹²³", "10123"}},
    {"ẛ̣", {"ẛ̣", "ṩ", "\u017f\u0323\u0307", "s\u0323\u0307"}},
    {"Google™Ḋ龟10¹²³", {"Google™Ḋ龟10¹²³", "GoogleTMḊ\u9F9F10123",
        "Google™D\u0307龟10¹²³", "GoogleTMD\u0307\u9F9F10123"}},

    // normalize_and_casefold(string [, mode]) -> string
    {NullString(), {NullString(), NullString(), NullString(), NullString()},
        true /* is_casefold */},
    {"", {"", "", "", ""}, true},
    {"abcABC", {"abcabc", "abcabc", "abcabc", "abcabc"}, true},
    {"abcabcжщфЖЩФ", {"abcabcжщфжщф", "abcabcжщфжщф", "abcabcжщфжщф",
        "abcabcжщфжщф"}, true},
    {"Ḋ", {"ḋ", "ḋ", "d\u0307", "d\u0307"}, true},
    {"D\u0307", {"ḋ", "ḋ", "d\u0307", "d\u0307"}, true},
    {"Google™", {"google™", "googletm", "google™", "googletm"}, true},
    {"龟", {"龟", "\u9F9F", "龟", "\u9F9F"}, true},
    {"10¹²³", {"10¹²³", "10123", "10¹²³", "10123"}, true},
    {"ẛ̣", {"ṩ", "ṩ", "s\u0323\u0307", "s\u0323\u0307"}, true},
    {"Google™Ḋ龟10¹²³", {"google™ḋ龟10¹²³", "googletmḋ\u9F9F10123",
        "google™d\u0307龟10¹²³", "googletmd\u0307\u9F9F10123"}, true},
  };
}

std::vector<FunctionTestCall> GetFunctionTestsNormalize() {
  const zetasql::EnumType* enum_type = nullptr;
  ZETASQL_CHECK_OK(type_factory()->MakeEnumType(
      zetasql::functions::NormalizeMode_descriptor(), &enum_type));

  // For each NormalizeTestCase, constructs 5 FunctionTestCalls for each
  // normalize mode (default/NFC, NFC, NFKC, NFD, NFKD), respectively.
  std::vector<FunctionTestCall> ret;
  for (const NormalizeTestCase& test_case : GetNormalizeTestCases()) {
    CHECK_EQ(functions::NormalizeMode_ARRAYSIZE, test_case.expected_nfs.size());
    const std::string function_name =
        test_case.is_casefold ? "normalize_and_casefold" : "normalize";
    ret.push_back(
        {function_name, {test_case.input}, test_case.expected_nfs[0]});
    for (int mode = 0; mode < functions::NormalizeMode_ARRAYSIZE; ++mode) {
      ret.push_back({function_name,
                     {test_case.input, Value::Enum(enum_type, mode)},
                     test_case.expected_nfs[mode]});
    }

    if (test_case.is_casefold) continue;
    // Adds more tests cases for NORMALIZE to verify the invariants between
    // normal forms.
    // 1. NFC(NFD(s)) = NFC(s)
    ret.push_back({"normalize",
                   {test_case.expected_nfs[functions::NormalizeMode::NFD],
                    Value::Enum(enum_type, functions::NormalizeMode::NFC)},
                   test_case.expected_nfs[functions::NormalizeMode::NFC]});
    // 2. NFD(NFC(s)) = NDF(s)
    ret.push_back({"normalize",
                   {test_case.expected_nfs[functions::NormalizeMode::NFC],
                    Value::Enum(enum_type, functions::NormalizeMode::NFD)},
                   test_case.expected_nfs[functions::NormalizeMode::NFD]});
    for (int mode = 0; mode < functions::NormalizeMode_ARRAYSIZE; ++mode) {
      // 3. NFKC(s) = NFKC(NFC(s)) = NFKC(NFD(s)) = NFKC(NFKC(s) = NFKC(NFKD(s))
      ret.push_back({"normalize",
                     {test_case.expected_nfs[mode],
                      Value::Enum(enum_type, functions::NormalizeMode::NFKC)},
                     test_case.expected_nfs[functions::NormalizeMode::NFKC]});
      // 4. NFKD(s) = NFKD(NFC(s)) = NFKD(NFD(s)) = NFKD(NFKC(s) = NFKD(NFKD(s))
      ret.push_back({"normalize",
                     {test_case.expected_nfs[mode],
                      Value::Enum(enum_type, functions::NormalizeMode::NFKD)},
                     test_case.expected_nfs[functions::NormalizeMode::NFKD]});
    }
  }
  return ret;
}

}  // namespace zetasql
