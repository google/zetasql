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

// The file functions_testlib.cc has been split into multiple files prefixed
// with "functions_testlib_" because an optimized compile with ASAN of the
// original single file timed out at 900 seconds.
#include <limits>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/compliance/functions_testlib_common.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/using_test_value.cc"
#include <cstdint>
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/map_util.h"
#include "re2/re2.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace {
constexpr zetasql_base::StatusCode INVALID_ARGUMENT =
    zetasql_base::StatusCode::kInvalidArgument;
constexpr zetasql_base::StatusCode OUT_OF_RANGE = zetasql_base::StatusCode::kOutOfRange;
}  // namespace

std::vector<FunctionTestCall> GetFunctionTestsRegexp() {
  Value null_string_array = Null(types::StringArrayType());
  Value null_bytes_array = Null(types::BytesArrayType());
  Value empty_string_array = EmptyArray(types::StringArrayType());
  Value empty_bytes_array = EmptyArray(types::BytesArrayType());

  return {
    // regexp_contains(std::string, std::string) -> bool
    {"regexp_contains", {"", NullString()}, NullBool()},
    {"regexp_contains", {NullString(), ""}, NullBool()},
    {"regexp_contains", {"", ""}, true},
    {"regexp_contains", {"", "abc"}, false},
    {"regexp_contains", {"abc", ""}, true},
    {"regexp_contains", {"abc", "abc"}, true},
    {"regexp_contains", {"abcdef", "a.c.*f"}, true},
    {"regexp_contains", {"abcdef", "ac.*e."}, false},
    {"regexp_contains", {"abcdef", "bcde"}, true},
    {"regexp_contains", {"abcdef", "b.*d"}, true},
    {"regexp_contains", {"щцфшцф", "щцф.{3}"}, true},
    {"regexp_contains", {"жэчыщчыщя", "чыщ.{2}"}, true},
    {"regexp_contains", {"жэчыщчыщя", "чы.{2}"}, true},
    {"regexp_contains", {"жэчыщчыщя", "чы{2}"}, false},
    {"regexp_contains", {"щцфшцфыщ", "(..){2}"}, true},

    {"regexp_contains", {"", "("}, NullBool(), OUT_OF_RANGE},

    // regexp_contains(bytes, bytes) -> bool
    {"regexp_contains", {Bytes(""), NullBytes()}, NullBool()},
    {"regexp_contains", {NullBytes(), Bytes("")}, NullBool()},
    {"regexp_contains", {Bytes(""), Bytes("")}, true},
    {"regexp_contains", {Bytes(""), Bytes("abc")}, false},
    {"regexp_contains", {Bytes("abc"), Bytes("")}, true},
    {"regexp_contains", {Bytes("abc"), Bytes("abc")}, true},
    {"regexp_contains", {Bytes("abcdef"), Bytes("a.c.*f")}, true},
    {"regexp_contains", {Bytes("abcdef"), Bytes("ac.*e.")}, false},
    {"regexp_contains", {Bytes("abcdef"), Bytes("bcde")}, true},
    {"regexp_contains", {Bytes("abcdef"), Bytes("b.*d")}, true},
    {"regexp_contains", {Bytes("щцфшцф"), Bytes("щцф.{3}")}, true},
    {"regexp_contains", {Bytes("жэчыщчыщя"), Bytes("чыщ.{2}")}, true},
    {"regexp_contains", {Bytes("жэчыщчыщя"), Bytes("чы.{2}")}, true},
    {"regexp_contains", {Bytes("жэчыщчыщя"), Bytes("чы{2}")}, false},
    {"regexp_contains", {Bytes("щцфшцфыщ"), Bytes("(..){2}")}, true},

    {"regexp_contains", {Bytes(""), Bytes("(")}, NullBool(), OUT_OF_RANGE},

    // regexp_match(std::string, std::string) -> bool
    {"regexp_match", {"", NullString()}, NullBool()},
    {"regexp_match", {NullString(), ""}, NullBool()},
    {"regexp_match", {"", ""}, true},
    {"regexp_match", {"", "abc"}, false},
    {"regexp_match", {"abc", ""}, false},
    {"regexp_match", {"abc", "abc"}, true},
    {"regexp_match", {"abcdef", "a.c.*f"}, true},
    {"regexp_match", {"abcdef", "ac.*e."}, false},
    {"regexp_match", {"abcdef", "bcde"}, false},
    {"regexp_match", {"щцфшцф", "щцф.{3}"}, true},
    {"regexp_match", {"щцфшцф", ".{12}"}, false},
    {"regexp_match", {"щцфшцф", "(..){3}"}, true},

    {"regexp_match", {"", "("}, NullBool(), OUT_OF_RANGE},

    // regexp_match(bytes, bytes) -> bool
    {"regexp_match", {Bytes(""), NullBytes()}, NullBool()},
    {"regexp_match", {NullBytes(), Bytes("")}, NullBool()},
    {"regexp_match", {Bytes(""), Bytes("")}, true},
    {"regexp_match", {Bytes(""), Bytes("abc")}, false},
    {"regexp_match", {Bytes("abc"), Bytes("")}, false},
    {"regexp_match", {Bytes("abc"), Bytes("abc")}, true},
    {"regexp_match", {Bytes("abcdef"), Bytes("a.c.*f")}, true},
    {"regexp_match", {Bytes("abcdef"), Bytes("ac.*e.")}, false},
    {"regexp_match", {Bytes("abcdef"), Bytes("bcde")}, false},
    {"regexp_match", {Bytes("щцфшцф"), Bytes(".{6}")}, false},
    {"regexp_match", {Bytes("щцфшцф"), Bytes(".{12}")}, true},
    {"regexp_match", {Bytes("щцфшцф"), Bytes("(..){3}")}, false},

    {"regexp_match", {Bytes(""), Bytes("(")}, NullBool(), OUT_OF_RANGE},

    // regexp_extract(std::string, std::string) -> std::string
    {"regexp_extract", {"", NullString()}, NullString()},
    {"regexp_extract", {NullString(), ""}, NullString()},
    {"regexp_extract", {"", ""}, ""},
    {"regexp_extract", {"", "abc"}, NullString()},
    {"regexp_extract", {"abc", "abc"}, "abc"},
    {"regexp_extract", {"abc", "^a"}, "a"},
    {"regexp_extract", {"abc", "^b"}, NullString()},
    {"regexp_extract", {"abcdef", "a.c.*f"}, "abcdef"},
    {"regexp_extract", {"abcdef", "ac.*e."}, NullString()},
    {"regexp_extract", {"abcdef", "bcde"}, "bcde"},
    {"regexp_extract", {"щцф", ".{3}"}, "щцф"},
    {"regexp_extract", {"щцф", ".{6}"}, NullString()},
    {"regexp_extract", {"", "()"}, ""},
    {"regexp_extract", {"", "(abc)"}, NullString()},
    {"regexp_extract", {"", "(abc)?"}, NullString()},
    {"regexp_extract", {"abc", "a(b)c"}, "b"},
    {"regexp_extract", {"abcdef", "a(.c.*)f"}, "bcde"},
    {"regexp_extract", {"abcdef", "(bcde)"}, "bcde"},
    {"regexp_extract", {"щцф", "щ(.)."}, "ц"},
    {"regexp_extract", {"щцф", "(?:щ|ц)(ц|ф)(?:щ|ф)"}, "ц"},
    {"regexp_extract", {"щцф", "(.{6})"}, NullString()},
    {"regexp_extract", {"abc", "((a))"}, NullString(), OUT_OF_RANGE},
    {"regexp_extract", {"abc", "(a)(b)"}, NullString(), OUT_OF_RANGE},

    {"regexp_extract", {"", "("}, NullString(), OUT_OF_RANGE},

    // regexp_extract(bytes, bytes) -> bytes
    {"regexp_extract", {Bytes(""), NullBytes()}, NullBytes()},
    {"regexp_extract", {NullBytes(), Bytes("")}, NullBytes()},
    {"regexp_extract", {Bytes(""), Bytes("")}, Bytes("")},
    {"regexp_extract", {Bytes(""), Bytes("abc")}, NullBytes()},
    {"regexp_extract", {Bytes("abc"), Bytes("abc")}, Bytes("abc")},
    {"regexp_extract", {Bytes("abcdef"), Bytes("a.c.*f")}, Bytes("abcdef")},
    {"regexp_extract", {Bytes("abcdef"), Bytes("ac.*e.")}, NullBytes()},
    {"regexp_extract", {Bytes("abcdef"), Bytes("bcde")}, Bytes("bcde")},
    {"regexp_extract", {Bytes("щцф"), Bytes(".{2}")}, Bytes("щ")},
    {"regexp_extract", {Bytes("щцф"), Bytes(".{6}")}, Bytes("щцф")},
    {"regexp_extract", {Bytes(""), Bytes("()")}, Bytes("")},
    {"regexp_extract", {Bytes(""), Bytes("(abc)")}, NullBytes()},
    {"regexp_extract", {Bytes(""), Bytes("(abc)?")}, NullBytes()},
    {"regexp_extract", {Bytes("abc"), Bytes("a(b)c")}, Bytes("b")},
    {"regexp_extract", {Bytes("abcdef"), Bytes("a(.c.*)f")}, Bytes("bcde")},
    {"regexp_extract", {Bytes("abcdef"), Bytes("(bcde)")}, Bytes("bcde")},
    {"regexp_extract", {Bytes("щцф"), Bytes("щ(..)..")}, Bytes("ц")},
    {"regexp_extract", {Bytes("щцф"), Bytes("(.{6})")}, Bytes("щцф")},
    {"regexp_extract", {Bytes("abc"), Bytes("((a))")},
        NullBytes(), OUT_OF_RANGE},
    {"regexp_extract", {Bytes("abc"), Bytes("(a)(b)")},
        NullBytes(), OUT_OF_RANGE},

    {"regexp_extract", {Bytes(""), Bytes("(")}, NullBytes(), OUT_OF_RANGE},

    // regexp_replace(std::string, std::string, std::string) -> std::string
    {"regexp_replace", {NullString(), "", ""}, NullString()},
    {"regexp_replace", {"", NullString(), ""}, NullString()},
    {"regexp_replace", {"", "", NullString()}, NullString()},
    {"regexp_replace", {"", "", ""}, ""},
    {"regexp_replace", {"abc", "", "x"}, "xaxbxcx"},
    {"regexp_replace", {"abc", "x?", "x"}, "xaxbxcx"},
    {"regexp_replace", {"abc", "b", "xyz"}, "axyzc"},
    {"regexp_replace", {"abcabc", "bc", "xyz"}, "axyzaxyz"},
    {"regexp_replace", {"abc", "abc", "xyz"}, "xyz"},
    {"regexp_replace", {"banana", "ana", "xyz"}, "bxyzna"},
    {"regexp_replace", {"banana", "ana", ""}, "bna"},
    {"regexp_replace", {"banana", "a", "z"}, "bznznz"},
    {"regexp_replace", {"banana", "(.)a(.)", "\\1\\2"}, "bnana"},
    {"regexp_replace", {"banana", ".", "x"}, "xxxxxx"},
    {"regexp_replace", {"щцфшцф", ".", "ы"}, "ыыыыыы"},
    {"regexp_replace", {"T€T", "", "K"}, "KTK€KTK"},
    {"regexp_replace", {"abc", "b*", "x"}, "xaxcx"},
    {"regexp_replace", {"ab", "b*", "x"}, "xax"},
    {"regexp_replace", {"bc", "b*", "x"}, "xcx"},
    {"regexp_replace", {"b", "b*", "x"}, "x"},
    {"regexp_replace", {"bb", "^b", "x"}, "xb"},
    {"regexp_replace", {"one", "\\b", "-"}, "-one-"},
    {"regexp_replace", {"one two  many", "\\b", "-"}, "-one- -two-  -many-"},
    // non-capturing group
    {"regexp_replace",
     {"http://www.google.com", "(?:http|ftp)://(.*?)\\.(.*?)\\.(com|org|net)",
      "\\3.\\2.\\1"},
     "com.google.www"},
    // nested non-capturing group
    {"regexp_replace",
     {"www3.archive.org", "(?:(?:http|ftp)://)?(.*?)\\.(.*?)\\.(com|org|net)",
      "\\3.\\2.\\1"},
     "org.archive.www3"},

    {"regexp_replace", {"", "(", ""}, NullString(), OUT_OF_RANGE},
    {"regexp_replace", {"", "", "\\x"}, NullString(), OUT_OF_RANGE},
    {"regexp_replace", {"", "", "\\"}, NullString(), OUT_OF_RANGE},
    {"regexp_replace", {"", "", "\\1"}, NullString(), OUT_OF_RANGE},

    // The regex is invalid.
    {"regexp_replace", {"a", "AI!\x1b\r)V\r\x1c\x1eP3]|\x17\n9", "def"},
          NullString(), OUT_OF_RANGE},
    // This is the test case from b/21080653 and b/21079403, where the
    // regex is invalid but the original pattern std::string is NULL.
    {"regexp_replace", {"CAST(NULL as STRING)",
                        "AI!\x1b\r)V\r\x1c\x1eP3]|\x17\n9", "b"},
          NullString(), OUT_OF_RANGE},
    {"regexp_replace", {"NULL",
                        "AI!\x1b\r)V\r\x1c\x1eP3]|\x17\n9", "b"},
          NullString(), OUT_OF_RANGE},

    // regexp_replace(bytes, bytes, bytes) -> bytes
    {"regexp_replace", {NullBytes(), Bytes(""), Bytes("")}, NullBytes()},
    {"regexp_replace", {Bytes(""), NullBytes(), Bytes("")}, NullBytes()},
    {"regexp_replace", {Bytes(""), Bytes(""), NullBytes()}, NullBytes()},
    {"regexp_replace", {Bytes(""), Bytes(""), Bytes("")}, Bytes("")},
    {"regexp_replace", {Bytes("abc"), Bytes(""), Bytes("x")}, Bytes("xaxbxcx")},
    {"regexp_replace", {Bytes("abc"), Bytes("x?"), Bytes("x")},
        Bytes("xaxbxcx")},
    {"regexp_replace", {Bytes("abc"), Bytes("b"), Bytes("xyz")},
        Bytes("axyzc")},
    {"regexp_replace", {Bytes("abcabc"), Bytes("bc"), Bytes("xyz")},
        Bytes("axyzaxyz")},
    {"regexp_replace", {Bytes("abc"), Bytes("abc"), Bytes("xyz")},
        Bytes("xyz")},
    {"regexp_replace", {Bytes("banana"), Bytes("ana"), Bytes("xyz")},
        Bytes("bxyzna")},
    {"regexp_replace", {Bytes("banana"), Bytes("ana"), Bytes("")},
        Bytes("bna")},
    {"regexp_replace", {Bytes("banana"), Bytes("a"), Bytes("z")},
        Bytes("bznznz")},
    {"regexp_replace", {Bytes("banana"), Bytes("(.)a(.)"), Bytes("\\1\\2")},
        Bytes("bnana")},
    {"regexp_replace", {Bytes("banana"), Bytes("."), Bytes("x")},
        Bytes("xxxxxx")},

    {"regexp_replace", {Bytes(""), Bytes("("), Bytes("")},
        NullBytes(), OUT_OF_RANGE},
    {"regexp_replace", {Bytes(""), Bytes(""), Bytes("\\x")},
        NullBytes(), OUT_OF_RANGE},
    {"regexp_replace", {Bytes(""), Bytes(""), Bytes("\\")},
        NullBytes(), OUT_OF_RANGE},
    {"regexp_replace", {Bytes(""), Bytes(""), Bytes("\\1")},
        NullBytes(), OUT_OF_RANGE},

    // regexp_extract_all(std::string, std::string) -> Array of std::string
    {"regexp_extract_all", {"", NullString()}, null_string_array},
    {"regexp_extract_all", {NullString(), ""}, null_string_array},
    {"regexp_extract_all", {"", ""}, StringArray({""})},
    {"regexp_extract_all", {"", "abc"}, empty_string_array},
    {"regexp_extract_all", {"abc", "."}, StringArray({"a", "b", "c"})},
    {"regexp_extract_all", {"aaa", "^a"}, StringArray({"a"})},
    {"regexp_extract_all", {"abc", "abc"}, StringArray({"abc"})},
    {"regexp_extract_all", {"abcdef", "a.c.*f"}, StringArray({"abcdef"})},
    {"regexp_extract_all", {"abcdef", "ac.*e."}, empty_string_array},
    {"regexp_extract_all", {"abcdef", "bcde"}, StringArray({"bcde"})},
    {"regexp_extract_all", {"aabca", "a*"}, StringArray({"aa", "", "", "a"})},
    {"regexp_extract_all", {"aaba", "a?"}, StringArray({"a", "a", "", "a"})},
    {"regexp_extract_all", {"baac", "a*"}, StringArray({"", "aa", ""})},
    {"regexp_extract_all", {"abcd", "a(bc)*"}, StringArray({"bc"})},
    {"regexp_extract_all", {"щцф", "."}, StringArray({"щ", "ц", "ф"})},
    {"regexp_extract_all", {"щцф", ".{3}"}, StringArray({"щцф"})},
    {"regexp_extract_all", {"щцф", ".{6}"}, empty_string_array},
    {"regexp_extract_all", {"щццф", "ц*"}, StringArray({"", "цц", ""})},
    {"regexp_extract_all", {"", "()"}, StringArray({""})},
    {"regexp_extract_all", {"", "(abc)"}, empty_string_array},
    {"regexp_extract_all", {"", "(abc)?"}, StringArray({""})},
    {"regexp_extract_all", {"abc", "a(b)c"}, StringArray({"b"})},
    {"regexp_extract_all", {"abbb", "^a*(b)"}, StringArray({"b"})},
    {"regexp_extract_all", {"XbASDZb", "(.)b"}, StringArray({"X", "Z"})},
    {"regexp_extract_all", {"abcdef", "a(.c.*)f"}, StringArray({"bcde"})},
    {"regexp_extract_all", {"abcdef", "(bcde)"}, StringArray({"bcde"})},
    {"regexp_extract_all", {"this_is__a___Test", "(.*?)(?:_|$)"},
        StringArray({"this", "is", "", "a", "", "", "Test"})},
    {"regexp_extract_all", {"щцф", "щ(.)."}, StringArray({"ц"})},
    {"regexp_extract_all", {"щцф", "(.{6})"}, empty_string_array},
    {"regexp_extract_all", {"abc", "((a))"},
        null_string_array, OUT_OF_RANGE},
    {"regexp_extract_all", {"abc", "(a)(b)"},
        null_string_array, OUT_OF_RANGE},

    {"regexp_extract_all", {"", "("}, null_string_array, OUT_OF_RANGE},

    // regexp_extract_all(bytes, bytes) -> array of bytes
    {"regexp_extract_all", {Bytes(""), NullBytes()}, null_bytes_array},
    {"regexp_extract_all", {NullBytes(), Bytes("")}, null_bytes_array},
    {"regexp_extract_all", {Bytes(""), Bytes("")}, BytesArray({""})},
    {"regexp_extract_all", {Bytes(""), Bytes("abc")}, empty_bytes_array},
    {"regexp_extract_all", {Bytes("abc"), Bytes(".")},
        BytesArray({"a", "b", "c"})},
    {"regexp_extract_all", {Bytes("aaa"), Bytes("^a")}, BytesArray({"a"})},
    {"regexp_extract_all", {Bytes("abc"), Bytes("abc")}, BytesArray({"abc"})},
    {"regexp_extract_all", {Bytes("abcdef"), Bytes("a.c.*f")},
        BytesArray({"abcdef"})},
    {"regexp_extract_all", {Bytes("abcdef"), Bytes("ac.*e.")},
        empty_bytes_array},
    {"regexp_extract_all", {Bytes("abcdef"), Bytes("bcde")},
        BytesArray({"bcde"})},
    {"regexp_extract_all", {Bytes("aabca"), Bytes("a*")},
        BytesArray({"aa", "", "", "a"})},
    {"regexp_extract_all", {Bytes("aaba"), Bytes("a?")},
        BytesArray({"a", "a", "", "a"})},
    {"regexp_extract_all", {Bytes("baac"), Bytes("a*")},
        BytesArray({"", "aa", ""})},
    {"regexp_extract_all", {Bytes("abcd"), Bytes("a(bc)*")},
        BytesArray({"bc"})},
    {"regexp_extract_all", {Bytes("щцф"), Bytes(".{2}")},
        BytesArray({"щ", "ц", "ф"})},
    {"regexp_extract_all", {Bytes("щцф"), Bytes(".{6}")}, BytesArray({"щцф"})},
    {"regexp_extract_all", {Bytes("\001\002\003"), Bytes("\002?")},
        BytesArray({"", "\002", ""})},
    {"regexp_extract_all", {Bytes(""), Bytes("()")}, BytesArray({""})},
    {"regexp_extract_all", {Bytes(""), Bytes("(abc)")}, empty_bytes_array},
    {"regexp_extract_all", {Bytes(""), Bytes("(abc)?")}, BytesArray({""})},
    {"regexp_extract_all", {Bytes("abc"), Bytes("a(b)c")}, BytesArray({"b"})},
    {"regexp_extract_all", {Bytes("abbb"), Bytes("^a*(b)")}, BytesArray({"b"})},
    {"regexp_extract_all", {Bytes("XbASDZb"), Bytes("(.)b")},
        BytesArray({"X", "Z"})},
    {"regexp_extract_all", {Bytes("abcdef"), Bytes("a(.c.*)f")},
        BytesArray({"bcde"})},
    {"regexp_extract_all", {Bytes("abcdef"), Bytes("(bcde)")},
        BytesArray({"bcde"})},
    {"regexp_extract_all", {Bytes("this__test_case"), Bytes("(.*?)(?:_|$)")},
        BytesArray({"this", "", "test", "case"})},
    {"regexp_extract_all", {Bytes("щцф"), Bytes("щ(..)..")}, BytesArray({"ц"})},
    {"regexp_extract_all", {Bytes("щцф"), Bytes("(.{6})")},
        BytesArray({"щцф"})},
    {"regexp_extract_all", {Bytes("abc"), Bytes("((a))")},
        null_bytes_array, OUT_OF_RANGE},
    {"regexp_extract_all", {Bytes("abc"), Bytes("(a)(b)")},
        null_bytes_array, OUT_OF_RANGE},

    {"regexp_extract_all", {Bytes(""), Bytes("(")},
        null_bytes_array, OUT_OF_RANGE},
  };
}

static Value StringToBytes(const Value& value) {
  CHECK_EQ(value.type_kind(), TYPE_STRING);
  if (value.is_null()) {
    return Value::NullBytes();
  } else {
    return Value::Bytes(value.string_value());
  }
}

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
  CHECK_EQ(a0c.size(), 3);

  // Tests for value LIKE value -> bool that work for std::string or bytes.
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

  // This 3-char std::string has a 3-byte utf8 character in it.  It is "a?b".
  const std::string utf8_string =
      "a"
      "\xe8\xb0\xb7"
      "b";
  LOG(INFO) << "utf8_string: " << utf8_string;

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

std::vector<FunctionTestCall> GetFunctionTestsNet() {
  std::vector<FunctionTestCall> result = {
      // NET.FORMAT_IP.
      {"net.format_ip", {0ll}, "0.0.0.0"},
      {"net.format_ip", {180332553ll}, "10.191.168.9"},
      {"net.format_ip", {2147483647ll}, "127.255.255.255"},
      {"net.format_ip", {2147483648ll}, "128.0.0.0"},
      {"net.format_ip", {4294967295ll}, "255.255.255.255"},
      {"net.format_ip", {4294967296ll}, NullString(), OUT_OF_RANGE},
      {"net.format_ip", {NullInt64()}, NullString()},
      {"net.format_ip", {1ll << 33}, NullString(), OUT_OF_RANGE},
      {"net.format_ip", {-1ll}, NullString(), OUT_OF_RANGE},

      // NET.IPV4_FROM_INT64.
      {"net.ipv4_from_int64", {0LL}, Bytes("\0\0\0\0")},
      {"net.ipv4_from_int64", {1LL}, Bytes("\0\0\0\1")},
      {"net.ipv4_from_int64", {0x13579BDFLL}, Bytes("\x13\x57\x9B\xDF")},
      {"net.ipv4_from_int64", {0x7FFFFFFFLL}, Bytes("\x7F\xFF\xFF\xFF")},
      {"net.ipv4_from_int64", {0x80000000LL}, Bytes("\x80\x00\x00\x00")},
      {"net.ipv4_from_int64", {-0x80000000LL}, Bytes("\x80\x00\x00\x00")},
      {"net.ipv4_from_int64", {0xFFFFFFFFLL}, Bytes("\xFF\xFF\xFF\xFF")},
      {"net.ipv4_from_int64", {-1LL}, Bytes("\xFF\xFF\xFF\xFF")},
      {"net.ipv4_from_int64", {NullInt64()}, NullBytes()},
      {"net.ipv4_from_int64", {0x100000000LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ipv4_from_int64", {-0x80000001LL}, NullBytes(), OUT_OF_RANGE},

      // NET.PARSE_IP.
      {"net.parse_ip", {"0.0.0.0"}, 0ll},
      {"net.parse_ip", {"10.191.168.9"}, 180332553ll},
      {"net.parse_ip", {"127.255.255.255"}, 2147483647ll},
      {"net.parse_ip", {"128.0.0.0"}, 2147483648ll},
      {"net.parse_ip", {"255.255.255.255"}, 4294967295ll},
      {"net.parse_ip", {"255.255.255.256"}, NullInt64(), OUT_OF_RANGE},
      {"net.parse_ip", {"256.0.0.0"}, NullInt64(), OUT_OF_RANGE},
      {"net.parse_ip", {NullString()}, NullInt64()},
      {"net.parse_ip", {"::1"}, NullInt64(), OUT_OF_RANGE},
      {"net.parse_ip", {"180332553"}, NullInt64(), OUT_OF_RANGE},

      // NET.IPV4_TO_INT64.
      {"net.ipv4_to_int64", {Bytes("\0\0\0\0")}, 0LL},
      {"net.ipv4_to_int64", {Bytes("\0\0\0\1")}, 1LL},
      {"net.ipv4_to_int64", {Bytes("\x13\x57\x9B\xDF")}, 0x13579BDFLL},
      {"net.ipv4_to_int64", {Bytes("\x7F\xFF\xFF\xFF")}, 0x7FFFFFFFLL},
      {"net.ipv4_to_int64", {Bytes("\x80\x00\x00\x00")}, 0x80000000LL},
      {"net.ipv4_to_int64", {Bytes("\xFF\xFF\xFF\xFF")}, 0xFFFFFFFFLL},
      {"net.ipv4_to_int64", {NullBytes()}, NullInt64()},
      {"net.ipv4_to_int64", {Bytes("")}, NullInt64(), OUT_OF_RANGE},
      {"net.ipv4_to_int64", {Bytes("\0\0\0")}, NullInt64(), OUT_OF_RANGE},
      {"net.ipv4_to_int64", {Bytes("\0\0\0\0\0")}, NullInt64(), OUT_OF_RANGE},
      {"net.ipv4_to_int64",
       {Bytes("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0")},
       NullInt64(),
       OUT_OF_RANGE},

      // NET.PARSE_PACKED_IP.
      //    IPv4 Success.
      {"net.parse_packed_ip", {"64.65.66.67"}, Bytes("@ABC")},
      {"net.parse_packed_ip", {"64.0.66.67"}, Bytes("@\0BC")},
      //    IPv4 Failure.
      {"net.parse_packed_ip", {"64.65.66.67 "}, NullBytes(), OUT_OF_RANGE},
      {"net.parse_packed_ip", {" 64.65.66.67"}, NullBytes(), OUT_OF_RANGE},
      //    IPv6 Success.
      {"net.parse_packed_ip",
       {"6465:6667:6869:6a6b:6c6d:6e6f:7071:7172"},
       Bytes("defghijklmnopqqr")},
      {"net.parse_packed_ip",
       {"6465:6667:6869:6A6B:6C6d:6e6f:7071:7172"},
       Bytes("defghijklmnopqqr")},
      {"net.parse_packed_ip",
       {"::1"},
       Bytes("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1")},
      {"net.parse_packed_ip",
       {"::ffff:64.65.66.67"},
       Bytes("\0\0\0\0\0\0\0\0\0\0\xff\xff@ABC")},
      //    IPv6 Failure.
      {"net.parse_packed_ip",
       {" 6465:6667:6869:6a6b:6c6d:6e6f:7071:7172"},
       NullBytes(),
       OUT_OF_RANGE},
      {"net.parse_packed_ip",
       {"6465:6667:6869:6a6b:6c6d:6e6f:7071:7172 "},
       NullBytes(),
       OUT_OF_RANGE},
      //    NULL input.
      {"net.parse_packed_ip", {NullString()}, NullBytes()},
      //    Input failure.
      {"net.parse_packed_ip", {"foo"}, NullBytes(), OUT_OF_RANGE},

      // NET.FORMAT_PACKED_IP.
      //    IPv4 Success.
      {"net.format_packed_ip", {Bytes("@ABC")}, "64.65.66.67"},
      {"net.format_packed_ip", {Bytes("@\0BC")}, "64.0.66.67"},
      //    IPv6 Success.
      {"net.format_packed_ip",
       {Bytes("defghijklmnopqqr")},
       "6465:6667:6869:6a6b:6c6d:6e6f:7071:7172"},
      {"net.format_packed_ip",
       {Bytes("defg\0ijklmnopqqr")},
       "6465:6667:69:6a6b:6c6d:6e6f:7071:7172"},
      {"net.format_packed_ip",
       {Bytes("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1")},
       "::1"},
      //    NULL input.
      {"net.format_packed_ip", {NullBytes()}, NullString()},
      //    Input failure.
      {"net.format_packed_ip", {Bytes("foo")}, NullString(), OUT_OF_RANGE},
      {"net.format_packed_ip", {Bytes("foobar")}, NullString(), OUT_OF_RANGE},

      // NET.IP_TO_STRING.
      //    IPv4 Success.
      {"net.ip_to_string", {Bytes("@ABC")}, "64.65.66.67"},
      {"net.ip_to_string", {Bytes("@\0BC")}, "64.0.66.67"},
      //    IPv6 Success.
      {"net.ip_to_string",
       {Bytes("defghijklmnopqqr")},
       "6465:6667:6869:6a6b:6c6d:6e6f:7071:7172"},
      {"net.ip_to_string",
       {Bytes("defg\0ijklmnopqqr")},
       "6465:6667:69:6a6b:6c6d:6e6f:7071:7172"},
      {"net.ip_to_string", {Bytes("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1")}, "::1"},
      //    NULL input.
      {"net.ip_to_string", {NullBytes()}, NullString()},
      //    Input failure.
      {"net.ip_to_string", {Bytes("foo")}, NullString(), OUT_OF_RANGE},
      {"net.ip_to_string", {Bytes("foobar")}, NullString(), OUT_OF_RANGE},

      // NET.IP_NET_MASK.
      {"net.ip_net_mask", {4LL, 32LL}, Bytes("\xFF\xFF\xFF\xFF")},
      {"net.ip_net_mask", {4LL, 28LL}, Bytes("\xFF\xFF\xFF\xF0")},
      {"net.ip_net_mask", {4LL, 24LL}, Bytes("\xFF\xFF\xFF\x00")},
      {"net.ip_net_mask", {4LL, 17LL}, Bytes("\xFF\xFF\x80\x00")},
      {"net.ip_net_mask", {4LL, 15LL}, Bytes("\xFF\xFE\x00\x00")},
      {"net.ip_net_mask", {4LL, 10LL}, Bytes("\xFF\xC0\x00\x00")},
      {"net.ip_net_mask", {4LL, 0LL}, Bytes("\x00\x00\x00\x00")},
      {"net.ip_net_mask", {16LL, 128LL}, Bytes(std::string(16, '\xFF'))},
      {"net.ip_net_mask",
       {16LL, 100LL},
       Bytes("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
             "\xFF\xFF\xFF\xFF\xF0\x00\x00\x00")},
      {"net.ip_net_mask", {16LL, 0LL}, Bytes(std::string(16, '\0'))},
      //    NULL input.
      {"net.ip_net_mask", {NullInt64(), 0LL}, NullBytes()},
      {"net.ip_net_mask", {4LL, NullInt64()}, NullBytes()},
      //    Input failure.
      {"net.ip_net_mask", {-4LL, 0LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_net_mask", {0LL, 0LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_net_mask", {3LL, 0LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_net_mask", {4LL, -1LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_net_mask", {4LL, 33LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_net_mask", {16LL, -1LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_net_mask", {16LL, 129LL}, NullBytes(), OUT_OF_RANGE},

      // NET.IP_TRUNC.
      {"net.ip_trunc",
       {Bytes("\x12\x34\x56\x78"), 32LL},
       Bytes("\x12\x34\x56\x78")},
      {"net.ip_trunc",
       {Bytes("\x12\x34\x56\x78"), 28LL},
       Bytes("\x12\x34\x56\x70")},
      {"net.ip_trunc",
       {Bytes("\x12\x34\x56\x78"), 24LL},
       Bytes("\x12\x34\x56\x00")},
      {"net.ip_trunc",
       {Bytes("\x12\x34\x56\x78"), 11LL},
       Bytes("\x12\x20\x00\x00")},
      {"net.ip_trunc",
       {Bytes("\x12\x34\x56\x78"), 0LL},
       Bytes("\x00\x00\x00\x00")},
      {"net.ip_trunc",
       {Bytes("defghijklmnopqqr"), 128LL},
       Bytes("defghijklmnopqqr")},
      {"net.ip_trunc",
       {Bytes("defghijklmno\x76\x54\x32\x10"), 100LL},
       Bytes("defghijklmno\x70\x00\x00\x00")},
      {"net.ip_trunc",
       {Bytes("defghijklmnopqqr"), 0LL},
       Bytes(std::string(16, '\0'))},
      //    NULL input.
      {"net.ip_trunc", {NullBytes(), 0LL}, NullBytes()},
      {"net.ip_trunc", {Bytes("\x12\x34\x56\x78"), NullInt64()}, NullBytes()},
      //    Input failure.
      {"net.ip_trunc", {Bytes(""), 0LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_trunc", {Bytes("\x12\x34\x56"), 0LL}, NullBytes(), OUT_OF_RANGE},
      {"net.ip_trunc",
       {Bytes("\x12\x34\x56\x78"), -1LL},
       NullBytes(),
       OUT_OF_RANGE},
      {"net.ip_trunc",
       {Bytes("\x12\x34\x56\x78"), 33LL},
       NullBytes(),
       OUT_OF_RANGE},
      {"net.ip_trunc",
       {Bytes("defghijklmnopqqr"), -1LL},
       NullBytes(),
       OUT_OF_RANGE},
      {"net.ip_trunc",
       {Bytes("defghijklmnopqqr"), 129LL},
       NullBytes(),
       OUT_OF_RANGE},

      // NET.MAKE_NET.
      //   IPv4 Success.
      {"net.make_net", {String("10.1.2.3"), 24}, String("10.1.2.0/24")},
      {"net.make_net", {String("10.1.2.3"), 8}, String("10.0.0.0/8")},
      {"net.make_net", {String("10.1.2.3"), 31}, String("10.1.2.2/31")},
      {"net.make_net", {String("10.1.2.3"), 32}, String("10.1.2.3/32")},
      {"net.make_net", {String("10.1.2.3/24"), 8}, String("10.0.0.0/8")},
      {"net.make_net", {String("10.1.2.3/24"), 24}, String("10.1.2.0/24")},
      //   IPv4 Failure.
      {"net.make_net", {String("10.1.2.3"), 33}, NullString(), OUT_OF_RANGE},
      {"net.make_net", {String("10.1.2.3/8"), 24}, NullString(), OUT_OF_RANGE},
      {"net.make_net", {String("10.1.2.3"), -1}, NullString(), OUT_OF_RANGE},
      {"net.make_net", {String(" 10.1.2.3"), 24}, NullString(), OUT_OF_RANGE},
      //   IPv6 Success.
      {"net.make_net", {String("2::1"), 16}, String("2::/16")},
      {"net.make_net", {String("1234::1"), 8}, String("1200::/8")},
      {"net.make_net", {String("2:1::1"), 48}, String("2:1::/48")},
      {"net.make_net", {String("2:1::/32"), 16}, String("2::/16")},
      {"net.make_net", {String("2:1::/32"), 32}, String("2:1::/32")},
      {"net.make_net", {String("2::1"), 128}, String("2::1/128")},
      {"net.make_net",
       {String("::ffff:192.168.1.1"), 112},
       String("::ffff:192.168.0.0/112")},
      //   IPv6 Failure.
      {"net.make_net", {String("2:1::/16"), 32}, NullString(), OUT_OF_RANGE},
      {"net.make_net", {String("2::1"), 129}, NullString(), OUT_OF_RANGE},
      {"net.make_net", {String(" 2::1"), 12}, NullString(), OUT_OF_RANGE},
      {"net.make_net",
       {String("::ffff:192.168.1.1.1"), 112},
       NullString(),
       OUT_OF_RANGE},
      {"net.make_net",
       {String("::ffff:192.168.1"), 112},
       NullString(),
       OUT_OF_RANGE},
      //    NULL input.
      {"net.make_net", {String("10.1.2.0"), NullInt32()}, NullString()},
      {"net.make_net", {String("2::1"), NullInt32()}, NullString()},
      {"net.make_net", {NullString(), 12}, NullString()},
      //   Input Failure.
      {"net.make_net", {String("foo"), 12}, NullString(), OUT_OF_RANGE},

      // NET.IP_IN_NET.
      //   IPv4 Success.
      {"net.ip_in_net", {String("10.1.2.3"), String("10.1.2.0/24")}, true},
      {"net.ip_in_net", {String("10.0.2.3"), String("10.0.2.0/24")}, true},
      {"net.ip_in_net", {String("10.1.3.3"), String("10.1.2.0/24")}, false},
      {"net.ip_in_net", {String("10.1.2.3/28"), String("10.1.2.0/24")}, true},
      {"net.ip_in_net", {String("10.1.2.3/24"), String("10.1.2.0/28")}, false},
      {"net.ip_in_net", {String("10.1.3.3"), String("10.1.2.0/24")}, false},
      //   IPv4 Failure.
      {"net.ip_in_net",
       {String(" 10.1.3.3"), String("10.1.2.0/24")},
       NullBool(),
       OUT_OF_RANGE},
      {"net.ip_in_net",
       {String("10.1.3.3"), String("10.1.2.0/24 ")},
       NullBool(),
       OUT_OF_RANGE},
      {"net.ip_in_net",
       {String("10.1.2.3/33"), String("10.1.2.0/28")},
       NullBool(),
       OUT_OF_RANGE},
      //   IPv6 Success.
      {"net.ip_in_net", {String("1::1"), String("1::/16")}, true},
      {"net.ip_in_net", {String("2::1"), String("1::/16")}, false},
      {"net.ip_in_net", {String("2:1::1/32"), String("2:1::/24")}, true},
      {"net.ip_in_net", {String("2:1::/24"), String("2:1::/48")}, false},
      //   IPv6 Failure.
      {"net.ip_in_net",
       {String(" 2::1"), String("1::/12")},
       NullBool(),
       OUT_OF_RANGE},
      {"net.ip_in_net",
       {String("2::1"), String(" 1::/12")},
       NullBool(),
       OUT_OF_RANGE},
      {"net.ip_in_net",
       {String(" 2::1"), String(" 1::/12")},
       NullBool(),
       OUT_OF_RANGE},
      //    NULL input.
      {"net.ip_in_net", {NullString(), String("1::/12")}, NullBool()},
      {"net.ip_in_net", {String("2::1"), NullString()}, NullBool()},
      {"net.ip_in_net", {NullString(), String("10.1.2.0/28")}, NullBool()},
      {"net.ip_in_net", {String("10.1.2.0/28"), NullString()}, NullBool()},
      //   Input Failure.
      {"net.ip_in_net",
       {String("foo"), String("foobar")},
       NullBool(),
       OUT_OF_RANGE},
  };

  const QueryParamsWithResult ip_from_string_test_items[] = {
      //    IPv4 Success.
      {{"64.65.66.67"}, Bytes("@ABC")},
      {{"64.0.66.67"}, Bytes("@\0BC")},
      //    IPv4 Failure.
      {{"64.65.66.67 "}, NullBytes(), OUT_OF_RANGE},
      {{" 64.65.66.67"}, NullBytes(), OUT_OF_RANGE},
      {{"64.65.66.67\0"}, NullBytes(), OUT_OF_RANGE},
      //    IPv6 Success.
      {{"6465:6667:6869:6a6b:6c6d:6e6f:7071:7172"}, Bytes("defghijklmnopqqr")},
      {{"6465:6667:6869:6A6B:6C6d:6e6f:7071:7172"}, Bytes("defghijklmnopqqr")},
      {{"::1"}, Bytes("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1")},
      {{"::ffff:64.65.66.67"}, Bytes("\0\0\0\0\0\0\0\0\0\0\xff\xff@ABC")},
      {{"6465:6667:6869:6a6b:6c6d:FFFF:128.129.130.131"},
       Bytes("defghijklm\xFF\xFF\x80\x81\x82\x83")},
      //    IPv6 Failure.
      {{" 6465:6667:6869:6a6b:6c6d:6e6f:7071:7172"}, NullBytes(), OUT_OF_RANGE},
      {{"6465:6667:6869:6a6b:6c6d:6e6f:7071:7172 "}, NullBytes(), OUT_OF_RANGE},
      {{"::1\0"}, NullBytes(), OUT_OF_RANGE},
      //    NULL input.
      {{NullString()}, NullBytes()},
      //    Input failure.
      {{"foo"}, NullBytes(), OUT_OF_RANGE},
  };
  for (const QueryParamsWithResult& item : ip_from_string_test_items) {
    result.push_back({"net.ip_from_string", item});

    CHECK(item.HasEmptyFeatureSetAndNothingElse());
    QueryParamsWithResult::ResultMap new_result_map = item.results();
    zetasql_base::FindOrDie(new_result_map, QueryParamsWithResult::kEmptyFeatureSet)
        .status = ::zetasql_base::OkStatus();

    QueryParamsWithResult new_item(item);
    new_item.set_results(new_result_map);
    result.push_back({"net.safe_ip_from_string", new_item});
  }

  struct UrlTestItem {
    ValueConstructor url;
    ValueConstructor host;
    ValueConstructor reg_domain;
    ValueConstructor public_suffix;
  };
  // These cases are copied from the examples at (broken link).
  const UrlTestItem kUrlTestItems[] = {
      // Input url, expected host, expected reg_domain, expected public_suffix.
      {NullString(), NullString(), NullString(), NullString()},

      // Cases without scheme.
      {"", NullString(), NullString(), NullString()},
      {" ", NullString(), NullString(), NullString()},
      {".", NullString(), NullString(), NullString()},
      {"com", "com", NullString(), "com"},
      {".com", "com", NullString(), "com"},
      {"com.", "com.", NullString(), "com."},
      {"Foo_bar", "Foo_bar", NullString(), NullString()},
      {"Google.COM", "Google.COM", "Google.COM", "COM"},
      {"Google..COM", "Google..COM", NullString(), NullString()},
      {"foo.com:12345", "foo.com", "foo.com", "com"},
      {"foo.com:", "foo.com", "foo.com", "com"},
      {"user:password@foo.com:12345", "foo.com", "foo.com", "com"},
      {"user:password@[2001:0db8:0000:0000:0000:ff00:0042:8329]:12345",
       "[2001:0db8:0000:0000:0000:ff00:0042:8329]", NullString(), NullString()},
      {"[2001:0db8:0000:0000:0000:ff00:0042:8329]",
       "[2001:0db8:0000:0000:0000:ff00:0042:8329]", NullString(), NullString()},
      {"[2001:0db8/0000:0000:0000:ff00:0042:8329]",
       "[2001:0db8", NullString(), NullString()},
      {"foo.com//bar", "foo.com", "foo.com", "com"},
      {"/google", NullString(), NullString(), NullString()},
      {"/Google.COM", NullString(), NullString(), NullString()},
      {"/www.Google.COM", NullString(), NullString(), NullString()},
      {"/://www.Google.COM", NullString(), NullString(), NullString()},
      {":/google", NullString(), NullString(), NullString()},
      {":/Google.COM", NullString(), NullString(), NullString()},
      {":/www.Google.COM", NullString(), NullString(), NullString()},
      {":google", NullString(), NullString(), NullString()},
      {":Google.COM", NullString(), NullString(), NullString()},
      {":www.Google.COM", NullString(), NullString(), NullString()},

      // Cases with relative schemes (starting with //).
      {"//", NullString(), NullString(), NullString()},
      {" // ", NullString(), NullString(), NullString()},
      {"//.", NullString(), NullString(), NullString()},
      {"//com", "com", NullString(), "com"},
      {"//.com", "com", NullString(), "com"},
      {"//com.", "com.", NullString(), "com."},
      {"//Foo_bar", "Foo_bar", NullString(), NullString()},
      // "google" is a registered public suffix, though not provided by Google.
      {"//google", "google", NullString(), "google"},
      {"//Google.COM", "Google.COM", "Google.COM", "COM"},
      {"//Google..COM", "Google..COM", NullString(), NullString()},
      {"//foo.com:12345", "foo.com", "foo.com", "com"},
      {"//foo.com:", "foo.com", "foo.com", "com"},
      {"//user:password@foo.com:12345", "foo.com", "foo.com", "com"},
      {"//user:password@[2001:0db8:0000:0000:0000:ff00:0042:8329]:12345",
       "[2001:0db8:0000:0000:0000:ff00:0042:8329]", NullString(), NullString()},
      {"//foo.com//bar", "foo.com", "foo.com", "com"},
      {"///google", NullString(), NullString(), NullString()},
      {"//:/google", NullString(), NullString(), NullString()},
      {"//:/Google.COM", NullString(), NullString(), NullString()},
      {"//:/www.Google.COM", NullString(), NullString(), NullString()},
      {"//:google", NullString(), NullString(), NullString()},
      {"//:Google.COM", NullString(), NullString(), NullString()},
      {"//:www.Google.COM", NullString(), NullString(), NullString()},

      // Cases with absolute schemes (containing "://")
      {"://google", "google", NullString(), "google"},
      {"://Google.COM", "Google.COM", "Google.COM", "COM"},
      {"://www.Google.COM", "www.Google.COM", "Google.COM", "COM"},
      {"http://", NullString(), NullString(), NullString()},
      {"http://.", NullString(), NullString(), NullString()},
      {"http://.com", "com", NullString(), "com"},
      {"http://com.", "com.", NullString(), "com."},
      {"http://google", "google", NullString(), "google"},
      {"http://Google.COM", "Google.COM", "Google.COM", "COM"},
      {"http://Google.COM..", "Google.COM.", "Google.COM.", "COM."},
      {"http://..Google.COM", "Google.COM", "Google.COM", "COM"},
      {"http://Google..COM", "Google..COM", NullString(), NullString()},
      {"http://www.Google.COM", "www.Google.COM", "Google.COM", "COM"},
      {"  http://www.Google.COM  ", "www.Google.COM", "Google.COM", "COM"},
      {"http://www.Google.co.uk", "www.Google.co.uk", "Google.co.uk", "co.uk"},
      {"http://www.Google.Co.uk", "www.Google.Co.uk", "Google.Co.uk", "Co.uk"},
      {"http://www.Google.co.UK", "www.Google.co.UK", "Google.co.UK", "co.UK"},
      {"http://foo.com:12345", "foo.com", "foo.com", "com"},
      {"http://foo.com:", "foo.com", "foo.com", "com"},
      {"http://www.Google.COM/foo", "www.Google.COM", "Google.COM", "COM"},
      {"http://www.Google.COM#foo", "www.Google.COM", "Google.COM", "COM"},
      {"http://www.Google.COM?foo=bar@", "www.Google.COM", "Google.COM", "COM"},
      {"http://user:password@www.Google.COM", "www.Google.COM", "Google.COM",
       "COM"},
      {"http://user:pass:word@www.Google.COM", "www.Google.COM", "Google.COM",
       "COM"},
      {"http://user:password@www.Goo@gle.COM", "gle.COM", "gle.COM", "COM"},
      {"@://user:password@www.Goo@gle.COM", "gle.COM", "gle.COM", "COM"},
      {"http://[2001:0db8:0000:0000:0000:ff00:0042:8329]:80",
       "[2001:0db8:0000:0000:0000:ff00:0042:8329]", NullString(), NullString()},
      {"http://[2001:0db8:0000:0000:0000:ff00:0042:8329]:80:90",
       "[2001:0db8:0000:0000:0000:ff00:0042:8329]", NullString(), NullString()},
      {"http://[[2001:0db8:0000:0000:0000:ff00:0042:8329]]:80", "[[2001",
       NullString(), NullString()},
      {"http://x@[2001:0db8:0000:0000:0000:ff00:0042:8329]",
       "[2001:0db8:0000:0000:0000:ff00:0042:8329]", NullString(), NullString()},
      {"http://x@[[2001:0db8:0000:0000:0000:ff00:0042:8329]]", "[[2001",
       NullString(), NullString()},
      {"http://1.2.3", "1.2.3", NullString(), NullString()},
      {"http://1.2.3.4", "1.2.3.4", NullString(), NullString()},
      {"http://1.2.3.4.5", "1.2.3.4.5", NullString(), NullString()},
      {"http://1000.2000.3000.4000", "1000.2000.3000.4000", NullString(),
          NullString()},
      {"http://a", "a", NullString(), NullString()},
      {"http://a.b", "a.b", NullString(), NullString()},
      {"http://abc.xyz", "abc.xyz", "abc.xyz", "xyz"},
      // "compute.amazonaws.com" is an entry in the private domain section of
      // the pubic suffix list. It should not be treated as a public suffix.
      {"http://compute.amazonaws.com", "compute.amazonaws.com", "amazonaws.com",
       "com"},
      // *.kh is a public suffix rule with a wildcard (*).
      {"http://foo.bar.kh", "foo.bar.kh", "foo.bar.kh", "bar.kh"},
      {" http://www.Google.COM ", "www.Google.COM", "Google.COM", "COM"},
      {"http://x y.com", "x y.com", "x y.com", "com"},
      {String("http://x\000y.com"), String("x\000y.com"), String("x\000y.com"),
       "com"},
      {"http://~.com", "~.com", "~.com", "com"},
      {"http://café.fr", "café.fr", "café.fr", "fr"},
      {"http://例子.卷筒纸", "例子.卷筒纸", NullString(), NullString()},
      {"http://例子.卷筒纸.中国", "例子.卷筒纸.中国", "卷筒纸.中国", "中国"},
      // "xn--fiqs8s" is the puny encoded result of "中国".
      // The output should be the original form, not the encoded result.
      {"http://例子.卷筒纸.xn--fiqs8s", "例子.卷筒纸.xn--fiqs8s",
       "卷筒纸.xn--fiqs8s", "xn--fiqs8s"},
      {"mailto://somebody@email.com", "email.com", "email.com", "com"},
      {"javascript://alert('hi')", "alert('hi')", NullString(), NullString()},

      // Cases with unsupported schemes.
      {"mailto:x@foo.com,y@bar.com", "bar.com", "bar.com", "com"},
      {"mailto:?to=&subject=&body=", "mailto", NullString(), NullString()},
      // Taken from https://tools.ietf.org/html/rfc3981
      {"iris.beep:dreg1//com/domain/example.com", "iris.beep", NullString(),
       NullString()},
      {"iris.beep:dreg1/bottom/example.com/domain/example.com", "iris.beep",
       NullString(), NullString()},
  };

  for (const UrlTestItem& item : kUrlTestItems) {
    result.push_back({"net.host", {item.url.get()}, item.host.get()});
    result.push_back(
        {"net.reg_domain", {item.url.get()}, item.reg_domain.get()});
    result.push_back(
        {"net.public_suffix", {item.url.get()}, item.public_suffix.get()});
  }
  return result;
}

std::vector<FunctionTestCall> GetFunctionTestsBitCast() {
  return {
    // Null -> Null
    {"bit_cast_to_int32", {NullInt32()}, NullInt32()},
    {"bit_cast_to_int32", {NullUint32()}, NullInt32()},
    {"bit_cast_to_int64", {NullInt64()}, NullInt64()},
    {"bit_cast_to_int64", {NullUint64()}, NullInt64()},
    {"bit_cast_to_uint32", {NullUint32()}, NullUint32()},
    {"bit_cast_to_uint32", {NullInt32()}, NullUint32()},
    {"bit_cast_to_uint64", {NullUint64()}, NullUint64()},
    {"bit_cast_to_uint64", {NullInt64()}, NullUint64()},

    // INT32 -> INT32
    {"bit_cast_to_int32", {Int32(0)}, Int32(0)},
    {"bit_cast_to_int32", {Int32(int32max)}, Int32(int32max)},
    {"bit_cast_to_int32", {Int32(int32min)}, Int32(int32min)},
    {"bit_cast_to_int32", {Int32(3)}, Int32(3)},
    {"bit_cast_to_int32", {Int32(-3)}, Int32(-3)},

    // UINT32 -> INT32
    {"bit_cast_to_int32", {Uint32(0)}, Int32(0)},
    {"bit_cast_to_int32", {Uint32(uint32max)}, Int32(-1)},
    {"bit_cast_to_int32", {Uint32(3)}, Int32(3)},
    {"bit_cast_to_int32", {Uint32(uint32max - 3)}, Int32(-4)},
    {"bit_cast_to_int32", {Uint32(uint32max >> 1)}, Int32(int32max)},

    // INT64 -> INT64
    {"bit_cast_to_int64", {Int64(0)}, Int64(0)},
    {"bit_cast_to_int64", {Int64(int64max)}, Int64(int64max)},
    {"bit_cast_to_int64", {Int64(int64min)}, Int64(int64min)},
    {"bit_cast_to_int64", {Int64(3)}, Int64(3)},
    {"bit_cast_to_int64", {Int64(-3)}, Int64(-3)},

    // UINT64 -> INT64
    {"bit_cast_to_int64", {Uint64(0)}, Int64(0)},
    {"bit_cast_to_int64", {Uint64(uint64max)}, Int64(-1)},
    {"bit_cast_to_int64", {Uint64(3)}, Int64(3)},
    {"bit_cast_to_int64", {Uint64(uint64max - 3)}, Int64(-4)},
    {"bit_cast_to_int64", {Uint64(uint64max >> 1)}, Int64(int64max)},

    // UINT32 -> UINT32
    {"bit_cast_to_uint32", {Uint32(0)}, Uint32(0)},
    {"bit_cast_to_uint32", {Uint32(uint32max)}, Uint32(uint32max)},
    {"bit_cast_to_uint32", {Uint32(3)}, Uint32(3)},

    // INT32 -> UINT32
    {"bit_cast_to_uint32", {Int32(0)}, Uint32(0)},
    {"bit_cast_to_uint32", {Int32(int32max)}, Uint32(int32max)},
    {"bit_cast_to_uint32", {Int32(3)}, Uint32(3)},
    {"bit_cast_to_uint32", {Int32(-3)}, Uint32(-3)},
    {"bit_cast_to_uint32", {Int32(int32min)}, Uint32(int32min)},
    {"bit_cast_to_uint32", {Int32(int32min + 3)}, Uint32(2147483651)},

    // UINT64 -> UINT64
    {"bit_cast_to_uint64", {Uint64(0)}, Uint64(0)},
    {"bit_cast_to_uint64", {Uint64(uint64max)}, Uint64(uint64max)},
    {"bit_cast_to_uint64", {Uint64(3)}, Uint64(3)},

    // INT64 -> UINT64
    {"bit_cast_to_uint64", {Int64(0)}, Uint64(0)},
    {"bit_cast_to_uint64", {Int64(int64max)}, Uint64(int64max)},
    {"bit_cast_to_uint64", {Int64(3)}, Uint64(3)},
    {"bit_cast_to_uint64", {Int64(-3)}, Uint64(-3)},
    {"bit_cast_to_uint64", {Int64(int64min)}, Uint64(int64min)},
    {"bit_cast_to_uint64", {Int64(int64min + 3)},
     Uint64(9223372036854775811ULL)},
  };
}

std::vector<FunctionTestCall> GetFunctionTestsGenerateArray() {
  const Value empty_int64_array = Int64Array({});
  const Value numeric_zero = Value::Numeric(NumericValue());
  const Value numeric_one = Value::Numeric(NumericValue(static_cast<int64_t>(1)));
  const Value numeric_two = Value::Numeric(NumericValue(static_cast<int64_t>(2)));
  const Value numeric_three =
      Value::Numeric(NumericValue(static_cast<int64_t>(3)));
  const Value numeric_pi = Value::Numeric(
      NumericValue::FromStringStrict("3.141592654").ValueOrDie());
  const Value numeric_pos_min = Value::Numeric(
      NumericValue::FromStringStrict("0.000000001").ValueOrDie());
  const Value numeric_negative_golden_ratio = Value::Numeric(
      NumericValue::FromStringStrict("-1.618033988").ValueOrDie());
  const Value numeric_eleven =
      Value::Numeric(NumericValue(static_cast<int64_t>(11)));
  const Value numeric_max = Value::Numeric(NumericValue::MaxValue());
  const Value numeric_min = Value::Numeric(NumericValue::MinValue());

  std::vector<FunctionTestCall> all_tests = {
      // Null inputs.
      {"generate_array", {NullInt64(), NullInt64()}, Null(Int64ArrayType())},
      {"generate_array",
       {NullInt64(), NullInt64(), NullInt64()},
       Null(Int64ArrayType())},
      {"generate_array", {Int64(1), NullInt64()}, Null(Int64ArrayType())},
      {"generate_array", {NullInt64(), Int64(1)}, Null(Int64ArrayType())},
      {"generate_array",
       {Int64(1), Int64(2), NullInt64()},
       Null(Int64ArrayType())},
      {"generate_array", {NullUint64(), NullUint64()}, Null(Uint64ArrayType())},
      {"generate_array", {NullDouble(), NullDouble()}, Null(DoubleArrayType())},
      {"generate_array",
       {NullDouble(), Double(2), Double(double_pos_inf)},
       Null(DoubleArrayType())},
      {"generate_array",
       {Double(0), NullDouble(), Double(double_neg_inf)},
       Null(DoubleArrayType())},
      {"generate_array",
       {NullDouble(), NullDouble(), Double(double_nan)},
       Null(DoubleArrayType())},
      {"generate_array",
       QueryParamsWithResult({numeric_zero, NullNumeric(), numeric_three},
                             Null(NumericArrayType()))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      // Empty generate_array.
      {"generate_array", {Int64(1), Int64(0)}, empty_int64_array},
      {"generate_array", {Int64(1), Int64(5), Int64(-1)}, empty_int64_array},
      {"generate_array", {Int64(5), Int64(1)}, empty_int64_array},
      {"generate_array", {Int64(1), Int64(0)}, empty_int64_array},
      {"generate_array", {Int64(5), Int64(0), Int64(2)}, empty_int64_array},
      {"generate_array",
       QueryParamsWithResult({numeric_three, numeric_zero, numeric_three},
                             NumericArray({}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      // Non-empty generate_array.
      {"generate_array", {Int64(2), Int64(2)}, Int64Array({2})},
      {"generate_array", {Uint64(2), Uint64(2)}, Uint64Array({2})},
      {"generate_array", {Double(2), Double(2)}, DoubleArray({2})},
      {"generate_array",
       {Int64(1), Int64(10)},
       Int64Array({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})},
      {"generate_array",
       {Uint64(1), Uint64(10)},
       Uint64Array({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})},
      {"generate_array",
       {Double(1), Double(10)},
       DoubleArray({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})},
      {"generate_array", {Int64(-5), Int64(-5)}, Int64Array({-5})},
      {"generate_array",
       {Int64(0), Int64(-10), Int64(-3)},
       Int64Array({0, -3, -6, -9})},
      {"generate_array",
       {Double(0), Double(-10), Double(-3)},
       DoubleArray({0, -3, -6, -9}),
       FloatMargin::UlpMargin(FloatMargin::kDefaultUlpBits)},
      {"generate_array",
       {Uint64(10), Uint64(20), Uint64(3)},
       Uint64Array({10, 13, 16, 19})},
      {"generate_array",
       {Double(10), Double(20), Double(3.33)},
       DoubleArray({10, 13.33, 16.66, 19.99}),
       FloatMargin::UlpMargin(FloatMargin::kDefaultUlpBits)},
      {"generate_array",
       {Double(0.05), Double(0.1), Double(0.01)},
       DoubleArray({0.05, 0.06, 0.07, 0.08, 0.09, 0.1}),
       FloatMargin::UlpMargin(FloatMargin::kDefaultUlpBits)},
      {"generate_array",
       {Double(5), Double(10), Double(1)},
       DoubleArray({5, 6, 7, 8, 9, 10}),
       FloatMargin::UlpMargin(FloatMargin::kDefaultUlpBits)},
      {"generate_array", {Int64(1), Int64(3)}, Int64Array({1, 2, 3})},
      {"generate_array", {Int64(4), Int64(8), Int64(2)}, Int64Array({4, 6, 8})},
      {"generate_array", {Int64(4), Int64(9), Int64(2)}, Int64Array({4, 6, 8})},
      {"generate_array",
       {Int64(8), Int64(4), Int64(-2)},
       Int64Array({8, 6, 4})},
      {"generate_array",
       {Int64(10), Int64(19), Int64(5)},
       Int64Array({10, 15})},
      {"generate_array",
       {Int64(-15), Int64(-10), Int64(5)},
       Int64Array({-15, -10})},
      {"generate_array",
       {Int64(-15), Int64(-10), Int64(5)},
       Int64Array({-15, -10})},
      {"generate_array",
       {Int64(int64max), Int64(int64max - 2), Int64(-1)},
       Int64Array({int64max, int64max - 1, int64max - 2})},
      {"generate_array",
       QueryParamsWithResult(
           {numeric_zero, numeric_three, numeric_one},
           Array({numeric_zero, numeric_one, numeric_two, numeric_three}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       QueryParamsWithResult(
           {numeric_one, numeric_eleven, numeric_three},
           NumericArray({NumericValue(static_cast<int64_t>(1)),
                         NumericValue(static_cast<int64_t>(4)),
                         NumericValue(static_cast<int64_t>(7)),
                         NumericValue(static_cast<int64_t>(10))}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       QueryParamsWithResult(
           {numeric_eleven, numeric_one,
            Value::Numeric(NumericValue(static_cast<int64_t>(-4)))},
           NumericArray({NumericValue(static_cast<int64_t>(11)),
                         NumericValue(static_cast<int64_t>(7)),
                         NumericValue(static_cast<int64_t>(3))}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       QueryParamsWithResult(
           {numeric_eleven, numeric_pi, numeric_negative_golden_ratio},
           NumericArray(
               {NumericValue(static_cast<int64_t>(11)),
                NumericValue::FromStringStrict("9.381966012").ValueOrDie(),
                NumericValue::FromStringStrict("7.763932024").ValueOrDie(),
                NumericValue::FromStringStrict("6.145898036").ValueOrDie(),
                NumericValue::FromStringStrict("4.527864048").ValueOrDie()}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       QueryParamsWithResult(
           {numeric_negative_golden_ratio, numeric_three, numeric_one},
           NumericArray(
               {numeric_negative_golden_ratio.numeric_value(),
                NumericValue::FromStringStrict("-0.618033988").ValueOrDie(),
                NumericValue::FromStringStrict("0.381966012").ValueOrDie(),
                NumericValue::FromStringStrict("1.381966012").ValueOrDie(),
                NumericValue::FromStringStrict("2.381966012").ValueOrDie()}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       QueryParamsWithResult({numeric_zero, numeric_pos_min, numeric_pos_min},
                             Array({numeric_zero, numeric_pos_min}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       QueryParamsWithResult({numeric_max, numeric_max, numeric_one},
                             Array({numeric_max}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       QueryParamsWithResult({numeric_max, numeric_max, numeric_pos_min},
                             Array({numeric_max}))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array", QueryParamsWithResult({numeric_min, numeric_min,
                                                numeric_negative_golden_ratio},
                                               Array({numeric_min}))
                             .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      // Guarding against overflows.
      {"generate_array",
       {Int64(int64max - 2), Int64(int64max), Int64(5)},
       Int64Array({int64max - 2})},
      {"generate_array",
       {Int64(int64min + 2), Int64(int64min), Int64(-5)},
       Int64Array({int64min + 2})},
      {"generate_array",
       {Uint64(uint64max - 2), Uint64(uint64max), Uint64(1)},
       Uint64Array({uint64max - 2, uint64max - 1, uint64max})},
      {"generate_array",
       {Double(doublemax), Double(doublemax), Double(doublemax)},
       DoubleArray({doublemax})},
      {"generate_array",
       {Double(0), Double(doubleminpositive), Double(doubleminpositive)},
       DoubleArray({0, doubleminpositive})},
      {"generate_array",
       {Double(0), Double(doubleminpositive * 3), Double(doubleminpositive)},
       DoubleArray({0, doubleminpositive, doubleminpositive * 2,
                    doubleminpositive * 3}),
       FloatMargin::UlpMargin(1)},
      {"generate_array",
       {Int64(int64min), Int64(int64min + 5), Int64(2)},
       Int64Array({int64min, int64min + 2, int64min + 4})},
      // Zero step size.
      {"generate_array",
       {Int64(1), Int64(2), Int64(0)},
       Null(Int64ArrayType()),
       OUT_OF_RANGE},
      {"generate_array",
       {Uint64(1), Uint64(2), Uint64(0)},
       Null(Uint64ArrayType()),
       OUT_OF_RANGE},
      {"generate_array",
       {Double(1), Double(2), Double(0.0)},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},
      {"generate_array",
       {Double(1), Double(2), Double(-0.0)},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},
      {"generate_array",
       QueryParamsWithResult({numeric_one, numeric_two, numeric_zero},
                             Null(NumericArrayType()), OUT_OF_RANGE)
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"generate_array",
       {Double(double_pos_inf), Double(double_pos_inf), Double(1)},
       DoubleArray({double_pos_inf})},  // Adding to +inf.
      {"generate_array",
       {Double(double_neg_inf), Double(double_neg_inf), Double(1)},
       DoubleArray({double_neg_inf})},  // Adding to -inf.
      {"generate_array",
       {Double(0), Double(10), Double(double_pos_inf)},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},  // +inf as a step.
      {"generate_array",
       {Double(10), Double(0), Double(double_neg_inf)},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},  // -inf as a step.
      {"generate_array",
       {Double(0), Double(10),
        Double(std::numeric_limits<double>::denorm_min())},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},  // Adding the minimum positive subnormal value.
      {"generate_array",
       {Double(0), Double(10), Double(std::numeric_limits<double>::epsilon())},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},  // Adding epsilon.
      {"generate_array",
       {Double(double_nan), Double(1), Double(1)},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},  // Adding to nan.
      {"generate_array",
       {Double(1), Double(double_nan), Double(1)},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},  // Adding to nan.
      {"generate_array",
       {Double(1), Double(2), Double(double_nan)},
       Null(DoubleArrayType()),
       OUT_OF_RANGE},  // nan as a step.
      {"generate_array",
       QueryParamsWithResult({numeric_zero, numeric_one, numeric_pos_min},
                             Null(NumericArrayType()), OUT_OF_RANGE)
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},  // Large NUMERIC range.
      {"generate_array",
       QueryParamsWithResult({numeric_min, numeric_max, numeric_one},
                             Null(NumericArrayType()), OUT_OF_RANGE)
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},  // Large NUMERIC range.
  };

  return all_tests;
}

namespace {

Value DateArray(const std::vector<int32_t>& values) {
  std::vector<Value> date_values;
  date_values.reserve(values.size());
  for (int32_t value : values) {
    date_values.push_back(Date(value));
  }
  return Value::Array(DateArrayType(), date_values);
}

}  // namespace

std::vector<FunctionTestCall> GetFunctionTestsRangeBucket() {
  const Value numeric_zero = Value::Numeric(NumericValue());
  const Value numeric_one = Value::Numeric(NumericValue(static_cast<int64_t>(1)));
  const Value numeric_max = Value::Numeric(NumericValue::MaxValue());
  const Value numeric_min = Value::Numeric(NumericValue::MinValue());

  std::vector<FunctionTestCall> all_tests = {
      // Null inputs.
      {"range_bucket", {NullInt64(), Int64Array({1})}, NullInt64()},
      {"range_bucket", {Int64(1), Null(Int64ArrayType())}, NullInt64()},
      {"range_bucket", {NullInt64(), Null(Int64ArrayType())}, NullInt64()},
      {"range_bucket",
       {Int64(5), values::Array(Int64ArrayType(), {NullInt64()})},
       NullInt64(),
       OUT_OF_RANGE},
      // Empty array.
      {"range_bucket", {Int64(1), Int64Array({})}, Int64(0)},
      // Non-empty array.
      {"range_bucket", {Int64(-1), Int64Array({0})}, Int64(0)},
      {"range_bucket", {Int64(1), Int64Array({1})}, Int64(1)},
      {"range_bucket", {Int64(1), Int64Array({1, 1, 2})}, Int64(2)},
      {"range_bucket", {Int64(1), Int64Array({1, 1, 1})}, Int64(3)},
      {"range_bucket", {Int64(25), Int64Array({10, 20, 30})}, Int64(2)},
      {"range_bucket", {String("b"), StringArray({"a", "c"})}, Int64(1)},
      // Bool.
      {"range_bucket", {Bool(false), BoolArray({false, true})}, Int64(1)},
      {"range_bucket", {Bool(true), BoolArray({false, true})}, Int64(2)},
      // Bytes.
      {"range_bucket", {Bytes("b"), BytesArray({"a", "c"})}, Int64(1)},
      // Date.
      {"range_bucket",
       {Date(date_min), DateArray({date_min, 1, date_max})},
       Int64(1)},
      {"range_bucket", {Date(2), DateArray({date_min, 1, date_max})}, Int64(2)},
      {"range_bucket",
       {Date(date_max), DateArray({date_min, 1, date_max})},
       Int64(3)},
      // Double.
      {"range_bucket",
       {Double(double_neg_inf), DoubleArray({double_neg_inf, doublemin, 10,
                                             doublemax, double_pos_inf})},
       Int64(1)},
      {"range_bucket",
       {Double(doublemin), DoubleArray({double_neg_inf, doublemin, 10,
                                        doublemax, double_pos_inf})},
       Int64(2)},
      {"range_bucket",
       {Double(20), DoubleArray({double_neg_inf, doublemin, 10, doublemax,
                                 double_pos_inf})},
       Int64(3)},
      {"range_bucket",
       {Double(doublemax), DoubleArray({double_neg_inf, doublemin, 10,
                                        doublemax, double_pos_inf})},
       Int64(4)},
      {"range_bucket",
       {Double(double_pos_inf), DoubleArray({double_neg_inf, doublemin, 10,
                                             doublemax, double_pos_inf})},
       Int64(5)},
      {"range_bucket",
       {Double(double_nan), DoubleArray({10, 20, 30})},
       NullInt64()},
      {"range_bucket",
       {Double(1), DoubleArray({double_nan, 20, 30})},
       NullInt64(),
       OUT_OF_RANGE},
      // Float.
      {"range_bucket",
       {Float(float_neg_inf),
        FloatArray({float_neg_inf, floatmin, 10, floatmax, float_pos_inf})},
       Int64(1)},
      {"range_bucket",
       {Float(floatmin),
        FloatArray({float_neg_inf, floatmin, 10, floatmax, float_pos_inf})},
       Int64(2)},
      {"range_bucket",
       {Float(20),
        FloatArray({float_neg_inf, floatmin, 10, floatmax, float_pos_inf})},
       Int64(3)},
      {"range_bucket",
       {Float(floatmax),
        FloatArray({float_neg_inf, floatmin, 10, floatmax, float_pos_inf})},
       Int64(4)},
      {"range_bucket",
       {Float(float_pos_inf),
        FloatArray({float_neg_inf, floatmin, 10, floatmax, float_pos_inf})},
       Int64(5)},
      {"range_bucket",
       {Float(float_nan), FloatArray({10, 20, 30})},
       NullInt64()},
      {"range_bucket",
       {Float(1), FloatArray({float_nan, 20, 30})},
       NullInt64(),
       OUT_OF_RANGE},
      // Int32.
      {"range_bucket",
       {Int32(int32min), Int32Array({int32min, 10, int32max})},
       Int64(1)},
      {"range_bucket",
       {Int32(20), Int32Array({int32min, 10, int32max})},
       Int64(2)},
      {"range_bucket",
       {Int32(int32max), Int32Array({int32min, 10, int32max})},
       Int64(3)},
      // Int64.
      {"range_bucket",
       {Int64(int64min), Int64Array({int64min, 10, int64max})},
       Int64(1)},
      {"range_bucket",
       {Int64(20), Int64Array({int64min, 10, int64max})},
       Int64(2)},
      {"range_bucket",
       {Int64(int64max), Int64Array({int64min, 10, int64max})},
       Int64(3)},
      // Uint32.
      {"range_bucket", {Uint32(0), Uint32Array({0, 10, uint32max})}, Int64(1)},
      {"range_bucket", {Uint32(20), Uint32Array({0, 10, uint32max})}, Int64(2)},
      {"range_bucket",
       {Uint32(uint32max), Uint32Array({0, 10, uint32max})},
       Int64(3)},
      // Uint64.
      {"range_bucket", {Uint64(0), Uint64Array({0, 10, uint64max})}, Int64(1)},
      {"range_bucket", {Uint64(20), Uint64Array({0, 10, uint64max})}, Int64(2)},
      {"range_bucket",
       {Uint64(uint64max), Uint64Array({0, 10, uint64max})},
       Int64(3)},
      // Numeric.
      {"range_bucket",
       QueryParamsWithResult(
           {numeric_min,
            values::Array(NumericArrayType(),
                          {numeric_min, numeric_zero, numeric_max})},
           Int64(1))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"range_bucket",
       QueryParamsWithResult(
           {numeric_one,
            values::Array(NumericArrayType(),
                          {numeric_min, numeric_zero, numeric_max})},
           Int64(2))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      {"range_bucket",
       QueryParamsWithResult(
           {numeric_max,
            values::Array(NumericArrayType(),
                          {numeric_min, numeric_zero, numeric_max})},
           Int64(3))
           .WrapWithFeature(FEATURE_NUMERIC_TYPE)},
      // Timestamp.
      {"range_bucket",
       {Timestamp(timestamp_min),
        values::Array(types::TimestampArrayType(),
                      {Timestamp(timestamp_min), Timestamp(10),
                       Timestamp(timestamp_max)})},
       Int64(1)},
      {"range_bucket",
       {Timestamp(20), values::Array(types::TimestampArrayType(),
                                     {Timestamp(timestamp_min), Timestamp(10),
                                      Timestamp(timestamp_max)})},
       Int64(2)},
      {"range_bucket",
       {Timestamp(timestamp_max),
        values::Array(types::TimestampArrayType(),
                      {Timestamp(timestamp_min), Timestamp(10),
                       Timestamp(timestamp_max)})},
       Int64(3)},
      // Error cases.
      {"range_bucket",
       {Double(15), DoubleArray({10, 20, 30, 25})},
       NullInt64(),
       OUT_OF_RANGE},  // Non-monotically increasing array.
  };

  return all_tests;
}

std::vector<FunctionTestCall> GetFunctionTestsGenerateDateArray() {
  const EnumType* part_enum;
  const google::protobuf::EnumDescriptor* enum_descriptor =
      functions::DateTimestampPart_descriptor();
  ZETASQL_CHECK_OK(type_factory()->MakeEnumType(enum_descriptor, &part_enum));

  const std::vector<FunctionTestCall> all_tests = {
      {"generate_date_array",
       {NullDate(), NullDate(), NullInt64(), Enum(part_enum, "DAY")},
       Null(DateArrayType())},
      // Empty generate_date_array.
      {"generate_date_array",
       {Date(10), Date(5), Int64(1), Enum(part_enum, "DAY")},
       DateArray({})},
      {"generate_date_array",
       {Date(90), Date(10), Int64(1), Enum(part_enum, "MONTH")},
       DateArray({})},
      // Non-empty generate_date_array.
      {"generate_date_array",
       {Date(10), Date(15), Int64(1), Enum(part_enum, "DAY")},
       DateArray({10, 11, 12, 13, 14, 15})},
      {"generate_date_array",
       {Date(10), Date(1000), Int64(100), Enum(part_enum, "DAY")},
       DateArray({10, 110, 210, 310, 410, 510, 610, 710, 810, 910})},
      {"generate_date_array",
       {Date(date_min), Date(date_max), Int64(date_max - date_min),
        Enum(part_enum, "DAY")},
       DateArray({date_min, date_max})},
      {"generate_date_array",
       {Date(10), Date(60), Int64(2), Enum(part_enum, "WEEK")},
       DateArray({10, 24, 38, 52})},
      {"generate_date_array",
       {Date(75), Date(30), Int64(-1), Enum(part_enum, "MONTH")},
       DateArray({75, 47})},
      // Guarding against overflows.
      {"generate_date_array",
       {Date(date_max - 3), Date(date_max), Int64(2), Enum(part_enum, "DAY")},
       DateArray({date_max - 3, date_max - 1})},
      {"generate_date_array",
       {Date(date_min), Date(date_min), Int64(-1), Enum(part_enum, "DAY")},
       DateArray({date_min})},
      {"generate_date_array",
       {Date(date_min), Date(date_max), int64max, Enum(part_enum, "DAY")},
       DateArray({date_min})},
      {"generate_date_array",
       {Date(date_max), Date(date_min), int64min, Enum(part_enum, "DAY")},
       DateArray({date_max})},
      {"generate_date_array",
       {Date(1), Date(2), Int64(1), Enum(part_enum, "NANOSECOND")},
       Null(DateArrayType()),
       OUT_OF_RANGE},  // Invalid date part.
      {"generate_date_array",
       {Date(1), Date(2), Int64(1), Enum(part_enum, "MICROSECOND")},
       Null(DateArrayType()),
       OUT_OF_RANGE},  // Invalid date part.
      {"generate_date_array",
       {Date(1), Date(2), Int64(1), Enum(part_enum, "SECOND")},
       Null(DateArrayType()),
       OUT_OF_RANGE},  // Invalid date part.
      {"generate_date_array",
       {Date(1), Date(2), Int64(1), Null(part_enum)},
       Null(DateArrayType()),
       INVALID_ARGUMENT},  // Invalid null date part.
      {"generate_date_array",
       {Date(1), Date(2), Int64(1)},
       Null(DateArrayType()),
       INVALID_ARGUMENT},  // No date part.
  };

  return all_tests;
}

namespace {

Value TimestampArray(
    const std::vector<std::string>& timestamp_strings,
    functions::TimestampScale scale = functions::kMicroseconds) {
  std::vector<Value> values;
  values.reserve(timestamp_strings.size());
  for (const std::string& timestamp_string : timestamp_strings) {
    values.push_back(TimestampFromStr(timestamp_string, scale));
  }
  return Value::Array(TimestampArrayType(), values);
}

}  // namespace

std::vector<FunctionTestCall> GetFunctionTestsGenerateTimestampArray() {
  const EnumType* part_enum;
  const google::protobuf::EnumDescriptor* enum_descriptor =
      functions::DateTimestampPart_descriptor();
  ZETASQL_CHECK_OK(type_factory()->MakeEnumType(enum_descriptor, &part_enum));

  const std::vector<FunctionTestCall> all_tests = {
      {"generate_timestamp_array",
       {NullTimestamp(), NullTimestamp(), NullInt64(), Enum(part_enum, "DAY")},
       Null(TimestampArrayType())},
      {"generate_timestamp_array",
       {NullTimestamp(), TimestampFromStr("2017-01-02"), Int64(1),
        Enum(part_enum, "DAY")},
       Null(TimestampArrayType())},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-02"), NullTimestamp(), Int64(1),
        Enum(part_enum, "DAY")},
       Null(TimestampArrayType())},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-02"), TimestampFromStr("2017-01-02"),
        NullInt64(), Enum(part_enum, "DAY")},
       Null(TimestampArrayType())},
      // Empty generate_timestamp_array.
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-02"), TimestampFromStr("2017-01-01"),
        Int64(1), Enum(part_enum, "DAY")},
       TimestampArray({})},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-02"), TimestampFromStr("2016-12-31"),
        Int64(1), Enum(part_enum, "MICROSECOND")},
       TimestampArray({})},
      // Non-empty generate_timestamp_array.
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-01"), TimestampFromStr("2017-01-04"),
        Int64(1), Enum(part_enum, "DAY")},
       TimestampArray(
           {"2017-01-01", "2017-01-02", "2017-01-03", "2017-01-04"})},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-01"), TimestampFromStr("2020-01-01"),
        Int64(100), Enum(part_enum, "DAY")},
       TimestampArray({"2017-01-01", "2017-04-11", "2017-07-20", "2017-10-28",
                       "2018-02-05", "2018-05-16", "2018-08-24", "2018-12-02",
                       "2019-03-12", "2019-06-20", "2019-09-28"})},
      {"generate_timestamp_array",
       {Timestamp(timestamp_min), Timestamp(timestamp_max),
        Int64(timestamp_max - timestamp_min), Enum(part_enum, "MICROSECOND")},
       Value::Array(TimestampArrayType(),
                    {Timestamp(timestamp_min), Timestamp(timestamp_max)})},
      {"generate_timestamp_array",
       QueryParamsWithResult({TimestampFromStr("2017-01-01 01:02:03.456789"),
                              TimestampFromStr("2017-01-01 01:02:03.456790"),
                              Int64(333), Enum(part_enum, "NANOSECOND")},
                             TimestampArray({"2017-01-01 01:02:03.456789",
                                             "2017-01-01 01:02:03.456789333",
                                             "2017-01-01 01:02:03.456789666",
                                             "2017-01-01 01:02:03.456789999"},
                                            functions::kNanoseconds))
           .WrapWithFeature(FEATURE_TIMESTAMP_NANOS)},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-01 01:02:03.456789"),
        TimestampFromStr("2017-01-01 01:02:03.456801"), Int64(3),
        Enum(part_enum, "MICROSECOND")},
       TimestampArray(
           {"2017-01-01 01:02:03.456789", "2017-01-01 01:02:03.456792",
            "2017-01-01 01:02:03.456795", "2017-01-01 01:02:03.456798",
            "2017-01-01 01:02:03.456801"})},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-01 01:02:03.456789"),
        TimestampFromStr("2017-01-01 01:02:03.490000"), Int64(11),
        Enum(part_enum, "MILLISECOND")},
       TimestampArray(
           {"2017-01-01 01:02:03.456789", "2017-01-01 01:02:03.467789",
            "2017-01-01 01:02:03.478789", "2017-01-01 01:02:03.489789"})},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-01 01:02:03.456789"),
        TimestampFromStr("2016-12-30 12:00:00"), Int64(-76543),
        Enum(part_enum, "SECOND")},
       TimestampArray(
           {"2017-01-01 01:02:03.456789", "2016-12-31 03:46:20.456789"})},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-01 01:02:03.456789"),
        TimestampFromStr("2017-01-01 01:10:00"), Int64(2),
        Enum(part_enum, "MINUTE")},
       TimestampArray(
           {"2017-01-01 01:02:03.456789", "2017-01-01 01:04:03.456789",
            "2017-01-01 01:06:03.456789", "2017-01-01 01:08:03.456789"})},
      {"generate_timestamp_array",
       {TimestampFromStr("2017-01-01"), TimestampFromStr("2017-01-05 12:00:00"),
        Int64(25), Enum(part_enum, "HOUR")},
       TimestampArray({"2017-01-01", "2017-01-02 01:00:00",
                       "2017-01-03 02:00:00", "2017-01-04 03:00:00",
                       "2017-01-05 04:00:00"})},
      // Guarding against overflows.
      {"generate_timestamp_array",
       {Timestamp(timestamp_max - 3), Timestamp(timestamp_max), Int64(2),
        Enum(part_enum, "MICROSECOND")},
       Value::Array(TimestampArrayType(), {Timestamp(timestamp_max - 3),
                                           Timestamp(timestamp_max - 1)})},
      {"generate_timestamp_array",
       {Timestamp(timestamp_min), Timestamp(timestamp_min), Int64(-1),
        Enum(part_enum, "MICROSECOND")},
       Value::Array(TimestampArrayType(), {Timestamp(timestamp_min)})},
      {"generate_timestamp_array",
       {Timestamp(timestamp_min), Timestamp(timestamp_max), int64max,
        Enum(part_enum, "MICROSECOND")},
       Value::Array(TimestampArrayType(), {Timestamp(timestamp_min)})},
      {"generate_timestamp_array",
       {Timestamp(timestamp_max), Timestamp(timestamp_min), int64min,
        Enum(part_enum, "MICROSECOND")},
       Value::Array(TimestampArrayType(), {Timestamp(timestamp_max)})},
  };

  return all_tests;
}

const std::string EscapeKey(bool sql_standard_mode, const std::string& key) {
  if (sql_standard_mode) {
    return absl::StrCat(".\"", key, "\"");
  } else {
    return absl::StrCat("['", key, "']");
  }
}

const std::vector<FunctionTestCall> GetJsonTestsCommon(bool sql_standard_mode) {
  std::string query_fn_name;
  std::string value_fn_name;
  if (sql_standard_mode) {
    query_fn_name = "json_query";
    value_fn_name = "json_value";
  } else {
    query_fn_name = "json_extract";
    value_fn_name = "json_extract_scalar";
  }
  const Value json1 =
      String(R"({"a":{"b":[{"c" : "foo", "d": 1.23, "f":null }], "e": true}})");
  const Value json2 =
      String(R"({"x": [1, 2, 3, 4, 5], "y": [{"a": "bar"}, {"b":"baz"}] })");
  const Value json3 = String(R"({"a.b.c": 5})");
  const Value json4 = String(R"({"longer_field_name": []})");
  const Value json5 = String("true");
  const Value json6 = String(R"({"a":[{"b": [{"c": [{"d": [3]}]}]}]})");
  const Value json7 = String(R"({"a":{"b": {"c": {"d": 3}}}})");
  const Value json8 =
      String(R"({"x" : [    ], "y"    :[1,2       ,      5,3  ,4]})");
  const Value malformed_json = String(R"({"a": )");

  // Note: not enclosed in {}.
  const std::string deep_json_string = R"(
  "a" : {
    "b" : {
      "c" : {
        "d" : {
          "e" : {
            "f" : {
              "g" : {
                "h" : {
                  "i" : {
                    "j" : {
                      "k" : {
                        "l" : {
                          "m" : {
                            "x" : "foo",
                            "y" : 10,
                            "z" : [1, 2, 3]
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  )";
  const std::string wide_json_string = R"(
  "a" : null, "b" : "bar", "c" : false, "d" : [4, 5], "e" : 0.123, "f" : "345",
  "g" : null, "h" : "baz", "i" : true, "j" : [-3, 0], "k" : 0.321, "l" : "678"
  )";

  const Value deep_json = String(absl::StrCat("{", deep_json_string, "}"));
  const Value wide_json = String(absl::StrCat("{", wide_json_string, "}"));

  const int kArrayElements = 20;
  std::vector<int> indexes(kArrayElements);
  std::iota(indexes.begin(), indexes.end(), 0);
  const Value array_of_wide_json = String(absl::Substitute(
      R"({"arr" : [$0]})",
      absl::StrJoin(indexes, ",", [&wide_json_string](std::string* out, int index) {
        absl::StrAppend(out, absl::Substitute(R"({"index" : $0, $1})", index,
                                              wide_json_string));
      })));
  const Value array_of_deep_json = String(absl::Substitute(
      R"({"arr" : [$0]})",
      absl::StrJoin(indexes, ",", [&deep_json_string](std::string* out, int index) {
        absl::StrAppend(out, absl::Substitute(R"({"index" : $0, $1})", index,
                                              deep_json_string));
      })));

  const Value json_with_wide_numbers = String(
      R"({
           "x":11111111111111111111,
           "y":3.14e314,
           "z":123456789012345678901234567890,
           "a":true,
           "s":"foo"
         })");

  std::vector<FunctionTestCall> all_tests = {
      {query_fn_name, {NullString(), NullString()}, NullString()},
      {query_fn_name, {json1, NullString()}, NullString()},
      {query_fn_name, {NullString(), "$"}, NullString()},
      {query_fn_name,
       {json1, "$"},
       String(R"({"a":{"b":[{"c":"foo","d":1.23,"f":null}],"e":true}})")},
      {query_fn_name,
       {json1, String("$.a")},
       String(R"({"b":[{"c":"foo","d":1.23,"f":null}],"e":true})")},
      {query_fn_name,
       {json1, absl::StrCat("$", EscapeKey(sql_standard_mode, "a"))},
       String(R"({"b":[{"c":"foo","d":1.23,"f":null}],"e":true})")},
      {query_fn_name,
       {json1, String("$.a.b")},
       String(R"([{"c":"foo","d":1.23,"f":null}])")},
      {query_fn_name,
       {json1, String("$.a.b[0]")},
       String(R"({"c":"foo","d":1.23,"f":null})")},
      {value_fn_name, {json1, String("$.a.b[0]")}, NullString()},
      {query_fn_name, {json1, String("$.a.b[0].c")}, String(R"("foo")")},
      {value_fn_name, {json1, String("$.a.b[0].c")}, String("foo")},
      {query_fn_name, {json1, String("$.a.b[0].d")}, String("1.23")},
      {value_fn_name, {json1, String("$.a.b[0].d")}, String("1.23")},
      {query_fn_name, {json1, String("$.a.b[0].f")}, NullString()},
      {value_fn_name, {json1, String("$.a.b[0].f")}, NullString()},
      {query_fn_name, {json1, String("$.a.b[0].g")}, NullString()},
      {query_fn_name, {json1, String("$.a.b[1]")}, NullString()},
      {query_fn_name, {json1, String("$.a.x[0]")}, NullString()},
      {query_fn_name, {json1, String("$.a.x")}, NullString()},
      {query_fn_name, {json1, String("$.a.e")}, String("true")},
      {value_fn_name, {json1, String("$.a.e")}, String("true")},
      {query_fn_name, {json1, String("abc")}, NullString(), OUT_OF_RANGE},
      {query_fn_name, {json1, String("")}, NullString(), OUT_OF_RANGE},
      {query_fn_name, {json2, String("$.x")}, String("[1,2,3,4,5]")},
      {query_fn_name, {json2, String("$.x[1]")}, String("2")},
      {query_fn_name,
       {json2, absl::StrCat("$", EscapeKey(sql_standard_mode, "x"), "[1]")},
       String("2")},
      {query_fn_name, {json2, String("$.x[-1]")}, NullString()},
      {query_fn_name,
       {json2, absl::StrCat("$.x", EscapeKey(sql_standard_mode, "a"))},
       NullString()},
      {query_fn_name,
       {json2, absl::StrCat("$.x", EscapeKey(sql_standard_mode, "1"))},
       String("2")},
      {query_fn_name, {json2, String("$.x[ 1]")}, String("2")},
      {query_fn_name, {json2, String("$.x[10]")}, NullString()},
      {query_fn_name, {json2, String("$.y.a")}, NullString()},
      {query_fn_name, {json2, String("$.y[0].a")}, String(R"("bar")")},
      {value_fn_name, {json2, String("$.y[0].a")}, String("bar")},
      {query_fn_name,
       {json2, absl::StrCat("$.y[0]", EscapeKey(sql_standard_mode, "a"))},
       String(R"("bar")")},
      {value_fn_name,
       {json2, absl::StrCat("$.y[0]", EscapeKey(sql_standard_mode, "a"))},
       String("bar")},
      {query_fn_name, {json2, String("$.y[0].b")}, NullString()},
      {query_fn_name, {json2, String("$.y[1].a")}, NullString()},
      {query_fn_name, {json2, String("$.y[1].b")}, String(R"("baz")")},
      {value_fn_name, {json2, String("$.y[1].b")}, String("baz")},
      {query_fn_name, {json3, String("$")}, String(R"({"a.b.c":5})")},
      // Query with dots in the middle of the key
      {query_fn_name,
       {json3, absl::StrCat("$", EscapeKey(sql_standard_mode, "a.b.c"))},
       String("5")},
      // If $[a.b.c] is a valid path then it should return "5"
      // Else it should return OUT_OF_RANGE current code fails to
      // handle this correctly.
      //
      // {query_fn_name, {json3, String("$[a.b.c]")}, NullString(),
      //  OUT_OF_RANGE},
      {query_fn_name, {json4, String("$.longer_field_name")}, String("[]")},
      {query_fn_name, {json5, String("$")}, String("true")},
      {value_fn_name, {json5, String("$")}, String("true")},
      {value_fn_name, {json7, String("$.a.b.c.d")}, String("3")},
      {value_fn_name,
       {json7, absl::StrCat("$", EscapeKey(sql_standard_mode, "a"),
                            EscapeKey(sql_standard_mode, "b"), ".c",
                            EscapeKey(sql_standard_mode, "d"))},
       String("3")},
      {query_fn_name,
       {json7, String("$")},
       String(R"({"a":{"b":{"c":{"d":3}}}})")},
      {query_fn_name,
       {json8, String("$")},
       String(R"({"x":[],"y":[1,2,5,3,4]})")},
      {query_fn_name, {json8, String("$.x")}, String(R"([])")},
      {value_fn_name, {json8, String("$.y[3]")}, String("3")},
      {query_fn_name, {malformed_json, String("$")}, NullString()},
      // Deep/wide json.
      {query_fn_name,
       {deep_json, String("$.a.b.c.d.e.f.g.h.i.j")},
       String(R"({"k":{"l":{"m":{"x":"foo","y":10,"z":[1,2,3]}}}})")},
      {query_fn_name,
       {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m")},
       String(R"({"x":"foo","y":10,"z":[1,2,3]})")},
      {query_fn_name,
       {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.z[1]")},
       String("2")},
      {query_fn_name,
       {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.x")},
       String(R"("foo")")},
      {value_fn_name,
       {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.x")},
       String("foo")},
      {query_fn_name, {wide_json, String("$.j")}, String("[-3,0]")},
      {value_fn_name, {wide_json, String("$.j[1]")}, String("0")},
      {query_fn_name,
       {array_of_deep_json, String("$.arr[13].index")},
       String("13")},
      {query_fn_name,
       {array_of_deep_json, String("$.arr[13].a.b.c.d.e.f.g.h.i.j.k.l.m.z[1]")},
       String("2")},
      {value_fn_name,
       {array_of_deep_json, String("$.arr[13].a.b.c.d.e.f.g.h.i.j.k.l.m.x")},
       String("foo")},
      {query_fn_name,
       {array_of_wide_json, String("$.arr[17].index")},
       String("17")},
      {query_fn_name,
       {array_of_wide_json, String("$.arr[17].k")},
       String("0.321")},
      {query_fn_name,
       {array_of_wide_json, String("$.arr[14]")},
       String(
           R"({"index":14,"a":null,"b":"bar","c":false,"d":[4,5],"e":0.123,)"
           R"("f":"345","g":null,"h":"baz","i":true,"j":[-3,0],"k":0.321,)"
           R"("l":"678"})")},
      {value_fn_name,
       {array_of_wide_json, String("$.arr[12].index")},
       String("12")},
      {value_fn_name,
       {array_of_wide_json, String("$.arr[12].h")},
       String("baz")},
      // Non-ASCII UTF-8 and special cases.
      {query_fn_name,
       {String(R"({"Моша_öá5ホバークラフト鰻鰻" : "x"})"), String("$")},
       String(R"({"Моша_öá5ホバークラフト鰻鰻":"x"})")},
      {value_fn_name,
       {String(R"({"Моша_öá5ホバークラフト鰻鰻" : "x"})"),
        String("$.Моша_öá5ホバークラフト鰻鰻")},
       String("x")},
      {value_fn_name,
       {String(R"({"1" : 2})"),
        absl::StrCat("$", EscapeKey(sql_standard_mode, "1"))},
       String("2")},
      {value_fn_name, {String(R"({"1" : 2})"), String("$.1")}, String("2")},
      // Wide numbers.
      {query_fn_name,
       {json_with_wide_numbers, String("$")},
       String(R"({"x":11111111111111111111,"y":3.14e314,)"
              R"("z":123456789012345678901234567890,"a":true,"s":"foo"})")},
      {value_fn_name,
       {json_with_wide_numbers, String("$.x")},
       String("11111111111111111111")},
      {value_fn_name,
       {json_with_wide_numbers, String("$.y")},
       String("3.14e314")},
      {value_fn_name,
       {json_with_wide_numbers, String("$.z")},
       String("123456789012345678901234567890")},
      {value_fn_name, {json_with_wide_numbers, String("$.s")}, String("foo")},
      // Characters should be escaped in JSON_EXTRACT.
      {query_fn_name,
       {String(R"({"a": "foo\t\\t\\\t\n\\nbar\ \"baz\\"})"), String("$.a")},
       String(R"("foo\t\\t\\\t\n\\nbar \"baz\\")")},
      // Characters should _not_ be escaped in JSON_EXTRACT_SCALAR.
      {value_fn_name,
       {String(R"({"a": "foo\t\\t\\\t\n\\nbar\ \"baz\\"})"), String("$.a")},
       String("foo\t\\t\\\t\n\\nbar \"baz\\")},
      // Unsupported/unimplemented JSONPath features.
      {query_fn_name, {json1, String("$.a.*")}, NullString(), OUT_OF_RANGE},
      {query_fn_name, {json1, String("$.a.b..c")}, NullString(), OUT_OF_RANGE},
      {value_fn_name,
       {json6, String("$.a[0].b[(@.length-1)].c[(@.length-1)].d")},
       NullString(),
       OUT_OF_RANGE},
      {query_fn_name,
       {json2, String("$.x[(@.length-1)]")},
       NullString(),
       OUT_OF_RANGE},
      {query_fn_name, {json2, String("$.x[-1:]")}, NullString(), OUT_OF_RANGE},
      {query_fn_name,
       {json2, String("$.x[0:4:2]")},
       NullString(),
       OUT_OF_RANGE},
      {query_fn_name, {json2, String("$.x[:2]")}, NullString(), OUT_OF_RANGE},
      {query_fn_name, {json2, String("$.x[0,1]")}, NullString(), OUT_OF_RANGE},
      {query_fn_name,
       {json2, String("$.y[?(@.a)]")},
       NullString(),
       OUT_OF_RANGE},
      {query_fn_name,
       {json2, String(R"($.y[?(@.a==='bar')])")},
       NullString(),
       OUT_OF_RANGE},
  };

  if (sql_standard_mode) {
    all_tests.push_back({query_fn_name,
                         {json3, String("$['a.b.c']")},
                         NullString(),
                         OUT_OF_RANGE});
    all_tests.push_back({value_fn_name,
                         {json3, String("$['a.b.c']")},
                         NullString(),
                         OUT_OF_RANGE});
  } else {
    all_tests.push_back({query_fn_name,
                         {json3, String("$.\"a.b.c\"")},
                         NullString(),
                         OUT_OF_RANGE});
    all_tests.push_back({value_fn_name,
                         {json3, String("$.\"a.b.c\"")},
                         NullString(),
                         OUT_OF_RANGE});
  }

  return all_tests;
}

std::vector<FunctionTestCall> GetFunctionTestsJson() {
  return GetJsonTestsCommon(/*sql_standard_mode=*/true);
}

std::vector<FunctionTestCall> GetFunctionTestsJsonExtract() {
  return GetJsonTestsCommon(/*sql_standard_mode=*/false);
}

std::vector<FunctionTestCall> GetFunctionTestsToJsonString(
    bool include_nano_timestamp) {
  const Value test_enum0 = Value::Enum(TestEnumType(), 0);
  const Value test_enum_negative = Value::Enum(TestEnumType(), -1);

  const std::string small_proto_value_string =
      R"(
        int64_key_1: 1
        int64_key_2: 2
        string_val: "foo"
        repeated_bool_val: true
        nested_value {
          nested_int64: 5
        }
        nested_repeated_value {
          nested_repeated_int64: 7
          nested_repeated_int64: 8
        })";
  const Value small_proto_value = KitchenSink(small_proto_value_string);

  const std::string proto_value_string =
      R"(
        int64_key_1: 1
        int64_key_2: 2
        string_val: "foo"
        repeated_int32_val: 3
        repeated_string_val: "hello"
        repeated_string_val: "world"
        repeated_bool_val: true
        repeated_bool_val: true
        nested_value {
          nested_int64: 5
        }
        nested_repeated_value {
        }
        nested_repeated_value {
          nested_int64: 6
        }
        nested_repeated_value {
          nested_repeated_int64: 7
          nested_repeated_int64: 8
        }
        test_enum: TESTENUM2147483647
        repeated_test_enum: TESTENUM1
        repeated_test_enum: TESTENUM2
        date: 9
        timestamp_seconds: 10
        timestamp_seconds_format: 11
        [zetasql_test.int_top_level_extension]: -10
        [zetasql_test.KitchenSinkExtension.int_extension]: 1234
        [zetasql_test.KitchenSinkExtension.optional_extension]: {
          value: "bar"
          repeated_value: ["baz"]
        })";
  const Value proto_value = KitchenSink(proto_value_string);

  const std::string proto_value_with_nonfinite_string =
      R"(
        int64_key_1: 1
        int64_key_2: 2
        double_val: inf
        float_val: nan
        repeated_double_val: -inf
        repeated_double_val: nan
        repeated_float_val: inf
        repeated_float_val: -inf
      )";
  const Value proto_value_with_nonfinite =
      KitchenSink(proto_value_with_nonfinite_string);

  // These tests follow the order of the table in (broken link).
  std::vector<FunctionTestCall> all_tests = {
      {"to_json_string", {NullInt64()}, String("null")},
      {"to_json_string", {Int64(0), NullBool()}, NullString()},
      {"to_json_string", {NullInt64(), NullBool()}, NullString()},
      {"to_json_string", {NullString()}, String("null")},
      {"to_json_string", {NullDouble()}, String("null")},
      {"to_json_string", {NullTimestamp()}, String("null")},
      {"to_json_string", {NullDate()}, String("null")},
      {"to_json_string", {Null(test_enum0.type())}, String("null")},
      {"to_json_string", {Null(proto_value.type())}, String("null")},
      {"to_json_string", {Null(Int64ArrayType())}, String("null")},
      {"to_json_string", {Null(EmptyStructType())}, String("null")},

      // BOOL
      {"to_json_string", {Bool(true)}, String("true")},
      {"to_json_string", {Bool(false)}, String("false")},

      // INT32, UINT32
      {"to_json_string", {Int32(0)}, String("0")},
      {"to_json_string", {Int32(-11)}, String("-11")},
      {"to_json_string", {Int32(132)}, String("132")},
      {"to_json_string", {Int32(int32min)}, String("-2147483648")},
      {"to_json_string", {Int32(int32max)}, String("2147483647")},
      {"to_json_string", {Uint32(0)}, String("0")},
      {"to_json_string", {Uint32(132)}, String("132")},
      {"to_json_string", {Uint32(uint32max)}, String("4294967295")},

      // INT64, UINT64
      // Values in [-9007199254740992, 9007199254740992] are not quoted.
      {"to_json_string", {Int64(0)}, String("0")},
      {"to_json_string", {Int64(10), Bool(true)}, String("10")},
      {"to_json_string", {Int64(10), Bool(false)}, String("10")},
      {"to_json_string", {Int64(-11)}, String("-11")},
      {"to_json_string", {Int64(132)}, String("132")},
      {"to_json_string", {Int64(int32min)}, String("-2147483648")},
      {"to_json_string",
       {Int64(-9007199254740991ll)},
       String("-9007199254740991")},
      {"to_json_string",
       {Int64(-9007199254740992ll)},
       String("-9007199254740992")},
      // Integers in the range of [-2^53, 2^53] can be represented losslessly as
      // a double-precision floating point number. Integers outside that range
      // are quoted.
      {"to_json_string",
       {Int64(-9007199254740993ll)},
       String("\"-9007199254740993\"")},
      {"to_json_string",
       {Int64(-12345678901234567ll)},
       String("\"-12345678901234567\"")},
      {"to_json_string", {Int64(int64min)}, String("\"-9223372036854775808\"")},
      {"to_json_string",
       {Int64(9007199254740991ll)},
       String("9007199254740991")},
      {"to_json_string",
       {Int64(9007199254740992ll)},
       String("9007199254740992")},
      {"to_json_string",
       {Int64(9007199254740993ll)},
       String("\"9007199254740993\"")},
      {"to_json_string",
       {Int64(12345678901234567ll)},
       String("\"12345678901234567\"")},
      {"to_json_string", {Int64(int64max)}, String("\"9223372036854775807\"")},
      {"to_json_string", {Uint64(0)}, String("0")},
      {"to_json_string", {Uint64(132)}, String("132")},
      {"to_json_string",
       {Uint64(9007199254740991ull)},
       String("9007199254740991")},
      {"to_json_string",
       {Uint64(9007199254740992ull)},
       String("9007199254740992")},
      {"to_json_string",
       {Uint64(9007199254740993ull)},
       String("\"9007199254740993\"")},
      {"to_json_string",
       {Uint64(12345678901234567ull)},
       String("\"12345678901234567\"")},
      {"to_json_string",
       {Uint64(uint64max)},
       String("\"18446744073709551615\"")},

      // FLOAT, DOUBLE
      // +Infinity, -Infinity, and Nan are represented as strings.
      {"to_json_string", {Float(0.0f)}, String("0")},
      {"to_json_string", {Float(0.0f), Bool(true)}, String("0")},
      {"to_json_string", {Float(0.0f), Bool(false)}, String("0")},
      {"to_json_string", {Float(-0.0f)}, String("-0")},
      {"to_json_string", {Float(3.14f)}, String("3.14")},
      {"to_json_string", {Float(1.618034f)}, String("1.618034")},
      {"to_json_string", {Float(floatmin)}, String("-3.4028235e+38")},
      {"to_json_string", {Float(floatmax)}, String("3.4028235e+38")},
      {"to_json_string", {Float(floatminpositive)}, String("1.1754944e-38")},
      {"to_json_string", {Float(float_pos_inf)}, String("\"Infinity\"")},
      {"to_json_string", {Float(float_neg_inf)}, String("\"-Infinity\"")},
      {"to_json_string", {Float(float_nan)}, String("\"NaN\"")},
      {"to_json_string", {Double(0.0)}, String("0")},
      {"to_json_string", {Double(-0.0)}, String("-0")},
      {"to_json_string", {Double(3.14)}, String("3.14")},
      {"to_json_string",
       {Double(1.61803398874989)},
       String("1.61803398874989")},
      {"to_json_string",
       {Double(doublemin)},
       String("-1.7976931348623157e+308")},
      {"to_json_string",
       {Double(doublemax)},
       String("1.7976931348623157e+308")},
      {"to_json_string",
       {Double(doubleminpositive)},
       String("2.2250738585072014e-308")},
      {"to_json_string", {Double(double_pos_inf)}, String("\"Infinity\"")},
      {"to_json_string", {Double(double_neg_inf)}, String("\"-Infinity\"")},
      {"to_json_string", {Double(double_nan)}, String("\"NaN\"")},

      // STRING
      // ", \, and characters with Unicode values from U+0000 to U+001F are
      // escaped.
      {"to_json_string", {String("foo")}, String("\"foo\"")},
      {"to_json_string", {String("")}, String("\"\"")},
      {"to_json_string",
       {String(R"(a"in"between\slashes\\)")},
       String(R"("a\"in\"between\\slashes\\\\")")},
      {"to_json_string",
       {String("Моша_öá5ホバークラフト鰻鰻")},
       String("\"Моша_öá5ホバークラフト鰻鰻\"")},
      // Note that in the expected output, \ is a literal backslash within the
      // std::string.
      {"to_json_string",
       {String("abca\x00\x01\x1A\x1F\x20")},
       String(R"("abca\u0000\u0001\u001a\u001f ")")},

      // BYTES
      // Contents are base64-escaped and quoted.
      {"to_json_string", {Bytes("")}, String("\"\"")},
      {"to_json_string", {Bytes(" ")}, String("\"IA==\"")},
      {"to_json_string", {Bytes("abcABC")}, String("\"YWJjQUJD\"")},
      {"to_json_string",
       {Bytes("abcABCжщфЖЩФ")},
       String("\"YWJjQUJD0LbRidGE0JbQqdCk\"")},
      {"to_json_string", {Bytes("Ḋ")}, String("\"4biK\"")},
      {"to_json_string", {Bytes("abca\0b\0c\0")}, String("\"YWJjYQBiAGMA\"")},

      // ENUM
      // Quoted enum names.
      {"to_json_string", {test_enum0}, String("\"TESTENUM0\"")},
      {"to_json_string", {test_enum_negative}, String("\"TESTENUMNEGATIVE\"")},

      // DATE
      // Quoted date.
      {"to_json_string", {DateFromStr("2017-06-25")}, String("\"2017-06-25\"")},
      {"to_json_string",
       {DateFromStr("2017-06-25"), Bool(true)},
       String("\"2017-06-25\"")},
      {"to_json_string",
       {DateFromStr("2017-06-25"), Bool(false)},
       String("\"2017-06-25\"")},
      {"to_json_string", {DateFromStr("1918-11-11")}, String("\"1918-11-11\"")},
      {"to_json_string", {DateFromStr("0001-01-01")}, String("\"0001-01-01\"")},
      {"to_json_string", {DateFromStr("9999-12-31")}, String("\"9999-12-31\"")},

      // TIMESTAMP
      // Quoted timestamp with a T separator and Z timezone suffix.
      {"to_json_string",
       {TimestampFromStr("2017-06-25 12:34:56.123456")},
       String("\"2017-06-25T12:34:56.123456Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 05:13:00")},
       String("\"2017-06-25T05:13:00Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 23:34:56.123456")},
       String("\"2017-06-25T23:34:56.123456Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 12:34:56.12345")},
       String("\"2017-06-25T12:34:56.123450Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 12:34:56.123")},
       String("\"2017-06-25T12:34:56.123Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 12:34:56.12")},
       String("\"2017-06-25T12:34:56.120Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 12:34:56.1")},
       String("\"2017-06-25T12:34:56.100Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 12:34:00")},
       String("\"2017-06-25T12:34:00Z\"")},
      {"to_json_string",
       {TimestampFromStr("2017-06-25 12:00:00")},
       String("\"2017-06-25T12:00:00Z\"")},
      {"to_json_string",
       {TimestampFromStr("1918-11-11")},
       String("\"1918-11-11T00:00:00Z\"")},
      {"to_json_string",
       {TimestampFromStr("0001-01-01")},
       String("\"0001-01-01T00:00:00Z\"")},
      {"to_json_string",
       {TimestampFromStr("9999-12-31 23:59:59.999999")},
       String("\"9999-12-31T23:59:59.999999Z\"")},

      // DATETIME and TIME tests are defined below.

      // ARRAY
      // Array elements are comma-separated and enclosed in brackets. Pretty-
      // printing results in newlines with two spaces of indentation between
      // elements.
      {"to_json_string", {Int64Array({-10000})}, String("[-10000]")},
      {"to_json_string",
       {Int64Array({-10000}), Bool(true)},  // pretty-print
       String("[\n  -10000\n]")},
      {"to_json_string",
       {Array({Int64(1), NullInt64(), Int64(-10)})},
       String("[1,null,-10]")},
      {"to_json_string",
       {Array({Int64(1), NullInt64(), Int64(-10)}), Bool(false)},
       String("[1,null,-10]")},
      {"to_json_string",
       {Array({Int64(1), NullInt64(), Int64(-10)}),
        Bool(true)},  // pretty-print
       String("[\n  1,\n  null,\n  -10\n]")},
      {"to_json_string", {Int64Array({}), Bool(false)}, String("[]")},
      {"to_json_string", {Int64Array({}), Bool(true)}, String("[]")},
      {"to_json_string",
       {StringArray({"foo", "", "bar"})},
       String(R"(["foo","","bar"])")},
      {"to_json_string",
       {StringArray({"foo", "", "bar"}), Bool(true)},
       String(R"([
  "foo",
  "",
  "bar"
])")},
      {"to_json_string",
       {Array({Int64(1), NullInt64(), Int64(-10)}),
        Bool(true)},  // pretty-print
       String("[\n  1,\n  null,\n  -10\n]")},

      // STRUCT
      // Struct fields are rendered as comma-separated
      // <field_name>:<field_value> pairs enclosed in braces. The same escaping
      // rules for std::string values apply to field names.
      {"to_json_string", {Struct({}, {})}, String("{}")},
      {"to_json_string", {Struct({}, {}), Bool(true)}, String("{}")},
      {"to_json_string", {Struct({"x"}, {Int64(5)})}, String(R"({"x":5})")},
      {"to_json_string",
       {Struct({"x"}, {Float(3.14)})},
       String(R"({"x":3.14})")},
      {"to_json_string",
       {Struct({"x"}, {Double(3.14)})},
       String(R"({"x":3.14})")},
      {"to_json_string",
       {Struct({"x", "y", "z"}, {Double(double_pos_inf), Float(float_neg_inf),
                                 Double(double_nan)})},
       String(R"({"x":"Infinity","y":"-Infinity","z":"NaN"})")},
      {"to_json_string",
       {Struct({"x"}, {Int64(5)}), Bool(true)},
       String("{\n  \"x\": 5\n}")},
      {"to_json_string",
       {Struct({"x", "", "foo", "x"}, {Int64(5), Bool(true), String("bar"),
                                       DateFromStr("2017-03-28")})},
       String(R"({"x":5,"":true,"foo":"bar","x":"2017-03-28"})")},
      {"to_json_string",
       {Struct({"x", "", "foo", "x"}, {Int64(5), Bool(true), String("bar"),
                                       DateFromStr("2017-03-28")}),
        Bool(true)},
       String(R"({
  "x": 5,
  "": true,
  "foo": "bar",
  "x": "2017-03-28"
})")},
      // Arrays within structs should have proper indentation with
      // pretty-printing.
      {"to_json_string",
       {Struct({"a", "b", "c", "x", "y", "z"},
               {Int64Array({10, 11, 12}), Int64Array({}),
                Null(EmptyStructType()), Struct({}, {}),
                Struct({"d", "e", "f"},
                       {Int64(20), StringArray({"foo", "bar", "baz"}),
                        Int64Array({})}),
                Array({Struct(
                    {"g"},
                    {Array({Struct({"h"}, {Int64Array({30, 31, 32})})})})})})},
       String(R"json({"a":[10,11,12],"b":[],"c":null,"x":{},)json"
              R"json("y":{"d":20,"e":["foo","bar","baz"],"f":[]},)json"
              R"json("z":[{"g":[{"h":[30,31,32]}]}]})json")},
      {"to_json_string",
       {Struct({"a", "b", "c", "x", "y", "z"},
               {Int64Array({10, 11, 12}), Int64Array({}),
                Null(EmptyStructType()), Struct({}, {}),
                Struct({"d", "e", "f"},
                       {Int64(20), StringArray({"foo", "bar", "baz"}),
                        Int64Array({})}),
                Array({Struct(
                    {"g"},
                    {Array({Struct({"h"}, {Int64Array({30, 31, 32})})})})})}),
        Bool(true)},
       String(R"json({
  "a": [
    10,
    11,
    12
  ],
  "b": [],
  "c": null,
  "x": {},
  "y": {
    "d": 20,
    "e": [
      "foo",
      "bar",
      "baz"
    ],
    "f": []
  },
  "z": [
    {
      "g": [
        {
          "h": [
            30,
            31,
            32
          ]
        }
      ]
    }
  ]
})json")},

      // PROTO
      // Proto values are converted to strings exactly according to the
      // (broken link), using the PROTO3 flag for JsonFormat.
      // Pretty-printed indentation should take struct/array indentation into
      // account.
      {"to_json_string",
       {small_proto_value},
       String(R"({"int64Key1":"1","int64Key2":"2",)"
              R"("nestedRepeatedValue":[{"nestedRepeatedInt64":["7","8"]}],)"
              R"("nestedValue":{"nestedInt64":"5"},"repeatedBoolVal":[true],)"
              R"("stringVal":"foo"})")},
      {"to_json_string",
       {proto_value, Bool(true)},
       // The std::string contains trailing whitespace, which is disallowed in .cc
       // files and has to be injected manually.
       String(absl::Substitute(
           R"json({
  "date": 9,
  "int64Key1": "1",
  "int64Key2": "2",
  "[zetasql_test.KitchenSinkExtension.int_extension]": "1234",
  "[zetasql_test.int_top_level_extension]": "-10",
  "nestedRepeatedValue": [ {
$0
  }, {
    "nestedInt64": "6"
  }, {
    "nestedRepeatedInt64": [ "7", "8" ]
  } ],
  "nestedValue": {
    "nestedInt64": "5"
  },
  "[zetasql_test.KitchenSinkExtension.optional_extension]": {
    "repeatedValue": [ "baz" ],
    "value": "bar"
  },
  "repeatedBoolVal": [ true, true ],
  "repeatedInt32Val": [ 3 ],
  "repeatedStringVal": [ "hello", "world" ],
  "repeatedTestEnum": [ "TESTENUM1", "TESTENUM2" ],
  "stringVal": "foo",
  "testEnum": "TESTENUM2147483647",
  "timestampSeconds": "10",
  "timestampSecondsFormat": "11"
})json",
           "  "))},
      {"to_json_string",
       {Array({proto_value, Null(proto_value.type()), small_proto_value}),
        Bool(true)},
       // The std::string contains trailing whitespace, which is disallowed in .cc
       // files and has to be injected manually.
       String(absl::Substitute(R"json([
  {
    "date": 9,
    "int64Key1": "1",
    "int64Key2": "2",
    "[zetasql_test.KitchenSinkExtension.int_extension]": "1234",
    "[zetasql_test.int_top_level_extension]": "-10",
    "nestedRepeatedValue": [ {
$0
    }, {
      "nestedInt64": "6"
    }, {
      "nestedRepeatedInt64": [ "7", "8" ]
    } ],
    "nestedValue": {
      "nestedInt64": "5"
    },
    "[zetasql_test.KitchenSinkExtension.optional_extension]": {
      "repeatedValue": [ "baz" ],
      "value": "bar"
    },
    "repeatedBoolVal": [ true, true ],
    "repeatedInt32Val": [ 3 ],
    "repeatedStringVal": [ "hello", "world" ],
    "repeatedTestEnum": [ "TESTENUM1", "TESTENUM2" ],
    "stringVal": "foo",
    "testEnum": "TESTENUM2147483647",
    "timestampSeconds": "10",
    "timestampSecondsFormat": "11"
  },
  null,
  {
    "int64Key1": "1",
    "int64Key2": "2",
    "nestedRepeatedValue": [ {
      "nestedRepeatedInt64": [ "7", "8" ]
    } ],
    "nestedValue": {
      "nestedInt64": "5"
    },
    "repeatedBoolVal": [ true ],
    "stringVal": "foo"
  }
])json",
                               "    "))},
      {"to_json_string",
       {Struct({"x", "y", "z"},
               {Array({small_proto_value, Null(proto_value.type())}), Int64(5),
                Int64Array({10, 11, 12})}),
        Bool(true)},
       String(R"json({
  "x": [
    {
      "int64Key1": "1",
      "int64Key2": "2",
      "nestedRepeatedValue": [ {
        "nestedRepeatedInt64": [ "7", "8" ]
      } ],
      "nestedValue": {
        "nestedInt64": "5"
      },
      "repeatedBoolVal": [ true ],
      "stringVal": "foo"
    },
    null
  ],
  "y": 5,
  "z": [
    10,
    11,
    12
  ]
})json")},
      // b/62650164
      {"to_json_string",
       {proto_value_with_nonfinite, Bool(true)},
       String(R"json({
  "doubleVal": Infinity,
  "floatVal": NaN,
  "int64Key1": "1",
  "int64Key2": "2",
  "repeatedDoubleVal": [ -Infinity, NaN ],
  "repeatedFloatVal": [ Infinity, -Infinity ]
})json")},
  };

  // NUMERIC
  const std::vector<std::pair<NumericValue, Value>> numeric_test_cases = {
      {NumericValue(static_cast<int64_t>(0)), String("0")},
      {NumericValue::FromDouble(3.14).ValueOrDie(), String("\"3.14\"")},
      {NumericValue::FromStringStrict("555551.618033989").ValueOrDie(),
       String("\"555551.618033989\"")},
      {NumericValue::FromStringStrict("0.000000001").ValueOrDie(),
       String("\"0.000000001\"")},
      {NumericValue::FromStringStrict("-0.000000001").ValueOrDie(),
       String("\"-0.000000001\"")},
      {NumericValue::FromStringStrict("55555551.618033989").ValueOrDie(),
       String("\"55555551.618033989\"")},
      {NumericValue::FromStringStrict("1234567890.123456789").ValueOrDie(),
       String("\"1234567890.123456789\"")},
      {NumericValue::FromStringStrict("1234567890.12345678").ValueOrDie(),
       String("\"1234567890.12345678\"")},
      {NumericValue(9007199254740992ll), String("9007199254740992")},
      {NumericValue(9007199254740993ll), String("\"9007199254740993\"")},
      {NumericValue(-9007199254740992ll), String("-9007199254740992")},
      {NumericValue(-9007199254740993ll), String("\"-9007199254740993\"")},
      {NumericValue::MaxValue(),
       String("\"99999999999999999999999999999.999999999\"")},
      {NumericValue::FromStringStrict("99999999999999999999999999999")
           .ValueOrDie(),
       String("\"99999999999999999999999999999\"")},
      {NumericValue::MinValue(),
       String("\"-99999999999999999999999999999.999999999\"")},
      {NumericValue::FromStringStrict("-99999999999999999999999999999")
           .ValueOrDie(),
       String("\"-99999999999999999999999999999\"")},
  };

  for (const auto& numeric_test_case : numeric_test_cases) {
    all_tests.emplace_back(
        "to_json_string",
        WrapResultForNumeric(
            {Value::Numeric(numeric_test_case.first)},
            QueryParamsWithResult::Result(numeric_test_case.second)));
  }
  all_tests.emplace_back(
      "to_json_string",
      WrapResultForNumeric({Value::NullNumeric()},
                           QueryParamsWithResult::Result(String("null"))));
  all_tests.emplace_back("to_json_string",
                         QueryParamsWithResult({Value::NullGeography()}, "null")
                             .WrapWithFeature(FEATURE_GEOGRAPHY));

  std::vector<CivilTimeTestCase> civil_time_test_cases = {
      // DATETIME
      // Quoted datetime with a T separator.
      {{NullDatetime()}, String("null")},
      {{DatetimeFromStr("2017-06-25 12:34:56.123456")},
       String("\"2017-06-25T12:34:56.123456\"")},
      {{DatetimeFromStr("2017-06-25 05:13:00")},
       String("\"2017-06-25T05:13:00\"")},
      {{DatetimeFromStr("2017-06-25 23:34:56.123456")},
       String("\"2017-06-25T23:34:56.123456\"")},
      {{DatetimeFromStr("2017-06-25 12:34:56.12345")},
       String("\"2017-06-25T12:34:56.123450\"")},
      {{DatetimeFromStr("2017-06-25 12:34:56.123")},
       String("\"2017-06-25T12:34:56.123\"")},
      {{DatetimeFromStr("2017-06-25 12:34:56.12")},
       String("\"2017-06-25T12:34:56.120\"")},
      {{DatetimeFromStr("2017-06-25 12:34:00")},
       String("\"2017-06-25T12:34:00\"")},
      {{DatetimeFromStr("2017-06-25 12:00:00")},
       String("\"2017-06-25T12:00:00\"")},
      {{DatetimeFromStr("1918-11-11")}, String("\"1918-11-11T00:00:00\"")},
      {{DatetimeFromStr("0001-01-01")}, String("\"0001-01-01T00:00:00\"")},
      {{DatetimeFromStr("9999-12-31 23:59:59.999999")},
       String("\"9999-12-31T23:59:59.999999\"")},

      // TIME
      // Quoted time.
      {{NullTime()}, String("null")},
      {{TimeFromStr("12:34:56.123456")}, String("\"12:34:56.123456\"")},
      {{TimeFromStr("05:13:00")}, String("\"05:13:00\"")},
      {{TimeFromStr("23:34:56.123456")}, String("\"23:34:56.123456\"")},
      {{TimeFromStr("08:34:56.12345")}, String("\"08:34:56.123450\"")},
      {{TimeFromStr("12:34:56.123")}, String("\"12:34:56.123\"")},
      {{TimeFromStr("12:34:56.12")}, String("\"12:34:56.120\"")},
      {{TimeFromStr("12:34:00")}, String("\"12:34:00\"")},
      {{TimeFromStr("12:00:00")}, String("\"12:00:00\"")},
      {{TimeFromStr("00:00:00")}, String("\"00:00:00\"")},
      {{TimeFromStr("23:59:59.999999")}, String("\"23:59:59.999999\"")}};
    for (const auto& test_case : civil_time_test_cases) {
      all_tests.emplace_back("to_json_string",
                             WrapResultForCivilTimeAndNanos(test_case));
    }

  // Time, and datetimes with nanosecond precision. While
  // CivilTimeTestCase supports different results depending on whether the
  // nanosecond feature is enabled, it doesn't actually avoid running
  // queries where values have nanosecond precision.
  std::vector<CivilTimeTestCase> nanos_civil_time_test_cases = {
      {{DatetimeFromStr("2017-06-25 12:34:56.123456789",
                        functions::kNanoseconds)},
       String("\"2017-06-25T12:34:56.123456789\"")},
      {{DatetimeFromStr("2017-06-25 12:34:56.1234567",
                        functions::kNanoseconds)},
       String("\"2017-06-25T12:34:56.123456700\"")},
      {{TimeFromStr("12:34:56.123456789", functions::kNanoseconds)},
       String("\"12:34:56.123456789\"")},
      {{TimeFromStr("12:34:56.1234567", functions::kNanoseconds)},
       String("\"12:34:56.123456700\"")}};
  if (include_nano_timestamp) {
    for (const auto& test_case : nanos_civil_time_test_cases) {
      all_tests.emplace_back("to_json_string",
                             WrapResultForCivilTimeAndNanos(test_case));
    }
  }

  if (include_nano_timestamp) {
    // Timestamp with nanosecond precision. While CivilTimeTestCase supports
    // different results depending on whether the nanosecond feature is enabled,
    // it doesn't actually avoid running queries where values have nanosecond
    // precision.
    all_tests.emplace_back(
        "to_json_string",
        WrapResultForCivilTimeAndNanos(
            {{TimestampFromStr("2017-06-25 12:34:56.123456789",
                               functions::kNanoseconds)},
             String("\"2017-06-25T12:34:56.123456789Z\"")}));
    all_tests.emplace_back("to_json_string",
                           WrapResultForCivilTimeAndNanos(
                               {{TimestampFromStr("2017-06-25 12:34:56.1234567",
                                                  functions::kNanoseconds)},
                                String("\"2017-06-25T12:34:56.123456700Z\"")}));
  }

  return all_tests;
}

std::vector<FunctionTestCall> GetFunctionTestsHash() {
  std::vector<FunctionTestCall> all_tests;

  // More interesting tests are in hash.test.
  const std::vector<std::string> function_names = {"md5", "sha1", "sha256",
                                                   "sha512"};
  for (const std::string& function_name : function_names) {
    all_tests.push_back({function_name, {NullBytes()}, NullBytes()});
    all_tests.push_back({function_name, {NullString()}, NullBytes()});
  }

  return all_tests;
}

std::vector<FunctionTestCall> GetFunctionTestsFarmFingerprint() {
  return std::vector<FunctionTestCall>{
      {"farm_fingerprint", {NullBytes()}, NullInt64()},
      {"farm_fingerprint", {NullString()}, NullInt64()},
      {"farm_fingerprint", {String("")}, Int64(-7286425919675154353)},
      {"farm_fingerprint", {String("\0")}, Int64(-4728684028706075820)},
      {"farm_fingerprint", {String("a b c d")}, Int64(-3676552216144541872)},
      {"farm_fingerprint",
       {String("abcABCжщфЖЩФ")},
       Int64(3736580821998982030)},
      {"farm_fingerprint",
       {String("Моша_öá5ホバークラフト鰻鰻")},
       Int64(4918483674106117036)},
      {"farm_fingerprint", {Bytes("")}, Int64(-7286425919675154353)},
      {"farm_fingerprint", {Bytes("\0")}, Int64(-4728684028706075820)},
      {"farm_fingerprint", {Bytes("a b c d")}, Int64(-3676552216144541872)},
      {"farm_fingerprint", {Bytes("a b\0c d")}, Int64(-4659737306420982693)},
      {"farm_fingerprint", {Bytes("abcABCжщфЖЩФ")}, Int64(3736580821998982030)},
      {"farm_fingerprint",
       {Bytes("Моша_öá5ホバークラフト鰻鰻")},
       Int64(4918483674106117036)},
  };
}

std::vector<FunctionTestCall> GetFunctionTestsError() {
  return std::vector<FunctionTestCall>{
      {"error", {NullString()}, NullInt64(), OUT_OF_RANGE},
      {"error", {String("message")}, NullInt64(), OUT_OF_RANGE},
  };
}

}  // namespace zetasql
