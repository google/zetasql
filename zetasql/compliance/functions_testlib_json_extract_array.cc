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

#include <string>

#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/compliance/functions_testlib_common.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"
#include "absl/strings/substitute.h"

namespace zetasql {
namespace {
constexpr absl::StatusCode OUT_OF_RANGE = absl::StatusCode::kOutOfRange;
}  // namespace

const std::vector<FunctionTestCall> GetJsonExtractArrayTestsCommon(
    bool sql_standard_mode, bool scalar_test) {
  const Value json1 =
      String(R"({"a":{"b":[{"c" : "foo", "d": 1.23, "f":null }], "e": true}})");
  const Value json2 =
      String(R"({"x": [1, 2, 3, 4, 5], "y": [{"a": "bar"}, {"b":"baz"}] })");
  const Value json3 = String(R"({"a":[{"b": [{"c": [{"d": [3]}]}]}]})");
  const Value json4 =
      String(R"({"a": ["foo", "bar", "baz"], "b": [0.123, 4.567, 8.901]})");
  const Value json5 = String(R"({"a": [[1, 2, 3], [3, 2, 1]]})");
  const Value json6 = String(R"({"d.e.f": [1, 2, 3]})");
  const Value json7 = String(R"({"longer_field_name": [7, 8, 9]})");
  const Value json8 =
      String(R"({"x" : [    ], "y"    :[1,2       ,      5,3  ,4]})");
  const Value json9 = String(R"([{"a": "foo"}, {"b": [0.1, 0.2]}])");
  const Value json10 =
      String(R"({"a": [1, null, 2], "b": [0.123, 4.567, 8.901]})");
  const Value json11 = String(R"({"a": ["foo", null, []], "b": [[[1]]]})");
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
      absl::StrJoin(
          indexes, ",", [&wide_json_string](std::string* out, int index) {
            absl::StrAppend(out, absl::Substitute(R"({"index" : $0, $1})",
                                                  index, wide_json_string));
          })));
  const Value array_of_deep_json = String(absl::Substitute(
      R"({"arr" : [$0]})",
      absl::StrJoin(
          indexes, ",", [&deep_json_string](std::string* out, int index) {
            absl::StrAppend(out, absl::Substitute(R"({"index" : $0, $1})",
                                                  index, deep_json_string));
          })));

  const Value json_array_with_wide_numbers = String(
      R"([
           1111111111111111111111111,
           123456789012345678901234567890
         ])");
  const Value malformed_json = String(R"({"a": )");
  std::string function_name;
  if (scalar_test) {
    function_name = "json_extract_string_array";
  } else {
    function_name = "json_extract_array";
  }
  std::vector<FunctionTestCall> test_cases = {
      // Null inputs
      {function_name,
       {NullString(), NullString()},
       Value::Null(StringArrayType())},
      {function_name, {json1, NullString()}, Null(StringArrayType())},
      {function_name, {NullString(), String("$")}, Null(StringArrayType())},
      // Non-array object
      {function_name, {json1, String("$.a")}, Null(StringArrayType())},
      // Key missing
      {function_name, {json1, String("$.g")}, Null(StringArrayType())},
      // Cases with inputs
      // - Integer array
      {function_name,
       {json2, String("$.x")},
       StringArray({"1", "2", "3", "4", "5"})},
      // - Decimal array
      {function_name,
       {json4, String("$.b")},
       StringArray({"0.123", "4.567", "8.901"})},
      // - Nested array
      {function_name,
       {json3, String("$.a[0].b[0].c[0].d")},
       StringArray({"3"})},
      {function_name,
       {json3, String("$.a[0].b[0].c[1].d")},
       Null(StringArrayType())},
      {function_name, {json5, String("$.a[1]")}, StringArray({"3", "2", "1"})},
      {function_name, {json5, String("$.a[2]")}, Null(StringArrayType())},
      {function_name, {json11, String("$.b[0][0]")}, StringArray({"1"})},
      // Deep JSON
      {function_name,
       {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.z")},
       StringArray({"1", "2", "3"})},
      // Deep JSON error cases
      {function_name,
       {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.x")},
       Null(StringArrayType())},
      {function_name, {wide_json, String("$.j")}, StringArray({"-3", "0"})},
      {function_name, {wide_json, String("$.k")}, Null(StringArrayType())},
      {function_name, {wide_json, String("$.e[0]")}, Null(StringArrayType())},
      // Invalid JSONPath syntax
      {function_name,
       {json1, String("abc")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json1, String("")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name, {json2, String("$.x[-1]")}, Null(StringArrayType())},
      {function_name, {json2, String("$.y.a")}, Null(StringArrayType())},
      {function_name,
       {json3, String("$[a.b.c]")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json7, String("$.longer_field_name")},
       StringArray({"7", "8", "9"})},
      {function_name,
       {json8, String("$.y")},
       StringArray({"1", "2", "5", "3", "4"})},
      {function_name, {json8, String("$.x")}, EmptyArray(StringArrayType())},
      {function_name, {malformed_json, String("$")}, Null(StringArrayType())},
      {function_name,
       {array_of_deep_json, String("$.arr[13]")},
       Null(StringArrayType())},
      {function_name,
       {array_of_deep_json, String("$.arr[13].a.b.c.d.e.f.g.h.i.j.k.l.m.z")},
       StringArray({"1", "2", "3"})},
      {function_name,
       {array_of_wide_json, String("$.arr[14].d")},
       StringArray({"4", "5"})},
      {function_name, {json9, String("$[1].b")}, StringArray({"0.1", "0.2"})},
      // Array with null element
      {function_name, {json10, String("$.a")}, StringArray({"1", "null", "2"})},
      // Non-ASCII UTF-8 and special cases.
      {function_name,
       {String(R"({"Моша_öá5ホバークラフト鰻鰻" : [1, 2, 3]})"),
        String("$.Моша_öá5ホバークラフト鰻鰻")},
       StringArray({"1", "2", "3"})},
      // Wide numbers.
      {function_name,
       {json_array_with_wide_numbers, String("$")},
       StringArray(
           {"1111111111111111111111111", "123456789012345678901234567890"})},
      // Unsupported/unimplemented JSONPath features.
      {function_name,
       {json1, String("$.a.*")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json1, String("$.a.b..c")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json2, String("$.x[(@.length-1)]")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json2, String("$.x[-1:]")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json2, String("$.x[0:4:2]")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json2, String("$.x[:2]")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json2, String("$.x[0,1]")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json2, String("$.y[?(@.a)]")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json2, String(R"($.y[?(@.a==='bar')])")},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      // Tests of which results vary by sql_standard_mode;
      // Bracket/Dot notation for children/sub-trees
      {function_name,
       {json2, sql_standard_mode ? String("$.x") : String("$['x']")},
       StringArray({"1", "2", "3", "4", "5"})},
      {function_name,
       {json2, sql_standard_mode ? String("$.x.a") : String("$.x['a']")},
       Null(StringArrayType())},
      // Query with dots in the middle of the key
      {function_name,
       {json6, absl::StrCat("$", EscapeKey(!sql_standard_mode, "d.e.f"))},
       Null(StringArrayType()),
       OUT_OF_RANGE},
      {function_name,
       {json6, absl::StrCat("$", EscapeKey(sql_standard_mode, "d.e.f"))},
       StringArray({"1", "2", "3"})},
  };
  if (scalar_test) {
    // Not scalar array - object array
    test_cases.push_back(
        {function_name, {json2, String("$.y")}, Null(StringArrayType())});
    // Not scalar array - nested array
    test_cases.push_back(
        {function_name, {json5, String("$.a")}, Null(StringArrayType())});
    test_cases.push_back(
        {function_name, {json11, String("$.b")}, Null(StringArrayType())});
    test_cases.push_back(
        {function_name, {json11, String("$.b[0]")}, Null(StringArrayType())});
    // Non-ASCII UTF-8 and special cases.
    test_cases.push_back(
        {function_name,
         {String(R"(["Моша_öá5ホバークラフト鰻鰻"])"), String("$")},
         StringArray({"Моша_öá5ホバークラフト鰻鰻"})});
    // Wide numbers.
    test_cases.push_back(
        {function_name,
         {String(R"({"a": ["foo\t\\t\\\t\n\\nbar \"baz\\"]})"), String("$.a")},
         StringArray({"foo\t\\t\\\t\n\\nbar \"baz\\"})});
  } else {
    // - String array
    test_cases.push_back({function_name,
                          {json4, String("$.a")},
                          StringArray({"\"foo\"", "\"bar\"", "\"baz\""})});
    // - Object array
    test_cases.push_back({function_name,
                          {json2, String("$.y")},
                          StringArray({"{\"a\":\"bar\"}", "{\"b\":\"baz\"}"})});
    // Non-ASCII UTF-8 and special cases.
    test_cases.push_back(
        {function_name,
         {String(R"(["Моша_öá5ホバークラフト鰻鰻"])"), String("$")},
         StringArray({R"("Моша_öá5ホバークラフト鰻鰻")"})});
    // Wide numbers.
    test_cases.push_back(
        {function_name,
         {String(R"({"a": ["foo\t\\t\\\t\n\\nbar \"baz\\"]})"), String("$.a")},
         StringArray({R"("foo\t\\t\\\t\n\\nbar \"baz\\")"})});
  }
  return test_cases;
}

std::vector<FunctionTestCall> GetFunctionTestsJsonExtractArray() {
  return GetJsonExtractArrayTestsCommon(/*sql_standard_mode=*/false,
                                        /*scalar_test=*/false);
}

std::vector<FunctionTestCall> GetFunctionTestsJsonExtractStringArray() {
  return GetJsonExtractArrayTestsCommon(/*sql_standard_mode=*/false,
                                        /*scalar_test=*/true);
}
}  // namespace zetasql
