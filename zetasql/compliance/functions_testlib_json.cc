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

#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/compliance/functions_testlib_common.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"
#include "absl/strings/substitute.h"

namespace zetasql {
namespace {
constexpr absl::StatusCode OUT_OF_RANGE = absl::StatusCode::kOutOfRange;
}  // namespace

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

}  // namespace zetasql
