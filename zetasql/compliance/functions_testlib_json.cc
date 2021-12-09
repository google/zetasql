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
#include "zetasql/public/json_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"

namespace zetasql {
namespace {
constexpr absl::StatusCode OUT_OF_RANGE = absl::StatusCode::kOutOfRange;

// Note: not enclosed in {}.
constexpr absl::string_view kDeepJsonString = R"(
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

constexpr absl::string_view kWideJsonString = R"(
  "a" : null, "b" : "bar", "c" : false, "d" : [4, 5], "e" : 0.123, "f" : "345",
  "g" : null, "h" : "baz", "i" : true, "j" : [-3, 0], "k" : 0.321, "l" : "678"
  )";

// 'sql_standard_mode': if true, uses the SQL2016 standard for JSON function
// names (e.g. JSON_QUERY instead of JSON_EXTRACT).
// 'scalar_test_cases': if true, returns the JSON_VALUE/JSON_EXTRACT_SCALAR test
// cases. Otherwise, returns the JSON_QUERY/JSON_EXTRACT test cases.
// 'json_constructor': function used for constructing json values.
const std::vector<FunctionTestCall> GetJsonTestsCommon(
    bool sql_standard_mode, bool scalar_test_cases,
    const std::function<Value(absl::optional<absl::string_view>)>&
        json_constructor) {
  std::string query_fn_name;
  std::string value_fn_name;
  if (sql_standard_mode) {
    query_fn_name = "json_query";
    value_fn_name = "json_value";
  } else {
    query_fn_name = "json_extract";
    value_fn_name = "json_extract_scalar";
  }

  const Value json1 = json_constructor(
      R"({"a":{"b":[{"c" : "foo", "d": 1.23, "f":null }], "e": true}})");
  const Value json2 = json_constructor(
      R"({"x": [1, 2, 3, 4, 5], "y": [{"a": "bar"}, {"b":"baz"}] })");
  const Value json3 = json_constructor(R"({"a.b.c": 5})");
  const Value json4 = json_constructor(R"({"longer_field_name": []})");
  const Value json5 = json_constructor("true");
  const Value json6 =
      json_constructor(R"({"a":[{"b": [{"c": [{"d": [3]}]}]}]})");
  const Value json7 = json_constructor(R"({"a":{"b": {"c": {"d": 3}}}})");
  const Value json8 =
      json_constructor(R"({"x" : [    ], "y"    :[1,2       ,      5,3  ,4]})");
  const Value json9 = json_constructor(R"({"a": 1, "": [5, "foo"]})");

  const Value deep_json =
      json_constructor(absl::StrCat("{", kDeepJsonString, "}"));
  const Value wide_json =
      json_constructor(absl::StrCat("{", kWideJsonString, "}"));

  const int kArrayElements = 20;
  std::vector<int> indexes(kArrayElements);
  std::iota(indexes.begin(), indexes.end(), 0);
  const Value array_of_wide_json = json_constructor(absl::Substitute(
      R"({"arr" : [$0]})",
      absl::StrJoin(indexes, ",", [](std::string* out, int index) {
        absl::StrAppend(out, absl::Substitute(R"({"index" : $0, $1})", index,
                                              kWideJsonString));
      })));
  const Value array_of_deep_json = json_constructor(absl::Substitute(
      R"({"arr" : [$0]})",
      absl::StrJoin(indexes, ",", [](std::string* out, int index) {
        absl::StrAppend(out, absl::Substitute(R"({"index" : $0, $1})", index,
                                              kDeepJsonString));
      })));

  std::vector<FunctionTestCall> all_tests;
  if (scalar_test_cases) {
    all_tests = {
        {value_fn_name, {json1}, NullString()},
        {value_fn_name, {json1, String("$.a.b[0]")}, NullString()},
        {value_fn_name, {json1, String("$.a.b[0].c")}, String("foo")},
        {value_fn_name, {json1, String("$.a.b[0].d")}, String("1.23")},
        {value_fn_name, {json1, String("$.a.b[0].f")}, NullString()},
        {value_fn_name, {json1, String("$.a.e")}, String("true")},
        {value_fn_name, {json2, String("$.y[0].a")}, String("bar")},
        {value_fn_name,
         {json2, absl::StrCat("$.y[0]", EscapeKey(sql_standard_mode, "a"))},
         String("bar")},
        {value_fn_name, {json2, String("$.y[1].b")}, String("baz")},
        {value_fn_name, {json5, String("$")}, String("true")},
        {value_fn_name, {json7, String("$.a.b.c.d")}, String("3")},
        {value_fn_name,
         {json7, absl::StrCat("$", EscapeKey(sql_standard_mode, "a"),
                              EscapeKey(sql_standard_mode, "b"), ".c",
                              EscapeKey(sql_standard_mode, "d"))},
         String("3")},
        {value_fn_name, {json8, String("$.y[3]")}, String("3")},
        // Deep/wide json.
        {value_fn_name,
         {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.x")},
         String("foo")},
        {value_fn_name, {wide_json, String("$.j[1]")}, String("0")},
        {value_fn_name,
         {array_of_deep_json, String("$.arr[13].a.b.c.d.e.f.g.h.i.j.k.l.m.x")},
         String("foo")},
        {value_fn_name,
         {array_of_wide_json, String("$.arr[12].index")},
         String("12")},
        {value_fn_name,
         {array_of_wide_json, String("$.arr[12].h")},
         String("baz")},
        // Non-ASCII UTF-8 and special cases.
        {value_fn_name,
         {json_constructor(R"({"Моша_öá5ホバークラフト鰻鰻" : "x"})"),
          String("$.Моша_öá5ホバークラフト鰻鰻")},
         String("x")},
        {value_fn_name,
         {json_constructor(R"({"1" : 2})"),
          absl::StrCat("$", EscapeKey(sql_standard_mode, "1"))},
         String("2")},
        {value_fn_name,
         {json_constructor(R"({"1" : 2})"), String("$.1")},
         String("2")},
        // Unsupported/unimplemented JSONPath features.
        {value_fn_name,
         {json6, String("$.a[0].b[(@.length-1)].c[(@.length-1)].d")},
         NullString(),
         OUT_OF_RANGE},
        {value_fn_name, {json9, String("$..1")}, NullString(), OUT_OF_RANGE},
        // Optional json_path argument
        {value_fn_name,
         {json_constructor("\"hello my friend\"")},
         String("hello my friend")},
        {value_fn_name, {json_constructor("-4.58295")}, String("-4.58295")},
    };
    if (sql_standard_mode) {
      all_tests.push_back({value_fn_name,
                           {json3, String("$['a.b.c']")},
                           NullString(),
                           OUT_OF_RANGE});
      all_tests.push_back(
          {value_fn_name, {json9, String("$.\"\"[1]")}, String("foo")});
    } else {
      all_tests.push_back({value_fn_name,
                           {json3, String("$.\"a.b.c\"")},
                           NullString(),
                           OUT_OF_RANGE});
      all_tests.push_back(
          {value_fn_name, {json9, String("$[''][1]")}, String("foo")});
      all_tests.push_back({value_fn_name,
                           {json9, String("$[][1]")},
                           NullString(),
                           OUT_OF_RANGE});
    }
  } else {
    all_tests = {
        {query_fn_name,
         {json_constructor(absl::nullopt), NullString()},
         json_constructor(absl::nullopt)},
        {query_fn_name, {json1, NullString()}, json_constructor(absl::nullopt)},
        {query_fn_name,
         {json_constructor(absl::nullopt), "$"},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json1, "$"},
         json_constructor(
             R"({"a":{"b":[{"c":"foo","d":1.23,"f":null}],"e":true}})")},
        {query_fn_name,
         {json1, String("$.a")},
         json_constructor(R"({"b":[{"c":"foo","d":1.23,"f":null}],"e":true})")},
        {query_fn_name,
         {json1, absl::StrCat("$", EscapeKey(sql_standard_mode, "a"))},
         json_constructor(R"({"b":[{"c":"foo","d":1.23,"f":null}],"e":true})")},
        {query_fn_name,
         {json1, String("$.a.b")},
         json_constructor(R"([{"c":"foo","d":1.23,"f":null}])")},
        {query_fn_name,
         {json1, String("$.a.b[0]")},
         json_constructor(R"({"c":"foo","d":1.23,"f":null})")},
        {query_fn_name,
         {json1, String("$.a.b[0].c")},
         json_constructor(R"("foo")")},
        {query_fn_name,
         {json1, String("$.a.b[0].d")},
         json_constructor("1.23")},
        {query_fn_name,
         {json1, String("$.a.b[0].g")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json1, String("$.a.b[1]")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json1, String("$.a.x[0]")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json1, String("$.a.x")},
         json_constructor(absl::nullopt)},
        {query_fn_name, {json1, String("$.a.e")}, json_constructor("true")},
        {query_fn_name,
         {json1, String("abc")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json1, String("")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String("$.x")},
         json_constructor("[1,2,3,4,5]")},
        {query_fn_name, {json2, String("$.x[1]")}, json_constructor("2")},
        {query_fn_name,
         {json2, absl::StrCat("$", EscapeKey(sql_standard_mode, "x"), "[1]")},
         json_constructor("2")},
        {query_fn_name,
         {json2, String("$.x[-1]")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json2, absl::StrCat("$.x", EscapeKey(sql_standard_mode, "a"))},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json2, absl::StrCat("$.x", EscapeKey(sql_standard_mode, "1"))},
         json_constructor("2")},
        {query_fn_name, {json2, String("$.x[ 1]")}, json_constructor("2")},
        {query_fn_name,
         {json2, String("$.x[10]")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json2, String("$.y.a")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json2, String("$.y[0].a")},
         json_constructor(R"("bar")")},
        {query_fn_name,
         {json2, absl::StrCat("$.y[0]", EscapeKey(sql_standard_mode, "a"))},
         json_constructor(R"("bar")")},
        {query_fn_name,
         {json2, String("$.y[0].b")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json2, String("$.y[1].a")},
         json_constructor(absl::nullopt)},
        {query_fn_name,
         {json2, String("$.y[1].b")},
         json_constructor(R"("baz")")},
        {query_fn_name,
         {json3, String("$")},
         json_constructor(R"({"a.b.c":5})")},
        // Query with dots in the middle of the key
        {query_fn_name,
         {json3, absl::StrCat("$", EscapeKey(sql_standard_mode, "a.b.c"))},
         json_constructor("5")},
        // If $[a.b.c] is a valid path then it should return "5"
        // Else it should return OUT_OF_RANGE current code fails to
        // handle this correctly.
        //
        // {query_fn_name, {json3, String("$[a.b.c]")}, NullString(),
        //  OUT_OF_RANGE},
        {query_fn_name,
         {json4, String("$.longer_field_name")},
         json_constructor("[]")},
        {query_fn_name, {json5, String("$")}, json_constructor("true")},
        {query_fn_name,
         {json7, String("$")},
         json_constructor(R"({"a":{"b":{"c":{"d":3}}}})")},
        {query_fn_name,
         {json8, String("$")},
         json_constructor(R"({"x":[],"y":[1,2,5,3,4]})")},
        {query_fn_name, {json8, String("$.x")}, json_constructor(R"([])")},
        // Deep/wide json.
        {query_fn_name,
         {deep_json, String("$.a.b.c.d.e.f.g.h.i.j")},
         json_constructor(
             R"({"k":{"l":{"m":{"x":"foo","y":10,"z":[1,2,3]}}}})")},
        {query_fn_name,
         {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m")},
         json_constructor(R"({"x":"foo","y":10,"z":[1,2,3]})")},
        {query_fn_name,
         {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.z[1]")},
         json_constructor("2")},
        {query_fn_name,
         {deep_json, String("$.a.b.c.d.e.f.g.h.i.j.k.l.m.x")},
         json_constructor(R"("foo")")},
        {query_fn_name, {wide_json, String("$.j")}, json_constructor("[-3,0]")},
        {query_fn_name,
         {array_of_deep_json, String("$.arr[13].index")},
         json_constructor("13")},
        {query_fn_name,
         {array_of_deep_json,
          String("$.arr[13].a.b.c.d.e.f.g.h.i.j.k.l.m.z[1]")},
         json_constructor("2")},
        {query_fn_name,
         {array_of_wide_json, String("$.arr[17].index")},
         json_constructor("17")},
        {query_fn_name,
         {array_of_wide_json, String("$.arr[17].k")},
         json_constructor("0.321")},
        {query_fn_name,
         {array_of_wide_json, String("$.arr[14]")},
         json_constructor(
             R"({"index":14,"a":null,"b":"bar","c":false,"d":[4,5],"e":0.123,)"
             R"("f":"345","g":null,"h":"baz","i":true,"j":[-3,0],"k":0.321,)"
             R"("l":"678"})")},
        // Non-ASCII UTF-8 and special cases.
        {query_fn_name,
         {json_constructor(R"({"Моша_öá5ホバークラフト鰻鰻" : "x"})"),
          String("$")},
         json_constructor(R"({"Моша_öá5ホバークラフト鰻鰻":"x"})")},
        // Unsupported/unimplemented JSONPath features.
        {query_fn_name,
         {json1, String("$.a.*")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json1, String("$.a.b..c")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String("$.x[(@.length-1)]")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String("$.x[-1:]")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String("$.x[0:4:2]")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String("$.x[:2]")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String("$.x[0,1]")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String("$.y[?(@.a)]")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json2, String(R"($.y[?(@.a==='bar')])")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
        {query_fn_name,
         {json9, String("$..1")},
         json_constructor(absl::nullopt),
         OUT_OF_RANGE},
    };
    if (sql_standard_mode) {
      all_tests.push_back({query_fn_name,
                           {json3, String("$['a.b.c']")},
                           json_constructor(absl::nullopt),
                           OUT_OF_RANGE});
      all_tests.push_back({query_fn_name,
                           {json9, String("$.\"\"")},
                           json_constructor(R"([5,"foo"])")});
      all_tests.push_back({query_fn_name,
                           {json9, String("$.\"\"[1]")},
                           json_constructor(R"("foo")")});
    } else {
      all_tests.push_back({query_fn_name,
                           {json3, String("$.\"a.b.c\"")},
                           json_constructor(absl::nullopt),
                           OUT_OF_RANGE});
      all_tests.push_back({query_fn_name,
                           {json9, String("$['']")},
                           json_constructor(R"([5,"foo"])")});
      all_tests.push_back({query_fn_name,
                           {json9, String("$[''][1]")},
                           json_constructor(R"("foo")")});
      all_tests.push_back({query_fn_name,
                           {json9, String("$[][1]")},
                           json_constructor(absl::nullopt),
                           OUT_OF_RANGE});
    }
  }
  return all_tests;
}

const std::vector<FunctionTestCall> GetStringJsonTests(bool sql_standard_mode,
                                                       bool scalar_test_cases) {
  std::string query_fn_name;
  std::string value_fn_name;
  if (sql_standard_mode) {
    query_fn_name = "json_query";
    value_fn_name = "json_value";
  } else {
    query_fn_name = "json_extract";
    value_fn_name = "json_extract_scalar";
  }
  // Wide numbers
  const Value json_with_wide_numbers = String(
      R"({
           "x":11111111111111111111,
           "y":3.14e314,
           "z":123456789012345678901234567890,
           "a":true,
           "s":"foo"
         })");

  std::vector<FunctionTestCall> tests =
      GetJsonTestsCommon(sql_standard_mode, scalar_test_cases,
                         [](absl::optional<absl::string_view> input) {
                           if (input.has_value()) return String(input.value());
                           return NullString();
                         });

  if (scalar_test_cases) {
    tests.push_back({value_fn_name,
                     {json_with_wide_numbers, String("$.x")},
                     String("11111111111111111111")});
    tests.push_back({value_fn_name,
                     {json_with_wide_numbers, String("$.y")},
                     String("3.14e314")});
    tests.push_back({value_fn_name,
                     {json_with_wide_numbers, String("$.z")},
                     String("123456789012345678901234567890")});
    tests.push_back({value_fn_name,
                     {json_with_wide_numbers, String("$.s")},
                     String("foo")});
    // Characters should _not_ be escaped in JSON_EXTRACT_SCALAR.
    tests.push_back(
        {value_fn_name,
         {String(R"({"a": "foo\t\\t\\\t\n\\nbar\ \"baz\\"})"), String("$.a")},
         String("foo\t\\t\\\t\n\\nbar \"baz\\")});
  } else {
    tests.push_back(
        {query_fn_name,
         {String(
              R"({"a":{"b":[{"c" : "foo", "d": 1.23, "f":null }], "e": true}})"),
          String("$.a.b[0].f")},
         NullString()});
    // Malformed JSON.
    tests.push_back(
        {query_fn_name, {String(R"({"a": )"), String("$")}, NullString()});
    // Wide numbers
    tests.push_back(
        {query_fn_name,
         {json_with_wide_numbers, String("$")},
         String(R"({"x":11111111111111111111,"y":3.14e314,)"
                R"("z":123456789012345678901234567890,"a":true,"s":"foo"})")});
    // Characters should be escaped in JSON_EXTRACT.
    tests.push_back(
        {query_fn_name,
         {String(R"({"a": "foo\t\\t\\\t\n\\nbar\ \"baz\\"})"), String("$.a")},
         String(R"("foo\t\\t\\\t\n\\nbar \"baz\\")")});
  }
  return tests;
}

const std::vector<FunctionTestCall> GetNativeJsonTests(bool sql_standard_mode,
                                                       bool scalar_test_cases) {
  std::string query_fn_name;
  std::string value_fn_name;
  if (sql_standard_mode) {
    query_fn_name = "json_query";
    value_fn_name = "json_value";
  } else {
    query_fn_name = "json_extract";
    value_fn_name = "json_extract_scalar";
  }

  // Wide numbers
  const Value json_with_wide_numbers =
      Json(JSONValue::ParseJSONString(R"({"x":11111111111111111111, )"
                                      R"("z":123456789012345678901234567890, )"
                                      R"("a":true, "s":"foo"})")
               .value());

  std::vector<FunctionTestCall> tests = GetJsonTestsCommon(
      sql_standard_mode, scalar_test_cases,
      [](absl::optional<absl::string_view> input) {
        if (input.has_value())
          return Json(JSONValue::ParseJSONString(input.value()).value());
        return NullJson();
      });

  if (scalar_test_cases) {
    tests.push_back({value_fn_name,
                     {json_with_wide_numbers, String("$.x")},
                     String("11111111111111111111")});
    tests.push_back({value_fn_name,
                     {json_with_wide_numbers, String("$.z")},
                     String("1.2345678901234568e+29")});
    tests.push_back({value_fn_name,
                     {json_with_wide_numbers, String("$.s")},
                     String("foo")});
    // Characters should _not_ be escaped in JSON_EXTRACT_SCALAR.
    tests.push_back({value_fn_name,
                     {Json(JSONValue::ParseJSONString(
                               R"({"a": "foo\t\\t\\\t\n\\nbar \"baz\\"})")
                               .value()),
                      String("$.a")},
                     String("foo\t\\t\\\t\n\\nbar \"baz\\")});
  } else {
    JSONValue null_json_value;
    // This case differs from the string version of STRING_EXTRACT/QUERY. The
    // result is a JSON 'null' which is different from a null JSON.
    tests.push_back(
        {query_fn_name,
         {Json(JSONValue::ParseJSONString(
                   R"({"a":{"b":[{"c" : "foo", "d": 1.23, "f":null }], )"
                   R"("e": true}})")
                   .value()),
          String("$.a.b[0].f")},
         Json(std::move(null_json_value))});
    // Malformed JSON.
    tests.push_back(
        {query_fn_name,
         QueryParamsWithResult(
             {Value::UnvalidatedJsonString(R"({"a": )"), String("$")},
             NullJson(), OUT_OF_RANGE)
             .WrapWithFeatureSet(
                 {FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION})});
    tests.push_back(
        {query_fn_name,
         {json_with_wide_numbers, String("$")},
         Json(JSONValue::ParseJSONString(
                  R"({"x":11111111111111111111,)"
                  R"("z":123456789012345678901234567890,"a":true,"s":"foo"})")
                  .value())});
    // Characters should be escaped in JSON_EXTRACT.
    tests.push_back(
        {query_fn_name,
         {Json(JSONValue::ParseJSONString(
                   R"({"a": "foo\t\\t\\\t\n\\nbar \"baz\\"})")
                   .value()),
          String("$.a")},
         Json(JSONValue::ParseJSONString(R"("foo\t\\t\\\t\n\\nbar \"baz\\")")
                  .value())});
  }

  return tests;
}

}  // namespace

std::vector<FunctionTestCall> GetFunctionTestsStringJsonQuery() {
  return GetStringJsonTests(/*sql_standard_mode=*/true,
                            /*scalar_test_cases=*/false);
}

std::vector<FunctionTestCall> GetFunctionTestsStringJsonExtract() {
  return GetStringJsonTests(/*sql_standard_mode=*/false,
                            /*scalar_test_cases=*/false);
}

std::vector<FunctionTestCall> GetFunctionTestsStringJsonValue() {
  return GetStringJsonTests(/*sql_standard_mode=*/true,
                            /*scalar_test_cases=*/true);
}

std::vector<FunctionTestCall> GetFunctionTestsStringJsonExtractScalar() {
  return GetStringJsonTests(/*sql_standard_mode=*/false,
                            /*scalar_test_cases=*/true);
}

std::vector<FunctionTestCall> GetFunctionTestsNativeJsonQuery() {
  return GetNativeJsonTests(/*sql_standard_mode=*/true,
                            /*scalar_test_cases=*/false);
}

std::vector<FunctionTestCall> GetFunctionTestsNativeJsonExtract() {
  return GetNativeJsonTests(/*sql_standard_mode=*/false,
                            /*scalar_test_cases=*/false);
}

std::vector<FunctionTestCall> GetFunctionTestsNativeJsonValue() {
  return GetNativeJsonTests(/*sql_standard_mode=*/true,
                            /*scalar_test_cases=*/true);
}

std::vector<FunctionTestCall> GetFunctionTestsNativeJsonExtractScalar() {
  return GetNativeJsonTests(/*sql_standard_mode=*/false,
                            /*scalar_test_cases=*/true);
}

std::vector<QueryParamsWithResult> GetFunctionTestsJsonIsNull() {
  std::vector<QueryParamsWithResult> v = {
      {{NullJson()}, True()},
      {{Json(JSONValue(1.1))}, False()},
      {{Json(JSONValue(std::string{"null"}))}, False()},
      {{Value::Null(types::JsonArrayType())}, True()},
  };
  return v;
}

std::vector<FunctionTestCall> GetFunctionTestsParseJson() {
  // TODO: Currently these tests only verify if PARSE_JSON produces
  // the same output as JSONValue::ParseJsonString. A better test would need
  // to evaluate the output of PARSE_JSON either through JSONValueConstRef
  // accessors or through the JSON_VALUE SQL function.
  enum WideNumberMode { kExact = 0x1, kRound = 0x2 };
  struct ParseJsonTestCase {
    std::string json_to_parse;
    // Bit flag representing the wide number parsing modes supported by the
    // test. 'json_to_parse' will be parsed in all supported modes.
    uint8_t wide_number_mode_flag;
  };
  std::vector<ParseJsonTestCase> valid_json_tests = {
      // 'exact' or 'round' mode
      {"123", kExact | kRound},
      {"\"string\"", kExact | kRound},
      {"12.3", kExact | kRound},
      {"true", kExact | kRound},
      {"null", kExact | kRound},
      {"[1, true, null]", kExact | kRound},
      {"{\"a\" : [ {\"b\": \"c\"}, {\"b\": false}]}", kExact | kRound},
      {absl::StrCat("{", kDeepJsonString, "}"), kExact | kRound},
      {absl::StrCat("{", kWideJsonString, "}"), kExact | kRound},
      {"11111111111111111111", kExact | kRound},
      {"1.2345678901234568e+29", kExact | kRound},
      {R"("foo\t\\t\\\t\n\\nbar \"baz\\")", kExact | kRound},
      {"[[1, 2, 3], [\"a\", \"b\", \"c\"], [[true, false]]]", kExact | kRound},
      {"\"\u005C\u005C\u0301\u263a\u2028\"", kExact | kRound},
      {"\"你好\"", kExact | kRound},
      {"{\"%2526%7C%2B\": null}", kExact | kRound},
      // 'round' mode only
      {"123456789012345678901234567890", kRound},
      {R"({"x":11111111111111111111, "z":123456789012345678901234567890})",
       kRound},
      {"-0.0000002414214151379150123", kRound},
      {"989124899124.1241251252125121285", kRound},
      {"9891248991241241251252125121285021782188712512512", kRound}};
  std::vector<ParseJsonTestCase> invalid_json_tests = {
      // Invalid regardless of wide number mode
      {"{\"foo\": 12", kExact | kRound},
      {"\"", kExact | kRound},
      {"string", kExact | kRound},
      {"[1, 2", kExact | kRound},
      {"[\"a\" \"b\"]", kExact | kRound},
      {"'foo'", kExact | kRound},
      {"True", kExact | kRound},
      {"FALSE", kExact | kRound},
      {"NULL", kExact | kRound},
      {"nan", kExact | kRound},
      {R"("\")", kExact | kRound},
      {".3", kExact | kRound},
      {"4.", kExact | kRound},
      {"0x3", kExact | kRound},
      {"1.e", kExact | kRound},
      {R"("f1":"v1")", kExact | kRound},
      {R"("":"v1")", kExact | kRound},
      {"3]", kExact | kRound},
      {"4}", kExact | kRound},
      {"-", kExact | kRound},
      {"", kExact | kRound},
      {" ", kExact | kRound},
      // Fails parsing in both 'exact' and 'round'. Intentionally kept separate
      // from cases above to allow for handling `kStringify` when implemented.
      {"-1.79769313486232e+309", kExact | kRound},
      {"1.79769313486232e+309", kExact | kRound},
      // Fails parsing in 'exact'
      {"123456789012345678901234567890", kExact},
      {R"({"x":11111111111111111111, "z":123456789012345678901234567890})",
       kExact},
      {"-0.0000002414214151379150123", kExact},
      {"989124899124.1241251252125121285", kExact},
      {"9891248991241241251252125121285021782188712512512", kExact}};
  std::vector<FunctionTestCall> v;
  v.reserve(valid_json_tests.size() * 3 + invalid_json_tests.size() * 3 + 6);
  for (const ParseJsonTestCase& test : valid_json_tests) {
    if (test.wide_number_mode_flag & kExact) {
      // Add both the case where mode is specified and mode is not specified.
      v.push_back(
          {"parse_json",
           QueryParamsWithResult(
               {test.json_to_parse},
               Json(JSONValue::ParseJSONString(
                        test.json_to_parse,
                        {.legacy_mode = false, .strict_number_parsing = true})
                        .value()))
               .WrapWithFeature(FEATURE_JSON_TYPE)});
      v.push_back(
          {"parse_json",
           QueryParamsWithResult(
               {test.json_to_parse, "exact"},
               Json(JSONValue::ParseJSONString(
                        test.json_to_parse,
                        {.legacy_mode = false, .strict_number_parsing = true})
                        .value()))
               .WrapWithFeatureSet(
                   {FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE})});
    }
    if (test.wide_number_mode_flag & kRound) {
      v.push_back(
          {"parse_json",
           QueryParamsWithResult(
               {test.json_to_parse, "round"},
               Json(JSONValue::ParseJSONString(test.json_to_parse).value()))
               .WrapWithFeatureSet(
                   {FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE})});
    }
  }
  for (const ParseJsonTestCase& test : invalid_json_tests) {
    if (test.wide_number_mode_flag & kExact) {
      // Add both the case where mode is specified and mode is not specified.
      v.push_back({"parse_json", QueryParamsWithResult({test.json_to_parse},
                                                       NullJson(), OUT_OF_RANGE)
                                     .WrapWithFeature(FEATURE_JSON_TYPE)});
      v.push_back(
          {"parse_json", QueryParamsWithResult({test.json_to_parse, "exact"},
                                               NullJson(), OUT_OF_RANGE)
                             .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS,
                                                  FEATURE_JSON_TYPE})});
    }
    if (test.wide_number_mode_flag & kRound) {
      v.push_back(
          {"parse_json", QueryParamsWithResult({test.json_to_parse, "round"},
                                               NullJson(), OUT_OF_RANGE)
                             .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS,
                                                  FEATURE_JSON_TYPE})});
    }
  }
  // Fail if invalid 'wide_number_mode' specified. 'wide_number_mode' is
  // case-sensitive.
  v.push_back(
      {"parse_json",
       QueryParamsWithResult({"2.5", "junk"}, NullJson(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE})});
  v.push_back(
      {"parse_json",
       QueryParamsWithResult({"2.5", "EXACT"}, NullJson(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE})});
  v.push_back(
      {"parse_json",
       QueryParamsWithResult({"2.5", "Round"}, NullJson(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE})});
  // Return NULL if either argument is NULL.
  v.push_back({"parse_json", QueryParamsWithResult({NullString()}, NullJson())
                                 .WrapWithFeatureSet({FEATURE_JSON_TYPE})});
  v.push_back(
      {"parse_json",
       QueryParamsWithResult({NullString(), "wide"}, NullJson())
           .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE})});
  v.push_back(
      {"parse_json",
       QueryParamsWithResult({"25", NullString()}, NullJson())
           .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE})});
  // Legacy parsing
  v.push_back(
      {"parse_json",
       QueryParamsWithResult(
           {"'str'", "round"},
           Json(JSONValue::ParseJSONString("'str'", {.legacy_mode = true})
                    .value()))
           .WrapWithFeatureSet({FEATURE_NAMED_ARGUMENTS, FEATURE_JSON_TYPE,
                                FEATURE_JSON_LEGACY_PARSE})});
  v.push_back({"parse_json", QueryParamsWithResult({NullString()}, NullJson())
                                 .WrapWithFeature(FEATURE_JSON_TYPE)});
  return v;
}

std::vector<FunctionTestCall> GetFunctionTestsConvertJson() {
  std::vector<FunctionTestCall> tests = {
      // INT64
      {"int64", {NullJson()}, NullInt64()},
      {"int64", {Json(JSONValue(int64_t{123}))}, Int64(123)},
      {"int64", {Json(JSONValue(int64_t{-123}))}, Int64(-123)},
      {"int64", {Json(JSONValue(10.0))}, Int64(10)},
      {"int64",
       {Json(JSONValue(std::numeric_limits<int64_t>::min()))},
       Int64(std::numeric_limits<int64_t>::min())},
      {"int64",
       {Json(JSONValue(std::numeric_limits<int64_t>::max()))},
       Int64(std::numeric_limits<int64_t>::max())},
      {"int64", {Json(JSONValue(1e100))}, NullInt64(), OUT_OF_RANGE},
      {"int64",
       {Json(JSONValue(uint64_t{std::numeric_limits<uint64_t>::max()}))},
       NullInt64(),
       OUT_OF_RANGE},
      {"int64",
       {Json(JSONValue(std::string{"123"}))},
       NullInt64(),
       OUT_OF_RANGE},
      {"int64",
       QueryParamsWithResult({Value::UnvalidatedJsonString("[10,3")},
                             NullInt64(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"int64",
       QueryParamsWithResult({Value::UnvalidatedJsonString("123")}, Int64(123))
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"int64", {Json(JSONValue(10.1))}, NullInt64(), OUT_OF_RANGE},
      // BOOL
      {"bool", {NullJson()}, NullBool()},
      {"bool", {Json(JSONValue(false))}, Value::Bool(false)},
      {"bool", {Json(JSONValue(true))}, Value::Bool(true)},
      {"bool",
       QueryParamsWithResult({Value::UnvalidatedJsonString("[true")},
                             NullBool(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // DOUBLE
      // Null input
      {"double", {NullJson()}, NullDouble()},
      {"double",
       QueryParamsWithResult({Json(JSONValue(1.0)), NullString()}, NullDouble())
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // Passing
      {"double", {Json(JSONValue(1.0))}, Double(1.0)},
      {"double", {Json(JSONValue(-1.0))}, Double(-1.0)},
      {"double",
       {Json(JSONValue(std::numeric_limits<double>::min()))},
       std::numeric_limits<double>::min()},
      {"double",
       {Json(JSONValue(std::numeric_limits<double>::max()))},
       std::numeric_limits<double>::max()},
      {"double",
       QueryParamsWithResult({Json(JSONValue(1.0)), "round"}, Double(1.0))
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult({Json(JSONValue(1.0)), "exact"}, Double(1.0))
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // Fails because value is case-sensitive and not in ["exact", "round"]
      {"double",
       QueryParamsWithResult({Json(JSONValue(1.0)), "EXACT"}, NullDouble(),
                             OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // Fails in "exact" and "round" due to number overflow parsing of json
      {"double",
       QueryParamsWithResult(
           {Value::UnvalidatedJsonString("-1.79769313486232e+309"), "exact"},
           NullDouble(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult(
           {Value::UnvalidatedJsonString("-1.79769313486232e+309"), "round"},
           NullDouble(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult(
           {Value::UnvalidatedJsonString("-1.79769313486232e+309")},
           NullDouble(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // Fails in "exact" but succeeds in "round"
      {"double",
       QueryParamsWithResult(
           {Value::UnvalidatedJsonString("-0.0000002414214151379150123"),
            "exact"},
           NullDouble(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult(
           {Value::UnvalidatedJsonString("-0.0000002414214151379150123"),
            "round"},
           Double(-0.000000241421415137915))
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult(
           {Value::UnvalidatedJsonString("-0.0000002414214151379150123")},
           Double(-0.000000241421415137915))
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},

      {"double",
       QueryParamsWithResult(
           {Json(JSONValue(std::numeric_limits<uint64_t>::max())), "exact"},
           NullDouble(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult(
           {Json(JSONValue(std::numeric_limits<uint64_t>::max())), "round"},
           Double(1.8446744073709552e+19))
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult({Json(JSONValue(1.0)), NullString()}, NullDouble())
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_NAMED_ARGUMENTS,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult({Value::UnvalidatedJsonString(std::to_string(
                                 std::numeric_limits<uint64_t>::max()))},
                             Double(1.8446744073709552e+19))
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // Other types should return an error
      {"double",
       {Json(JSONValue(std::string{"TesT"}))},
       NullDouble(),
       OUT_OF_RANGE},
      {"double",
       {Json(JSONValue(std::string{"3"}))},
       NullDouble(),
       OUT_OF_RANGE},
      {"double", {Json(JSONValue(false))}, NullDouble(), OUT_OF_RANGE},
      {"double",
       QueryParamsWithResult(
           {Json(JSONValue::ParseJSONString(R"({"a": 1})").value())},
           NullDouble(), OUT_OF_RANGE)
           .WrapWithFeatureSet(
               {FEATURE_JSON_TYPE, FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      {"double",
       QueryParamsWithResult(
           {Json(JSONValue::ParseJSONString(R"([10, 20])").value())},
           NullDouble(), OUT_OF_RANGE)
           .WrapWithFeatureSet(
               {FEATURE_JSON_TYPE, FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // STRING
      {"string", {NullJson()}, NullString()},
      {"string", {Json(JSONValue(std::string{""}))}, ""},
      {"string", {Json(JSONValue(std::string{"TesT"}))}, "TesT"},
      {"string", {Json(JSONValue(std::string{"1"}))}, "1"},
      {"string", {Json(JSONValue(std::string{"abc123"}))}, "abc123"},
      {"string", {Json(JSONValue(std::string{"12¿©?Æ"}))}, "12¿©?Æ"},
      {"string",
       QueryParamsWithResult({Value::UnvalidatedJsonString("[string")},
                             NullString(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})},
      // TYPE
      {"json_type", {NullJson()}, NullString()},
      {"json_type", {Json(JSONValue())}, Value::String("null")},
      {"json_type", {Json(JSONValue(std::string{"10"}))}, "string"},
      {"json_type", {Json(JSONValue(int64_t{1}))}, "number"},
      {"json_type", {Json(JSONValue(2.0))}, "number"},
      {"json_type", {Json(JSONValue(-1.0))}, "number"},
      {"json_type",
       {Json(JSONValue(uint64_t{std::numeric_limits<uint64_t>::max()}))},
       "number"},
      {"json_type", {Json(JSONValue(true))}, "boolean"},
      {"json_type",
       {Json(JSONValue::ParseJSONString(R"({"a": 1})").value())},
       "object"},
      {"json_type",
       {Json(JSONValue::ParseJSONString(R"([10,20])").value())},
       "array"},
      {"json_type",
       {Json(JSONValue::ParseJSONString(R"({"a": {"b": {"c": 1}}, "d": 4})")
                 .value())},

       "object"},
      {"json_type",
       {Json(JSONValue::ParseJSONString(
                 R"([{"a": {"b": {"c": 1}}, "d": 4}, {"e": 1}])")
                 .value())},
       "array"},

      {"json_type",
       QueryParamsWithResult({Value::UnvalidatedJsonString("[10,3")},
                             NullString(), OUT_OF_RANGE)
           .WrapWithFeatureSet({FEATURE_JSON_TYPE, FEATURE_JSON_NO_VALIDATION,
                                FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS})}};
  return tests;
}

std::vector<FunctionTestCall> GetFunctionTestsConvertJsonIncompatibleTypes() {
  std::vector<std::string> type_functions = {"int64", "double", "string",
                                             "bool"};
  std::map<std::string, ValueConstructor> null_lookup = {
      {"int64", NullInt64()},
      {"double", NullDouble()},
      {"string", NullString()},
      {"bool", NullBool()}};
  std::map<std::string, ValueConstructor> input_type_value_lookup = {
      {"object", Json(JSONValue::ParseJSONString(R"({"a": 2})").value())},
      {"array", Json(JSONValue::ParseJSONString(R"([20, 30])").value())},
      {"bool", Json(JSONValue(true))},
      {"double",
       {Json(JSONValue(
           2.1))}},  // incompatible with int64_t due to its fractional part
      {"int64", Json(JSONValue(int64_t{321}))},
      {"string", Json(JSONValue(std::string{"abc321"}))}};

  std::vector<FunctionTestCall> tests;
  for (const std::string& type_function : type_functions) {
    for (const auto& [input_type, input_value] : input_type_value_lookup) {
      if (type_function != input_type &&
          // double(int64_t) is compatible
          !(type_function == "double" && input_type == "int64")) {
        std::vector<ValueConstructor> input = {input_value};
        tests.emplace_back(type_function, input,
                           null_lookup.find(type_function)->second,
                           OUT_OF_RANGE);
      }
    }
  }
  return tests;
}
}  // namespace zetasql
