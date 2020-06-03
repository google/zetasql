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
       {Int64(int64_t{-9007199254740991})},
       String("-9007199254740991")},
      {"to_json_string",
       {Int64(int64_t{-9007199254740992})},
       String("-9007199254740992")},
      // Integers in the range of [-2^53, 2^53] can be represented losslessly as
      // a double-precision floating point number. Integers outside that range
      // are quoted.
      {"to_json_string",
       {Int64(int64_t{-9007199254740993})},
       String("\"-9007199254740993\"")},
      {"to_json_string",
       {Int64(int64_t{-12345678901234567})},
       String("\"-12345678901234567\"")},
      {"to_json_string", {Int64(int64min)}, String("\"-9223372036854775808\"")},
      {"to_json_string",
       {Int64(int64_t{9007199254740991})},
       String("9007199254740991")},
      {"to_json_string",
       {Int64(int64_t{9007199254740992})},
       String("9007199254740992")},
      {"to_json_string",
       {Int64(int64_t{9007199254740993})},
       String("\"9007199254740993\"")},
      {"to_json_string",
       {Int64(int64_t{12345678901234567})},
       String("\"12345678901234567\"")},
      {"to_json_string", {Int64(int64max)}, String("\"9223372036854775807\"")},
      {"to_json_string", {Uint64(0)}, String("0")},
      {"to_json_string", {Uint64(132)}, String("132")},
      {"to_json_string",
       {Uint64(uint64_t{9007199254740991})},
       String("9007199254740991")},
      {"to_json_string",
       {Uint64(uint64_t{9007199254740992})},
       String("9007199254740992")},
      {"to_json_string",
       {Uint64(uint64_t{9007199254740993})},
       String("\"9007199254740993\"")},
      {"to_json_string",
       {Uint64(uint64_t{12345678901234567})},
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
      // string.
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
      // rules for string values apply to field names.
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
       // The string contains trailing whitespace, which is disallowed in .cc
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
       // The string contains trailing whitespace, which is disallowed in .cc
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
      {NumericValue::FromDouble(3.14).value(), String("\"3.14\"")},
      {NumericValue::FromStringStrict("555551.618033989").value(),
       String("\"555551.618033989\"")},
      {NumericValue::FromStringStrict("0.000000001").value(),
       String("\"0.000000001\"")},
      {NumericValue::FromStringStrict("-0.000000001").value(),
       String("\"-0.000000001\"")},
      {NumericValue::FromStringStrict("55555551.618033989").value(),
       String("\"55555551.618033989\"")},
      {NumericValue::FromStringStrict("1234567890.123456789").value(),
       String("\"1234567890.123456789\"")},
      {NumericValue::FromStringStrict("1234567890.12345678").value(),
       String("\"1234567890.12345678\"")},
      {NumericValue(9007199254740992ll), String("9007199254740992")},
      {NumericValue(9007199254740993ll), String("\"9007199254740993\"")},
      {NumericValue(-9007199254740992ll), String("-9007199254740992")},
      {NumericValue(-9007199254740993ll), String("\"-9007199254740993\"")},
      {NumericValue(2147483647), String("2147483647")},
      {NumericValue(2147483648), String("2147483648")},
      {NumericValue(-2147483648), String("-2147483648")},
      {NumericValue(-2147483649), String("-2147483649")},
      {NumericValue::MaxValue(),
       String("\"99999999999999999999999999999.999999999\"")},
      {NumericValue::FromStringStrict("99999999999999999999999999999").value(),
       String("\"99999999999999999999999999999\"")},
      {NumericValue::MinValue(),
       String("\"-99999999999999999999999999999.999999999\"")},
      {NumericValue::FromStringStrict("-99999999999999999999999999999").value(),
       String("\"-99999999999999999999999999999\"")},
  };
  // BIGNUMERIC
  const std::vector<std::pair<BigNumericValue, Value>> big_numeric_test_cases =
      {
          {BigNumericValue::MaxValue(),
           String("\"578960446186580977117854925043439539266."
                  "34992332820282019728792003956564819967\"")},
          {BigNumericValue::MinValue(),
           String("\"-578960446186580977117854925043439539266."
                  "34992332820282019728792003956564819968\"")},
          {BigNumericValue::FromStringStrict(
               "99999999999999999999999999999.000000001")
               .ValueOrDie(),
           String("\"99999999999999999999999999999.000000001\"")},
          {BigNumericValue::FromStringStrict(
               "-99999999999999999999999999999.000000001")
               .ValueOrDie(),
           String("\"-99999999999999999999999999999.000000001\"")},
          {BigNumericValue::FromStringStrict(
               "10000000000000000000000000000000000000.1")
               .ValueOrDie(),
           String("\"10000000000000000000000000000000000000.1\"")},
          {BigNumericValue::FromStringStrict(
               "-10000000000000000000000000000000000000.1")
               .ValueOrDie(),
           String("\"-10000000000000000000000000000000000000.1\"")},
          {BigNumericValue::FromStringStrict("1e38").ValueOrDie(),
           String("\"100000000000000000000000000000000000000\"")},
          {BigNumericValue::FromStringStrict("1e-38").ValueOrDie(),
           String("\"0.00000000000000000000000000000000000001\"")},
          {BigNumericValue::FromStringStrict(
               "1.00000000000000000000000000000000000001")
               .ValueOrDie(),
           String("\"1.00000000000000000000000000000000000001\"")},
          {BigNumericValue::FromStringStrict(
               "-1.00000000000000000000000000000000000001")
               .ValueOrDie(),
           String("\"-1.00000000000000000000000000000000000001\"")},
          {BigNumericValue::FromStringStrict(
               "10000000000000000000000000000000000000."
               "00000000000000000000000000000000000001")
               .ValueOrDie(),
           String("\"10000000000000000000000000000000000000."
                  "00000000000000000000000000000000000001\"")},
          {BigNumericValue::FromStringStrict(
               "-10000000000000000000000000000000000000."
               "00000000000000000000000000000000000001")
               .ValueOrDie(),
           String("\"-10000000000000000000000000000000000000."
                  "00000000000000000000000000000000000001\"")},
      };

  for (const auto& numeric_test_case : numeric_test_cases) {
    all_tests.emplace_back(
        "to_json_string",
        WrapResultForNumeric(
            {Value::Numeric(numeric_test_case.first)},
            QueryParamsWithResult::Result(numeric_test_case.second)));
    // Reuse the numeric cases for bignumeric
    all_tests.emplace_back(
        "to_json_string",
        WrapResultForBigNumeric(
            {Value::BigNumeric(BigNumericValue(numeric_test_case.first))},
            QueryParamsWithResult::Result(numeric_test_case.second)));
  }
  for (const auto& big_numeric_test_case : big_numeric_test_cases) {
    all_tests.emplace_back(
        "to_json_string",
        WrapResultForBigNumeric(
            {Value::BigNumeric(big_numeric_test_case.first)},
            QueryParamsWithResult::Result(big_numeric_test_case.second)));
  }
  all_tests.emplace_back(
      "to_json_string",
      WrapResultForNumeric({Value::NullNumeric()},
                           QueryParamsWithResult::Result(String("null"))));
  all_tests.emplace_back(
      "to_json_string",
      WrapResultForBigNumeric({Value::NullBigNumeric()},
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

}  // namespace zetasql
