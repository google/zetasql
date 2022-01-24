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

#include "zetasql/common/json_parser.h"

#include <cstdint>
#include <memory>

#include "zetasql/base/logging.h"
#include "gtest/gtest.h"
#include <cstdint>
#include "absl/flags/flag.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"

using zetasql::JSONParser;

namespace {
// Exclude '\0' in the end to catch bugs like b/153040983.
class StringNoTerminator {
 public:
  explicit StringNoTerminator(absl::string_view input)
      : str_no_terminator_(input.begin(), input.end()) {}
  absl::string_view data() const {
    return {str_no_terminator_.data(), str_no_terminator_.size()};
  }
  std::vector<char> str_no_terminator_;
};

// Constructs a pretty-printed js string from the parser calls.
class JSONToJSON : private StringNoTerminator, public JSONParser {
 private:
  std::string output_;
  int indent_;
  bool indent_next_;
  std::string error_;

 public:
  explicit JSONToJSON(absl::string_view js)
      : StringNoTerminator(js),
        JSONParser(data()),
        indent_(0),
        indent_next_(true) {}

  // The pretty-printed js that resulted from the call to Parse().
  const std::string& output() const { return output_; }
  void clear_output() { output_.clear(); }
  const std::string& error() const { return error_; }

 protected:
  bool BeginObject() override {
    Add("{\n");
    indent_++;
    return true;
  }
  bool EndObject() override {
    indent_--;
    Add("}");
    return true;
  }

  bool BeginMember(const std::string& key) override {
    Add(absl::StrFormat("\"%s\" : ", key));
    indent_next_ = false;
    return true;
  }
  bool EndMember(bool last) override {
    if (!last)
      output_.append(",\n");
    else
      output_.append("\n");
    return true;
  }

  bool BeginArray() override {
    Add("[\n");
    indent_++;
    return true;
  }
  bool EndArray() override {
    indent_--;
    Add("]");
    return true;
  }

  bool BeginArrayEntry() override { return true; }

  bool EndArrayEntry(bool last) override {
    if (!last)
      output_.append(",\n");
    else
      output_.append("\n");
    return true;
  }

  bool ParsedString(const std::string& str) override {
    Add(absl::StrFormat("\"%s\"", absl::CEscape(str)));
    return true;
  }
  bool ParsedNumber(absl::string_view str) override {
    Add(str);
    return true;
  }
  bool ParsedBool(bool val) override {
    Add(val ? "true" : "false");
    return true;
  }
  bool ParsedNull() override {
    Add("null");
    return true;
  }

  bool ReportFailure(const std::string& error_message) override {
    error_ = error_message;
    return JSONParser::ReportFailure(error_message);
  }

 private:
  void Add(absl::string_view str) {
    if (indent_next_) PrintIndent();
    indent_next_ = true;
    output_.append(str.begin(), str.end());
  }

  void PrintIndent() {
    for (int i = 0; i < indent_; ++i) output_.append("  ");
  }
};

// Parse `input` and test that the pretty-printed result matches `expected`.
static void ParseAndCompare(absl::string_view input,
                            absl::string_view expected) {
  JSONToJSON j(input);
  ASSERT_TRUE(j.Parse()) << "Parse failed: " << input;
  EXPECT_EQ(expected, j.output()) << "Input: " << input;
}

static void ParseAndExpectUnchanged(absl::string_view input) {
  return ParseAndCompare(input, input);
}

// Try to parse `input` but expect failure.
static void ParseAndExpectFail(absl::string_view input) {
  JSONToJSON j(input);
  ASSERT_FALSE(j.Parse()) << "Input: " << input;
}

TEST(JSONParserTest, ParseIdempotency) {
  const char* str = "\"idempotency\"";
  JSONToJSON j(str);

  // The first parse should succeed.
  ASSERT_TRUE(j.Parse());
  EXPECT_EQ(str, j.output()) << "Input: " << str;

  // The second parse should also succeed with the same output.
  j.clear_output();
  ASSERT_TRUE(j.Parse());
  EXPECT_EQ(str, j.output()) << "Input: " << str;
}

TEST(JSONParserTest, ParseString) {
  const char* str = "\"neat\"";
  ParseAndExpectUnchanged(str);
}

struct ParseStringComplicated_case {
  const char* orig;
  const char* expected;
} ParseStringComplicated_cases[] = {
    {"\"\"",  // empty string
     "\"\""},
    {"\"\\b\"",  // string with a \b only
     "\"\\010\""},
    {"\"hello\\nworld\"",  // \n
     "\"hello\\nworld\""},
    {"\"hello\\t\"",  // terminal \t
     "\"hello\\t\""},
    {"\"hello \\0\"",  // zero single octal escape sequence
     "\"hello \\000\""},
    {"\"hello \\1\"",  // non-zero single octal escape sequence
     "\"hello \\001\""},
    {"\"hello\\2world\"",  // 1 digit octal escape sequence
     "\"hello\\002world\""},
    {"\"hello \\01\"",  // 2 digit octal escape sequence
     "\"hello \\001\""},
    {"\"hello \\019\"",  // 2 digit octal escape sequence followed by non-octal
     "\"hello \\0019\""},
    {"\"hello\\16world\"",  // 2 digit octal escape sequence
     "\"hello\\016world\""},
    {"\"hello\\251world\"",  // full octal escape sequence
     "\"hello\\302\\251world\""},
    {"\"hello \\2519\"",  // full octal escape sequence followed by non-octal
     "\"hello \\302\\2519\""},
    {"\"hello \\2517\"",  // full octal escape sequence followed by octal digit
     "\"hello \\302\\2517\""},
    {"\"hello\\xa9world\"",  // hex escape sequence
     "\"hello\\302\\251world\""},
    {"\"helloworld\\xa9\"",  // terminal hex escape sequence
     "\"helloworld\\302\\251\""},
    {"\"helloworld\\xA9\"",  // terminal hex escape sequence with upper case
     "\"helloworld\\302\\251\""},
    {"\"helloworld\\xA9f\"",  // hex escape sequence followed by hex digit
     "\"helloworld\\302\\251f\""},
    {"\"helloworld\\xA9g\"",  // hex escape sequence followed by non hex
     "\"helloworld\\302\\251g\""},
    {"\"parecer\\u00e1 n\\u00famero\"",  // unicode spanish with accents
     "\"parecer\\303\\241 n\\303\\272mero\""},
    {"\"parecer\\u00e1\\n n\\u00famero\"",  // unicode spanish with accents
                                            // followed by another escape
                                            // sequence
     "\"parecer\\303\\241\\n n\\303\\272mero\""},
    {"\"\\u4e2d\\u65b0\\u7f5111\\u67087\\u65e5\\u7535\"",  // unicode chinese
     "\"\\344\\270\\255\\346\\226\\260\\347\\275\\22111"
     "\\346\\234\\2107\\346\\227\\245\\347\\224\\265\""},
    {"'n\\e\\a\\t'", "\"nea\\t\""},
    {"'neat'", "\"neat\""},
    {"\"neat\\\\\"", "\"neat\\\\\""},
    {"'ne\\\"at'", "\"ne\\\"at\""},
    {"\"ne'at\"", "\"ne\\\'at\""},  // ' is escaped unnecessarily by test code.
    {"   \"neat\"", "\"neat\""},
    {"\"neat\"   ", "\"neat\""},
    {"\"ne\\\"at\"", "\"ne\\\"at\""},
};

TEST(JSONParserTest, ParseStringComplicated) {
  for (int i = 0; i < ABSL_ARRAYSIZE(ParseStringComplicated_cases); ++i) {
    const char* orig = ParseStringComplicated_cases[i].orig;
    const char* expected = ParseStringComplicated_cases[i].expected;
    ParseAndCompare(orig, expected);
  }
}

const char* ParseStringFail_cases[] = {
    "\"\\",      // escape nothing and don't terminate string
    "\"\\\"",    // escape nothing
    "\"\\x\"",   // empty hex encoding
    "\"\\xf\"",  // short hex encoding with high bit set
    "\"neat",
    "neat\"",
    "ne\"at\"",
    "'neat",
    "neat'",
    "\"neat\\\\\\\"",  // three backslashes
    "\xEF\xBB",  // incomplete UTF-8 BOM; shouldn't parse as whitespace or json
    "\x80",      // invalid UTF-8 becomes U+FFFD, which is an "unexpected token"
    "test\xEF\xBB\xBF",  // UTF-8 BOM not at the beginning is an "unexpected
                         // token"
};

TEST(JSONParserTest, ParseStringFail) {
  for (int i = 0; i < ABSL_ARRAYSIZE(ParseStringFail_cases); ++i) {
    const char* test = ParseStringFail_cases[i];
    ParseAndExpectFail(test);
  }

  // Parse special case to test the ZETASQL_DCHECK in ParseOctalDigits.
  // Resizing to special.size() because it strips out the null terminator.
  std::string special = "\"\\4";
  special.resize(special.size());
  ParseAndExpectFail(special);
}

TEST(JSONParserTest, ParseUTF7FalsePositive) {
  const std::string str = "\"+2011+Abc\"";

  // Explicitly set the encoding so that it is not mistaken for UTF-7.
  JSONToJSON j(str);

  ASSERT_TRUE(j.Parse());
  EXPECT_EQ(str, j.output());
}

TEST(JSONParserTest, ParseDouble) {
  const char* str = "5.734";
  ParseAndCompare(str, "5.734");
}

struct ParseDoubleComplicated_case {
  const char* orig;
  const char* expected;
} ParseDoubleComplicated_cases[] = {
    {"5", "5"},
    {"-5", "-5"},
    {"-5.734", "-5.734"},
    {"-5.734e2", "-5.734e2"},
    {"-5.734e+2", "-5.734e+2"},
    {"5.734e-2", "5.734e-2"},
    {"-5e2", "-5e2"},
    {"-5E2", "-5E2"},
    {"    -5e2", "-5e2"},
    {"-5e2    ", "-5e2"},
};

TEST(JSONParserTest, ParseDoubleComplicated) {
  for (int i = 0; i < ABSL_ARRAYSIZE(ParseDoubleComplicated_cases); ++i) {
    const char* orig = ParseDoubleComplicated_cases[i].orig;
    const char* expected = ParseDoubleComplicated_cases[i].expected;
    ParseAndCompare(orig, expected);
  }
}

const char* ParseDoubleFail_cases[] = {
    "-5e2abc",
    "-ab5e2",
    "-5e",
};

TEST(JSONParserTest, ParseDoubleFail) {
  for (int i = 0; i < ABSL_ARRAYSIZE(ParseDoubleFail_cases); ++i) {
    const char* test = ParseDoubleFail_cases[i];
    ParseAndExpectFail(test);
  }
}

TEST(JSONParserTest, ParseNumber) {
  JSONToJSON parser("5.734");
  ASSERT_TRUE(parser.Parse()) << parser.error();
  EXPECT_EQ("5.734", parser.output());
}

struct ParseNumberComplicated_case {
  const char* orig;
  const char* expected;
} ParseNumberComplicated_cases[] = {
    {"5", "5"},
    {"-5", "-5"},
    {"-5.734", "-5.734"},
    {"-5.734e2", "-5.734e2"},
    {"-5.734e+2", "-5.734e+2"},
    {"5.734e-2", "5.734e-2"},
    {"-5e2", "-5e2"},
    {"-5E2", "-5E2"},
    {"    -5e2", "-5e2"},
    {"-5e2    ", "-5e2"},
    {"999E9999", "999E9999"},  // Out of range for double.
};

TEST(JSONParserTest, ParseNumberComplicated) {
  for (auto test : ParseNumberComplicated_cases) {
    JSONToJSON parser(test.orig);
    EXPECT_TRUE(parser.Parse()) << parser.error();
    EXPECT_EQ(test.expected, parser.output()) << "Input: " << test.orig;
  }
}

const char* ParseNumberFail_cases[] = {
    "-5e2abc", "-ab5e2",                        // Invalid characters.
    "-.003",   "-E30",   "-+",    "--",   "-",  // - not followed by digits.
    "00.0",    "01",     "-04",  // Leading 0 cannot be followed by digits.
    "1.e3",    "1.+",    "1.-",   "1..",  "1.",    // . not followed by digits.
    "1.5E",    "1.5E.",  "1.5eE",                  // E not followed by digits.
    "5E+",     "5E+.",   "5E+-",  "5E+e", "5E++",  // + not followed by digits.
    "5E-",     "5E-.",   "5E-+",  "5E-e", "5E--",  // - not followed by digits.
    ".2",      "e20",    "+100",  // Must start with - or digit.
};

TEST(JSONParserTest, ParseNumberFail) {
  for (auto test : ParseNumberFail_cases) {
    JSONToJSON parser(test);
    EXPECT_FALSE(parser.Parse())
        << "Input: " << test << " should fail the parsing but succeeded."
        << " Output: " << parser.output();
  }
}

TEST(JSONParserTest, ParseBool) {
  ParseAndExpectUnchanged("true");
  ParseAndExpectUnchanged("false");
}

TEST(JSONParserTest, ParseBoolFail) {
  ParseAndExpectFail("treu");   // NOTYPO
  ParseAndExpectFail("fasle");  // NOTYPO
  ParseAndExpectFail("tr");
  ParseAndExpectFail("fa");
}

TEST(JSONParserTest, ParseNull) { ParseAndExpectUnchanged("null"); }

TEST(JSONParserTest, ParseNullFail) { ParseAndExpectFail("nul"); }

TEST(JSONParserTest, ParseObject) {
  const char* str;
  const char* exp;
  str =
      "{\n"
      "  \"a\" : true,\n"
      "  \"b\" : false,\n"
      "}";
  exp =
      "{\n"
      "  \"a\" : true,\n"
      "  \"b\" : false\n"
      "}";
  ParseAndCompare(str, exp);
  str =
      "{\n"
      "  \"a\" : true,\n"
      "  \"b\" : false\n"
      "}";
  ParseAndExpectUnchanged(str);
}

TEST(JSONParserTest, ParseObjectFail) {
  const char* str =
      "{\n"
      "  \"a\"";  // no colon
  ParseAndExpectFail(str);
  str =
      "{\n"
      "  \"a\" : true\n"  // no comma
      "  \"b\" : false,\n"
      "}";
  ParseAndExpectFail(str);
  str =
      "{\n"
      "  \"a\" : true,\n"
      "  \"b\" : false\n"
      "]";  // ] instead of }
  ParseAndExpectFail(str);
  // Array in object brackets.
  str =
      "{\n"
      "  \"a\",\n"
      "  \"b\"\n"
      "}";
  ParseAndExpectFail(str);
  // Repeated commas
  str =
      "{\n"
      "  \"a\" : true,,\n"
      "  \"b\" : false\n"
      "}";
  ParseAndExpectFail(str);

    // Repeated commas
  str =
      "{\n"
      "  \"a\" : true,\n"
      "  \"b\" : false"
      "";  // missing final token.
  ParseAndExpectFail(str);
}

TEST(JSONParserTest, ParseArraySimple) {
  const char* str;
  str =
      "[\n"
      "  true,\n"
      "  false\n"
      "]";
  ParseAndExpectUnchanged(str);
}

TEST(JSONParserTest, ParseArrayTrailingComma) {
  std::unique_ptr<JSONToJSON> parser;
  const char *str, *exp;
  str =
      "[\n"
      "  true,\n"
      "  false,\n"
      "]";
  exp =
      "[\n"
      "  true,\n"
      "  false\n"
      "]";
  parser = std::make_unique<JSONToJSON>(str);
  ASSERT_TRUE(parser->Parse());
  EXPECT_EQ(exp, parser->output());
}

TEST(JSONParserTest, ParseArrayRepeatedComma) {
  std::unique_ptr<JSONToJSON> parser;
  const char *str, *exp;
  str = "[true,,false]";
  exp =
      "[\n"
      "  true,\n"
      "  null,\n"
      "  false\n"
      "]";
  parser = std::make_unique<JSONToJSON>(str);
  ASSERT_TRUE(parser->Parse());
  EXPECT_EQ(exp, parser->output());
}

TEST(JSONParserTest, ParseArrayLeadingCommas) {
  std::unique_ptr<JSONToJSON> parser;
  const char *str, *exp;
  str = "[,,null]";
  exp =
      "[\n"
      "  null,\n"
      "  null,\n"
      "  null\n"
      "]";
  parser = std::make_unique<JSONToJSON>(str);
  ASSERT_TRUE(parser->Parse());
  EXPECT_EQ(exp, parser->output());
}

TEST(JSONParserTest, ParseArrayFail) {
  const char* str;
  str =
      "[\n"
      "  true\n"  // no comma
      "  false,\n"
      "]";
  ParseAndExpectFail(str);

  str =
      "[\n"
      "  true,\n"
      "  false\n"
      "}";  // } instead of ]
  ParseAndExpectFail(str);

  // Object in array brackets.
  str =
      "[\n"
      "  \"a\" : true,\n"
      "  \"b\" : false\n"
      "]";
  ParseAndExpectFail(str);
}

TEST(JSONParserTest, UTF8) {
  const char* str = "\"\xE6\x95\x8F\xE6\x84\x9F\"";
  const char* exp = "\"\\346\\225\\217\\346\\204\\237\"";
  ParseAndCompare(str, exp);
}

TEST(JSONParserTest, ParseComplicated1) {
  const char* str =
      "{\n"
      "  \"glossary\" : {\n"
      "    \"title\" : \"example glossary\",\n"
      "    \"GlossDiv\" : {\n"
      "      \"title\" : \"S\",\n"
      "      \"GlossList\" : [\n"
      "        {\n"
      "          \"ID\" : \"SGML\",\n"
      "          \"SortAs\" : \"SGML\",\n"
      "          \"GlossTerm\" : \"Standard Generalized Markup Language\",\n"
      "          \"Acronym\" : \"SGML\",\n"
      "          \"Abbrev\" : \"ISO 8879:1986\",\n"
      "          \"GlossDef\" : \"A meta-markup language.\",\n"
      "          \"GlossSeeAlso\" : [\n"
      "            \"GML\",\n"
      "            \"XML\",\n"
      "            \"markup\"\n"
      "          ]\n"
      "        }\n"
      "      ]\n"
      "    }\n"
      "  }\n"
      "}";
  ParseAndExpectUnchanged(str);
}

TEST(JSONParserTest, ParseComplicated2) {
  const char* str =
      "{\n"
      "  \"a\" : [\n"
      "    [\n"
      "      5.200000,\n"
      "      false,\n"
      "      null\n"
      "    ],\n"
      "    [\n"
      "    ]\n"
      "  ],\n"
      "  \"b\" : {\n"
      "  },\n"
      "  \"c\" : false\n"
      "}";
  ParseAndExpectUnchanged(str);
}

// --- Optional extensions ---

// Test non-string keys in objects

TEST(JSONParserTest, KeyValueSimple) {
  const char* str =
      "{\n"
      "  str : \"foo\",\n"
      "  num : 5.000000,\n"
      "  bool : true,\n"
      "  bool : false,\n"
      "  obj : null\n,"
      "  5 : 2\n"
      "}";
  // Verify failure in non-extended mode
  JSONParser parser(str);
  EXPECT_FALSE(parser.Parse()) << "Input: " << str;
}

TEST(JSONParserTest, KeyValueComplicated) {
  const char* str =
      "{\n"
      "  f13s : 1.000000,\n"
      "  f__ : 2.000000,\n"
      "  $f_d : 3.000000\n"
      "}";
  // Verify failure in non-extended mode
  JSONParser parser(str);
  EXPECT_FALSE(parser.Parse()) << "Input: " << str;
}

TEST(JSONParserTest, KeyValueFail) {
  // Bad keys
  {
    const char* str = "{1f : 1}";
    JSONParser parser(str);
    EXPECT_FALSE(parser.Parse()) << "Input: " << str;
  }
  {
    const char* str = "{*f : 2}";
    JSONParser parser(str);
    EXPECT_FALSE(parser.Parse()) << "Input: " << str;
  }
}

// Functions

const char* FunctionSimple_cases[] = {
    "function ( ) { }",
    "function (arg) { }",                   // 1 arg
    "function (arg1, _arg$3, foo5__) { }",  // 3 args
    "function ( ) {return \"{\"}",          // Open brace in string
    "function ( ) {return \"}\"}",          // Close brace in string
    "function ( ) {{{}{{}}{}}}",            // Matching braces
};

TEST(JSONParserTest, FunctionSimple) {
  for (int i = 0; i < ABSL_ARRAYSIZE(FunctionSimple_cases); ++i) {
    // Verify functions not supported.
    JSONParser parser(FunctionSimple_cases[i]);
    EXPECT_FALSE(parser.Parse()) << "Input: " << FunctionSimple_cases[i];
  }
}

}  // namespace
