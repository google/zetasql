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

// See (broken link) for details on these functions.

#ifndef ZETASQL_PUBLIC_FUNCTIONS_JSON_H_
#define ZETASQL_PUBLIC_FUNCTIONS_JSON_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/string_numbers.h"  
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace functions {

namespace json_internal {

class ValidJSONPathIterator;

}  // namespace json_internal

// Utility class for Json extraction. Optimized for reuse of a constant
// json_path, whereas the above functions normalize json_path on every call.
class JsonPathEvaluator {
 public:
  ~JsonPathEvaluator();
  // Creates a new evaluator for a constant json_path. json_path does not need
  // to persist beyond the call to Create().
  //
  // sql_standard_mode: The JSON Path is interpreted differently with this
  // flag. For full details please see (broken link)
  // 1) True: Only number subscripts are allowed. It allows the . child
  // operator as a quoted field name. The previous example could be rewritten
  // as $.a."b"
  // 2) False: String subscripts are allowed. For example: $.a["b"] is a valid
  // JSON path in this mode and equivalent to $.a.b
  //
  // New callers to this API should be using sql_standard_mode = true.
  //
  // Error cases:
  // * OUT_OF_RANGE - json_path is malformed.
  // * OUT_OF_RANGE - json_path uses a (currently) unsupported expression type.
  static zetasql_base::StatusOr<std::unique_ptr<JsonPathEvaluator>> Create(
      absl::string_view json_path, bool sql_standard_mode);

  // Extracts a value from 'json' according to the JSONPath string 'json_path'
  // provided in Create().
  // For example:
  //   json: {"a": {"b": [ { "c" : "foo" } ] } }
  //   json_path: $
  //   value -> {"a":{"b":[{"c":"foo"}]}}
  //
  //   json: {"a": {"b": [ { "c" : "foo" } ] } }
  //   json_path: $.a.b[0]
  //   value -> {"c":"foo"}
  //
  //   json: {"a": {"b": ["c" : "foo"]  } }
  //   json_path: $.a.b[0].c
  //   value -> "foo" (i.e. quoted string)
  //
  //  Since JsonPathEvaluator is only created on valid JSON paths this
  //  will always return OK.
  //
  // Null cases:
  // * json is malformed and fails to parse.
  // * json_path does not match anything.
  // * json_path uses an array index but the value is not an array, or the path
  //   uses a name but the value is not an object.
  absl::Status Extract(absl::string_view json, std::string* value,
                       bool* is_null) const;

  // Similar to the above, but the 'json_path' provided in Create() must refer
  // to a scalar value in 'json'.
  // For example:
  //   json: {"a": {"b": ["c" : "foo"] } }
  //   json_path: $.a.b[0].c
  //   value -> foo (i.e. unquoted string)
  //
  //   json: {"a": 3.14159 }
  //   json_path: $.a
  //   value -> 3.14159 (i.e. unquoted string)
  //
  // Error cases are the same as in JsonExtract.
  // Null cases are the same as in JsonExtract, except for the addition of:
  // * json_path does not correspond to a scalar value in json.
  absl::Status ExtractScalar(absl::string_view json, std::string* value,
                             bool* is_null) const;

  // Extracts an array from 'json' according to the JSONPath string 'json_path'
  // provided in Create(). The value in 'json' that 'json_path' refers to should
  // be an JSON array. Then the output of the function will be in the form of an
  // ARRAY.
  // For example:
  //   json: ["foo","bar","baz"]
  //   json_path: $
  //   value -> ["\"foo\"", "\"bar\"", "\"baz\""] (ARRAY, quotes kept)
  //
  //   json: [1,2,3]
  //   value -> [1, 2, 3] (ARRAY, JSONPath is $ by default if not provided)
  //
  //   json: {"a":[{"b":"foo","c":1},{"b":"bar","c":2}],"d":"baz"}
  //   json_path: $.a
  //   value -> ["{\"b\":\"foo\",\"c\":1}","{\"b\":\"bar\",\"c\":2}"]
  //       (ARRAY, objects in the form of strings)
  //
  // Error cases are the same as in JsonExtract function.
  // Null cases are the same as in JsonExtract, except for the addition of:
  // * json_path does not correspond to an array in json.
  absl::Status ExtractArray(absl::string_view json,
                            std::vector<std::string>* value,
                            bool* is_null) const;

  // Enables the escaping of special characters for JSON_EXTRACT.
  //
  // Escaping special characters is part of the behavior detailed in the
  // ISO/IEC TR 19075-6 report on SQL support for JavaScript Object Notation.
  //
  // This implementation follows the proto3 JSON spec ((broken link),
  // (broken link)), where special characters include the
  // following:
  //    * Quotation mark, "
  //    * Reverse solidus, \
  //    * Control characters (U+0000 through U+001F).
  void enable_special_character_escaping() {
    escape_special_characters_ = true;
  }

  // Sets the callback to be invoked when a string with special characters was
  // returned for JSON_EXTRACT, but special character escaping was turned off.
  // No callback will be made if this is set to an empty target.
  void set_escaping_needed_callback(
      std::function<void(absl::string_view)> callback) {
    escaping_needed_callback_ = std::move(callback);
  }

 private:
  explicit JsonPathEvaluator(
      std::unique_ptr<json_internal::ValidJSONPathIterator> itr);
  const std::unique_ptr<json_internal::ValidJSONPathIterator> path_iterator_;
  bool escape_special_characters_ = false;
  std::function<void(absl::string_view)> escaping_needed_callback_;
};

}  // namespace functions
}  // namespace zetasql
#endif  // ZETASQL_PUBLIC_FUNCTIONS_JSON_H_
