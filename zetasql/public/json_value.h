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

#ifndef ZETASQL_PUBLIC_JSON_VALUE_H_
#define ZETASQL_PUBLIC_JSON_VALUE_H_

#include <stddef.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>


#include <cstdint>  
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace zetasql {

class JSONValueConstRef;
class JSONValueRef;

// Options for parsing an input JSON-formatted string.
struct JSONParsingOptions {
  // If 'legacy_mode' is set to true, the parsing will be done using the legacy
  // proto JSON parser. The legacy parser supports strings that are not
  // valid JSON documents according to JSON RFC (such as single quote strings).
  bool legacy_mode;
  // If 'strict_number_parsing' is set to true, parsing will fail if there is at
  // least one number value in 'str' that does not round-trip from
  // string -> number -> string. 'strict_number_parsing' only affects non-legacy
  // parsing (i.e. 'legacy_mode' = true and 'strict_number_parsing' = true
  // returns an error).
  bool strict_number_parsing;
  // If 'max_nesting' is set to a non-negative number, parsing will fail if the
  // JSON document has more than 'max_nesting' levels of nesting. If it is set
  // to a negative number, the max nesting will be set to 0 instead (i.e. only
  // allowing scalar JSONs). JSON Arrays and Objects increase nesting levels.
  absl::optional<int> max_nesting;
};

// JSONValue stores a JSON document. Access to read and update the values and
// their members and elements is provided through JSONValueRef and
// JSONValueConstRef. JSONValue owns the document it represents.
class JSONValue final {
 public:
  // Constructs an empty JSON document.
  JSONValue();

  // Constructs JSON document containing the given value.
  explicit JSONValue(int64_t value);
  explicit JSONValue(uint64_t value);
  explicit JSONValue(double value);
  explicit JSONValue(bool value);
  explicit JSONValue(std::string value);

  JSONValue(JSONValue&& value);
  ~JSONValue();

  JSONValue(const JSONValue&) = delete;
  JSONValue& operator=(const JSONValue&) = delete;

  JSONValue& operator=(JSONValue&& value);

  // Returns a read/write reference to the JSON value. The reference can be used
  // to access or update the value including object members and array elements.
  JSONValueRef GetRef();
  // Returns a read-only reference to the JSON value. The reference can be used
  // to read the value including object members and array elements.
  JSONValueConstRef GetConstRef() const;

  // Parses a given JSON document string and returns a JSON value.
  static absl::StatusOr<JSONValue> ParseJSONString(
      absl::string_view str,
      JSONParsingOptions parsing_options = {.legacy_mode = false,
                                            .strict_number_parsing = false,
                                            .max_nesting = absl::nullopt});

  // Decodes a binary representation of a JSON value produced by
  // JSONValueConstRef::SerializeAndAppendToProtoBytes(). Returns an error if
  // 'str' is not a valid binary representation.
  // Returns an error if the max nesting level of the JSON value exceeds
  // 'max_nesting_level'. If 'max_nesting_level' < 0, 0 will be used instead.
  static absl::StatusOr<JSONValue> DeserializeFromProtoBytes(
      absl::string_view str,
      absl::optional<int> max_nesting_level = absl::nullopt);

  // Returns a JSON value that is a deep copy of the given value.
  static JSONValue CopyFrom(JSONValueConstRef value);

 private:
  struct Impl;

  std::unique_ptr<Impl> impl_;

  friend class JSONValueConstRef;
  friend class JSONValueRef;
};

// JSONValueConstRef is a read-only reference to a JSON document stored by
// JSONValue. The JSONValue instance referenced should outlive the
// JSONValueConstRef instance.
class JSONValueConstRef {
 public:
  JSONValueConstRef() = delete;
  JSONValueConstRef(const JSONValueConstRef& pointer) = default;
  JSONValueConstRef(JSONValueConstRef&& pointer) = default;

  JSONValueConstRef& operator=(const JSONValueConstRef&) = default;
  JSONValueConstRef& operator=(JSONValueConstRef&&) = default;

  bool IsNumber() const;
  bool IsString() const;
  bool IsBoolean() const;
  bool IsNull() const;
  bool IsObject() const;
  bool IsArray() const;

  bool IsInt64() const;
  bool IsUInt64() const;
  bool IsDouble() const;

  // Returns a JSON number value as int64_t.
  // Requires IsInt64() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  int64_t GetInt64() const;
  // Returns a JSON number value as uint64_t.
  // Requires IsUInt64() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  uint64_t GetUInt64() const;
  // Returns a JSON number value as double.
  // Requires IsDouble() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  double GetDouble() const;
  // Returns a JSON string value.
  // Requires IsString() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  std::string GetString() const;
  // Returns a JSON boolean value.
  // Requires IsBoolean() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  bool GetBoolean() const;

  // If the JSON value being referenced is an object, returns the number of
  // elements.
  //
  // Requires IsObject() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  size_t GetObjectSize() const;
  // If the JSON value being referenced is an object, returns whether the 'key'
  // references an existing member. If the JSON value is not an object, returns
  // false.
  bool HasMember(absl::string_view key) const;
  // If the JSON value being referenced is an object, returns the member
  // corresponding to the given 'key'. If such 'key' does not exist, then
  // the call results in ZETASQL_LOG(FATAL).
  //
  // Requires IsObject() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  JSONValueConstRef GetMember(absl::string_view key) const;
  // If the JSON value being referenced is an object, returns the member
  // corresponding to the given 'key' if it exists. If such 'key' does not
  // exist or if the JSON value is not an object, then returns absl::nullopt.
  absl::optional<JSONValueConstRef> GetMemberIfExists(
      absl::string_view key) const;
  // If the JSON value being referenced is an object, returns all the key/value
  // pairs corresponding to members of the object.
  //
  // Requires IsObject() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  std::vector<std::pair<absl::string_view, JSONValueConstRef>> GetMembers()
      const;

  // If the JSON value being referenced is an array, returns the number of
  // elements.
  //
  // Requires IsArray() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  size_t GetArraySize() const;
  // If the JSON value being referenced is an array, returns the element at
  // 'index'. If such element does not exists (index >= GetArraySize()), then
  // the returned value is an invalid object.
  //
  // Requires IsArray() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  JSONValueConstRef GetArrayElement(size_t index) const;
  // If the JSON value being referenced is an array, returns all the elements.
  //
  // Requires IsArray() to be true. Otherwise, the call results in ZETASQL_LOG(FATAL).
  std::vector<JSONValueConstRef> GetArrayElements() const;

  // Serializes the JSON value into a compact string representation.
  // Note: For a human-friendly representation, please use Format().
  std::string ToString() const;

  // Serializes the JSON value into a multiline, human-friendly formatted string
  // representation.
  std::string Format() const;

  // Encodes the JSON value into a binary representation using UBJSON format and
  // append to 'output'. The binary representation can be decoded using
  // JSONValue::DeserializeFromProtoBytes().
  void SerializeAndAppendToProtoBytes(std::string* output) const;

  // Returns the number of bytes used to store the JSON value.
  uint64_t SpaceUsed() const;

  // Returns true iff the nesting level of JSON value larger than `max_nesting`.
  // JSON Scalar value has level 0. Example JSON '{"a":1}', '[1,2]' has level 1.
  // Space complexity: O(min(depth(JSON), max_nesting)).
  bool NestingLevelExceedsMax(int64_t max_nesting) const;

  // Returns true if the JSON value referenced by 'this' equals the one
  // referenced by 'that'. This is only used in testing and is *not* the SQL
  // equality.
  bool NormalizedEquals(JSONValueConstRef that) const;

 protected:
  explicit JSONValueConstRef(const JSONValue::Impl* value_pointer);

 private:
  const JSONValue::Impl* impl_;

  friend class JSONValue;
};

// JSONValueRef is a read/write reference to a JSON document stored by
// JSONValue. The JSONValue instance referenced should outlive the
// JSONValueRef instance.
class JSONValueRef : public JSONValueConstRef {
 public:
  JSONValueRef() = delete;
  JSONValueRef(const JSONValueRef& pointer) = default;
  JSONValueRef(JSONValueRef&& pointer) = default;

  JSONValueRef& operator=(const JSONValueRef&) = default;
  JSONValueRef& operator=(JSONValueRef&&) = default;

  // Sets the JSON value to null.
  void SetNull();
  // Sets the JSON value to the given int64_t value.
  void SetInt64(int64_t value);
  // Sets the JSON value to the given uin64 value.
  void SetUInt64(uint64_t value);
  // Sets the JSON value to the given double value.
  void SetDouble(double value);
  // Sets the JSON value to the given string value.
  void SetString(absl::string_view value);
  // Sets the JSON value to the given boolean value.
  void SetBoolean(bool value);
  // Sets the JSON value to the given json value.
  void Set(JSONValue json_value);
  // Sets the JSON value to an empty object.
  void SetToEmptyObject();
  // Sets the JSON value to an empty array.
  void SetToEmptyArray();

  // If the JSON value being referenced is an object, returns the member
  // corresponding to the given 'key'. If such 'key' does not exists, creates
  // a null value corresponding the 'key' and returns it.
  //
  // Requires IsObject() or IsNull() to be true. Otherwise, the call results in
  // ZETASQL_LOG(FATAL).
  JSONValueRef GetMember(absl::string_view key);
  // If the JSON value being referenced is an object, returns all the key/value
  // pairs corresponding to members of the object.
  //
  // Requires IsObject() or IsNull() to be true. Otherwise, the call results in
  // ZETASQL_LOG(FATAL).
  std::vector<std::pair<absl::string_view, JSONValueRef>> GetMembers();

  // If the JSON value being referenced is an array, returns the element at
  // 'index'. If the element does not exist, resizes the array with null
  // elements and returns a reference to the newly created null element.
  //
  // Requires IsArray() or IsNull() to be true. Otherwise, the call results in
  // ZETASQL_LOG(FATAL).
  JSONValueRef GetArrayElement(size_t index);
  // If the JSON value being referenced is an array, returns all the elements.
  //
  // Requires IsArray() or IsNull() to be true. Otherwise, the call results in
  // ZETASQL_LOG(FATAL).
  std::vector<JSONValueRef> GetArrayElements();

 private:
  explicit JSONValueRef(JSONValue::Impl* impl);

  JSONValue::Impl* impl_;

  friend class JSONValue;
};

namespace internal {

// Returns absl::OkStatus() if the number in string 'lhs' is numerically
// equivalent to the number in the string created by serializing a JSON document
// containing 'val' to a string. Returns an error status otherwise.
//
// Restrictions:
//   - Input string ('lhs') can be at most 1500 characters in length.
//   - Numbers represented by 'lhs' and 'val' can have at most 1074 significant
//   fractional decimal digits, and at most 1500 significant digits.
//
// To understand the restrictions above, we must consider the following:
//
//  1) The 64-bit IEEE 754 double only guarantees roundtripping from text ->
//  double -> text for numbers that have 15 significant digits at most. Values
//  with more significant digits than 15 may or may not round-trip.
//
//  2) The conversion of double -> text -> double will produce the exact same
//  double value if at least 17 significant digits are used in serialization of
//  the double value to text (e.g. "10.000000000000001" -> double and
//  "10.0000000000000019" -> double return the same double value).
//
// The implication of the above two facts might suggest that numbers with more
// than 17 significant digits should be rejected for round-tripping. However,
// there are 2 categories of numbers that can exceed 17 significant digits but
// still round-trip. The first category consists of very large whole numbers
// with a continuous sequence of trailing zeros before the decimal point
// (e.g. 1e+200). The second category consists of very small purely fractional
// numbers with a continuous sequence of leading zeros after the decimal point
// (e.g. 1e-200).
//
// To ensure that the numbers in these additional categories are correctly
// considered for round-tripping, we allow for a maximum of 1074 significant
// fractional digits and a maximum total string length of 1500 characters.
//
// For ASCII decimal input strings, this allows for a maximum of 424 significant
// whole decimal digits, which exceeds the number of significant whole digits
// for the maximum value of double (~1.7976931348623157E+308), while still
// capturing the maximum number of fractional digits for a double (1074).
// Scientific notation input strings can potentially express more than 424
// significant decimal digits, while still being constrained to 1074 significant
// fractional decimal digits.
absl::Status CheckNumberRoundtrip(absl::string_view lhs, double val);

}  // namespace internal

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_JSON_VALUE_H_
