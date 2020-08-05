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

#ifndef ZETASQL_PUBLIC_JSON_VALUE_H_
#define ZETASQL_PUBLIC_JSON_VALUE_H_

#include <stddef.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>


#include <cstdint>  
#include "absl/strings/string_view.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

class JSONValueConstRef;
class JSONValueRef;

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

  // Parses a given JSON document string and returns a JSON value. If
  // 'legacy_mode' is set to true, the parsing will be done using the legacy
  // proto JSON parser. The legacy parser supports strings that are not
  // valid JSON documents according to JSON RFC (such as single quote strings).
  static zetasql_base::StatusOr<JSONValue> ParseJSONString(absl::string_view str,
                                                   bool legacy_mode = false);

  // Decodes a binary representation of a JSON value produced by
  // JSONValueConstRef::SerializeAndAppendToProtoBytes(). Returns an error if
  // 'str' is not a valid binary representation.
  static zetasql_base::StatusOr<JSONValue> DeserializeFromProtoBytes(
      absl::string_view str);

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
  // Requires IsInt64() to be true. Otherwise, the call results in LOG(FATAL).
  int64_t GetInt64() const;
  // Returns a JSON number value as uint64_t.
  // Requires IsUInt64() to be true. Otherwise, the call results in LOG(FATAL).
  uint64_t GetUInt64() const;
  // Returns a JSON number value as double.
  // Requires IsDouble() to be true. Otherwise, the call results in LOG(FATAL).
  double GetDouble() const;
  // Returns a JSON string value.
  // Requires IsString() to be true. Otherwise, the call results in LOG(FATAL).
  std::string GetString() const;
  // Returns a JSON boolean value.
  // Requires IsBoolean() to be true. Otherwise, the call results in LOG(FATAL).
  bool GetBoolean() const;

  // If the JSON value being referenced is an object, returns whether the 'key'
  // references an existing member. If the JSON value is not an object, returns
  // false.
  bool HasMember(absl::string_view key) const;
  // If the JSON value being referenced is an object, returns the member
  // corresponding to the given 'key'. If such 'key' does not exist, then
  // the call results in LOG(FATAL).
  //
  // Requires IsObject() to be true. Otherwise, the call results in LOG(FATAL).
  JSONValueConstRef GetMember(absl::string_view key) const;
  // If the JSON value being referenced is an object, returns the member
  // corresponding to the given 'key' if it exists. If such 'key' does not
  // exist or if the JSON value is not an object, then returns absl::nullopt.
  absl::optional<JSONValueConstRef> GetMemberIfExists(
      absl::string_view key) const;
  // If the JSON value being referenced is an object, returns all the key/value
  // pairs corresponding to members of the object.
  //
  // Requires IsObject() to be true. Otherwise, the call results in LOG(FATAL).
  std::vector<std::pair<absl::string_view, JSONValueConstRef>> GetMembers()
      const;

  // If the JSON value being referenced is an array, returns the number of
  // elements.
  //
  // Requires IsArray() to be true. Otherwise, the call results in LOG(FATAL).
  size_t GetArraySize() const;
  // If the JSON value being referenced is an array, returns the element at
  // 'index'. If such element does not exists (index >= GetArraySize()), then
  // the returned value is an invalid object.
  //
  // Requires IsArray() to be true. Otherwise, the call results in LOG(FATAL).
  JSONValueConstRef GetArrayElement(size_t index) const;
  // If the JSON value being referenced is an array, returns all the elements.
  //
  // Requires IsArray() to be true. Otherwise, the call results in LOG(FATAL).
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

  // If the JSON value being referenced is an object, returns the member
  // corresponding to the given 'key'. If such 'key' does not exists, creates
  // a null value corresponding the 'key' and returns it.
  //
  // Requires IsObject() or IsNull() to be true. Otherwise, the call results in
  // LOG(FATAL).
  JSONValueRef GetMember(absl::string_view key);
  // If the JSON value being referenced is an object, returns all the key/value
  // pairs corresponding to members of the object.
  //
  // Requires IsObject() or IsNull() to be true. Otherwise, the call results in
  // LOG(FATAL).
  std::vector<std::pair<absl::string_view, JSONValueRef>> GetMembers();

  // If the JSON value being referenced is an array, returns the element at
  // 'index'. If the element does not exist, resizes the array with null
  // elements and returns a reference to the newly created null element.
  //
  // Requires IsArray() or IsNull() to be true. Otherwise, the call results in
  // LOG(FATAL).
  JSONValueRef GetArrayElement(size_t index);
  // If the JSON value being referenced is an array, returns all the elements.
  //
  // Requires IsArray() or IsNull() to be true. Otherwise, the call results in
  // LOG(FATAL).
  std::vector<JSONValueRef> GetArrayElements();

 private:
  explicit JSONValueRef(JSONValue::Impl* impl);

  JSONValue::Impl* impl_;

  friend class JSONValue;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_JSON_VALUE_H_
