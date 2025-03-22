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

#include "zetasql/public/json_value.h"

#define JSON_NOEXCEPTION
#define JSON_THROW_USER(exception) ABSL_LOG(FATAL) << (exception).what();

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <queue>
#include <stack>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/numeric_parser.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "single_include/nlohmann/json.hpp"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

using JSON = ::nlohmann::json;
using ::absl::StatusOr;
using WideNumberMode = JSONParsingOptions::WideNumberMode;

namespace {

// A helper class that is used by the two parser implementations,
// JSONValueLegacyParser and JSONValueStandardParser to construct a JSON
// document tree from a given JSON string.
class JSONValueBuilder {
 public:
  // Constructs a builder that adds content to the given 'value'. If
  // 'max_nesting' has a value, then the parser will return an error when the
  // JSON document exceeds the max level of nesting. If 'max_nesting' is
  // negative, 0 will be set instead.
  explicit JSONValueBuilder(JSON& value, std::optional<int> max_nesting)
      : value_(value), max_nesting_(max_nesting) {
    if (max_nesting_.has_value() && *max_nesting_ < 0) {
      max_nesting_ = 0;
    }
  }

  // Resets the builder with a new 'value' to construct.
  void Reset(JSON& value) {
    value_ = value;
    ref_stack_.clear();
    object_member_ = nullptr;
  }

  absl::Status BeginObject() {
    if (max_nesting_.has_value() && ref_stack_.size() >= *max_nesting_) {
      return absl::OutOfRangeError(
          absl::StrCat("Max nesting of ", *max_nesting_,
                       " has been exceeded while parsing JSON document"));
    }
    auto result = HandleValue(JSON::value_t::object);
    ZETASQL_ASSIGN_OR_RETURN(ref_stack_.emplace_back(), result);
    return absl::OkStatus();
  }

  absl::Status EndObject() {
    ref_stack_.pop_back();
    ABSL_DCHECK(GetSkippingNodeMarker()->is_null());
    return absl::OkStatus();
  }

  absl::Status BeginMember(absl::string_view key) {
    // Skipping mode
    if (ref_stack_.back() == GetSkippingNodeMarker()) {
      return absl::OkStatus();
    }

    // Insert JSON null at the `key` spot, if an element with such `key` doesn't
    // exist already.
    auto [it, inserted] =
        ref_stack_.back()->emplace(key, nlohmann::detail::value_t::null);

    // If object already contains key, enter skipping mode
    if (!inserted) {
      object_member_ = GetSkippingNodeMarker();
      return absl::OkStatus();
    }
    // Store the freshly added null reference for later
    object_member_ = &(*it);
    return absl::OkStatus();
  }

  absl::Status BeginArray() {
    if (max_nesting_.has_value() && ref_stack_.size() >= *max_nesting_) {
      return absl::OutOfRangeError(
          absl::StrCat("Max nesting of ", *max_nesting_,
                       " has been exceeded while parsing JSON document"));
    }
    auto result = HandleValue(JSON::value_t::array);
    ZETASQL_ASSIGN_OR_RETURN(ref_stack_.emplace_back(), result);
    return absl::OkStatus();
  }

  absl::Status EndArray() {
    ref_stack_.pop_back();
    return absl::OkStatus();
  }

  absl::Status ParsedString(absl::string_view str) {
    return HandleValue(str).status();
  }

  absl::Status ParsedNumber(absl::string_view str) {
    // To match the nlohmann json library behavior, first try to parse 'str' as
    // unsigned int and only fallback to int if the value is signed integer.
    // This is to make sure that is_number_unsigned() and is_number_integer()
    // both return true for unsigned integers.
    uint64_t uint64_value;
    if (absl::SimpleAtoi(str, &uint64_value)) {
      return HandleValue(uint64_value).status();
    }
    int64_t int64_value;
    if (absl::SimpleAtoi(str, &int64_value)) {
      return HandleValue(int64_value).status();
    }
    double double_value;
    if (absl::SimpleAtod(str, &double_value)) {
      return HandleValue(double_value).status();
    }
    return absl::InternalError(
        absl::Substitute("Attempting to parse invalid JSON number $0", str));
  }

  absl::Status ParsedInt(int64_t val) { return HandleValue(val).status(); }
  absl::Status ParsedUInt(uint64_t val) { return HandleValue(val).status(); }
  absl::Status ParsedDouble(double val) { return HandleValue(val).status(); }
  absl::Status ParsedBool(bool val) { return HandleValue(val).status(); }
  absl::Status ParsedNull() { return HandleValue(nullptr).status(); }

 private:
  // Adds the given value into the currently constructed document tree.
  // Returns non-ok status in case of failure. Otherwise, returns the pointer
  // to the added value.
  template <typename Value>
  absl::StatusOr<JSON*> HandleValue(Value&& v) {
    if (ref_stack_.empty()) {
      value_ = JSON(std::forward<Value>(v));
      return &value_;
    }

    // If subtree being processed is to be skipped, return early.
    if (ref_stack_.back() == GetSkippingNodeMarker()) {
      return GetSkippingNodeMarker();
    }

    if (!ref_stack_.back()->is_array() && !ref_stack_.back()->is_object()) {
      return absl::InternalError(
          "Encountered invalid state while parsing JSON.");
    }

    if (ref_stack_.back()->is_array()) {
      ref_stack_.back()->emplace_back(std::forward<Value>(v));
      return &(ref_stack_.back()->back());
    }
    ABSL_CHECK(object_member_);
    // If here, the subtree should not be skipped, and the subtree is an object.
    // Since the subtree is an object, this code can only be reached if the key
    // associated with the value v has already been seen. If the key is a
    // duplicate, object_member_ will be set to skipping_mode_marker_ to
    // indicate it should be skipped.
    if (object_member_ != GetSkippingNodeMarker()) {
      *object_member_ = JSON(std::forward<Value>(v));
    }
    return object_member_;
  }

  // Marker used to indicate that the current JSON subtree being parsed should
  // be skipped. Set when a duplicate key is found for a given object.
  static JSON* GetSkippingNodeMarker() {
    static JSON* const skipping_mode_marker = new JSON();
    return skipping_mode_marker;
  }

  // The parsed JSON value.
  JSON& value_;
  // Max nesting allowed.
  std::optional<int> max_nesting_;
  // Stack to model hierarchy of values.
  std::vector<JSON*> ref_stack_;
  // Helper to hold the reference for the next object element.
  JSON* object_member_ = nullptr;
};

// The base class for JSONValue parsers that provides status tracking.
class JSONValueParserBase {
 public:
  JSONValueParserBase() = default;

  // The current status of the parser.
  absl::Status status() const { return status_; }

  bool parse_error(std::size_t /*unused*/, absl::string_view /*unused*/,
                   const nlohmann::detail::exception& ex) {
    absl::string_view error = ex.what();
    // Strip the error code specific to the nlohmann JSON library.
    // See the list of error messages here:
    // https://json.nlohmann.me/home/exceptions/
    //
    // There are 2 types of errors:
    // [json.exception.parse_error.101] parse error at 2: unexpected end of
    // input; expected string literal
    //
    // and
    //
    // [json.exception.out_of_range.406] number overflow parsing '1e9999'
    // [json.exception.out_of_range.408] excessive array size:
    // 8658170730974374167
    //
    // Parse errors always indicate the index followed by ':'. We want to remove
    // this as it is indexed from the JSON input, not the SQL input.
    // However, out of range errors don't always have ':' and we don't want to
    // remove everything before ':'.
    if (ABSL_PREDICT_FALSE(!absl::StartsWith(error, "[json.exception."))) {
      return MaybeUpdateStatus(absl::OutOfRangeError(error));
    }
    if (ABSL_PREDICT_TRUE(
            absl::StartsWith(error, "[json.exception.parse_error"))) {
      std::pair<absl::string_view, absl::string_view> splits =
          absl::StrSplit(error, absl::MaxSplits(": ", 1));
      if (ABSL_PREDICT_TRUE(!splits.second.empty())) {
        return MaybeUpdateStatus(absl::OutOfRangeError(splits.second));
      }
    }

    std::pair<absl::string_view, absl::string_view> splits =
        absl::StrSplit(error, absl::MaxSplits("] ", 1));
    if (ABSL_PREDICT_TRUE(!splits.second.empty())) {
      error = splits.second;
    }

    return MaybeUpdateStatus(absl::OutOfRangeError(error));
  }

 protected:
  // If the given 'status' is not ok, updates the state of the parser to reflect
  // the error only if the parser is not in the error state already. Otherwise
  // does nothing. Returns true if the given 'status' was ok and false
  // otherwise.
  bool MaybeUpdateStatus(absl::Status status) {
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      status_.Update(status);
      return false;
    }
    return true;
  }

 private:
  // Holds any errors encountered by the parser.
  absl::Status status_;
};

// The parser implementation that uses nlohmann library implementation based on
// the JSON RFC.
//
// NOTE: Method names are specific requirement of nlohmann SAX parser interface.
class JSONValueStandardParser : public JSONValueParserBase {
 public:
  JSONValueStandardParser(JSON& value, WideNumberMode wide_number_mode,
                          std::optional<int> max_nesting)
      : value_builder_(value, max_nesting),
        wide_number_mode_(wide_number_mode) {}
  JSONValueStandardParser() = delete;

  bool null() { return MaybeUpdateStatus(value_builder_.ParsedNull()); }

  bool boolean(bool val) {
    return MaybeUpdateStatus(value_builder_.ParsedBool(val));
  }

  bool number_integer(std::int64_t val) {
    return MaybeUpdateStatus(value_builder_.ParsedInt(val));
  }

  bool number_unsigned(std::uint64_t val) {
    return MaybeUpdateStatus(value_builder_.ParsedUInt(val));
  }

  bool number_float(double val, absl::string_view input_str) {
    if (wide_number_mode_ == WideNumberMode::kExact) {
      auto status = CheckNumberRoundtrip(input_str, val);
      if (!status.ok()) {
        return MaybeUpdateStatus(status);
      }
    }
    return MaybeUpdateStatus(value_builder_.ParsedDouble(val));
  }

  bool string(std::string& val) {
    return MaybeUpdateStatus(value_builder_.ParsedString(val));
  }

  bool binary(std::vector<std::uint8_t>& val) {
    // TODO: Implement the binary value type.
    return MaybeUpdateStatus(absl::UnimplementedError(
        "Binary JSON subtypes have not been implemented"));
  }

  bool start_object(std::size_t /*unused*/) {
    return MaybeUpdateStatus(value_builder_.BeginObject());
  }

  bool key(std::string& val) {
    return MaybeUpdateStatus(value_builder_.BeginMember(val));
  }

  bool end_object() { return MaybeUpdateStatus(value_builder_.EndObject()); }

  bool start_array(std::size_t /*unused*/) {
    return MaybeUpdateStatus(value_builder_.BeginArray());
  }

  bool end_array() { return MaybeUpdateStatus(value_builder_.EndArray()); }

  bool is_errored() const { return !status().ok(); }

 private:
  JSONValueBuilder value_builder_;
  const WideNumberMode wide_number_mode_;
};

// The parser implementation that uses nlohmann library implementation based on
// the JSON RFC. This parser only checks some general properties and is used for
// validation.
//
// NOTE: Method names are specific requirement of nlohmann SAX parser interface.
class JSONValueStandardValidator : public JSONValueParserBase {
 public:
  JSONValueStandardValidator(bool strict_number_parsing,
                             std::optional<int> max_nesting)
      : strict_number_parsing_(strict_number_parsing),
        max_nesting_(max_nesting) {
    if (max_nesting_.has_value() && *max_nesting_ < 0) {
      max_nesting_ = 0;
    }
  }

  JSONValueStandardValidator() = delete;

  bool null() { return true; }
  bool boolean(bool val) { return true; }
  bool number_integer(std::int64_t val) { return true; }
  bool number_unsigned(std::uint64_t val) { return true; }
  bool number_float(double val, absl::string_view input_str) {
    if (strict_number_parsing_) {
      auto status = CheckNumberRoundtrip(input_str, val);
      if (!status.ok()) {
        return MaybeUpdateStatus(status);
      }
    }
    return true;
  }
  bool string(std::string& val) { return true; }
  bool binary(std::vector<std::uint8_t>& val) {
    // TODO: Implement the binary value type.
    return MaybeUpdateStatus(absl::UnimplementedError(
        "Binary JSON subtypes have not been implemented"));
  }
  bool start_object(std::size_t /*unused*/) {
    if (max_nesting_.has_value() && current_nesting_ >= *max_nesting_) {
      return MaybeUpdateStatus(absl::OutOfRangeError(
          absl::StrCat("Max nesting of ", *max_nesting_,
                       " has been exceeded while parsing JSON document")));
    }
    ++current_nesting_;
    return true;
  }

  bool key(std::string& val) { return true; }
  bool end_object() {
    --current_nesting_;
    return true;
  }

  bool start_array(std::size_t /*unused*/) {
    if (max_nesting_.has_value() && current_nesting_ >= *max_nesting_) {
      return MaybeUpdateStatus(absl::OutOfRangeError(
          absl::StrCat("Max nesting of ", *max_nesting_,
                       " has been exceeded while parsing JSON document")));
    }
    ++current_nesting_;
    return true;
  }

  bool end_array() {
    --current_nesting_;
    return true;
  }

  bool is_errored() const { return !status().ok(); }

 private:
  const bool strict_number_parsing_;
  std::optional<int> max_nesting_;
  int current_nesting_ = 0;
};

}  // namespace

absl::Status IsValidJSON(absl::string_view str,
                         const JSONParsingOptions& parsing_options) {
  JSONValueStandardValidator validator(
      parsing_options.wide_number_mode == WideNumberMode::kExact,
      parsing_options.max_nesting);
  JSON::sax_parse(str, &validator);
  return validator.status();
}

// NOTE: DO NOT CHANGE THIS STRUCT. The JSONValueRef code assumes that
// JSONValue::Impl* can be casted to nlohmann::JSON*.
struct JSONValue::Impl {
  JSON value;
};

StatusOr<JSONValue> JSONValue::ParseJSONString(
    absl::string_view str, JSONParsingOptions parsing_options) {
  JSONValue json;
  JSONValueStandardParser parser(json.impl_->value,
                                 parsing_options.wide_number_mode,
                                 parsing_options.max_nesting);
  JSON::sax_parse(str, &parser);
  ZETASQL_RETURN_IF_ERROR(parser.status());
  return json;
}

StatusOr<JSONValue> JSONValue::DeserializeFromProtoBytes(
    absl::string_view str, std::optional<int> max_nesting_level) {
  JSONValue json;
  JSONValueStandardParser parser(json.impl_->value, WideNumberMode::kRound,
                                 max_nesting_level);
  JSON::sax_parse(str, &parser, JSON::input_format_t::ubjson);
  ZETASQL_RETURN_IF_ERROR(parser.status());
  return json;
}

JSONValue JSONValue::CopyFrom(JSONValueConstRef value) {
  JSONValue copy;
  copy.impl_->value = value.impl_->value;
  return copy;
}

JSONValue JSONValue::MoveFrom(JSONValueRef value) {
  JSONValue moved_json;
  moved_json.impl_->value = std::move(value.impl_->value);
  return moved_json;
}

JSONValue::JSONValue() : impl_(std::make_unique<Impl>()) {}

JSONValue::JSONValue(int64_t value) : impl_(new Impl{value}) {}
JSONValue::JSONValue(uint64_t value) : impl_(new Impl{value}) {}
JSONValue::JSONValue(double value) : impl_(new Impl{value}) {}
JSONValue::JSONValue(bool value) : impl_(new Impl{value}) {}
JSONValue::JSONValue(std::string_view value) : impl_(new Impl{value}) {}

JSONValue::JSONValue(JSONValue&& value) : impl_(std::move(value.impl_)) {}

JSONValue::~JSONValue() = default;

JSONValue& JSONValue::operator=(JSONValue&& value) {
  impl_ = std::move(value.impl_);
  return *this;
}

JSONValueRef JSONValue::GetRef() { return JSONValueRef(impl_.get()); }

JSONValueConstRef JSONValue::GetConstRef() const {
  return JSONValueConstRef(impl_.get());
}

JSONValueConstRef::JSONValueConstRef(const JSONValue::Impl* value_pointer)
    : impl_(value_pointer) {}

bool JSONValueConstRef::IsBoolean() const { return impl_->value.is_boolean(); }

bool JSONValueConstRef::IsNumber() const { return impl_->value.is_number(); }

bool JSONValueConstRef::IsNull() const { return impl_->value.is_null(); }

bool JSONValueConstRef::IsString() const { return impl_->value.is_string(); }

bool JSONValueConstRef::IsObject() const { return impl_->value.is_object(); }

bool JSONValueConstRef::IsArray() const { return impl_->value.is_array(); }

bool JSONValueConstRef::IsInt64() const {
  // is_number_integer() returns true for both signed and unsigned values. We
  // need to make sure that the value fits int64_t if it is unsigned.
  return impl_->value.is_number_integer() &&
         (!impl_->value.is_number_unsigned() ||
          impl_->value.get<int64_t>() >= 0);
}

bool JSONValueConstRef::IsUInt64() const {
  return impl_->value.is_number_unsigned();
}

bool JSONValueConstRef::IsDouble() const {
  return impl_->value.is_number_float();
}

int64_t JSONValueConstRef::GetInt64() const {
  return impl_->value.get<int64_t>();
}

uint64_t JSONValueConstRef::GetUInt64() const {
  return impl_->value.get<uint64_t>();
}

double JSONValueConstRef::GetDouble() const {
  return impl_->value.get<double>();
}

std::string JSONValueConstRef::GetString() const {
  return impl_->value.get<std::string>();
}

const std::string& JSONValueConstRef::GetStringRef() const {
  return impl_->value.get_ref<const std::string&>();
}

bool JSONValueConstRef::GetBoolean() const { return impl_->value.get<bool>(); }

size_t JSONValueConstRef::GetObjectSize() const {
  if (ABSL_PREDICT_FALSE(!IsObject())) {
    ABSL_LOG(FATAL) << "JSON value is not an object";
  }
  return impl_->value.size();
}

bool JSONValueConstRef::HasMember(absl::string_view key) const {
  return impl_->value.find(key) != impl_->value.end();
}

JSONValueConstRef JSONValueConstRef::GetMember(absl::string_view key) const {
  return JSONValueConstRef(reinterpret_cast<const JSONValue::Impl*>(
      &impl_->value[std::string(key)]));
}

std::optional<JSONValueConstRef> JSONValueConstRef::GetMemberIfExists(
    absl::string_view key) const {
  auto iter = impl_->value.find(key);
  if (iter == impl_->value.end()) {
    return std::nullopt;
  }
  return JSONValueConstRef(
      reinterpret_cast<const JSONValue::Impl*>(&iter.value()));
}

std::vector<std::pair<absl::string_view, JSONValueConstRef>>
JSONValueConstRef::GetMembers() const {
  std::vector<std::pair<absl::string_view, JSONValueConstRef>> members;
  for (auto& member : impl_->value.items()) {
    members.push_back(
        {member.key(),
         JSONValueConstRef(
             reinterpret_cast<const JSONValue::Impl*>(&member.value()))});
  }
  return members;
}

size_t JSONValueConstRef::GetArraySize() const {
  if (ABSL_PREDICT_FALSE(!IsArray())) {
    ABSL_LOG(FATAL) << "JSON value is not an array";
  }
  return impl_->value.size();
}

JSONValueConstRef JSONValueConstRef::GetArrayElement(size_t index) const {
  return JSONValueConstRef(
      reinterpret_cast<const JSONValue::Impl*>(&impl_->value[index]));
}

std::vector<JSONValueConstRef> JSONValueConstRef::GetArrayElements() const {
  std::vector<JSONValueConstRef> elements;
  for (auto& element : impl_->value) {
    elements.emplace_back(
        JSONValueConstRef(reinterpret_cast<const JSONValue::Impl*>(&element)));
  }
  return elements;
}

std::string JSONValueConstRef::ToString() const { return impl_->value.dump(); }

std::string JSONValueConstRef::Format() const {
  return impl_->value.dump(/*indent=*/2);
}

void JSONValueConstRef::SerializeAndAppendToProtoBytes(
    std::string* output) const {
  JSON::to_ubjson(impl_->value, *output);
}

namespace {

uint64_t EstimateStringSpaceUsed(const std::string& str) {
  size_t size = str.capacity() + 1;
  // Small strings are allocated inline in typical string implementations.
  return size < sizeof(JSON::string_t) ? sizeof(JSON::string_t)
                                       : size + sizeof(JSON::string_t);
}

}  // namespace

uint64_t JSONValueConstRef::SpaceUsed() const {
  uint64_t space_used = sizeof(JSONValue);
  std::queue<const JSON*> nodes;
  nodes.push(&impl_->value);
  while (!nodes.empty()) {
    const JSON* node = nodes.front();
    nodes.pop();
    // All values except for objects, arrays, and strings are stored inline in
    // JSON::JSON_value. For objects, arrays, and string, the calculation
    // accounts for the data structure size as well.
    space_used += sizeof(JSON);
    if (node->is_object()) {
      space_used += sizeof(JSON::object_t);
      for (auto& el : node->items()) {
        space_used += EstimateStringSpaceUsed(el.key());
        // Estimate per-element memory usage of std::map using 4 pointers.
        space_used += 4 * sizeof(void*);
        nodes.push(&el.value());
      }
    } else if (node->is_array()) {
      space_used += sizeof(JSON::array_t);
      for (const JSON& element : *node) {
        nodes.push(&element);
      }
    } else if (node->is_string()) {
      space_used += EstimateStringSpaceUsed(node->get<std::string>());
    }
  }
  return space_used;
}

bool JSONValueConstRef::NestingLevelExceedsMax(int64_t max_nesting) const {
  // Align the max_nesting behavior with definition in `JSONParsingOptions`.
  if (max_nesting < 0) {
    max_nesting = 0;
  }
  if (!IsArray() && !IsObject()) {
    return false;
  }
  // For each element in the stack, it holds the [begin,end) iterators of
  // unproccessed JSON document.
  std::stack<std::pair<JSON::const_iterator, JSON::const_iterator>> stack;
  stack.emplace(impl_->value.cbegin(), impl_->value.cend());
  while (!stack.empty()) {
    if (stack.size() > max_nesting) {
      return true;
    }
    if (stack.top().first == stack.top().second) {
      stack.pop();
      continue;
    }
    JSON::const_iterator first_child = stack.top().first;
    // Advance the iterator to next unprocessed child JSON document.
    ++stack.top().first;
    // Push the first child's [begin,end) iff it's nonscalar json document.
    if (first_child->is_array() || first_child->is_object()) {
      stack.emplace(first_child->cbegin(), first_child->cend());
    }
  }
  return false;
}

// This equality operation uses nlohmann's implementation.
//
// In this implementation, integers and floating points can be equal by
// casting the integer into a floating point and comparing the numbers as
// floating points. Signed and unsigned integers can also be equal.
bool JSONValueConstRef::NormalizedEquals(JSONValueConstRef that) const {
  return impl_->value == that.impl_->value;
}

JSONValueRef::JSONValueRef(JSONValue::Impl* impl)
    : JSONValueConstRef(impl), impl_(impl) {}

JSONValueRef JSONValueRef::GetMember(absl::string_view key) {
  return JSONValueRef(
      reinterpret_cast<JSONValue::Impl*>(&impl_->value[std::string(key)]));
}

std::optional<JSONValueRef> JSONValueRef::GetMemberIfExists(
    absl::string_view key) {
  auto iter = impl_->value.find(key);
  if (iter == impl_->value.end()) {
    return std::nullopt;
  }
  return JSONValueRef(reinterpret_cast<JSONValue::Impl*>(&iter.value()));
}

std::vector<std::pair<absl::string_view, JSONValueRef>>
JSONValueRef::GetMembers() {
  std::vector<std::pair<absl::string_view, JSONValueRef>> members;
  for (auto& member : impl_->value.items()) {
    members.push_back(
        {member.key(),
         JSONValueRef(reinterpret_cast<JSONValue::Impl*>(&member.value()))});
  }
  return members;
}

absl::StatusOr<bool> JSONValueRef::RemoveMember(absl::string_view key) {
  if (ABSL_PREDICT_FALSE(!IsObject())) {
    return absl::InvalidArgumentError("JSON value is not an object");
  }
  return impl_->value.erase(std::string(key)) > 0;
}

JSONValueRef JSONValueRef::GetArrayElement(size_t index) {
  return JSONValueRef(reinterpret_cast<JSONValue::Impl*>(&impl_->value[index]));
}

std::vector<JSONValueRef> JSONValueRef::GetArrayElements() {
  std::vector<JSONValueRef> elements;
  for (auto& element : impl_->value) {
    elements.emplace_back(
        JSONValueRef(reinterpret_cast<JSONValue::Impl*>(&element)));
  }
  return elements;
}

absl::Status JSONValueRef::InsertArrayElement(JSONValue json_value,
                                              size_t index) {
  if (ABSL_PREDICT_FALSE(!IsArray())) {
    return absl::InvalidArgumentError("JSON value is not an array");
  }

  if (ABSL_PREDICT_FALSE(std::max(index, GetArraySize()) >=
                         kJSONMaxArraySize)) {
    return absl::OutOfRangeError(
        absl::StrCat("Exceeded maximum array size of ", kJSONMaxArraySize));
  }

  if (index > impl_->value.size()) {
    // This will expand the array and fills it with JSON nulls.
    impl_->value[index - 1];
  }

  // insert(const_iterator pos, basic_json&& val) is actually calling
  // insert(const_iterator pos, const basic_json& val), thus makes a copy...
  //
  // insert_iterator will forward the value to std::vector::insert which
  // accepts a rvalue and therefore avoids a copy.
  impl_->value.insert_iterator(std::next(impl_->value.begin(), index),
                               std::move(json_value.impl_->value));
  return absl::OkStatus();
}

absl::Status JSONValueRef::InsertArrayElements(
    std::vector<JSONValue> json_values, size_t index) {
  if (ABSL_PREDICT_FALSE(!IsArray())) {
    return absl::InvalidArgumentError("JSON value is not an array");
  }

  if (json_values.empty()) {
    if (index >= impl_->value.size()) {
      if (ABSL_PREDICT_FALSE(index >= kJSONMaxArraySize)) {
        return absl::OutOfRangeError(
            absl::StrCat("Exceeded maximum array size of ", kJSONMaxArraySize));
      }
      // This will expand the array and fills it with JSON nulls.
      impl_->value[index];
    }
    return absl::OkStatus();
  } else if (ABSL_PREDICT_FALSE(
                 json_values.size() >= kJSONMaxArraySize ||
                 std::max(index, GetArraySize()) >=
                     (kJSONMaxArraySize + 1 - json_values.size()))) {
    return absl::OutOfRangeError(
        absl::StrCat("Exceeded maximum array size of ", kJSONMaxArraySize));
  }

  if (index > impl_->value.size()) {
    // This will expand the array and fills it with JSON nulls.
    impl_->value[index - 1];
  }

  std::vector<JSON> raw_values;
  raw_values.reserve(json_values.size());
  for (JSONValue& value : json_values) {
    raw_values.push_back(std::move(value.impl_->value));
  }

  // insert_iterator will forward the move iterators to std::vector::insert
  // (range version) which will perform the insert without copy.
  impl_->value.insert_iterator(std::next(impl_->value.begin(), index),
                               std::make_move_iterator(raw_values.begin()),
                               std::make_move_iterator(raw_values.end()));
  return absl::OkStatus();
}

absl::Status JSONValueRef::AppendArrayElement(JSONValue json_value) {
  if (ABSL_PREDICT_FALSE(!IsArray())) {
    return absl::InvalidArgumentError("JSON value is not an array");
  }
  if (ABSL_PREDICT_FALSE(GetArraySize() >= kJSONMaxArraySize)) {
    return absl::OutOfRangeError(
        absl::StrCat("Exceeded maximum array size of ", kJSONMaxArraySize));
  }
  impl_->value.push_back(std::move(json_value.impl_->value));
  return absl::OkStatus();
}

absl::Status JSONValueRef::AppendArrayElements(
    std::vector<JSONValue> json_values) {
  if (ABSL_PREDICT_FALSE(!IsArray())) {
    return absl::InvalidArgumentError("JSON value is not an array");
  }
  if (json_values.empty()) {
    return absl::OkStatus();
  }
  return InsertArrayElements(std::move(json_values), impl_->value.size());
}

absl::StatusOr<bool> JSONValueRef::RemoveArrayElement(int64_t index) {
  if (ABSL_PREDICT_FALSE(!IsArray())) {
    return absl::InvalidArgumentError("JSON value is not an array");
  }

  if (index < 0 || index >= impl_->value.size()) {
    return false;
  }
  impl_->value.erase(index);
  return true;
}

absl::Status JSONValueRef::CleanupJsonObject(RemoveEmptyOptions options) {
  if (ABSL_PREDICT_FALSE(!IsObject())) {
    return absl::InvalidArgumentError("JSON value is not an object.");
  }

  auto* map = impl_->value.get_ptr<nlohmann::json::object_t*>();

  zetasql_base::AssociativeEraseIf(map, [&options](const auto& entry) {
    const JSON& val = entry.second;
    if (val.is_null()) {
      return true;
    }
    if (!val.empty()) {
      return false;
    }
    if (val.is_object()) {
      return options == RemoveEmptyOptions::kObject ||
             options == RemoveEmptyOptions::kObjectAndArray;
    }
    if (val.is_array()) {
      return options == RemoveEmptyOptions::kArray ||
             options == RemoveEmptyOptions::kObjectAndArray;
    }
    return false;
  });
  return absl::OkStatus();
}

absl::Status JSONValueRef::CleanupJsonArray(
    RemoveEmptyOptions remove_empty_options) {
  if (ABSL_PREDICT_FALSE(!IsArray())) {
    return absl::InvalidArgumentError("JSON value is not an array.");
  }

  impl_->value.erase(
      std::remove_if(
          impl_->value.begin(), impl_->value.end(),
          [&remove_empty_options](JSON json) {
            if (json.is_null()) {
              return true;
            }
            if (!json.empty()) {
              return false;
            }
            if (json.is_object()) {
              return remove_empty_options == RemoveEmptyOptions::kObject ||
                     remove_empty_options ==
                         RemoveEmptyOptions::kObjectAndArray;
            }
            if (json.is_array()) {
              return remove_empty_options == RemoveEmptyOptions::kArray ||
                     remove_empty_options ==
                         RemoveEmptyOptions::kObjectAndArray;
            }
            return false;
          }),
      impl_->value.end());
  return absl::OkStatus();
}

void JSONValueRef::SetNull() { impl_->value = nlohmann::detail::value_t::null; }

void JSONValueRef::SetInt64(int64_t value) { impl_->value = value; }

void JSONValueRef::SetUInt64(uint64_t value) { impl_->value = value; }

void JSONValueRef::SetDouble(double value) { impl_->value = value; }

void JSONValueRef::SetString(absl::string_view value) { impl_->value = value; }

void JSONValueRef::SetBoolean(bool value) { impl_->value = value; }

void JSONValueRef::Set(JSONValue json_value) {
  impl_->value = std::move(json_value.impl_->value);
}

void JSONValueRef::SetToEmptyObject() { impl_->value = JSON::object(); }

void JSONValueRef::SetToEmptyArray() { impl_->value = JSON::array(); }

absl::Status CheckNumberRoundtrip(absl::string_view lhs, double val) {
  constexpr uint32_t kMaxStringLength = 1500;
  // Reject round-trip if input string is too long
  if (lhs.length() > kMaxStringLength) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "Input number " << lhs << " is too long.";
  }

  // Serialize 'val' to its string representation.
  const std::string rhs = JSONValue(val).GetConstRef().ToString();
  // Simple check - if strings are equal, return early.
  if (rhs == lhs) {
    return absl::OkStatus();
  }

  // Else, parse each string into a fixed precision representation and compare
  // the resulting representations.
  constexpr uint32_t word_count =
      (kMaxStringLength /
       multiprecision_int_impl::IntTraits<64>::kMaxWholeDecimalDigits) +
      1;
  FixedPointRepresentation<word_count> lhs_number;
  FixedPointRepresentation<word_count> rhs_number;
  auto status = ParseJSONNumber(lhs, lhs_number);
  ZETASQL_RETURN_IF_ERROR(status);
  status = ParseJSONNumber(rhs, rhs_number);
  ZETASQL_RETURN_IF_ERROR(status);
  if (lhs_number.is_negative == rhs_number.is_negative &&
      lhs_number.output == rhs_number.output) {
    return absl::OkStatus();
  }
  return zetasql_base::OutOfRangeErrorBuilder()
         << "Input number: " << lhs
         << " cannot round-trip through string representation";
}

}  // namespace zetasql
