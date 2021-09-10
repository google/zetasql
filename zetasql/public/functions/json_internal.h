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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_JSON_INTERNAL_H_
#define ZETASQL_PUBLIC_FUNCTIONS_JSON_INTERNAL_H_

#include <stdio.h>

#include <cstddef>
#include <memory>
#include <stack>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/json_parser.h"
#include "zetasql/common/json_util.h"
#include "zetasql/base/string_numbers.h"  
#include "absl/base/attributes.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "re2/re2.h"

namespace zetasql {
namespace functions {
namespace json_internal {

void RemoveBackSlashFollowedByChar(std::string* token, char esc_chr);

// Checks if the given JSON path is supported and valid.
absl::Status IsValidJSONPath(absl::string_view text, bool sql_standard_mode);

// Bi-directed iterator over tokens in a valid JSON path, used by the extraction
// algorithm. Functions in this class are inlined due to performance reasons.
class ValidJSONPathIterator {
 public:
  typedef std::string Token;

  static absl::StatusOr<std::unique_ptr<ValidJSONPathIterator>> Create(
      absl::string_view js_path, bool sql_standard_mode);

  // Rewind the iterator and reuse the tokens already parsed.
  inline void Rewind() {
    if (!tokens_.empty() && tokens_[0].empty()) {
      depth_ = 1;
      is_valid_ = true;
    }
  }

  inline size_t Depth() { return depth_; }

  inline void Scan() {
    for (; !End(); ++(*this)) {
    }
    if (text_ == "." && !sql_standard_mode_) {
      // It's allowed to have a trailing '.' in the path in non-standard mode.
      // It needs to be stripped. Otherwise, it will cause use-after-free issue
      // like b/125933506.
      text_.remove_suffix(1);
    }
    ZETASQL_DCHECK(text_.empty());
  }

  inline bool End() { return !is_valid_; }

  bool operator++();

  inline bool operator--() {
    if (depth_ > 0) {
      --depth_;
    }
    is_valid_ = (depth_ > 0 && depth_ <= tokens_.size());
    return is_valid_;
  }

  // Precondition: End() is false.
  inline const Token& operator*() const {
    ZETASQL_DCHECK(depth_ > 0 && depth_ <= tokens_.size());
    return tokens_[depth_ - 1];
  }

  inline bool NoSuffixToken() {
    return text_.empty() && depth_ == tokens_.size();
  }

 private:
  ValidJSONPathIterator(absl::string_view input, bool sql_standard_mode);

  void Init();

  bool sql_standard_mode_ = false;
  const RE2* offset_lexer_ = nullptr;   // NOT OWNED
  const RE2* esc_key_lexer_ = nullptr;  // NOT OWNED
  char esc_chr_ = 0;
  absl::string_view text_;
  bool is_valid_ = false;
  std::vector<Token> tokens_;
  size_t depth_ = 0;
};

//
// An efficient algorithm to extract the specified JSON path from the JSON text.
// Let $n$ be the number of nodes in the tree corresponding to the JSON text
// then the runtime of the algorithm is $\Theta(n)$ string comparisions in the
// worst case. The space complexity is the depth of the JSON path.
// The fundamental idea behind this algorithm is fully general to support all
// JSON path operators (@, *, .. etc.).
//
// Invariant maintaining functions are inlined for performance reasons.
//
class JSONPathExtractor : public zetasql::JSONParser {
 public:
  typedef ValidJSONPathIterator::Token Token;
  // Maximum recursion depth when parsing JSON. We check both for stack space
  // and for depth relative to this limit when parsing; in practice, the
  // expectation is that we should reach this limit prior to exhausting stack
  // space in the interest of avoiding crashes.
  static constexpr int kMaxParsingDepth = 1000;

  // `iter` and the object underlying `json` must outlive this object.
  JSONPathExtractor(absl::string_view json, ValidJSONPathIterator* iter)
      : zetasql::JSONParser(json),
        stack_(),
        curr_depth_(),
        matching_token_(),
        result_json_(),
        path_iterator_(*iter),
        escaping_needed_callback_(nullptr) {
    set_special_character_escaping(false);
    Init();
  }

  void set_special_character_escaping(bool escape_special_characters) {
    escape_special_characters_ = escape_special_characters;
  }

  // Sets the callback to be invoked when a string with special characters was
  // parsed, but special character escaping was turned off.  The caller is
  // responsible for ensuring that the lifetime of the callback object lasts as
  // long as any parsing calls that may invoke it.  No callback will be made if
  // this is set to nullptr or points to an empty target.
  void set_escaping_needed_callback(
      const std::function<void(absl::string_view)>* callback) {
    escaping_needed_callback_ = callback;
  }

  bool Extract(std::string* result, bool* is_null) {
    bool parse_success = zetasql::JSONParser::Parse() || stop_on_first_match_;

    // Parse-failed OR no-match-found OR null-Value
    *is_null = !parse_success || !stop_on_first_match_ || parsed_null_result_;
    if (parse_success) {
      result->assign(result_json_);
    }
    return parse_success;
  }

  bool StoppedOnFirstMatch() { return stop_on_first_match_; }

  // Returns whether parsing failed due to running out of stack space.
  bool StoppedDueToStackSpace() const { return stopped_due_to_stack_space_; }

 protected:
  bool BeginObject() override {
    if (!MaintainInvariantMovingDown()) {
      return false;
    }
    if (accept_) {
      absl::StrAppend(&result_json_, "{");
    }
    return true;
  }

  bool EndObject() override {
    if (accept_) {
      absl::StrAppend(&result_json_, "}");
    }
    MaintainInvariantMovingUp();
    return !stop_on_first_match_;
  }

  bool BeginArray() override {
    if (!MaintainInvariantMovingDown()) {
      return false;
    }

    // Stack Usage Invariant: !accept_ && match_
    if (!accept_ && extend_match_) {
      const std::string& token = *path_iterator_;
      // TODO: we can save more runtime by doing a sscanf in each
      // record. The tokens in the path iterator can store the index value
      // instead of doing sscanf in every record scan.
      has_index_token_ = (sscanf(token.c_str(), "%u", &index_token_) == 1);
      stack_.push(0);
    }

    // This is an array object so initialize the array index.
    if (accept_) {
      absl::StrAppend(&result_json_, "[");
      if (curr_depth_ == path_iterator_.Depth()) {
        array_accepted_ = true;
      }
    }
    return true;
  }

  bool EndArray() override {
    if (accept_) {
      absl::StrAppend(&result_json_, "]");
    }

    // Stack Usage Invariant: !accept_ && match_
    if (!accept_ && extend_match_) {
      stack_.pop();
    }
    MaintainInvariantMovingUp();
    return !stop_on_first_match_;
  }

  bool BeginMember(const std::string& key) override {
    if (accept_) {
      absl::StrAppend(&result_json_, "\"", key, "\":");
    } else if (extend_match_) {
      MatchAndMaintainInvariant(key, false);
    }
    return true;
  }

  bool BeginArrayEntry() override {
    // Stack Usage Invariant: !accept_ && match_
    if (!accept_ && extend_match_) {
      MatchAndMaintainInvariant("", true);
    }
    return true;
  }

  bool EndMember(bool last) override {
    if (accept_ && !last) {
      absl::StrAppend(&result_json_, ",");
    }
    return true;
  }

  bool EndArrayEntry(bool last) override {
    if (!accept_ && extend_match_) {
      stack_.top()++;
    }

    if (accept_ && !last) {
      absl::StrAppend(&result_json_, ",");
    }
    return true;
  }

  bool ParsedNumber(absl::string_view str) override {
    if (AcceptableLeaf()) {
      absl::StrAppend(&result_json_, str);
    }
    return !stop_on_first_match_;
  }

  bool ParsedString(const std::string& str) override {
    if (AcceptableLeaf()) {
      if (escape_special_characters_) {
        std::string s;
        JsonEscapeString(str, &s);
        absl::StrAppend(&result_json_, s);  // EscapeString adds quotes.
      } else {
        if (escaping_needed_callback_ != nullptr &&
            *escaping_needed_callback_ /* contains a callable target */ &&
            JsonStringNeedsEscaping(str)) {
          (*escaping_needed_callback_)(str);
        }
        absl::StrAppend(&result_json_, "\"", str, "\"");
      }
    }
    return !stop_on_first_match_;
  }

  bool ParsedBool(bool val) override {
    if (AcceptableLeaf()) {
      absl::StrAppend(&result_json_, zetasql_base::SimpleBtoa(val));
    }
    return !stop_on_first_match_;
  }

  bool ParsedNull() override {
    if (AcceptableLeaf()) {
      parsed_null_result_ = stop_on_first_match_;
      absl::StrAppend(&result_json_, "null");
    }
    return !stop_on_first_match_;
  }

  bool ReportFailure(const std::string& error_message) override {
    return JSONParser::ReportFailure(error_message);
  }

  void Init() {
    path_iterator_.Rewind();
    curr_depth_ = 1;
    matching_token_ = true;
    accept_ = false;
    accept_array_elements_ = false;
    stop_on_first_match_ = false;
    parsed_null_result_ = false;
  }

  // Returns false and sets stopped_due_to_stack_space_ if there is no more
  // stack space to continue parsing.
  ABSL_MUST_USE_RESULT inline bool MaintainInvariantMovingDown() {
    if (curr_depth_ >= kMaxParsingDepth + 1  //
    ) {                                      //
      stopped_due_to_stack_space_ = true;
      return false;
    }
    ++curr_depth_;
    extend_match_ = matching_token_;
    if (extend_match_) {
      matching_token_ = false;
      ++path_iterator_;
      accept_ = path_iterator_.End();
    }
    accept_array_elements_ = accept_ && (curr_depth_ == path_iterator_.Depth());
    return true;
  }

  inline void MaintainInvariantMovingUp() {
    if (extend_match_) {
      --path_iterator_;
      stop_on_first_match_ = accept_ && !path_iterator_.End();
      accept_ = path_iterator_.End();
    }
    --curr_depth_;
    accept_array_elements_ = accept_ && (curr_depth_ == path_iterator_.Depth());
    extend_match_ = (curr_depth_ == path_iterator_.Depth());
  }

  // This will be only called when extend_match_ is true.
  inline void MatchAndMaintainInvariant(const std::string& key,
                                        bool is_array_index) {
    matching_token_ = false;
    if (is_array_index) {
      // match array index token.
      matching_token_ = has_index_token_ && (index_token_ == stack_.top());
    } else {
      matching_token_ = (*path_iterator_ == key);
    }
  }

  // To accept the leaf its either in an acceptable sub-tree or a having
  // a parent with a matching token.
  bool AcceptableLeaf() {
    return accept_ || (stop_on_first_match_ =
                           (matching_token_ && path_iterator_.NoSuffixToken()));
  }

  // Stack will only be used to keep track of index of nested arrays.
  //
  std::stack<size_t> stack_;
  // Invariant: path_iterator_.Depth() <= curr_depth_
  size_t curr_depth_;
  // Invariant: if true the last compared token at this level has matched the
  // corresponding token in the path.
  bool matching_token_;
  std::string result_json_;
  ValidJSONPathIterator path_iterator_;
  // Invariant: if true the tokens till curr_depth_-1 are matched.
  bool extend_match_ = false;
  // Accept the entire sub-tree.
  bool accept_ = false;
  // Accept the element under the accepted array/object.
  bool accept_array_elements_ = false;
  // To report all matches remove this variable.
  bool stop_on_first_match_ = false;
  bool parsed_null_result_ = false;
  bool has_index_token_ = false;
  unsigned int index_token_;
  // Whether to escape special JSON characters (e.g. newlines).
  bool escape_special_characters_;
  // Callback to pass any strings that needed escaping when escaping special
  // characters is turned off.  No callback needed if set to nullptr.
  const std::function<void(absl::string_view)>* escaping_needed_callback_;
  // Whether parsing failed due to running out of stack space.
  bool stopped_due_to_stack_space_ = false;
  // Whether the JSONPath points to an array.
  bool array_accepted_ = false;
};

//
// Scalar version of JSONPathExtractor, stops parsing as soon we find an
// acceptable sub-tree which is a leaf.
//
class JSONPathExtractScalar final : public JSONPathExtractor {
 public:
  // `iter` and the object underlying `json` must outlive this object.
  JSONPathExtractScalar(absl::string_view json, ValidJSONPathIterator* iter)
      : JSONPathExtractor(json, iter) {}

  bool Extract(std::string* result, bool* is_null) {
    bool parse_success =
        zetasql::JSONParser::Parse() || accept_ || stop_on_first_match_;

    // Parse-failed  OR Subtree-Node OR null-Value OR no-match-found
    *is_null = !parse_success || accept_ || parsed_null_result_ ||
               !stop_on_first_match_;

    if (!(*is_null)) {
      result->assign(result_json_);
    }
    return parse_success;
  }

 protected:
  bool BeginObject() override {
    if (!JSONPathExtractor::BeginObject()) {
      return false;
    }
    return !accept_;
  }

  bool BeginArray() override {
    if (!JSONPathExtractor::BeginArray()) {
      return false;
    }
    return !accept_;
  }

  bool ParsedString(const std::string& str) override {
    if (AcceptableLeaf()) {
      absl::StrAppend(&result_json_, str);
    }
    return !stop_on_first_match_;
  }
};

// A JSONPath extractor that extracts array referred to by JSONPath. Similar to
// the scalar version of JSONPath extractor, it finds the first sub-tree
// matching the JSONPath. If it is not an array, returns null. Otherwise it
// will iterate over all the elements of the array and append them to
// 'result_array_' as strings and finally return the array.
class JSONPathArrayExtractor final : public JSONPathExtractor {
 public:
  // `iter` and the object underlying `json` must outlive this object.
  JSONPathArrayExtractor(absl::string_view json, ValidJSONPathIterator* iter)
      : JSONPathExtractor(json, iter) {}

  bool ExtractArray(std::vector<std::string>* result, bool* is_null) {
    bool parse_success = zetasql::JSONParser::Parse() || stop_on_first_match_;

    // Parse-failed OR no-match-found OR null-Value OR not-an-array
    *is_null = !parse_success || !stop_on_first_match_ || parsed_null_result_ ||
               !array_accepted_;
    if (parse_success) {
      result->assign(result_array_.begin(), result_array_.end());
    }
    return parse_success;
  }

 protected:
  bool BeginArrayEntry() override {
    // Stack Usage Invariant: !accept_ && match_
    if (!accept_ && extend_match_) {
      MatchAndMaintainInvariant("", true);
    } else if (accept_array_elements_) {
      result_json_.clear();
    }
    return true;
  }
  bool EndArrayEntry(bool last) override {
    if (!accept_ && extend_match_) {
      stack_.top()++;
    }
    if (accept_array_elements_) {
      result_array_.push_back(result_json_);
    } else if (accept_ && !last) {
      absl::StrAppend(&result_json_, ",");
    }
    return true;
  }
  std::vector<std::string> result_array_;
};

// A JSONPath extractor that extracts array referred to by JSONPath. Similar to
// the scalar version of JSONPath extractor, it finds the first sub-tree
// matching the JSONPath. If it is not an array, returns null. Otherwise it
// will iterate over all the elements of the array and append them to
// 'result_array_' as strings and finally return the array.
// An absl::nullopt value in 'result_array_' represents the SQL NULL value.
class JSONPathStringArrayExtractor final : public JSONPathExtractor {
 public:
  // `iter` and the object underlying `json` must outlive this object.
  JSONPathStringArrayExtractor(absl::string_view json,
                               ValidJSONPathIterator* iter)
      : JSONPathExtractor(json, iter) {}

  bool ExtractStringArray(std::vector<absl::optional<std::string>>* result,
                          bool* is_null) {
    bool parse_success = zetasql::JSONParser::Parse() || stop_on_first_match_;

    // Parse-failed OR no-match-found OR null-Value OR not-an-array
    *is_null = !parse_success || !stop_on_first_match_ || parsed_null_result_ ||
               !scalar_array_accepted_;
    if (!(*is_null)) {
      result->assign(result_array_.begin(), result_array_.end());
    }
    return parse_success;
  }

 protected:
  bool BeginObject() override {
    if (!JSONPathExtractor::BeginObject()) {
      return false;
    }
    scalar_array_accepted_ = false;
    return true;
  }

  bool BeginArray() override {
    if (!JSONPathExtractor::BeginArray()) {
      return false;
    }
    scalar_array_accepted_ = accept_array_elements_;
    return true;
  }

  bool BeginArrayEntry() override {
    // Stack Usage Invariant: !accept_ && match_
    if (!accept_ && extend_match_) {
      MatchAndMaintainInvariant("", true);
    } else if (accept_array_elements_) {
      result_json_.clear();
    }
    return true;
  }

  bool EndArrayEntry(bool last) override {
    if (!accept_ && extend_match_) {
      stack_.top()++;
    }
    if (accept_array_elements_) {
      if (result_json_.empty()) {
        // This means the array element is the JSON null.
        result_array_.push_back(absl::nullopt);
      } else {
        result_array_.push_back(result_json_);
      }
    } else if (accept_ && !last) {
      absl::StrAppend(&result_json_, ",");
    }
    return true;
  }

  bool ParsedString(const std::string& str) override {
    if (AcceptableLeaf()) {
      absl::StrAppend(&result_json_, str);
    }
    return !stop_on_first_match_;
  }

  bool ParsedNull() override {
    if (AcceptableLeaf()) {
      parsed_null_result_ = stop_on_first_match_;
    }
    return !stop_on_first_match_;
  }

  // Whether the JSONPath points to an array with scalar elements.
  bool scalar_array_accepted_ = false;
  std::vector<absl::optional<std::string>> result_array_;
};

}  // namespace json_internal
}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_JSON_INTERNAL_H_
