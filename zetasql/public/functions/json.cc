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

#include "zetasql/public/functions/json.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "zetasql/common/errors.h"
#include "zetasql/public/functions/json_internal.h"
#include "zetasql/public/json_value.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace functions {
namespace {
using json_internal::JSONPathExtractor;
using json_internal::ValidJSONPathIterator;
}  // namespace

JsonPathEvaluator::~JsonPathEvaluator() {}

JsonPathEvaluator::JsonPathEvaluator(std::unique_ptr<ValidJSONPathIterator> itr)
    : path_iterator_(std::move(itr)) {}

// static
zetasql_base::StatusOr<std::unique_ptr<JsonPathEvaluator>> JsonPathEvaluator::Create(
    absl::string_view json_path, bool sql_standard_mode) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValidJSONPathIterator> itr,
                   ValidJSONPathIterator::Create(json_path, sql_standard_mode));
  // Scan tokens as json_path may not persist beyond this call.
  itr->Scan();
  return absl::WrapUnique(new JsonPathEvaluator(std::move(itr)));
}

absl::Status JsonPathEvaluator::Extract(absl::string_view json,
                                        std::string* value,
                                        bool* is_null) const {
  JSONPathExtractor parser(json, path_iterator_.get());
  parser.set_special_character_escaping(escape_special_characters_);
  parser.set_escaping_needed_callback(&escaping_needed_callback_);
  value->clear();
  parser.Extract(value, is_null);
  if (parser.StoppedDueToStackSpace()) {
    return MakeEvalError() << "JSON parsing failed due to deeply nested "
                              "array/struct. Maximum nesting depth is "
                           << JSONPathExtractor::kMaxParsingDepth;
  }
  return absl::OkStatus();
}

absl::optional<JSONValueConstRef> JsonPathEvaluator::Extract(
    JSONValueConstRef input) const {
  for (path_iterator_->Rewind(); !path_iterator_->End(); ++(*path_iterator_)) {
    const ValidJSONPathIterator::Token& token = *(*path_iterator_);

    if (token.empty()) {
      // The JSONPath "$.a[1].b" will result in the following list of tokens:
      // "", "a", "1", "b". The first token is always the empty token
      // corresponding to the whole JSON document, and we don't need to do
      // anything in that iteration. There shouldn't be any empty tokens after
      // the first one as ValidJSONPathIterator would reject those invalid
      // paths.
      continue;
    }

    if (input.IsObject()) {
      absl::optional<JSONValueConstRef> optional_member =
          input.GetMemberIfExists(token);
      if (!optional_member.has_value()) {
        return absl::nullopt;
      }
      input = optional_member.value();
    } else if (input.IsArray()) {
      int64_t index;
      if (!absl::SimpleAtoi(token, &index) || index < 0 ||
          index >= input.GetArraySize()) {
        return absl::nullopt;
      }
      input = input.GetArrayElement(index);
    } else {
      // The path is not present in the JSON object.
      return absl::nullopt;
    }
  }

  return input;
}

absl::Status JsonPathEvaluator::ExtractScalar(absl::string_view json,
                                              std::string* value,
                                              bool* is_null) const {
  json_internal::JSONPathExtractScalar scalar_parser(json,
                                                     path_iterator_.get());
  value->clear();
  scalar_parser.Extract(value, is_null);
  if (scalar_parser.StoppedDueToStackSpace()) {
    return MakeEvalError() << "JSON parsing failed due to deeply nested "
                              "array/struct. Maximum nesting depth is "
                           << JSONPathExtractor::kMaxParsingDepth;
  }
  return absl::OkStatus();
}

absl::optional<std::string> JsonPathEvaluator::ExtractScalar(
    JSONValueConstRef input) const {
  absl::optional<JSONValueConstRef> optional_json = Extract(input);
  if (!optional_json.has_value() || optional_json->IsNull() ||
      optional_json->IsObject() || optional_json->IsArray()) {
    return absl::nullopt;
  }

  if (optional_json->IsString()) {
    // ToString() adds extra quotes and escapes special characters,
    // which we don't want.
    return optional_json->GetString();
  }

  return optional_json->ToString();
}

absl::Status JsonPathEvaluator::ExtractArray(absl::string_view json,
                                             std::vector<std::string>* value,
                                             bool* is_null) const {
  json_internal::JSONPathArrayExtractor array_parser(json,
                                                     path_iterator_.get());
  array_parser.set_special_character_escaping(escape_special_characters_);
  value->clear();
  array_parser.ExtractArray(value, is_null);
  if (array_parser.StoppedDueToStackSpace()) {
    return MakeEvalError() << "JSON parsing failed due to deeply nested "
                              "array/struct. Maximum nesting depth is "
                           << JSONPathExtractor::kMaxParsingDepth;
  }
  return absl::OkStatus();
}

}  // namespace functions
}  // namespace zetasql
