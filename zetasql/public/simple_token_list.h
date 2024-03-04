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

#ifndef ZETASQL_PUBLIC_SIMPLE_TOKEN_LIST_H_
#define ZETASQL_PUBLIC_SIMPLE_TOKEN_LIST_H_

#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql::tokens {

// TextToken stores token data for snippeting and scoring. TextTokens usually
// come in a list.
class TextToken {
 public:
  static TextToken Make(std::string text);

  TextToken() = default;

  void set_text(std::string text) { text_ = std::move(text); }
  absl::string_view text() const { return text_; }

  std::string DebugString() const;

  friend bool operator==(const TextToken& a, const TextToken& b) {
    return a.text_ == b.text_;
  }

  friend bool operator!=(const TextToken& a, const TextToken& b) {
    return !(a == b);
  }

  template <typename H>
  friend H AbslHashValue(H h, const TextToken& t) {
    return H::combine(std::move(h), t.text_);
  }

 private:
  std::string text_;
};

// A TokenList is an ordered list of tokens (used for snippeting, etc).
class TokenList {
 public:
  TokenList() = default;
  explicit TokenList(std::vector<std::string> data) : data_(std::move(data)) {}

  TokenList(TokenList&& other) = default;
  TokenList& operator=(TokenList&& other) = default;

  std::string GetBytes() const;
  // Constructs a TokenList from serialized bytes.
  static TokenList FromBytes(std::string serialized);

  // Allows iteration over this TokenList.
  class Iterator {
   public:
    // Returns true if there is no more data to be iterated over.
    bool done() const { return cur_ == data_.size(); }
    // Retrieves the next TextToken from the TokenList being iterated over.
    absl::Status Next(TextToken& token) {
      token.set_text(data_[cur_++]);
      return absl::OkStatus();
    }

   private:
    friend class TokenList;
    explicit Iterator(const std::vector<std::string>& data) : data_(data) {}

    const std::vector<std::string>& data_;
    size_t cur_ = 0;
  };

  absl::StatusOr<Iterator> GetIterator() const { return Iterator(data_); }

  // Logical equality comparison for TokenList objects.
  bool EquivalentTo(const TokenList& other) const {
    return data_ == other.data_;
  }

  // Hash method for the TokenList objects. Not meant to be efficient.
  template <typename H>
  friend H AbslHashValue(H h, const TokenList& token_list) {
    auto iter = token_list.GetIterator();
    if (!iter.ok()) {
      return H::combine(std::move(h), 0);
    }
    TextToken t;
    while (!iter->done()) {
      if (!iter->Next(t).ok()) {
        return H::combine(std::move(h), 0);
      }
      h = H::combine(std::move(h), t);
    }
    return h;
  }

  // Convenience: checks if the TokenList is well-formed, by attempting to
  // fully decode. An empty TokenList is considered valid.
  bool IsValid() const;

  // Estimated in-memory byte size.
  size_t SpaceUsed() const;

 private:
  std::vector<std::string> data_;
};

}  // namespace zetasql::tokens

#endif  // ZETASQL_PUBLIC_SIMPLE_TOKEN_LIST_H_
