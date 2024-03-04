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

#include "zetasql/public/simple_token_list.h"

#include <cstddef>
#include <numeric>
#include <string>
#include <vector>

#include "zetasql/public/simple_token_list.pb.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

namespace zetasql::tokens {

TextToken TextToken::Make(std::string text) {
  TextToken text_token;
  text_token.text_ = text;
  return text_token;
}

std::string TextToken::DebugString() const {
  std::string out;
  absl::StrAppend(&out, "{");
  absl::StrAppendFormat(&out, "text: '%s'", absl::Utf8SafeCEscape(text()));
  absl::StrAppend(&out, "}");
  return out;
}

std::string TokenList::GetBytes() const {
  SimpleTokenListProto proto;
  proto.mutable_token()->Assign(data_.begin(), data_.end());
  std::string serialized;
  proto.SerializeToString(&serialized);
  return serialized;
}

TokenList TokenList::FromBytes(std::string serialized) {
  SimpleTokenListProto proto;
  if (proto.ParseFromString(serialized)) {
    std::vector<std::string> tokenlist(proto.token().begin(),
                                       proto.token().end());
    return TokenList(tokenlist);
  } else {
    return TokenList();
  }
}

bool TokenList::IsValid() const { return true; }

size_t TokenList::SpaceUsed() const {
  size_t allocated_string_size = std::accumulate(
      data_.begin(), data_.end(), 0,
      [](size_t size, const auto& str) -> size_t {
        size +=
            (str.capacity() > std::string().capacity() ? str.capacity() : 0);
        return size;
      });

  return sizeof(TokenList) + data_.capacity() * sizeof(std::string) +
         allocated_string_size;
}

}  // namespace zetasql::tokens
