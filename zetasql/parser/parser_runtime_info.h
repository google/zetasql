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

#ifndef ZETASQL_PARSER_PARSER_RUNTIME_INFO_H_
#define ZETASQL_PARSER_PARSER_RUNTIME_INFO_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "zetasql/common/timer_util.h"

namespace zetasql {

class ParserRuntimeInfo {
 public:
  absl::Duration parser_elapsed_duration() const {
    return parser_timed_value_.elapsed_duration();
  }
  internal::TimedValue& parser_timed_value() { return parser_timed_value_; }

  void AccumulateAll(const ParserRuntimeInfo& rhs) {
    parser_timed_value_.Accumulate(rhs.parser_timed_value_);
    num_lexical_tokens_ += rhs.num_lexical_tokens_;
  }

  void add_lexical_tokens(int64_t tokens) { num_lexical_tokens_ += tokens; }

  int64_t num_lexical_tokens() const { return num_lexical_tokens_; }

 private:
  internal::TimedValue parser_timed_value_;
  int64_t num_lexical_tokens_ = 0;
};
}  // namespace zetasql
#endif  // ZETASQL_PARSER_PARSER_RUNTIME_INFO_H_
