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

#include "zetasql/common/multiprecision_int_impl.h"

#include <cstdint>

namespace zetasql {
namespace multiprecision_int_impl {

inline int Print9Digits(uint32_t digits, bool skip_leading_zeros,
                        char output[9]) {
  for (int j = 8; j >= 0; --j) {
    output[j] = static_cast<char>(digits % 10) + '0';
    digits /= 10;
    if (skip_leading_zeros && digits == 0) {
      memmove(output, output + j, 9 - j);
      return 9 - j;
    }
  }
  return 9;
}

void AppendSegmentsToString(const uint32_t segments[], size_t num_segments,
                            std::string* result) {
  if (num_segments == 0) {
    result->push_back('0');
    return;
  }
  size_t old_size = result->size();
  size_t new_size = old_size + num_segments * 9;
  result->resize(new_size);
  char* output = result->data() + old_size;
  const uint32_t* segment = &segments[num_segments - 1];
  int num_digits_in_first_segment =
      Print9Digits(*segment, /* skip_leading_zeros */ true, output);
  output += num_digits_in_first_segment;
  while (segment != segments) {
    --segment;
    output += Print9Digits(*segment, /* skip_leading_zeros */ false, output);
  }
  result->resize(new_size - (9 - num_digits_in_first_segment));
}

}  // namespace multiprecision_int_impl
}  // namespace zetasql
