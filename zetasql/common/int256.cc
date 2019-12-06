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

#include "zetasql/common/int256.h"

#include <iomanip>
#include <ostream>
#include <sstream>

#include <cstdint>

namespace zetasql {
std::ostream& operator<<(std::ostream& o, const int256& b) {
  std::ostringstream strstream;
  std::ios_base::fmtflags mask =
      std::ios::basefield | std::ios::showbase | std::ios::uppercase;
  strstream.setf(o.flags(), mask);

  int256 abs = b;
  if (b.is_negative()) {
    abs = -abs;
    strstream << '-';
  }
  unsigned __int128 hi = abs.hi();
  unsigned __int128 lo = abs.lo();

  uint64_t parts[5] = {0};
  int width = 0;
  constexpr uint64_t kUint64Mask = ~uint64_t{0};
  switch (o.flags() & std::ios::basefield) {
    case std::ios::hex:
      width = 16;  // 4 bits per character, 16 characters per part
      parts[0] = static_cast<uint64_t>(lo & kUint64Mask);
      parts[1] = static_cast<uint64_t>(lo >> 64);
      parts[2] = static_cast<uint64_t>(hi & kUint64Mask);
      parts[3] = static_cast<uint64_t>(hi >> 64);
      break;
    case std::ios::oct: {
      width = 21;  // 3 bits per character, 21 characters per part
      constexpr uint64_t mask = kUint64Mask >> 1;
      parts[0] = static_cast<uint64_t>(lo & mask);
      parts[1] = static_cast<uint64_t>((lo >> 63) & mask);
      parts[2] = static_cast<uint64_t>(((lo >> 126) | (hi << 2)) & mask);
      parts[3] = static_cast<uint64_t>((hi >> 61) & mask);
      parts[4] = static_cast<uint64_t>(hi >> 124);
    } break;
    default:  // case std::ios::dec:
      width = 19;
      {
        constexpr uint64_t base = 10000000000000000000ull;
        uint64_t hex[4];
        hex[0] = static_cast<uint64_t>(lo & kUint64Mask);
        hex[1] = static_cast<uint64_t>(lo >> 64);
        hex[2] = static_cast<uint64_t>(hi & kUint64Mask);
        hex[3] = static_cast<uint64_t>(hi >> 64);
        int k = 3;
        for (int j = 0; j < 5; ++j) {
          while (k >= 0 && hex[k] == 0) --k;
          if (k < 0) break;
          unsigned __int128 carry = 0;
          int i = k;
          do {
            carry <<= 64;
            carry |= hex[i];
            hex[i] = static_cast<uint64_t>(carry / base);
            carry = carry % base;
          } while (--i >= 0);
          parts[j] = carry;
        }
      }
  }
  int j = 4;
  while (j > 0 && parts[j] == 0) --j;
  for (; j > 0; --j) {
    strstream << parts[j] << std::noshowbase << std::setw(width)
              << std::setfill('0');
  }
  strstream << parts[0];
  return o << strstream.str();
}

}  // namespace zetasql
