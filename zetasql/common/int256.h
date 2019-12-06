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

#ifndef ZETASQL_COMMON_INT256_H_
#define ZETASQL_COMMON_INT256_H_

#include <ostream>

namespace zetasql {

// A class for supporting signed 256-bit integers.
// This class does not support logical operators or multiplication/division/
// modulo operators (can be added later if needed).
class int256 {
 public:
  constexpr int256() {}
  constexpr int256(__int128 hi, unsigned __int128 lo) : hi_(hi), lo_(lo) {}
  template <typename T>
  constexpr int256(T src) : hi_(-(src < 0)), lo_(src) {  // NOLINT
    static_assert(sizeof(T) <= sizeof(__int128));
  }

  // Arithmetic operators.
  int256& operator+=(const int256& b) {
    uint128 lo = lo_ + b.lo_;
    hi_ += b.hi_ + (lo < lo_);
    lo_ = lo;
    return *this;
  }
  int256& operator-=(const int256& b) {
    hi_ -= b.hi_ + (b.lo_ > lo_);
    lo_ -= b.lo_;
    return *this;
  }
  int256 operator++(int) {
    int256 rv = *this;
    ++(*this);
    return rv;
  }
  int256 operator--(int) {
    int256 rv = *this;
    --(*this);
    return rv;
  }
  int256& operator++() {
    hi_ += (++lo_ == 0);
    return *this;
  }
  int256& operator--() {
    hi_ -= (lo_-- == 0);
    return *this;
  }

  // This method takes fewer instructions than "*this < 0".
  // TODO: See if there is a way to optimize operator<.
  constexpr bool is_negative() const { return hi() < 0; }
  constexpr __int128 hi() const {
    static_assert(static_cast<__int128>(~uint128{0}) == -1);
    return static_cast<__int128>(hi_);
  }
  constexpr unsigned __int128 lo() const { return lo_; }

 private:
  friend constexpr int256 operator-(const int256& val);
  friend constexpr int256 operator+(const int256& lhs, const int256& rhs);
  friend constexpr int256 operator-(const int256& lhs, const int256& rhs);

  using uint128 = unsigned __int128;
  // Add an unused argument to avoid conflict with the public constructors.
  constexpr int256(uint128 hi, uint128 lo, int unused) : hi_(hi), lo_(lo) {}

  // Use uint128 to avoid undefined signed integer overflow in addition,
  // subtraction and negation.
  uint128 hi_ = 0;
  uint128 lo_ = 0;
};

inline constexpr int256 operator-(const int256& val) {
  return {~val.hi_ + (val.lo_ == 0), ~val.lo_ + 1, 0};
}

inline constexpr int256 operator+(const int256& lhs, const int256& rhs) {
  return {lhs.hi_ + rhs.hi_ + (lhs.lo_ + rhs.lo_ < lhs.lo_),
          lhs.lo_ + rhs.lo_, 0};
}

inline constexpr int256 operator-(const int256& lhs, const int256& rhs) {
  return {lhs.hi_ - rhs.hi_ - (rhs.lo_ > lhs.lo_), lhs.lo_ - rhs.lo_, 0};
}

inline constexpr bool operator==(const int256& lhs, const int256& rhs) {
  return lhs.lo() == rhs.lo() && lhs.hi() == rhs.hi();
}

inline constexpr bool operator!=(const int256& lhs, const int256& rhs) {
  return lhs.lo() != rhs.lo() || lhs.hi() != rhs.hi();
}

inline constexpr bool operator>(const int256& lhs, const int256& rhs) {
  return lhs.hi() == rhs.hi() ? lhs.lo() > rhs.lo() : lhs.hi() > rhs.hi();
}

inline constexpr bool operator<(const int256& lhs, const int256& rhs) {
  return lhs.hi() == rhs.hi() ? lhs.lo() < rhs.lo() : lhs.hi() < rhs.hi();
}

inline constexpr bool operator>=(const int256& lhs, const int256& rhs) {
  return lhs.hi() == rhs.hi() ? lhs.lo() >= rhs.lo() : lhs.hi() >= rhs.hi();
}

inline constexpr bool operator<=(const int256& lhs, const int256& rhs) {
  return lhs.hi() == rhs.hi() ? lhs.lo() <= rhs.lo() : lhs.hi() <= rhs.hi();
}

std::ostream& operator<<(std::ostream& o, const int256& b);
}  // namespace zetasql
#endif  // ZETASQL_COMMON_INT256_H_
