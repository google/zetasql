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

// FixedUint and FixedInt are designed for multi- but fixed- precision integer
// arithmetics.

#ifndef ZETASQL_COMMON_FIXED_INT_H_
#define ZETASQL_COMMON_FIXED_INT_H_
#include <array>
#include <limits>
#include <string>

#include "zetasql/base/logging.h"
#include "absl/base/attributes.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/bits.h"
#include "zetasql/base/endian.h"

namespace zetasql {

namespace fixed_int_internal {

template <int num_bits>  // num_bits must be 32, 64, or 128
struct IntTraits;

// The version of <type_traits> used in zetasql does not support
// std::make_unsigned<__int128> or the std::make_signed counterpart,
// so we have to define both Int and Uint explicitly.
template <>
struct IntTraits<32> {
  using Int = int32_t;
  using Uint = uint32_t;
};

template <>
struct IntTraits<64> {
  using Int = int64_t;
  using Uint = uint64_t;
};

template <>
struct IntTraits<128> {
  using Int = __int128;
  using Uint = unsigned __int128;
};

template <int num_bits>
using Int = typename IntTraits<num_bits>::Int;

template <int num_bits>
using Uint = typename IntTraits<num_bits>::Uint;

inline int FindMSBSetNonZero(uint32_t x) { return zetasql_base::Bits::FindMSBSetNonZero(x); }
inline int FindMSBSetNonZero(uint64_t x) { return zetasql_base::Bits::FindMSBSetNonZero64(x); }

// Helper functions for converting between a bigger integer type and an array of
// smaller integer type in little-endian order. These functions do not check
// array sizes.
// Requirements: n must be >= m and (n/m) must be a power of 2.

template <int n, int m>
inline std::enable_if_t<n == m> UintToArray(Uint<n> src, Uint<m> dest[]) {
  *dest = src;
}

template <int n, int m>
inline std::enable_if_t<n != m> UintToArray(Uint<n> src, Uint<m> dest[]) {
  constexpr int k = n / 2;
  UintToArray<k, m>(static_cast<Uint<k>>(src), dest);
  UintToArray<k, m>(static_cast<Uint<k>>(src >> k), &dest[k / m]);
}

template <int n, int m>
inline std::enable_if_t<n == m, Uint<n>> ArrayToUint(const Uint<m> src[]) {
  return src[0];
}

template <int n, int m>
inline std::enable_if_t<n != m, Uint<n>> ArrayToUint(const Uint<m> src[]) {
  constexpr int k = n / 2;
  return (static_cast<Uint<n>>(ArrayToUint<k, m>(&src[k / m])) << k) |
         ArrayToUint<k, m>(src);
}

template <int k>
inline Uint<k * 2> MakeDword(const Uint<k> x[2]) {
  return static_cast<Uint<k * 2>>(x[1]) << k | x[0];
}

// bits must be > 0 and < 64.
inline uint64_t ShiftRightAndGetLowWord(const uint64_t x[2], uint bits) {
  return (x[1] << (64 - bits)) | (x[0] >> bits);
}

// bits must be < 64.
inline uint32_t ShiftRightAndGetLowWord(const uint32_t x[2], uint bits) {
  return static_cast<uint32_t>(MakeDword<32>(x) >> bits);
}

template <typename Word>
inline void ShiftLeftFast(Word* number, int num_words, uint bits) {
  constexpr int kNumBitsPerWord = sizeof(Word) * 8;
  DCHECK_GT(bits, 0);
  DCHECK_LT(bits, kNumBitsPerWord);
  int s = kNumBitsPerWord - bits;
  for (int i = num_words - 1; i > 0; --i) {
    number[i] = ShiftRightAndGetLowWord(number + (i - 1), s);
  }
  number[0] <<= bits;
}

template <typename Word>
void ShiftLeft(Word* number, int num_words, uint bits) {
  constexpr int kNumBitsPerWord = sizeof(Word) * 8;
  if (ABSL_PREDICT_FALSE(bits >= num_words * kNumBitsPerWord)) {
    std::fill(number, number + num_words, 0);
    return;
  }
  int q = bits / kNumBitsPerWord;
  int r = bits % kNumBitsPerWord;
  int s = kNumBitsPerWord - r;
  for (int i = num_words - 1; i > q; --i) {
    auto tmp = MakeDword<kNumBitsPerWord>(number + (i - q - 1));
    number[i] = static_cast<Word>(tmp >> s);
  }
  number[q] = number[0] << r;
  for (int i = 0; i < q; ++i) {
    number[i] = 0;
  }
}

template <typename LastWord, typename Word>
inline void ShiftRightFast(Word* number, int num_words, uint bits) {
  constexpr int kNumBitsPerWord = sizeof(Word) * 8;
  DCHECK_GT(bits, 0);
  DCHECK_LT(bits, kNumBitsPerWord);
  for (int i = 0; i < num_words - 1; ++i) {
    number[i] = ShiftRightAndGetLowWord(number + i, bits);
  }
  number[num_words - 1] = static_cast<LastWord>(number[num_words - 1]) >> bits;
}

template <typename Filler, typename Word>
void ShiftRight(Filler filler, Word* number, int num_words, uint bits) {
  static_assert(sizeof(Filler) == sizeof(Word));
  constexpr int kNumBitsPerWord = sizeof(Word) * 8;
  if (ABSL_PREDICT_FALSE(bits >= num_words * kNumBitsPerWord)) {
    std::fill(number, number + num_words, filler);
    return;
  }
  int q = bits / kNumBitsPerWord;
  int r = bits % kNumBitsPerWord;
  int mid = num_words - q;
  for (int i = 0; i < mid - 1; ++i) {
    auto tmp = MakeDword<kNumBitsPerWord>(number + (i + q));
    number[i] = static_cast<Word>(tmp >> r);
  }
  number[mid - 1] = static_cast<Filler>(number[num_words - 1]) >> r;
  for (int i = mid; i < num_words; ++i) {
    number[i] = filler;
  }
}

template <int k>
bool LessWithVariableSize(const Uint<k>* lhs, const Uint<k>* rhs, int size) {
  for (int i = size - 1; i >= 0; --i) {
    if (lhs[i] != rhs[i]) {
      return lhs[i] < rhs[i];
    }
  }
  return false;
}

// When the size is a small compile-time constant, Less<k, size> is much more
// efficient.
template <int k, int size>
inline std::enable_if_t<size == 0, bool> Less(const Uint<k>* lhs,
                                              const Uint<k>* rhs) {
  return false;
}

template <int k, int size>
inline std::enable_if_t<size == 1, bool> Less(const Uint<k>* lhs,
                                              const Uint<k>* rhs) {
  return lhs[0] < rhs[0];
}

template <int k, int size>
inline std::enable_if_t<size == 2, bool> Less(const Uint<k>* lhs,
                                              const Uint<k>* rhs) {
  return MakeDword<k>(lhs) < MakeDword<k>(rhs);
}

template <int k, int size>
inline std::enable_if_t<(size > 2 && size <= 8), bool> Less(
    const Uint<k>* lhs, const Uint<k>* rhs) {
  auto lh_dword = MakeDword<k>(lhs + size - 2);
  auto rh_dword = MakeDword<k>(rhs + size - 2);
  if (lh_dword != rh_dword) {
    return lh_dword < rh_dword;
  }
  return Less<k, size - 2>(lhs, rhs);
}

template <int k, int size>
inline std::enable_if_t<(size > 8), bool> Less(const Uint<k>* lhs,
                                               const Uint<k>* rhs) {
  return LessWithVariableSize<k>(lhs, rhs, size);
}

#ifdef __x86_64__
inline uint8_t SubtractWithBorrow(uint32_t* x, uint32_t y, uint8_t carry) {
  return _subborrow_u32(carry, *x, y, x);
}

inline uint8_t SubtractWithBorrow(uint64_t* x, uint64_t y, uint8_t carry) {
  static_assert(sizeof(uint64_t) == sizeof(unsigned long long));  // NOLINT
  unsigned long long tmp;                                       // NOLINT
  carry = _subborrow_u64(carry, *x, y, &tmp);
  *x = tmp;
  return carry;
}

#else

template <typename Word>
inline uint8_t SubtractWithBorrow(Word* x, Word y, uint8_t carry) {
  constexpr int k = sizeof(Word) * 8;
  Uint<k * 2> lhs = *x;
  Uint<k * 2> rhs = y;
  rhs += carry;
  *x = static_cast<Word>(lhs - rhs);
  return lhs < rhs;
}

#endif

template <typename Word>
inline void SubtractWithVariableSize(Word lhs[], const Word rhs[], int size) {
  uint8_t carry = 0;
  for (int i = 0; i < size; ++i) {
    carry = SubtractWithBorrow(&lhs[i], rhs[i], carry);
  }
}

template <int size>
inline void Subtract(std::array<uint32_t, size>& lhs,
                     const std::array<uint32_t, size>& rhs) {
  uint8_t carry = 0;
  for (int i = 0; i < (size & ~1); i += 2) {
    uint64_t tmp = MakeDword<32>(lhs.data() + i);
    carry = SubtractWithBorrow(&tmp, MakeDword<32>(rhs.data() + i), carry);
    lhs[i + 1] = static_cast<uint32_t>(tmp >> 32);
    lhs[i] = static_cast<uint32_t>(tmp);
  }
  if (size & 1) {
    SubtractWithBorrow(&lhs[size - 1], rhs[size - 1], carry);
  }
}

template <int size>
inline void Subtract(std::array<uint64_t, size>& lhs,
                     const std::array<uint64_t, size>& rhs) {
  if (size == 2) {
    // For unknown reasons, the following code is faster than
    // SubtractWithVariableSize for size = 2.
    uint8_t carry = 0;
    for (int i = 0; i < size; ++i) {
      auto lh_tmp = static_cast<unsigned __int128>(lhs[i]);
      auto rh_tmp = static_cast<unsigned __int128>(rhs[i]) + carry;
      carry = lh_tmp < rh_tmp;
      lhs[i] = static_cast<uint64_t>(lh_tmp - rh_tmp);
    }
  } else {
    SubtractWithVariableSize(lhs.data(), rhs.data(), size);
  }
}

template <int k, int n1, int n2>
inline std::array<Uint<k>, n1 + n2> ExtendAndMultiply(
    const std::array<fixed_int_internal::Uint<k>, n1>& lh,
    const std::array<fixed_int_internal::Uint<k>, n2>& rh) {
  using Word = Uint<k>;
  using DWord = Uint<k * 2>;
  std::array<Word, n1 + n2> res;
  res.fill(0);
  Word carry = 0;
  for (int j = 0; j < n2; ++j) {
    for (int i = 0; i < n1; ++i) {
      DWord tmp = static_cast<DWord>(lh[i]) * rh[j] + res[i + j] + carry;
      res[i + j] = static_cast<Word>(tmp);
      carry = static_cast<Word>(tmp >> k);
    }
    res[n1 + j] = carry;
    carry = 0;
  }
  return res;
}

template <int k>
inline void Copy(const Uint<k>* src, int src_size, Uint<k>* dest, int dest_size,
                 Uint<k> filler) {
  int size = std::min(src_size, dest_size);
  Uint<k>* p = std::copy(src, src + size, dest);
  std::fill(p, dest + dest_size, filler);
}

template <int k>
inline void Copy(const Uint<k>* src, int src_size, Uint<k * 2>* dest,
                 int dest_size, Uint<k * 2> filler) {
  int copy_size = std::min(src_size / 2, dest_size);
  for (int i = 0; i < copy_size; ++i) {
    dest[i] = (static_cast<Uint<2 * k>>(src[2 * i + 1]) << k) | src[2 * i];
  }
  if ((src_size & 1) != 0 && copy_size < dest_size) {
    dest[copy_size] = (filler << k) | src[2 * copy_size];
    ++copy_size;
  }
  std::fill(dest + copy_size, dest + dest_size, filler);
}

template <int k>
inline void Copy(const Uint<k * 2>* src, int src_size, Uint<k>* dest,
                 int dest_size, Uint<k> filler) {
  int copy_size = std::min(src_size * 2, (dest_size & ~1));
  for (int i = 0; i < copy_size; i += 2) {
    dest[i] = static_cast<Uint<k>>(src[i / 2]);
    dest[i + 1] = static_cast<Uint<k>>(src[i / 2] >> k);
  }
  if (copy_size < dest_size && copy_size < src_size * 2) {
    dest[copy_size] = static_cast<Uint<k>>(src[copy_size / 2]);
    ++copy_size;
  }
  std::fill(dest + copy_size, dest + dest_size, filler);
}

// allow_optimization is used only for testing.
template <int k1, int n1, int k2, int n2, bool allow_optimization = true>
inline std::array<Uint<k1>, n1> Convert(const std::array<Uint<k2>, n2>& src,
                                        bool negative) {
  std::array<Uint<k1>, n1> res;
#ifndef IS_BIG_ENDIAN
  if (allow_optimization) {
    int copy_size = std::min(sizeof(src), sizeof(res));
    memcpy(res.data(), src.data(), copy_size);
    memset(reinterpret_cast<uint8_t*>(res.data()) + copy_size,
           -static_cast<int8_t>(negative), sizeof(res) - copy_size);
    return res;
  }
#endif
  Copy<std::min(k1, k2)>(src.data(), n2, res.data(), n1,
                         negative ? ~Uint<k1>{0} : 0);
  return res;
}

template <int kNumBitsPerWord, int kNumWords>
std::array<fixed_int_internal::Uint<kNumBitsPerWord>, kNumWords> toLittleEndian(
    const std::array<fixed_int_internal::Uint<kNumBitsPerWord>, kNumWords>&
        in) {
  std::array<fixed_int_internal::Uint<kNumBitsPerWord>, kNumWords> tmp(in);
#ifdef IS_BIG_ENDIAN
  for (auto& word : tmp) {
    word = zetasql_base::LittleEndian::FromHost(word);
  }
#endif
  return tmp;
}

// Deserialize from minimum number of bytes needed to represent the number.
// Similarly to Serialize, if use_twos_compl is true then a high 0x80 bit
// will result in padding with 0xff rather than 0x00
template <int kNumBitsPerWord, int kNumWords, bool use_twos_compl>
static bool Deserialize(
    absl::string_view bytes,
    std::array<fixed_int_internal::Uint<kNumBitsPerWord>, kNumWords>* out) {
  // We allow one extra byte in case of the 0x80 high byte case, see
  // comments on Serialize for details
  if (bytes.empty() || bytes.size() > sizeof(*out)) {
    return false;
  }
  memcpy(out->data(), &bytes[0], bytes.size());
  // Sign extend based on high bit
  uint8_t extension = use_twos_compl && bytes.back() & 0x80 ? 0xff : 0x00;
  memset(reinterpret_cast<uint8_t*>(out->data()) + bytes.size(), extension,
         sizeof(*out) - bytes.size());
#ifdef IS_BIG_ENDIAN
  for (auto& word : *out) {
    word = BigEndian::ToHost(word);
  }
#endif
  return true;
}

}  // namespace fixed_int_internal

template <int kNumBitsPerWord, int kNumWords>
class FixedInt;

// kNumBitsPerWord must be 32 or 64. Given the same kNumBitsPerWord * kNumWords
// value, kNumBitsPerWord=64 is generally faster, except the following cases:
// 1) Division by a value with more than 64 bits.
// 2) Division by a compile-time constant that is a product of 32-bit integers.
///   In this case, repeated division by 32-bit constant values is typically
//    much faster because the compiler can replace the divisions with
//    multiplications. This optimization works only with 32-bit constant
//    divisors.
//
// Unless otherwise documented, the operators of this class do not check
// overflows. If a result overflows, the result bits higher than the kNumBits
// are silently dropped, for consistency with the primitive integer types.
template <int kNumBitsPerWord, int kNumWords>
class FixedUint final {
 public:
  using Word = fixed_int_internal::Uint<kNumBitsPerWord>;
  using DWord = fixed_int_internal::Uint<kNumBitsPerWord * 2>;
  static constexpr int kNumBits = kNumBitsPerWord * kNumWords;
  // The max bits an integer can have to be casted to double when all the bits
  // in the integer are set. Also limited by the kNumBitsPerWord.
  static constexpr int kMaxBitsToDouble = 992;

  FixedUint() { number_.fill(0); }

  explicit FixedUint(unsigned __int128 x) {
    static_assert(kNumBits >= 128, "Size too small");
    number_.fill(0);
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(x, number_.data());
  }

  explicit FixedUint(uint64_t x) {
    static_assert(kNumBits >= 64, "Size too small");
    number_.fill(0);
    fixed_int_internal::UintToArray<64, kNumBitsPerWord>(x, number_.data());
  }

  FixedUint(uint64_t hi, unsigned __int128 low) {
    static_assert(kNumBits >= 192, "Size too small");
    number_.fill(0);
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(low, number_.data());
    fixed_int_internal::UintToArray<64, kNumBitsPerWord>(
        hi, &number_[128 / kNumBitsPerWord]);
  }

  FixedUint(unsigned __int128 hi, unsigned __int128 low) {
    static_assert(kNumBits >= 256, "Size too small");
    number_.fill(0);
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(low, number_.data());
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(
        hi, &number_[128 / kNumBitsPerWord]);
  }

  // If k * n > kNumBits, then the k * n - kNumBits most significant bits are
  // dropped.
  template <int k, int n>
  explicit FixedUint(const FixedUint<k, n>& src)
      : number_(fixed_int_internal::Convert<kNumBitsPerWord, kNumWords, k, n>(
            src.number(), false)) {}

  explicit constexpr FixedUint(
      const std::array<Word, kNumWords>& little_endian_number)
      : number_(little_endian_number) {}

  explicit operator unsigned __int128() const {
    return fixed_int_internal::ArrayToUint<128, kNumBitsPerWord>(
        number_.data());
  }

  // Cast FixedUint<kNumBitsPerWord, kNumWords> into double. When there is a
  // loss of significance during conversion, we will use the rounding rule that
  // rounds half to even. It is the same rule being used in the built-in cast
  // functions for integer to double numbers. For example, 0xfffffffffffff400
  // (2^64 - 3 * 2^10) should be rounded down to 0xfffffffffffff000 (2^64 - 4 *
  // 2^10) while 0xfffffffffffffc00 (2^64 - 2^10) should be rounded up to
  // 0x10000000000000000 (2^64) since the values rounded to have even mantissas
  // in double form while 0xfffffffffffff800 (2^64 - 2 * 2^10) has an odd
  // mantissa.
  explicit operator double() const;

  // Shifts the number left by the given number of bits.
  FixedUint& operator<<=(uint bits) {
    if (ABSL_PREDICT_TRUE(bits != 0)) {
      if (ABSL_PREDICT_TRUE(bits < kNumBitsPerWord)) {
        fixed_int_internal::ShiftLeftFast(number_.data(), kNumWords, bits);
        return *this;
      }
      fixed_int_internal::ShiftLeft(number_.data(), kNumWords, bits);
    }
    return *this;
  }

  // Shifts the number right by the given number of bits.
  FixedUint& operator>>=(uint bits) {
    if (ABSL_PREDICT_TRUE(bits != 0)) {
      if (ABSL_PREDICT_TRUE(bits < kNumBitsPerWord)) {
        fixed_int_internal::ShiftRightFast<Word>(number_.data(), kNumWords,
                                                 bits);
        return *this;
      }
      fixed_int_internal::ShiftRight(Word{0}, number_.data(), kNumWords, bits);
    }
    return *this;
  }

  // Multiplies this number by the given Word value.
  FixedUint& operator*=(Word x) {
    Word carry = 0;
    for (int i = 0; i < kNumWords; ++i) {
      DWord tmp = static_cast<DWord>(number_[i]) * x + carry;
      number_[i] = static_cast<Word>(tmp);
      carry = static_cast<Word>(tmp >> kNumBitsPerWord);
    }
    return *this;
  }

  // Multiplies this number by the given value. If the multiplication overflows,
  // the result bits higher than the kNumBits are silently dropped.
  FixedUint& operator*=(const FixedUint& rh);

  // Multiplies this number by the given value. Returns false iff the result of
  // the multiplication overflows, in which case the data in *this is unchanged.
  ABSL_MUST_USE_RESULT bool MultiplyAndCheckOverflow(const FixedUint& rh);

  FixedUint& operator+=(Word x) {
    Word carry = x;
    for (int i = 0; i < kNumWords; ++i) {
      DWord tmp = static_cast<DWord>(number_[i]) + carry;
      number_[i] = static_cast<Word>(tmp);
      carry = static_cast<Word>(tmp >> kNumBitsPerWord);
      if (carry == 0) {
        break;
      }
    }
    return *this;
  }

  FixedUint& operator+=(const FixedUint& rh) {
    Word carry = 0;
    for (int i = 0; i < kNumWords; ++i) {
      DWord tmp = static_cast<DWord>(number_[i]) + carry + rh.number_[i];
      number_[i] = static_cast<Word>(tmp);
      carry = static_cast<Word>(tmp >> kNumBitsPerWord);
    }
    return *this;
  }

  FixedUint& operator-=(Word x) {
    uint8_t carry = fixed_int_internal::SubtractWithBorrow(&number_[0], x, 0);
    for (int i = 1; i < kNumWords; ++i) {
      carry =
          fixed_int_internal::SubtractWithBorrow(&number_[i], Word{0}, carry);
    }
    return *this;
  }

  FixedUint& operator-=(const FixedUint& rh) {
    fixed_int_internal::Subtract<kNumWords>(number_, rh.number_);
    return *this;
  }

  // The caller is responsible for ensuring that the value is not zero.
  FixedUint& operator/=(Word x) {
    for (int i = kNumWords - 1; i >= 0; --i) {
      if (number_[i] != 0) {
        DWord carry = 0;
        do {
          carry <<= kNumBitsPerWord;
          carry |= number_[i];
          number_[i] = static_cast<Word>(carry / x);
          carry = carry % x;
        } while (--i >= 0);
        return *this;
      }
    }
    return *this;
  }

  FixedUint& operator/=(const FixedUint& x) {
    if (x.NonZeroLength() <= 1) {
      return *this /= x.number_[0];
    }
    LongDivision(x);
    return *this;
  }

  // Returns the number of words excluding leading zero words.
  int NonZeroLength() const;

  // Returns the first set most significant bit index, 0 based. If this number
  // is 0 then this function will return 0;
  int FindMSBSetNonZero() const;

  const std::array<Word, kNumWords>& number() const { return number_; }

  // Serializes to minimum number of bytes needed to represent the number.
  std::string SerializeToBytes() const {
    std::string out;

    FixedUint<kNumBitsPerWord, kNumWords> little_endian;
    little_endian.number_ =
        fixed_int_internal::toLittleEndian<kNumBitsPerWord, kNumWords>(
            number());

    int non_zero_bit_idx = FindMSBSetNonZero();
    int non_zero_byte_idx = non_zero_bit_idx / 8;
    out.resize(non_zero_byte_idx + 1);
    memcpy(&out[0], little_endian.number().data(), non_zero_byte_idx + 1);

    return out;
  }

  // Deserializes the output of Serialize() from a FixedUint (not FixedInt) with
  // the same template arguments. If the input is valid, false is returned and
  // this instance is unchanged.
  ABSL_MUST_USE_RESULT bool DeserializeFromBytes(absl::string_view bytes) {
    return fixed_int_internal::Deserialize<kNumBitsPerWord, kNumWords, false>(
        bytes, &number_);
  }

 private:
  friend class FixedUint<kNumBitsPerWord, kNumWords - 1>;
  friend class FixedInt<kNumBitsPerWord, kNumWords>;

  // Performs a long division by the given number.
  void LongDivision(const FixedUint& x);

  // The number is
  // stored in the little-endian order with the least significant word being at
  // the index 0.
  std::array<Word, kNumWords> number_;
};

template <int kNumBitsPerWord, int kNumWords>
FixedUint<kNumBitsPerWord, kNumWords>::operator double() const {
  static_assert(kNumBits <= kMaxBitsToDouble,
                "Size too big to convert to double.");
  static_assert(kNumBits > 0, "The number has less than one bit.");
  // DOUBLE can have 53 bits in the significand, which is less than 64. We will
  // keep 55 bits at most for rounding. We take at most 54 bits from the
  // FixedUint, and decide the 55th by the trailing bits if needed. Keeping
  // two more bits for rounding halves ties to the nearest even number.
  uint64_t significand = 0;
  int word_idx = NonZeroLength() - 1;
  if (word_idx == -1) {
    return 0.0;
  }
  int bit_idx = fixed_int_internal::FindMSBSetNonZero(number_[word_idx]);
  int significant_bits = 0;
  while (true) {
    if (significant_bits + bit_idx >= 54) {
      significand <<= (54 - significant_bits);
      bit_idx += (significant_bits - 54);
      significand |= (number_[word_idx] >> bit_idx);
      // Set the 55th bit for rounding. Set it to 1 if any non-zero in trailing
      // digits.
      significand <<= 1;
      int exp = word_idx * kNumBitsPerWord + bit_idx - 1;
      Word remainder = number_[word_idx] & ~(~Word{0} << bit_idx);
      while (remainder == 0) {
        if (--word_idx < 0) {
          return std::ldexp(significand, exp);
        }
        remainder = number_[word_idx];
      }
      return std::ldexp(significand | 1, exp);
    }
    significand = (significand << bit_idx) | number_[word_idx];
    significant_bits += bit_idx;
    bit_idx = kNumBitsPerWord;
    if (--word_idx < 0) {
      DCHECK_LT(significant_bits, 54);
      return static_cast<double>(significand);
    }
  }
}

template <int kNumBitsPerWord, int kNumWords>
inline int FixedUint<kNumBitsPerWord, kNumWords>::NonZeroLength() const {
  for (int i = kNumWords - 1; i >= 0; --i) {
    if (number_[i] != 0) {
      return i + 1;
    }
  }
  return 0;
}

template <int kNumBitsPerWord, int kNumWords>
inline int FixedUint<kNumBitsPerWord, kNumWords>::FindMSBSetNonZero() const {
  const int nzlen = NonZeroLength();
  if (nzlen == 0) {
    return 0;
  }
  return fixed_int_internal::FindMSBSetNonZero(number_[nzlen - 1]) +
      (nzlen - 1) * kNumBitsPerWord;
}

template <int kNumBitsPerWord, int kNumWords>
inline FixedUint<kNumBitsPerWord, kNumWords>&
FixedUint<kNumBitsPerWord, kNumWords>::operator*=(const FixedUint& rh) {
  FixedUint res;
  for (int j = 0; j < kNumWords; ++j) {
    Word carry = 0;
    for (int i = 0; i < kNumWords - j; ++i) {
      DWord tmp = static_cast<DWord>(number_[i]) * rh.number_[j] +
                  res.number_[i + j] + carry;
      res.number_[i + j] = static_cast<Word>(tmp);
      carry = static_cast<Word>(tmp >> kNumBitsPerWord);
    }
  }
  return *this = res;
}

template <int kNumBitsPerWord, int kNumWords>
inline bool FixedUint<kNumBitsPerWord, kNumWords>::MultiplyAndCheckOverflow(
    const FixedUint& rh) {
  const int this_msb = FindMSBSetNonZero();
  const int rh_msb = rh.FindMSBSetNonZero();
  if (this_msb + rh_msb >= kNumBits) {
    return false;
  }

  FixedUint res;
  Word carry = 0;
  for (int j = 0; j < rh.number_.size(); ++j) {
    for (int i = 0; i < number_.size(); ++i) {
      if (i + j >= number_.size()) {
        if (ABSL_PREDICT_FALSE(carry != 0)) {
          return false;
        }
        continue;
      }
      DWord tmp = static_cast<DWord>(number_[i]) * rh.number_[j] +
                  res.number_[i + j] + carry;
      res.number_[i + j] = static_cast<Word>(tmp);
      carry = static_cast<Word>(tmp >> kNumBitsPerWord);
    }
  }
  *this = res;
  return true;
}

template <int kNumBitsPerWord, int kNumWords>
void FixedUint<kNumBitsPerWord, kNumWords>::LongDivision(const FixedUint& x) {
  FixedUint<kNumBitsPerWord, kNumWords + 1> dividend(*this);
  FixedUint<kNumBitsPerWord, kNumWords + 1> divisor(x);

  // Find the actual length of the dividend and the divisor.
  int n = divisor.NonZeroLength();
  int m = NonZeroLength();

  // First we need to normalize the divisor to make the most significant digit
  // of it larger than radix/2 (radix being 2^kNumBitsPerWord in our case). This
  // is necessary for accurate guesses of the quotent digits. See Knuth "The Art
  // of Computer Programming" Vol.2 for details.
  //
  // We perform normalization by finding how far we need to shift to make the
  // most significant bit of the divisor 1. And then we shift both the divident
  // and the divisor thus preserving the result of the division.
  int non_zero_bit_idx =
      fixed_int_internal::FindMSBSetNonZero(divisor.number_[n - 1]);
  int shift_amount = kNumBitsPerWord - non_zero_bit_idx - 1;

  if (shift_amount > 0) {
    dividend <<= shift_amount;
    divisor <<= shift_amount;
  }

  FixedUint quotient;

  for (int i = m - n; i >= 0; --i) {
    // Make the guess of the quotent digit. The guess we take here is:
    //
    //   qhat = min((divident[m] * b + divident[m-1]) / divisor[n], b-1)
    //
    // where b is the radix, which in our case is 2^kNumBitsPerWord. In "The Art
    // of Computer Programming" Vol.2 Knuth proves that given the normalization
    // above this guess is often accurate and when not it is always larger than
    // the actual quotent digit and is no larger than 2 removed from it.
    DWord tmp =
        (static_cast<DWord>(dividend.number_[i + n]) << kNumBitsPerWord) |
        dividend.number_[i + n - 1];
    DWord quotent_candidate = tmp / divisor.number_[n - 1];
    if (quotent_candidate > std::numeric_limits<Word>::max()) {
      quotent_candidate = std::numeric_limits<Word>::max();
    }

    FixedUint<kNumBitsPerWord, kNumWords + 1> dq = divisor;
    dq *= static_cast<Word>(quotent_candidate);

    // If the guess was not accurate, adjust it. As stated above, at the worst,
    // the original guess qhat is q + 2, where q is the actual quotent digit, so
    // this loop will not be executed more than 2 iterations.
    for (int iter = 0;
         fixed_int_internal::LessWithVariableSize<kNumBitsPerWord>(
             dividend.number().data() + i, dq.number().data(), n + 1);
         ++iter) {
      DCHECK_LT(iter, 2);
      --quotent_candidate;
      dq = divisor;
      dq *= static_cast<Word>(quotent_candidate);
    }

    fixed_int_internal::SubtractWithVariableSize(dividend.number_.data() + i,
                                                 dq.number().data(), n + 1);
    quotient.number_[i] = static_cast<Word>(quotent_candidate);
  }

  *this = quotient;
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator==(const FixedUint<kNumBitsPerWord, kNumWords>& lh,
                       const FixedUint<kNumBitsPerWord, kNumWords>& rh) {
  return lh.number() == rh.number();
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator!=(const FixedUint<kNumBitsPerWord, kNumWords>& lh,
                       const FixedUint<kNumBitsPerWord, kNumWords>& rh) {
  return lh.number() != rh.number();
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator<(const FixedUint<kNumBitsPerWord, kNumWords>& lh,
                      const FixedUint<kNumBitsPerWord, kNumWords>& rh) {
  return fixed_int_internal::Less<kNumBitsPerWord, kNumWords>(
      lh.number().data(), rh.number().data());
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator>(const FixedUint<kNumBitsPerWord, kNumWords>& lh,
                      const FixedUint<kNumBitsPerWord, kNumWords>& rh) {
  return rh < lh;
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator<=(const FixedUint<kNumBitsPerWord, kNumWords>& lh,
                       const FixedUint<kNumBitsPerWord, kNumWords>& rh) {
  return !(rh < lh);
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator>=(const FixedUint<kNumBitsPerWord, kNumWords>& lh,
                       const FixedUint<kNumBitsPerWord, kNumWords>& rh) {
  return !(lh < rh);
}

template <int kNumBitsPerWord, int kNumWords>
class FixedInt final {
 public:
  // Word is unsigned by design. Be careful when calling the binary operators
  // with Word type; they do not check whether the argument is negative, for
  // performance.
  using Word = fixed_int_internal::Uint<kNumBitsPerWord>;
  using DWord = fixed_int_internal::Uint<kNumBitsPerWord * 2>;
  static constexpr int kNumBits = kNumBitsPerWord * kNumWords;
  static constexpr int kMaxBitsToDouble = 992;

  FixedInt() {}

  explicit FixedInt(__int128 x) {
    static_assert(kNumBits >= 128, "Size too small");
    rep_.number_.fill(x >= 0 ? 0 : ~Word{0});
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(
        static_cast<unsigned __int128>(x), rep_.number_.data());
  }

  explicit FixedInt(int64_t x) {
    static_assert(kNumBits >= 64, "Size too small");
    rep_.number_.fill(x >= 0 ? 0 : ~Word{0});
    fixed_int_internal::UintToArray<64, kNumBitsPerWord>(static_cast<uint64_t>(x),
                                                         rep_.number_.data());
  }

  FixedInt(int64_t hi, unsigned __int128 low) {
    static_assert(kNumBits >= 192, "Size too small");
    rep_.number_.fill(hi >= 0 ? 0 : ~Word{0});
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(low,
                                                          rep_.number_.data());
    fixed_int_internal::UintToArray<64, kNumBitsPerWord>(
        static_cast<uint64_t>(hi), &rep_.number_[128 / kNumBitsPerWord]);
  }

  FixedInt(__int128 hi, unsigned __int128 low) {
    static_assert(kNumBits >= 256, "Size too small");
    rep_.number_.fill(hi >= 0 ? 0 : ~Word{0});
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(low,
                                                          rep_.number_.data());
    fixed_int_internal::UintToArray<128, kNumBitsPerWord>(
        static_cast<unsigned __int128>(hi),
        &rep_.number_[128 / kNumBitsPerWord]);
  }

  template <int k, int n>
  explicit FixedInt(const FixedInt<k, n>& src)
      : rep_(fixed_int_internal::Convert<kNumBitsPerWord, kNumWords, k, n>(
            src.number(), src.is_negative())) {}

  explicit constexpr FixedInt(
      const std::array<Word, kNumWords>& little_endian_number)
      : rep_(little_endian_number) {}

  explicit operator __int128() const {
    return static_cast<unsigned __int128>(rep_);
  }

  explicit operator double() const {
    static_assert(kNumBits <= kMaxBitsToDouble,
                  "Size too big to convert to double.");
    static_assert(kNumBits > 0, "The number has less than one bit.");
    if (is_negative()) {
      return -static_cast<double>((-(*this)).rep_);
    }
    return static_cast<double>(rep_);
  }

  FixedInt& operator<<=(uint bits) {
    rep_ <<= bits;
    return *this;
  }

  FixedInt& operator>>=(uint bits) {
    using SignedWord = std::make_signed_t<Word>;
    if (ABSL_PREDICT_TRUE(bits != 0)) {
      if (ABSL_PREDICT_TRUE(bits < kNumBitsPerWord)) {
        fixed_int_internal::ShiftRightFast<SignedWord>(rep_.number_.data(),
                                                       kNumWords, bits);
        return *this;
      }
      SignedWord filler = -SignedWord{is_negative()};
      fixed_int_internal::ShiftRight(filler, rep_.number_.data(), kNumWords,
                                     bits);
    }
    return *this;
  }

  FixedInt& operator+=(Word x) {
    rep_ += x;
    return *this;
  }

  FixedInt& operator+=(const FixedInt& rh) {
    rep_ += rh.rep_;
    return *this;
  }

  FixedInt& operator-=(Word x) {
    rep_ -= x;
    return *this;
  }

  FixedInt& operator-=(const FixedInt& rh) {
    rep_ -= rh.rep_;
    return *this;
  }

  FixedInt& operator*=(Word x) {
    rep_ *= x;
    return *this;
  }

  FixedInt& operator*=(const FixedInt& x) {
    rep_ *= x.rep_;
    return *this;
  }

  FixedInt& operator/=(Word x) {
    bool neg = is_negative();
    if (!neg) {
      rep_ /= x;
      return *this;
    }
    negate();
    rep_ /= x;
    negate();
    return *this;
  }

  FixedInt& operator/=(const FixedInt& x) {
    bool neg = is_negative();
    bool x_neg = x.is_negative();
    // Must make a copy even if x_neg is false, in case &x == this.
    const FixedInt abs_x = x_neg ? -x : x;
    if (neg) {
      negate();
    }
    rep_ /= abs_x.rep_;
    if (neg != x_neg) {
      negate();
    }
    return *this;
  }

  bool is_negative() const {
    return static_cast<std::make_signed_t<Word>>(*rep_.number().rbegin()) < 0;
  }
  void negate() {
    for (Word& w : rep_.number_) {
      w = ~w;
    }
    rep_ += 1;
  }
  FixedInt operator-() const {
    FixedInt res = *this;
    res.negate();
    return res;
  }
  const std::array<Word, kNumWords>& number() const { return rep_.number_; }

  // Serializes to minimum number of bytes needed to represent the number,
  // using two's complement
  std::string SerializeToBytes() const {
    std::string out;

    FixedInt<kNumBitsPerWord, kNumWords> little_endian(
        fixed_int_internal::toLittleEndian<kNumBitsPerWord, kNumWords>(
            number()));

    const FixedInt<kNumBitsPerWord, kNumWords>& abs_val =
        is_negative() ? -*this : *this;

    int non_zero_bit_idx = abs_val.rep_.FindMSBSetNonZero();
    int non_zero_byte_idx = non_zero_bit_idx / 8;
    const char* little_endian_byte_ptr =
        reinterpret_cast<const char*>(little_endian.number().data());
    int copy_size = non_zero_byte_idx + 1;
    // Sign extend the byte representation when the original non zero byte
    // differs from one before it, i.e. if we have 0x..ff7f.. we want to copy
    // out 0xff7f.. not 0x7f to ensure we distinguish between negative and non
    // negatives.
    if (copy_size < sizeof(*this) &&
        ((little_endian_byte_ptr[copy_size] ^
          little_endian_byte_ptr[non_zero_byte_idx]) &
         '\x80') != 0) {
      ++copy_size;
    }
    out.resize(copy_size);
    memcpy(&out[0], little_endian.number().data(), copy_size);

    return out;
  }

  // Deserializes the output of Serialize() from a FixedInt (not FixedUint) with
  // the same template arguments. If the input is valid, false is returned and
  // this instance is unchanged.
  ABSL_MUST_USE_RESULT bool DeserializeFromBytes(absl::string_view bytes) {
    return fixed_int_internal::Deserialize<kNumBitsPerWord, kNumWords, true>(
        bytes, &rep_.number_);
  }

 private:
  FixedUint<kNumBitsPerWord, kNumWords> rep_;
};

template <int kNumBitsPerWord, int kNumWords>
inline bool operator==(const FixedInt<kNumBitsPerWord, kNumWords>& lh,
                       const FixedInt<kNumBitsPerWord, kNumWords>& rh) {
  return lh.number() == rh.number();
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator!=(const FixedInt<kNumBitsPerWord, kNumWords>& lh,
                       const FixedInt<kNumBitsPerWord, kNumWords>& rh) {
  return lh.number() != rh.number();
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator<(const FixedInt<kNumBitsPerWord, kNumWords>& lh,
                      const FixedInt<kNumBitsPerWord, kNumWords>& rh) {
  if (kNumWords == 1) {
    using SignedWord = fixed_int_internal::Int<kNumBitsPerWord>;
    return static_cast<SignedWord>(lh.number()[0]) <
           static_cast<SignedWord>(rh.number()[0]);
  }
  using SignedDword = fixed_int_internal::Int<kNumBitsPerWord * 2>;
  auto lh_hi =
      static_cast<SignedDword>(fixed_int_internal::MakeDword<kNumBitsPerWord>(
          lh.number().data() + kNumWords - 2));
  auto rh_hi =
      static_cast<SignedDword>(fixed_int_internal::MakeDword<kNumBitsPerWord>(
          rh.number().data() + kNumWords - 2));
  if (lh_hi != rh_hi) {
    return lh_hi < rh_hi;
  }
  return fixed_int_internal::Less<kNumBitsPerWord, kNumWords - 2>(
      lh.number().data(), rh.number().data());
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator>(const FixedInt<kNumBitsPerWord, kNumWords>& lh,
                      const FixedInt<kNumBitsPerWord, kNumWords>& rh) {
  return rh < lh;
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator<=(const FixedInt<kNumBitsPerWord, kNumWords>& lh,
                       const FixedInt<kNumBitsPerWord, kNumWords>& rh) {
  return !(rh < lh);
}

template <int kNumBitsPerWord, int kNumWords>
inline bool operator>=(const FixedInt<kNumBitsPerWord, kNumWords>& lh,
                       const FixedInt<kNumBitsPerWord, kNumWords>& rh) {
  return !(lh < rh);
}

// Equivalent to FixedUint<k, n1 + n2>(x) *= FixedUint<k, n1 + n2>(y)
// except that this method is typically 50-60% faster than the above code.
// This method never overflows.
template <int k, int n1, int n2>
inline FixedUint<k, n1 + n2> ExtendAndMultiply(const FixedUint<k, n1>& lh,
                                               const FixedUint<k, n2>& rh) {
  return FixedUint<k, n1 + n2>(fixed_int_internal::ExtendAndMultiply<k, n1, n2>(
      lh.number(), rh.number()));
}

// Equivalent to FixedInt<k, n1 + n2>(x) *= FixedInt<k, n1 + n2>(y)
// except that this method is typically 30% faster than the above code.
// This method never overflows.
template <int k, int n1, int n2>
inline FixedInt<k, n1 + n2> ExtendAndMultiply(const FixedInt<k, n1>& lh,
                                              const FixedInt<k, n2>& rh) {
  bool lh_is_negative = lh.is_negative();
  bool rh_is_negative = rh.is_negative();
  const FixedInt<k, n1>& lh_abs = !lh_is_negative ? lh : -lh;
  const FixedInt<k, n2>& rh_abs = !rh_is_negative ? rh : -rh;
  FixedInt<k, n1 + n2> res(fixed_int_internal::ExtendAndMultiply<k, n1, n2>(
      lh_abs.number(), rh_abs.number()));
  return lh_is_negative == rh_is_negative ? res : -res;
}

}  // namespace zetasql

#endif  // ZETASQL_COMMON_FIXED_INT_H_
