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

#include "zetasql/public/numeric_parser.h"

#include <cstdint>

#include "zetasql/common/multiprecision_int.h"
#include "zetasql/public/numeric_constants.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

#define RETURN_FALSE_IF(x) \
  if (ABSL_PREDICT_FALSE(x)) return false

struct ENotationParts {
  bool negative = false;
  absl::string_view int_part;
  absl::string_view fract_part;
  absl::string_view exp_part;
};

bool SplitENotationParts(absl::string_view str, ENotationParts* parts) {
  const char* start = str.data();
  const char* end = str.data() + str.size();

  // Skip whitespace.
  for (; start < end && (absl::ascii_isspace(*start)); ++start) {
  }
  for (; start < end && absl::ascii_isspace(*(end - 1)); --end) {
  }

  // Empty or only spaces
  RETURN_FALSE_IF(start == end);
  *parts = ENotationParts();

  parts->negative = (*start == '-');
  start += (*start == '-' || *start == '+');
  for (const char* c = end; --c >= start;) {
    if (*c == 'e' || *c == 'E') {
      parts->exp_part = absl::string_view(c + 1, end - c - 1);
      RETURN_FALSE_IF(parts->exp_part.empty());
      end = c;
      break;
    }
  }
  for (const char* c = start; c < end; ++c) {
    if (*c == '.') {
      parts->fract_part = absl::string_view(c + 1, end - c - 1);
      end = c;
      break;
    }
  }
  parts->int_part = absl::string_view(start, end - start);
  return true;
}

// Parses <exp_part> and add <extra_scale> to the result.
// If <exp_part> represents an integer that is below int64min, the result is
// int64min.
// If <exp_part> represents an integer that is above int64max, the result is
// int64max.
bool ParseExponent(absl::string_view exp_part, uint extra_scale, int64_t* exp) {
  *exp = extra_scale;
  if (!exp_part.empty()) {
    FixedInt<64, 1> exp_fixed_int;
    if (ABSL_PREDICT_TRUE(exp_fixed_int.ParseFromStringStrict(exp_part))) {
      RETURN_FALSE_IF(exp_fixed_int.AddOverflow(*exp));
      *exp = exp_fixed_int.number()[0];
    } else if (exp_part.size() > 1 && exp_part[0] == '-') {
      // Still need to check whether exp_part contains only digits after '-'.
      exp_part.remove_prefix(1);
      for (char c : exp_part) {
        RETURN_FALSE_IF(!std::isdigit(c));
      }
      *exp = std::numeric_limits<int64_t>::min();
    } else {
      for (char c : exp_part) {
        RETURN_FALSE_IF(!std::isdigit(c));
      }
      *exp = std::numeric_limits<int64_t>::max();
    }
  }
  return true;
}

// PowersAsc<Word, first_value, base, size>() returns a std::array<Word, size>
// {first_value, first_value * base, ..., first_value * pow(base, size - 1)}.
template <typename Word, Word first_value, Word base, int size, typename... T>
constexpr std::array<Word, size> PowersAsc(T... v) {
  if constexpr (sizeof...(T) < size) {
    return PowersAsc<Word, first_value, base, size>(first_value, v * base...);
  } else {
    return std::array<Word, size>{v...};
  }
}

// Parses <int_part>.<fract_part>E<exp> to FixedUint.
// If <strict> is true, treats the input as invalid if it does not represent
// a whole number. If <strict> is false, rounds the input away from zero to a
// whole number. Returns true iff the input is valid.
template <int n>
bool ParseNumber(absl::string_view int_part, absl::string_view fract_part,
                 int64_t exp, bool strict, FixedUint<64, n>* output) {
  *output = FixedUint<64, n>();
  bool round_up = false;
  if (exp >= 0) {
    // Promote up to exp fractional digits to the integer part.
    size_t num_promoted_fract_digits = fract_part.size();
    if (exp < fract_part.size()) {
      round_up = fract_part[exp] >= '5';
      num_promoted_fract_digits = exp;
    }
    absl::string_view promoted_fract_part(fract_part.data(),
                                          num_promoted_fract_digits);
    fract_part.remove_prefix(num_promoted_fract_digits);
    if (int_part.empty()) {
      RETURN_FALSE_IF(!output->ParseFromStringStrict(promoted_fract_part));
    } else {
      RETURN_FALSE_IF(
          !output->ParseFromStringSegments(int_part, {promoted_fract_part}));
      int_part = absl::string_view();
    }
    // If output is zero, avoid scaling.
    if (!output->is_zero()) {
      // If exp is greater than the number of promoted fractional digits,
      // scale the result up by pow(10, exp - num_promoted_fract_digits).
      size_t extra_exp = static_cast<size_t>(exp) - num_promoted_fract_digits;
      for (; extra_exp >= 19; extra_exp -= 19) {
        RETURN_FALSE_IF(output->MultiplyOverflow(internal::k1e19));
      }
      if (extra_exp != 0) {
        static constexpr std::array<uint64_t, 19> kPowers =
            PowersAsc<uint64_t, 1, 10, 19>();
        RETURN_FALSE_IF(output->MultiplyOverflow(kPowers[extra_exp]));
      }
    }
  } else {  // exp < 0
    RETURN_FALSE_IF(int_part.size() + fract_part.size() == 0);
    // Demote up to -exp digits from int_part
    if (exp >= static_cast<int64_t>(-int_part.size())) {
      size_t int_digits = int_part.size() + exp;
      round_up = int_part[int_digits] >= '5';
      RETURN_FALSE_IF(int_digits != 0 &&
                      !output->ParseFromStringStrict(
                          absl::string_view(int_part.data(), int_digits)));
      int_part.remove_prefix(int_digits);
    }
  }
  // The remaining characters in int_part and fract_part have not been visited.
  // They represent the fractional digits to be discarded. In strict mode, they
  // must be zeros; otherwise they must be digits.
  if (strict) {
    for (char c : int_part) {
      RETURN_FALSE_IF(c != '0');
    }
    for (char c : fract_part) {
      RETURN_FALSE_IF(c != '0');
    }
  } else {
    for (char c : int_part) {
      RETURN_FALSE_IF(!std::isdigit(c));
    }
    for (char c : fract_part) {
      RETURN_FALSE_IF(!std::isdigit(c));
    }
  }
  RETURN_FALSE_IF(round_up && output->AddOverflow(uint64_t{1}));
  return true;
}

#undef RETURN_FALSE_IF

template <uint32_t word_count, uint32_t scale, bool strict_parsing>
absl::Status ParseNumber(absl::string_view str,
                         FixedPointRepresentation<word_count>& parsed) {
  ENotationParts parts;
  int64_t exp;
  if (ABSL_PREDICT_TRUE(SplitENotationParts(str, &parts)) &&
      ABSL_PREDICT_TRUE(ParseExponent(parts.exp_part, scale, &exp)) &&
      ABSL_PREDICT_TRUE(ParseNumber(parts.int_part, parts.fract_part, exp,
                                    strict_parsing, &parsed.output))) {
    parsed.is_negative = parts.negative;
    return absl::OkStatus();
  }
  return ::zetasql_base::InvalidArgumentErrorBuilder()
         << "Failed to parse " << str << " . word_count: " << word_count
         << " scale: " << scale << " strict_parsing: " << strict_parsing;
}

template <bool strict_parsing>
absl::Status ParseNumeric(absl::string_view str,
                          FixedPointRepresentation<2>& parsed) {
  return ParseNumber<2, 9, strict_parsing>(str, parsed);
}

template <bool strict_parsing>
absl::Status ParseBigNumeric(absl::string_view str,
                             FixedPointRepresentation<4>& parsed) {
  return ParseNumber<4, 38, strict_parsing>(str, parsed);
}

absl::Status ParseJSONNumber(absl::string_view str,
                             FixedPointRepresentation<79>& parsed) {
  return ParseNumber<79, 1074, true>(str, parsed);
}

// Explicit template instantiations for NUMERIC
template absl::Status ParseNumeric</*strict_parsing=*/true>(
    absl::string_view str, FixedPointRepresentation<2>& parsed);
template absl::Status ParseNumeric</*strict_parsing=*/false>(
    absl::string_view str, FixedPointRepresentation<2>& parsed);
// Explicit template instantiations for BIGNUMERIC
template absl::Status ParseBigNumeric</*strict_parsing=*/true>(
    absl::string_view str, FixedPointRepresentation<4>& parsed);
template absl::Status ParseBigNumeric</*strict_parsing=*/false>(
    absl::string_view str, FixedPointRepresentation<4>& parsed);

}  // namespace zetasql
