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

// Utilities for percentiles.

#ifndef ZETASQL_PUBLIC_FUNCTIONS_PERCENTILE_H_
#define ZETASQL_PUBLIC_FUNCTIONS_PERCENTILE_H_

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <type_traits>
#include <vector>

#include <cstdint>
#include "absl/strings/string_view.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

class PercentileEvaluator {
 public:
  // percentile must be in [0, 1].
  static zetasql_base::StatusOr<PercentileEvaluator> Create(double percentile);

  // Computes the integral part and the fractional part of
  // <max_index> * <percentile_>.
  // The returned value (returned_index) = floor(<max_index> * <percentile_>).
  // <right_weight> = <max_index> * <percentile_> - (returned_index).
  // <left_weight> = 1 - <right_weight>.
  // For a sorted array X with size = <max_index> + 1, the percentile value
  // of X = X[(returned_index)] * <left_weight> +
  //        X[(returned_index) + 1] * <right_weight>.
  // (note: it is possible that (returned_index) = <max_index> and
  // <right_weight> = 0)
  //
  // This function handles <max_index> > std::numeric_limits<int32_t>::max().
  // (returned_index) is precise; <left_weight> and <right_weight> are as
  // precise as long double supports.
  //
  // The reason why not return only <right_weight> is that this function
  // computes both weights in a way to minimize the error for both; always
  // calculating <left_weight> as 1 - <right_weight> or vice versa could result
  // in high relative error when one of them is close to 0. This is also why
  // 'long double' is used rather than 'double'.
  size_t ComputePercentileIndex(size_t max_index,
                                long double* left_weight,
                                long double* right_weight) const;

  // Computes PERCENTILE_CONT(x, percentile) from values in
  // [nonnull_values_begin, nonnull_values_end) PLUS <num_nulls> nulls,
  // according to docs/document/d/1XECBsOd5pzMtnJBIMAS8jwiWa7RMPoPSayUBuC9Mm4g.
  //
  // Itr must be random accessible, and it must be dereferenced to double or a
  // type that can be implicitly cast to double.
  //
  // [nonnull_values_begin, nonnull_values_end) may contain NaNs but not NULLs.
  //
  // If <sorted> is false:
  //   It means [nonnull_values_begin, nonnull_values_end) is not sorted. Itr
  //   must be mutable, and this method will reorder some of the elements in
  //   this range. This method has a time complexity of O(n) where
  //   n = nonnull_values_end - nonnull_values_begin.
  // If <sorted> is true:
  //   It means [nonnull_values_begin, nonnull_values_end) is sorted in
  //   ascending order. If this range contains NaNs, NaNs must be placed in the
  //   beginning. This method has a constant time complexity.
  //
  // If the percentile value is null, returns false and leaves *result
  // unchanged. Otherwise returns true and populates *result with the computed
  // percentile value.
  //
  // This method is not ideal for handling sliding windows; an alternative is to
  // use PercentileHeap, which does not support n >
  // std::numeric_limits<int32_t>::max(), though.
  template <bool sorted, typename Itr>
  bool ComputePercentileCont(
      Itr nonnull_values_begin, Itr nonnull_values_end,
      size_t num_nulls, double* result) const {
    size_t num_nonnull_values = nonnull_values_end - nonnull_values_begin;
    if (num_nonnull_values == 0) {
      return false;
    }
    long double left_weight = 0;
    long double right_weight = 0;
    size_t index = ComputePercentileIndex(
        num_nonnull_values - 1 + num_nulls,
        &left_weight, &right_weight);
    using Accessor = ElementAccessor<double, sorted>;
    if (index >= num_nulls) {
      // The percentile is within normal values.
      index -= num_nulls;
      const size_t num_nans = Accessor::template PartitionNaNs<Itr>(
          nonnull_values_begin, nonnull_values_end);
      *result = *Accessor::template GetNthElement<Itr>(
          nonnull_values_begin, nonnull_values_end, index, num_nans);
      if (right_weight > 0) {
        const double right_value = *Accessor::template GetNthElement<Itr>(
                nonnull_values_begin, nonnull_values_end, index + 1, num_nans);
        *result = left_weight * (*result) + right_weight * right_value;
      }
    } else if (index == num_nulls - 1 && right_weight > 0) {
      // The percentile is between a null value and the minimum normal value.
      const size_t num_nans = Accessor::template PartitionNaNs<Itr>(
          nonnull_values_begin, nonnull_values_end);
      *result = *Accessor::template GetNthElement<Itr>(
          nonnull_values_begin, nonnull_values_end, 0, num_nans);
    } else {
      // The percentile is within null values.
      return false;
    }
    return true;
  }

  // Computes PERCENTILE_DISC(x, percentile) from values in
  // [nonnull_values_begin, nonnull_values_end) PLUS <num_nulls> nulls,
  // according to docs/document/d/1XECBsOd5pzMtnJBIMAS8jwiWa7RMPoPSayUBuC9Mm4g.
  //
  // T must be comparable. Itr must be random accessible, and it must be
  // dereferenced to T or a type that can be implicitly cast to T.
  //
  // [nonnull_values_begin, nonnull_values_end) may contain NaNs but not NULLs.
  //
  // If <sorted> is false:
  //   It means [nonnull_values_begin, nonnull_values_end) is not sorted. Itr
  //   must be mutable, and this method will reorder some of the elements in
  //   this range. This method has a time complexity of O(n) where
  //   n = nonnull_values_end - nonnull_values_begin.
  // If <sorted> is true:
  //   It means [nonnull_values_begin, nonnull_values_end) is sorted in
  //   ascending order. If this range contains NaNs, NaNs must be placed in the
  //   beginning. This method has a constant time complexity.
  //
  // If the percentile value is null, returns nonnull_values_end. Otherwise
  // returns the iterator pointing to the percentile value in
  // [nonnull_values_begin, nonnull_values_end). If the percentile lands between
  // two values, the iterator to the second ordered value is returned.
  //
  // This method is not ideal for handling sliding windows; an alternative is to
  // use PercentileHeap, which does not support n >
  // std::numeric_limits<int32_t>::max(), though.
  template <typename T, bool sorted, typename Itr>
  Itr ComputePercentileDisc(
      Itr nonnull_values_begin, Itr nonnull_values_end,
      size_t num_nulls) const {
    size_t num_nonnull_values = nonnull_values_end - nonnull_values_begin;
    if (num_nonnull_values == 0) {
      return nonnull_values_end;
    }
    long double left_weight = 0;
    long double right_weight = 0;
    size_t index = ComputePercentileIndex(num_nonnull_values + num_nulls,
                                          &left_weight, &right_weight);
    if (index > 0 && right_weight == 0) {
      --index;
    }
    if (index >= num_nulls) {
      // The percentile is within normal values.
      index -= num_nulls;
      using Accessor = ElementAccessor<T, sorted>;
      const size_t num_nans = Accessor::template PartitionNaNs<Itr>(
          nonnull_values_begin, nonnull_values_end);
      return Accessor::template GetNthElement<Itr>(
          nonnull_values_begin, nonnull_values_end, index, num_nans);
    }
    return nonnull_values_end;
  }

 private:
  template <typename T, bool sorted>
  struct ElementAccessor {
    static constexpr bool kIsFloatingPoint = std::is_floating_point<T>::value;
    static constexpr bool kNeedsPartition = !sorted && kIsFloatingPoint;

    struct IsNaN {
      bool operator()(T value) const {
        return std::isnan(value);
      }
    };

    template <typename Itr>
    static size_t PartitionNaNs(
        typename std::enable_if<kNeedsPartition, Itr>::type begin, Itr end) {
      return std::partition(begin, end, IsNaN()) - begin;
    }
    template <typename Itr>
    static size_t PartitionNaNs(
        typename std::enable_if<!kNeedsPartition, Itr>::type begin, Itr end) {
      return 0;
    }

    template <typename Itr>
    static Itr GetNthElement(
        typename std::enable_if<sorted, Itr>::type begin, Itr end,
        size_t index, size_t num_nans) {
      return begin + index;
    }

    template <typename Itr>
    static Itr GetNthElement(
        typename std::enable_if<!sorted, Itr>::type begin, Itr end,
        size_t index, size_t num_nans) {
      Itr itr = begin + index;
      if (index >= num_nans) {
        std::nth_element(begin + num_nans, itr, end);
      }
      return itr;
    }
  };

  PercentileEvaluator(double percentile, int64_t percentile_mantissa,
                      int percentile_exponent)
      : percentile_(percentile), percentile_mantissa_(percentile_mantissa),
        percentile_exponent_(percentile_exponent),
        num_fractional_bits_(-percentile_exponent) {
  }

  const double percentile_;
  const int64_t percentile_mantissa_;
  const int percentile_exponent_;
  const int num_fractional_bits_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_PERCENTILE_H_
