//
// Copyright 2018 ZetaSQL Authors
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

// Some of these functions are faster than their built-in alternatives. Some
// have a more friendly API and are easier to use.
#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_STL_UTIL_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_STL_UTIL_H_

#include <algorithm>
#include <cassert>
#include <functional>
#include <iterator>
#include <type_traits>

namespace zetasql_base {

namespace stl_util_internal {

// Like std::less, but allows heterogeneous arguments.
struct TransparentLess {
  template <typename T>
  bool operator()(const T& a, const T& b) const {
    // std::less is better than '<' here, because it can order pointers.
    return std::less<T>()(a, b);
  }
  template <typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    return a < b;
  }
};

}  // namespace stl_util_internal

// STLSetDifference:
//
//     In1 STLSetDifference(a, b);
//     In1 STLSetDifference(a, b, compare);
//     void STLSetDifference(a, b, &out);
//     void STLSetDifference(a, b, &out, compare);
//     Out STLSetDifferenceAs<Out>(a, b);
//     Out STLSetDifferenceAs<Out>(a, b, compare);
//
// Appends the elements in "a" that are not in "b" to an output container.
// Optionally specify a comparator, or '<' is used by default.  Both input
// containers must be sorted with respect to the comparator.  If specified,
// the output container must be distinct from both "a" and "b".
//
// If an output container pointer is not given, a container will be returned
// by value. The return type can be explicitly specified by calling
// STLSetDifferenceAs, but it defaults to the type of argument "a".
//
// See std::set_difference() for details on how set difference is computed.
//
// The form taking 4 arguments. All other forms call into this one.
// Explicit comparator, append to output container.
template<typename In1, typename In2, typename Out, typename Compare>
void STLSetDifference(const In1& a, const In2& b, Out* out, Compare compare) {
  assert(std::is_sorted(a.begin(), a.end(), compare));
  assert(std::is_sorted(b.begin(), b.end(), compare));
  assert(static_cast<const void*>(&a) != static_cast<const void*>(out));
  assert(static_cast<const void*>(&b) != static_cast<const void*>(out));
  std::set_difference(a.begin(), a.end(), b.begin(), b.end(),
                      std::inserter(*out, out->end()), compare);
}
// Append to output container, Implicit comparator.
// Note: The 'enable_if' keeps this overload from participating in
// overload resolution if 'out' is a function pointer, gracefully forcing
// the 3-argument overload that treats the third argument as a comparator.
template <typename In1, typename In2, typename Out>
typename std::enable_if<!std::is_function<Out>::value, void>::type
STLSetDifference(const In1& a, const In2& b, Out* out) {
  STLSetDifference(a, b, out,
                   zetasql_base::stl_util_internal::TransparentLess());
}
// Explicit comparator, explicit return type.
template<typename Out, typename In1, typename In2, typename Compare>
Out STLSetDifferenceAs(const In1& a, const In2& b, Compare compare) {
  Out out;
  STLSetDifference(a, b, &out, compare);
  return out;
}
// Implicit comparator, explicit return type.
template<typename Out, typename In1, typename In2>
Out STLSetDifferenceAs(const In1& a, const In2& b) {
  return STLSetDifferenceAs<Out>(
      a, b, zetasql_base::stl_util_internal::TransparentLess());
}
// Explicit comparator, implicit return type.
template<typename In1, typename In2, typename Compare>
In1 STLSetDifference(const In1& a, const In2& b, Compare compare) {
  return STLSetDifferenceAs<In1>(a, b, compare);
}
// Implicit comparator, implicit return type.
template<typename In1, typename In2>
In1 STLSetDifference(const In1& a, const In2& b) {
  return STLSetDifference(a, b,
                          zetasql_base::stl_util_internal::TransparentLess());
}
template<typename In1>
In1 STLSetDifference(const In1& a, const In1& b) {
  return STLSetDifference(a, b,
                          zetasql_base::stl_util_internal::TransparentLess());
}

// STLSetUnion:
//
//     In1 STLSetUnion(a, b);
//     In1 STLSetUnion(a, b, compare);
//     void STLSetUnion(a, b, &out);
//     void STLSetUnion(a, b, &out, compare);
//     Out STLSetUnionAs<Out>(a, b);
//     Out STLSetUnionAs<Out>(a, b, compare);
// Appends the elements in one or both of the input containers to output
// container "out". Both input containers must be sorted with operator '<',
// or with the comparator if specified. "out" must be distinct from both "a"
// and "b".
//
// See std::set_union() for how set union is computed.
template<typename In1, typename In2, typename Out, typename Compare>
void STLSetUnion(const In1& a, const In2& b, Out* out, Compare compare) {
  assert(std::is_sorted(a.begin(), a.end(), compare));
  assert(std::is_sorted(b.begin(), b.end(), compare));
  assert(static_cast<const void*>(&a) != static_cast<const void*>(out));
  assert(static_cast<const void*>(&b) != static_cast<const void*>(out));
  std::set_union(a.begin(), a.end(), b.begin(), b.end(),
                 std::inserter(*out, out->end()), compare);
}
// Note: The 'enable_if' keeps this overload from participating in
// overload resolution if 'out' is a function pointer, gracefully forcing
// the 3-argument overload that treats the third argument as a comparator.
template <typename In1, typename In2, typename Out>
typename std::enable_if<!std::is_function<Out>::value, void>::type
STLSetUnion(const In1& a, const In2& b, Out* out) {
  return STLSetUnion(a, b, out,
                     zetasql_base::stl_util_internal::TransparentLess());
}
template<typename Out, typename In1, typename In2, typename Compare>
Out STLSetUnionAs(const In1& a, const In2& b, Compare compare) {
  Out out;
  STLSetUnion(a, b, &out, compare);
  return out;
}
template<typename Out, typename In1, typename In2>
Out STLSetUnionAs(const In1& a, const In2& b) {
  return STLSetUnionAs<Out>(a, b,
                            zetasql_base::stl_util_internal::TransparentLess());
}
template<typename In1, typename In2, typename Compare>
In1 STLSetUnion(const In1& a, const In2& b, Compare compare) {
  return STLSetUnionAs<In1>(a, b, compare);
}
template<typename In1, typename In2>
In1 STLSetUnion(const In1& a, const In2& b) {
  return STLSetUnion(a, b, zetasql_base::stl_util_internal::TransparentLess());
}
template<typename In1>
In1 STLSetUnion(const In1& a, const In1& b) {
  return STLSetUnion(a, b, zetasql_base::stl_util_internal::TransparentLess());
}

// STLSetIntersection:
//
//     In1 STLSetIntersection(a, b);
//     In1 STLSetIntersection(a, b, compare);
//     void STLSetIntersection(a, b, &out);
//     void STLSetIntersection(a, b, &out, compare);
//     Out STLSetIntersectionAs<Out>(a, b);
//     Out STLSetIntersectionAs<Out>(a, b, compare);
//
// Appends the elements that are in both "a" and "b" to output container
// "out".  Both input containers must be sorted with operator '<' or with
// "compare" if specified. "out" must be distinct from both "a" and "b".
//
// See std::set_intersection() for how set intersection is computed.
template<typename In1, typename In2, typename Out, typename Compare>
void STLSetIntersection(const In1& a, const In2& b, Out* out, Compare compare) {
  assert(std::is_sorted(a.begin(), a.end(), compare));
  assert(std::is_sorted(b.begin(), b.end(), compare));
  assert(static_cast<const void*>(&a) != static_cast<const void*>(out));
  assert(static_cast<const void*>(&b) != static_cast<const void*>(out));
  std::set_intersection(a.begin(), a.end(), b.begin(), b.end(),
                        std::inserter(*out, out->end()), compare);
}
// Note: The 'enable_if' keeps this overload from participating in
// overload resolution if 'out' is a function pointer, gracefully forcing
// the 3-argument overload that treats the third argument as a comparator.
template <typename In1, typename In2, typename Out>
typename std::enable_if<!std::is_function<Out>::value, void>::type
STLSetIntersection(const In1& a, const In2& b, Out* out) {
  return STLSetIntersection(a, b, out,
                            zetasql_base::stl_util_internal::TransparentLess());
}
template<typename Out, typename In1, typename In2, typename Compare>
Out STLSetIntersectionAs(const In1& a, const In2& b, Compare compare) {
  Out out;
  STLSetIntersection(a, b, &out, compare);
  return out;
}
template<typename Out, typename In1, typename In2>
Out STLSetIntersectionAs(const In1& a, const In2& b) {
  return STLSetIntersectionAs<Out>(
      a, b, zetasql_base::stl_util_internal::TransparentLess());
}
template<typename In1, typename In2, typename Compare>
In1 STLSetIntersection(const In1& a, const In2& b, Compare compare) {
  return STLSetIntersectionAs<In1>(a, b, compare);
}
template<typename In1, typename In2>
In1 STLSetIntersection(const In1& a, const In2& b) {
  return STLSetIntersection(a, b,
                            zetasql_base::stl_util_internal::TransparentLess());
}
template<typename In1>
In1 STLSetIntersection(const In1& a, const In1& b) {
  return STLSetIntersection(a, b,
                            zetasql_base::stl_util_internal::TransparentLess());
}

// SortedRangesHaveIntersection:
//
//     bool SortedRangesHaveIntersection(begin1, end1, begin2, end2);
//     bool SortedRangesHaveIntersection(begin1, end1, begin2, end2,
//                                       comparator);
//
// Returns true iff any element in the sorted range [begin1, end1) is
// equivalent to any element in the sorted range [begin2, end2). The iterators
// themselves do not have to be the same type, but the value types must be
// sorted either by the specified comparator, or by '<' if no comparator is
// given.
// [Two elements a,b are considered equivalent if !(a < b) && !(b < a) ].
template<typename InputIterator1, typename InputIterator2, typename Comp>
bool SortedRangesHaveIntersection(InputIterator1 begin1, InputIterator1 end1,
                                  InputIterator2 begin2, InputIterator2 end2,
                                  Comp comparator) {
  assert(std::is_sorted(begin1, end1, comparator));
  assert(std::is_sorted(begin2, end2, comparator));
  while (begin1 != end1 && begin2 != end2) {
    if (comparator(*begin1, *begin2)) {
      ++begin1;
      continue;
    }
    if (comparator(*begin2, *begin1)) {
      ++begin2;
      continue;
    }
    return true;
  }
  return false;
}
template<typename InputIterator1, typename InputIterator2>
bool SortedRangesHaveIntersection(InputIterator1 begin1, InputIterator1 end1,
                                  InputIterator2 begin2, InputIterator2 end2) {
  return SortedRangesHaveIntersection(
      begin1, end1, begin2, end2,
      zetasql_base::stl_util_internal::TransparentLess());
}

// Returns true iff the ordered containers 'in1' and 'in2' have a non-empty
// intersection. The container elements do not have to be the same type, but the
// elements must be sorted either by the specified comparator, or by '<' if no
// comparator is given.
template <typename In1, typename In2, typename Comp>
bool SortedContainersHaveIntersection(const In1& in1, const In2& in2,
                                      Comp comparator) {
  return SortedRangesHaveIntersection(in1.begin(), in1.end(), in2.begin(),
                                      in2.end(), comparator);
}
template <typename In1, typename In2>
bool SortedContainersHaveIntersection(const In1& in1, const In2& in2) {
  return SortedContainersHaveIntersection(
      in1, in2, zetasql_base::stl_util_internal::TransparentLess());
}

}  // namespace zetasql_base

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_STL_UTIL_H_
