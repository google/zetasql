/*
 *
 * Copyright 2018 The Abseil Authors.
 * Copyright 2019 ZetaSQL Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Implementation details for `zetasql_base::bind_front()`.

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_FRONT_BINDER_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_FRONT_BINDER_H_

#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "zetasql/base/compressed_tuple.h"
#include "zetasql/base/invoke.h"

namespace zetasql_base {
namespace zetasql_base_internal {

template <typename T>
struct unwrap_ref_decay;

template <typename T>
struct unwrap_ref_decay<std::reference_wrapper<T>> {
  using type = T&;
};

template <typename T>
struct unwrap_ref_decay
    : absl::conditional_t<!std::is_same<absl::decay_t<T>, T>::value,
                          unwrap_ref_decay<absl::decay_t<T>>, std::decay<T>> {};

template <typename T>
using unwrap_ref_decay_t = typename unwrap_ref_decay<T>::type;

// Invoke the method, expanding the tuple of bound arguments.
template <class Tuple, size_t... Idx, class... Args>
auto Apply(Tuple&& bound, absl::index_sequence<Idx...>, Args&&... free)
    -> decltype(zetasql_base_internal::Invoke(
        absl::forward<Tuple>(bound).template get<Idx>()...,
        absl::forward<Args>(free)...)) {
  return zetasql_base_internal::Invoke(
      absl::forward<Tuple>(bound).template get<Idx>()...,
      absl::forward<Args>(free)...);
}

template <class... BoundArgs>
class FrontBinder {
  using BoundArgsT = zetasql_base_internal::CompressedTuple<BoundArgs...>;
  using Idx = absl::make_index_sequence<sizeof...(BoundArgs)>;

  BoundArgsT bound_args_;

 public:
  template <class... Ts>
  constexpr explicit FrontBinder(absl::in_place_t, Ts&&... ts)
      : bound_args_(absl::forward<Ts>(ts)...) {}

  template <class... FreeArgs>
  auto operator()(FreeArgs&&... free_args) & -> decltype(
      zetasql_base_internal::Apply(bound_args_, Idx(),
                                   absl::forward<FreeArgs>(free_args)...)) {
    return zetasql_base_internal::Apply(bound_args_, Idx(),
                                        absl::forward<FreeArgs>(free_args)...);
  }

  template <class... FreeArgs>
  auto operator()(FreeArgs&&... free_args) const& -> decltype(
      zetasql_base_internal::Apply(bound_args_, Idx(),
                                   absl::forward<FreeArgs>(free_args)...)) {
    return zetasql_base_internal::Apply(bound_args_, Idx(),
                                        absl::forward<FreeArgs>(free_args)...);
  }

  template <class... FreeArgs>
  auto operator()(FreeArgs&&... free_args) && -> decltype(
      zetasql_base_internal::Apply(absl::move(bound_args_), Idx(),
                                   absl::forward<FreeArgs>(free_args)...)) {
    // This overload is called when *this is an rvalue. If some of the bound
    // arguments are stored by value or rvalue reference, we move them.
    return zetasql_base_internal::Apply(absl::move(bound_args_), Idx(),
                                        absl::forward<FreeArgs>(free_args)...);
  }

  template <class... FreeArgs>
  auto operator()(FreeArgs&&... free_args) const&& -> decltype(
      zetasql_base_internal::Apply(absl::move(bound_args_), Idx(),
                                   absl::forward<FreeArgs>(free_args)...)) {
    // This overload is called when *this is an rvalue. If some of the bound
    // arguments are stored by value or rvalue reference, we move them.
    return zetasql_base_internal::Apply(absl::move(bound_args_), Idx(),
                                        absl::forward<FreeArgs>(free_args)...);
  }
};

template <class F, class... BoundArgs>
using bind_front_t =
    FrontBinder<absl::decay_t<F>, unwrap_ref_decay_t<BoundArgs>...>;

}  // namespace zetasql_base_internal
}  // namespace zetasql_base

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_FRONT_BINDER_H_
