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

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUSOR_INTERNALS_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUSOR_INTERNALS_H_

#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"

namespace zetasql_base {

template <typename T>
class ABSL_MUST_USE_RESULT StatusOr;

namespace statusor_internal {

template <typename T, typename U>
using IsStatusOrConversionAmbiguous =
    absl::disjunction<std::is_constructible<T, StatusOr<U>&>,
                      std::is_constructible<T, const StatusOr<U>&>,
                      std::is_constructible<T, StatusOr<U>&&>,
                      std::is_constructible<T, const StatusOr<U>&&>,
                      std::is_convertible<StatusOr<U>&, T>,
                      std::is_convertible<const StatusOr<U>&, T>,
                      std::is_convertible<StatusOr<U>&&, T>,
                      std::is_convertible<const StatusOr<U>&&, T>>;

template <typename T, typename U>
using IsStatusOrConversionAssigmentAmbiguous =
    absl::disjunction<IsStatusOrConversionAmbiguous<T, U>,
                      std::is_assignable<T&, StatusOr<U>&>,
                      std::is_assignable<T&, const StatusOr<U>&>,
                      std::is_assignable<T&, StatusOr<U>&&>,
                      std::is_assignable<T&, const StatusOr<U>&&>>;

template <typename T, typename U>
struct IsAmbiguousStatusOrForInitialization
    :  // Strip const-value references from the type and check again, else
       // false_type.
       public absl::conditional_t<
           std::is_same<absl::remove_cv_t<absl::remove_reference_t<U>>,
                        U>::value,
           std::false_type,
           IsAmbiguousStatusOrForInitialization<
               T, absl::remove_cv_t<absl::remove_reference_t<U>>>> {};

template <typename T, typename U>
struct IsAmbiguousStatusOrForInitialization<T, StatusOr<U>>
    : public IsStatusOrConversionAmbiguous<T, U> {};

template <typename T, typename U>
using IsStatusOrDirectInitializationAmbiguous = absl::disjunction<
    std::is_same<StatusOr<T>, absl::remove_cv_t<absl::remove_reference_t<U>>>,
    std::is_same<absl::Status, absl::remove_cv_t<absl::remove_reference_t<U>>>,
    std::is_same<absl::in_place_t,
                 absl::remove_cv_t<absl::remove_reference_t<U>>>,
    IsAmbiguousStatusOrForInitialization<T, U>>;

template <typename T, typename U>
using IsStatusOrDirectInitializationValid = absl::disjunction<
    // The is same allows nested status ors to ignore this check iff they are
    // the same type.
    std::is_same<T, absl::remove_cv_t<absl::remove_reference_t<U>>>,
    absl::negation<IsStatusOrDirectInitializationAmbiguous<T, U>>>;

class Helper {
 public:
  // Move type-agnostic error handling to the .cc.
  static void HandleInvalidStatusCtorArg(absl::Status*);
  ABSL_ATTRIBUTE_NORETURN static void Crash(const absl::Status& status);
};

// Construct an instance of T in `p` through placement new, passing Args... to
// the constructor.
// This abstraction is here mostly for the gcc performance fix.
template <typename T, typename... Args>
void PlacementNew(void* p, Args&&... args) {
#if defined(__GNUC__) && !defined(__clang__)
  // Teach gcc that 'p' cannot be null, fixing code size issues.
  if (p == nullptr) __builtin_unreachable();
#endif
  new (p) T(std::forward<Args>(args)...);
}

// Helper base class to hold the data and all operations.
// We move all this to a base class to allow mixing with the appropriate
// TraitsBase specialization.
template <typename T>
class StatusOrData {
  template <typename U>
  friend class StatusOrData;

 public:
  StatusOrData() = delete;

  StatusOrData(const StatusOrData& other) {
    if (other.ok()) {
      MakeValue(other.data_);
      MakeStatus();
    } else {
      MakeStatus(other.status_);
    }
  }

  StatusOrData(StatusOrData&& other) noexcept {
    if (other.ok()) {
      MakeValue(std::move(other.data_));
      MakeStatus();
    } else {
      MakeStatus(std::move(other.status_));
    }
  }

  template <typename U>
  explicit StatusOrData(const StatusOrData<U>& other) {
    if (other.ok()) {
      MakeValue(other.data_);
      MakeStatus();
    } else {
      MakeStatus(other.status_);
    }
  }

  template <typename U>
  explicit StatusOrData(StatusOrData<U>&& other) {
    if (other.ok()) {
      MakeValue(std::move(other.data_));
      MakeStatus();
    } else {
      MakeStatus(std::move(other.status_));
    }
  }

  template <typename... Args>
  explicit StatusOrData(absl::in_place_t, Args&&... args)
      : data_(std::forward<Args>(args)...) {
    MakeStatus();
  }

  explicit StatusOrData(const T& value) : data_(value) {
    MakeStatus();
  }
  explicit StatusOrData(T&& value) : data_(std::move(value)) {
    MakeStatus();
  }

  explicit StatusOrData(const absl::Status& status) : status_(status) {
    EnsureNotOk();
  }
  explicit StatusOrData(absl::Status&& status) : status_(std::move(status)) {
    EnsureNotOk();
  }

  StatusOrData& operator=(const StatusOrData& other) {
    if (this == &other) return *this;
    if (other.ok())
      Assign(other.data_);
    else
      Assign(other.status_);
    return *this;
  }

  StatusOrData& operator=(StatusOrData&& other) {
    if (this == &other) return *this;
    if (other.ok())
      Assign(std::move(other.data_));
    else
      Assign(std::move(other.status_));
    return *this;
  }

  ~StatusOrData() {
    if (ok()) {
      status_.~Status();
      data_.~T();
    } else {
      status_.~Status();
    }
  }

  void Assign(const T& value) {
    if (ok()) {
      data_.~T();
      MakeValue(value);
    } else {
      MakeValue(value);
      status_ = absl::OkStatus();
    }
  }

  void Assign(T&& value) {
    if (ok()) {
      data_.~T();
      MakeValue(std::move(value));
    } else {
      MakeValue(std::move(value));
      status_ = absl::OkStatus();
    }
  }

  void Assign(const absl::Status& status) {
    Clear();
    status_ = status;
    EnsureNotOk();
  }

  void Assign(absl::Status&& status) {
    Clear();
    status_ = std::move(status);
    EnsureNotOk();
  }

  bool ok() const { return status_.ok(); }

 protected:
  // status_ will always be active after the constructor.
  // We make it a union to be able to initialize exactly how we need without
  // waste.
  // Eg. in the copy constructor we use the default constructor of
  // Status in the ok() path to avoid an extra Ref call.
  union {
    absl::Status status_;
  };

  // data_ is active iff status_.ok()==true
  struct Dummy {};
  union {
    // When T is const, we need some non-const object we can cast to void* for
    // the placement new. dummy_ is that object.
    Dummy dummy_;
    T data_;
  };

  void Clear() {
    if (ok()) data_.~T();
  }

  void EnsureOk() const {
    if (!ok()) Helper::Crash(status_);
  }

  void EnsureNotOk() {
    if (ok()) Helper::HandleInvalidStatusCtorArg(&status_);
  }

  // Construct the value (ie. data_) through placement new with the passed
  // argument.
  template <typename Arg>
  void MakeValue(Arg&& arg) {
    statusor_internal::PlacementNew<T>(&dummy_, std::forward<Arg>(arg));
  }

  // Construct the status (ie. status_) through placement new with the passed
  // argument.
  template <typename... Args>
  void MakeStatus(Args&&... args) {
    statusor_internal::PlacementNew<absl::Status>(&status_,
                                                  std::forward<Args>(args)...);
  }
};

// Helper base class to allow implicitly deleted constructors and assignment
// operations in StatusOr.
// TraitsBase will explicitly delete what it can't support and StatusOr will
// inherit that behavior implicitly.
template <bool Copy, bool Move>
struct TraitsBase {
  TraitsBase() = default;
  TraitsBase(const TraitsBase&) = default;
  TraitsBase(TraitsBase&&) = default;
  TraitsBase& operator=(const TraitsBase&) = default;
  TraitsBase& operator=(TraitsBase&&) = default;
};

template <>
struct TraitsBase<false, true> {
  TraitsBase() = default;
  TraitsBase(const TraitsBase&) = delete;
  TraitsBase(TraitsBase&&) = default;
  TraitsBase& operator=(const TraitsBase&) = delete;
  TraitsBase& operator=(TraitsBase&&) = default;
};

template <>
struct TraitsBase<false, false> {
  TraitsBase() = default;
  TraitsBase(const TraitsBase&) = delete;
  TraitsBase(TraitsBase&&) = delete;
  TraitsBase& operator=(const TraitsBase&) = delete;
  TraitsBase& operator=(TraitsBase&&) = delete;
};

}  // namespace statusor_internal

}  // namespace zetasql_base

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_STATUSOR_INTERNALS_H_
