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
#include "zetasql/base/bind_front.h"

#include <stddef.h>
#include <functional>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/memory/memory.h"

namespace {

char CharAt(const char* s, size_t index) { return s[index]; }

TEST(BindTest, Meta) {
  static_assert(
      std::is_same<
          zetasql_base::zetasql_base_internal::unwrap_ref_decay_t<int&>,
          int>::value,
      "reference should decay as std::decay");
  static_assert(
      std::is_same<zetasql_base::zetasql_base_internal::unwrap_ref_decay_t<
                       std::reference_wrapper<int>>,
                   int&>::value,
      "reference_wrapper should decay to a reference");

  static_assert(
      std::is_same<zetasql_base::zetasql_base_internal::unwrap_ref_decay_t<
                       std::reference_wrapper<std::reference_wrapper<int>>>,
                   std::reference_wrapper<int>&>::value,
      "reference_wrapper should only decay once");
}

TEST(BindTest, Basics) {
  EXPECT_EQ('C', zetasql_base::bind_front(CharAt)("ABC", 2));
  EXPECT_EQ('C', zetasql_base::bind_front(CharAt, "ABC")(2));
  EXPECT_EQ('C', zetasql_base::bind_front(CharAt, "ABC", 2)());
}

TEST(BindTest, Lambda) {
  auto lambda = [](int x, int y, int z) { return x + y + z; };
  EXPECT_EQ(6, zetasql_base::bind_front(lambda)(1, 2, 3));
  EXPECT_EQ(6, zetasql_base::bind_front(lambda, 1)(2, 3));
  EXPECT_EQ(6, zetasql_base::bind_front(lambda, 1, 2)(3));
  EXPECT_EQ(6, zetasql_base::bind_front(lambda, 1, 2, 3)());
}

struct Functor {
  std::string operator()() & { return "&"; }
  std::string operator()() const& { return "const&"; }
  std::string operator()() && { return "&&"; }
  std::string operator()() const && { return "const&&"; }
};

TEST(BindTest, PerfectForwardingOfBoundArgs) {
  auto f = zetasql_base::bind_front(Functor());
  const auto& cf = f;
  EXPECT_EQ("&", f());
  EXPECT_EQ("const&", cf());
  EXPECT_EQ("&&", std::move(f)());
  EXPECT_EQ("const&&", std::move(cf)());
}

struct ArgDescribe {
  std::string operator()(int&) const { return "&"; }             // NOLINT
  std::string operator()(const int&) const { return "const&"; }  // NOLINT
  std::string operator()(int&&) const { return "&&"; }
  std::string operator()(const int&&) const { return "const&&"; }
};

TEST(BindTest, PerfectForwardingOfFreeArgs) {
  ArgDescribe f;
  int i;
  EXPECT_EQ("&", zetasql_base::bind_front(f)(static_cast<int&>(i)));
  EXPECT_EQ("const&", zetasql_base::bind_front(f)(static_cast<const int&>(i)));
  EXPECT_EQ("&&", zetasql_base::bind_front(f)(static_cast<int&&>(i)));
  EXPECT_EQ("const&&",
            zetasql_base::bind_front(f)(static_cast<const int&&>(i)));
}

struct NonCopyableFunctor {
  NonCopyableFunctor() = default;
  NonCopyableFunctor(const NonCopyableFunctor&) = delete;
  NonCopyableFunctor& operator=(const NonCopyableFunctor&) = delete;
  const NonCopyableFunctor* operator()() const { return this; }
};

TEST(BindTest, RefToFunctor) {
  // It won't copy/move the functor and use the original object.
  NonCopyableFunctor ncf;
  auto bound_ncf = zetasql_base::bind_front(std::ref(ncf));
  auto bound_ncf_copy = bound_ncf;
  EXPECT_EQ(&ncf, bound_ncf_copy());
}

struct Struct {
  std::string value;
};

TEST(BindTest, StoreByCopy) {
  Struct s = {"hello"};
  auto f = zetasql_base::bind_front(&Struct::value, s);
  auto g = f;
  EXPECT_EQ("hello", f());
  EXPECT_EQ("hello", g());
  EXPECT_NE(&s.value, &f());
  EXPECT_NE(&s.value, &g());
  EXPECT_NE(&g(), &f());
}

struct NonCopyable {
  explicit NonCopyable(const std::string& s) : value(s) {}
  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;

  std::string value;
};

const std::string& GetNonCopyableValue(const NonCopyable& n) {
  return n.value;
}

TEST(BindTest, StoreByRef) {
  NonCopyable s("hello");
  auto f = zetasql_base::bind_front(&GetNonCopyableValue, std::ref(s));
  EXPECT_EQ("hello", f());
  EXPECT_EQ(&s.value, &f());
  auto g = std::move(f);  // NOLINT
  EXPECT_EQ("hello", g());
  EXPECT_EQ(&s.value, &g());
  s.value = "goodbye";
  EXPECT_EQ("goodbye", g());
}

TEST(BindTest, StoreByCRef) {
  NonCopyable s("hello");
  auto f = zetasql_base::bind_front(&GetNonCopyableValue, std::cref(s));
  EXPECT_EQ("hello", f());
  EXPECT_EQ(&s.value, &f());
  auto g = std::move(f);  // NOLINT
  EXPECT_EQ("hello", g());
  EXPECT_EQ(&s.value, &g());
  s.value = "goodbye";
  EXPECT_EQ("goodbye", g());
}

const std::string& GetNonCopyableValueByWrapper(
    std::reference_wrapper<NonCopyable> n) {
  return n.get().value;
}

TEST(BindTest, StoreByRefInvokeByWrapper) {
  NonCopyable s("hello");
  auto f = zetasql_base::bind_front(GetNonCopyableValueByWrapper, std::ref(s));
  EXPECT_EQ("hello", f());
  EXPECT_EQ(&s.value, &f());
  auto g = std::move(f);
  EXPECT_EQ("hello", g());
  EXPECT_EQ(&s.value, &g());
  s.value = "goodbye";
  EXPECT_EQ("goodbye", g());
}

TEST(BindTest, StoreByPointer) {
  NonCopyable s("hello");
  auto f = zetasql_base::bind_front(&NonCopyable::value, &s);
  EXPECT_EQ("hello", f());
  EXPECT_EQ(&s.value, &f());
  auto g = std::move(f);
  EXPECT_EQ("hello", g());
  EXPECT_EQ(&s.value, &g());
}

int Sink(std::unique_ptr<int> p) {
  return *p;
}

std::unique_ptr<int> Factory(int n) { return absl::make_unique<int>(n); }

TEST(BindTest, NonCopyableArg) {
  EXPECT_EQ(42, zetasql_base::bind_front(Sink)(absl::make_unique<int>(42)));
  EXPECT_EQ(42, zetasql_base::bind_front(Sink, absl::make_unique<int>(42))());
}

TEST(BindTest, NonCopyableResult) {
  EXPECT_THAT(zetasql_base::bind_front(Factory)(42), ::testing::Pointee(42));
  EXPECT_THAT(zetasql_base::bind_front(Factory, 42)(), ::testing::Pointee(42));
}

// is_copy_constructible<FalseCopyable<unique_ptr<T>> is true but an attempt to
// instantiate the copy constructor leads to a compile error. This is similar
// to how standard containers behave.
template <class T>
struct FalseCopyable {
  FalseCopyable() {}
  FalseCopyable(const FalseCopyable& other) : m(other.m) {}
  FalseCopyable(FalseCopyable&& other) : m(std::move(other.m)) {}
  T m;
};

int GetMember(FalseCopyable<std::unique_ptr<int>> x) { return *x.m; }

TEST(BindTest, WrappedMoveOnly) {
  FalseCopyable<std::unique_ptr<int>> x;
  x.m = absl::make_unique<int>(42);
  auto f = zetasql_base::bind_front(&GetMember, std::move(x));
  EXPECT_EQ(42, std::move(f)());
}

TEST(BindTest, ConstExpr) {
  constexpr auto f = zetasql_base::bind_front(CharAt);
  EXPECT_EQ(f("ABC", 1), 'B');
  static constexpr char data[] = "DEF";
  constexpr auto g = zetasql_base::bind_front(CharAt, data);
  EXPECT_EQ(g(1), 'E');
}

}  // namespace
