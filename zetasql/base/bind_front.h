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

// `zetasql_base::bind_front()` returns a functor by binding a number of
// arguments to the front of a provided functor, allowing you to avoid known
// problems with `std::bind()`.  It is a form of partial function application
// https://en.wikipedia.org/wiki/Partial_application.
//
// Like `std::bind()` it is implicitly convertible to `std::function`.  In
// particular, it may be used as a simpler replacement for `std::bind()` in most
// cases, as it does not require  placeholders to be specified.  More
// importantly, it provides more reliable correctness guarantees than
// `std::bind()`.
//
// zetasql_base::bind_front(a...) can be seen as storing the results of
// std::make_tuple(a...).
//
// Example: Binding a free function.
//
//   int Minus(int a, int b) { return a - b; }
//
//   assert(zetasql_base::bind_front(Minus)(3, 2) == 3 - 2);
//   assert(zetasql_base::bind_front(Minus, 3)(2) == 3 - 2);
//   assert(zetasql_base::bind_front(Minus, 3, 2)() == 3 - 2);
//
// Example: Binding a member function.
//
//   struct Math {
//     int Double(int a) const { return 2 * a; }
//   };
//
//   Math math;
//
//   assert(zetasql_base::bind_front(&Math::Double)(&math, 3) == 2 * 3);
//   // Stores a pointer to math inside the functor.
//   assert(zetasql_base::bind_front(&Math::Double, &math)(3) == 2 * 3);
//   // Stores a copy of math inside the functor.
//   assert(zetasql_base::bind_front(&Math::Double, math)(3) == 2 * 3);
//   // Stores std::unique_ptr<Math> inside the functor.
//   assert(zetasql_base::bind_front(&Math::Double,
//                           std::unique_ptr<Math>(new Math))(3) == 2 * 3);
//
// Example: Using `zetasql_base::bind_front()`, instead of `std::bind()`, with
//          `std::function`.
//
//   class FileReader {
//    public:
//     void ReadFileAsync(const std::string& filename, std::string* content,
//                        const std::function<void()>& done) {
//       // Calls Executor::Schedule(std::function<void()>).
//       Executor::DefaultExecutor()->Schedule(
//           zetasql_base::bind_front(&FileReader::BlockingRead, this,
//                            filename, content, done));
//     }
//
//    private:
//     void BlockingRead(const std::string& filename, std::string* content,
//                       const std::function<void()>& done) {
//       ZETASQL_CHECK_OK(zetasql_base::GetContents(filename, content, {}));
//       done();
//     }
//   };
//
// `zetasql_base::bind_front()` stores bound arguments explicitly using the type
// passed rather than implicitly based on the type accepted by its functor.
//
// Example: Binding arguments explicitly.
//
//   void LogStringView(absl::string_view sv) {
//     LOG(INFO) << sv;
//   }
//
//   Executor* e = Executor::DefaultExecutor();
//   std::string s = "hello";
//   absl::string_view sv = s;
//
//   // zetasql_base::bind_front(LogStringView, arg) makes a copy of arg and
//   stores it.
//     // ERROR: dangling string_view.
//     e->Schedule(zetasql_base::bind_front(LogStringView, sv));
//     // OK: stores a copy of s.
//     e->Schedule(zetasql_base::bind_front(LogStringView, s));
//
// To store some of the arguments passed to `zetasql_base::bind_front()` by
// reference,
//  use std::ref()` and `std::cref()`.
//
// Example: Storing some of the bound arguments by reference.
//
//   class Service {
//    public:
//     void Serve(const Request& req, std::function<void()>* done) {
//       // The request protocol buffer won't be deleted until done is called.
//       // It's safe to store a reference to it inside the functor.
//       Executor::DefaultExecutor()->Schedule(
//           zetasql_base::bind_front(&Service::BlockingServe, this,
//                                    std::cref(req), done));
//     }
//
//    private:
//     void BlockingServe(const Request& req, std::function<void()>* done);
//   };
//
// Example: Storing bound arguments by reference.
//
//   void Print(const std::string& a, const std::string& b) { LOG(INFO) << a << b; }
//
//   std::string hi = "Hello, ";
//   vector<std::string> names = {"Chuk", "Gek"};
//   // Doesn't copy hi.
//   for_each(names.begin(), names.end(),
//            zetasql_base::bind_front(Print, std::ref(hi)));
//
//   // DO NOT DO THIS: the functor may outlive "hi", resulting in
//   // dangling references.
//   foo->DoInFuture(bind_front(Print, std::ref(hi), "Guest")); // BAD!
//   auto f = bind_front(Print, std::ref(hi), "Guest"); // BAD!

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_BIND_FRONT_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_BIND_FRONT_H_

#include "absl/utility/utility.h"
#include "zetasql/base/front_binder.h"

namespace zetasql_base {

// Binds the first N arguments of an invocable object and stores them by value,
// except types of std::reference_wrapper which are 'unwound' and stored by
// reference.
template <class F, class... BoundArgs>
constexpr zetasql_base_internal::bind_front_t<F, BoundArgs...> bind_front(
    F&& func, BoundArgs&&... args) {
  return zetasql_base_internal::bind_front_t<F, BoundArgs...>(
      absl::in_place, absl::forward<F>(func),
      absl::forward<BoundArgs>(args)...);
}

}  // namespace zetasql_base

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_BIND_FRONT_H_
