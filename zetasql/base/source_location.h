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

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_SOURCE_LOCATION_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_SOURCE_LOCATION_H_

// API for capturing source-code location information.
// Based on http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2015/n4519.pdf.
//
// To define a function that has access to the source location of the
// callsite, define it with a parameter of type `zetasql_base::SourceLocation`.
// The caller can then invoke the function, passing `ZETASQL_LOC` as the
// argument.
//

#include <cstdint>

namespace zetasql_base {

// Class representing a specific location in the source code of a program.
// `zetasql_base::SourceLocation` is copyable.
//
// TODO: remove when absl::SourceLocation is available.
class SourceLocation {
 public:
  // Avoid this constructor; it populates the object with dummy values.
  constexpr SourceLocation()
      : line_(0),
        file_name_(nullptr) {}

  // Wrapper to invoke the private constructor below. This should only be used
  // by the `ZETASQL_LOC` macro, hence the name.
  static constexpr SourceLocation DoNotInvokeDirectly(std::uint_least32_t line,
                                                      const char* file_name) {
    return SourceLocation(line, file_name);
  }

  // The line number of the captured source location.
  constexpr std::uint_least32_t line() const { return line_; }

  // The file name of the captured source location.
  constexpr const char* file_name() const { return file_name_; }

  // `column()` and `function_name()` are omitted because we don't have a way to
  // support them.

 private:
  // Do not invoke this constructor directly. Instead, use the `ZETASQL_LOC`
  // macro below.
  //
  // `file_name` must outlive all copies of the `zetasql_base::SourceLocation`
  // object, so in practice it should be a std::string literal.
  constexpr SourceLocation(std::uint_least32_t line, const char* file_name)
      : line_(line),
        file_name_(file_name) {}

  friend constexpr int UseUnused() {
    static_assert(SourceLocation(0, nullptr).unused_column_ == 0,
                  "Use the otherwise-unused member.");
    static_assert(SourceLocation(0, nullptr).unused_function_name_ == nullptr,
                  "Use the otherwise-unused member.");
    return 0;
  }

  // "unused" members are present to minimize future changes in the size of this
  // type.
  std::uint_least32_t line_;
  std::uint_least32_t unused_column_ = 0;
  const char* file_name_;
  const char* unused_function_name_ = nullptr;
};

}  // namespace zetasql_base

// If a function takes an `zetasql_base::SourceLocation` parameter, pass this as
// the argument.
#define ZETASQL_LOC \
  ::zetasql_base::SourceLocation::DoNotInvokeDirectly(__LINE__, __FILE__)

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_SOURCE_LOCATION_H_
