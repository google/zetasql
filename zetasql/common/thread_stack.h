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

#ifndef ZETASQL_COMMON_THREAD_STACK_H_
#define ZETASQL_COMMON_THREAD_STACK_H_

#include <string>
#include <utility>

namespace zetasql {

// Returns a string representing a stack trace of the current
// thread.
//
// This function is intended for logging/debugging purposes only; it is computed
// on a best-effort basis, and the format is unspecified.
//
std::string CurrentStackTrace();

}  // namespace zetasql

#endif  // ZETASQL_COMMON_THREAD_STACK_H_
