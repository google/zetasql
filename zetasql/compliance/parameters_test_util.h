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

#ifndef ZETASQL_COMPLIANCE_PARAMETERS_TEST_UTIL_H_
#define ZETASQL_COMPLIANCE_PARAMETERS_TEST_UTIL_H_

#include <map>
#include <memory>
#include <string>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
class ReferenceDriver;
class TestDatabase;

// Parses query parameters from a string in the format that appears in
// ZetaSQL compliance test files.
absl::Status ParseTestFileParameters(absl::string_view param_string,
                                     ReferenceDriver* reference_driver,
                                     TypeFactory* type_factory,
                                     std::map<std::string, Value>* parameters);

// A helper class which wraps ParseTestFileParameters and also owns the
// TypeFactory and a ReferenceDriver with an empty database. This lets
// external tools parse parameters without exposing the ReferenceDriver.
class TestFileParameterParser {
 public:
  TestFileParameterParser();
  ~TestFileParameterParser();

  // Initializes the parameter parser with a TestDatabase that may contain
  // proto and enum type information.
  absl::Status Init(const TestDatabase& database);

  // Parses a set of parameters from 'param_string' populating 'parameters'.
  absl::Status Parse(absl::string_view param_string,
                     std::map<std::string, Value>* parameters);

 private:
  std::unique_ptr<ReferenceDriver> reference_impl_;
  std::unique_ptr<TypeFactory> type_factory_;
  bool initialized_ = false;
};

}  // namespace zetasql

#endif  // ZETASQL_COMPLIANCE_PARAMETERS_TEST_UTIL_H_
