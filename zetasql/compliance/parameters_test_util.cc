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

#include "zetasql/compliance/parameters_test_util.h"

#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/language_options.h"
#include "zetasql/reference_impl/reference_driver.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::Status ParseTestFileParameters(absl::string_view param_string,
                                     ReferenceDriver* reference_driver,
                                     TypeFactory* type_factory,
                                     std::map<std::string, Value>* parameters) {
  parameters->clear();

  const std::string sql = absl::StrCat("SELECT ", param_string);

  bool is_deterministic_output;
  bool uses_unsupported_type = false;  // unused
  ZETASQL_ASSIGN_OR_RETURN(
      Value value,
      reference_driver->ExecuteStatementForReferenceDriver(
          sql, /*parameters=*/{}, ReferenceDriver::ExecuteStatementOptions(),
          type_factory, &is_deterministic_output, &uses_unsupported_type));
  ZETASQL_RET_CHECK(is_deterministic_output);

  // Expect the result is in Array[Struct{...}].
  ZETASQL_RET_CHECK(value.is_valid() && !value.is_null() && value.type()->IsArray() &&
            value.num_elements() == 1)
      << "Generated values are not in format Array[...] when evaluating "
      << sql;

  const Value& value_struct = value.element(0);
  ZETASQL_RET_CHECK(value_struct.type()->IsStruct())
      << "Generated values are not in format Array[Struct{...}] when "
      << "evaluating " << sql;

  for (int i = 0; i < value_struct.num_fields(); i++) {
    // Create a copy of the value. Otherwise the value will be invalidated by
    // the next call of TestDriver::ExecuteStatement(...).
    parameters->emplace(value_struct.type()->AsStruct()->field(i).name,
                        Value(value_struct.field(i)));
  }
  return absl::OkStatus();
}

TestFileParameterParser::TestFileParameterParser()
    : reference_impl_(new ReferenceDriver), type_factory_(new TypeFactory) {}

TestFileParameterParser::~TestFileParameterParser() = default;

absl::Status TestFileParameterParser::Init(const TestDatabase& database) {
  initialized_ = true;
  LanguageOptions lang_options = reference_impl_->GetSupportedLanguageOptions();
  reference_impl_->SetLanguageOptions(lang_options);
  return reference_impl_->CreateDatabase(database);
}

absl::Status TestFileParameterParser::Parse(
    absl::string_view param_string, std::map<std::string, Value>* parameters) {
  if (!initialized_) {
    // Intiailize to a default empty database.
    ZETASQL_RETURN_IF_ERROR(Init(TestDatabase{}));
  }
  return ParseTestFileParameters(param_string, reference_impl_.get(),
                                 type_factory_.get(), parameters);
}

}  // namespace zetasql
