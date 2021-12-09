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

#include "zetasql/public/templated_sql_function.h"

#include "zetasql/base/logging.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/strings.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/status_payload.h"

namespace zetasql {

// static
const char TemplatedSQLFunction::kTemplatedSQLFunctionGroup[] =
    "Templated_SQL_Function";

TemplatedSQLFunction::TemplatedSQLFunction(
    const std::vector<std::string>& function_name_path,
    const FunctionSignature& signature,
    const std::vector<std::string>& argument_names,
    const ParseResumeLocation& parse_resume_location, Mode mode,
    const FunctionOptions& options)
    : Function(function_name_path, kTemplatedSQLFunctionGroup, mode,
               {signature}, options),
      argument_names_(argument_names),
      parse_resume_location_(parse_resume_location) {
  ZETASQL_CHECK_OK(signature.IsValidForFunction());
}

// static
absl::Status TemplatedSQLFunction::Deserialize(
    const FunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<Function>* result) {
  std::vector<std::string> name_path;
  for (const std::string& name : proto.name_path()) {
    name_path.push_back(name);
  }

  std::unique_ptr<FunctionSignature> function_signature;
  // Note that ZetaSQL does not yet support overloaded templated functions.
  // So we check that there is exactly one signature and retrieve it.
  ZETASQL_RET_CHECK_EQ(1, proto.signature_size()) << proto.DebugString();
  std::unique_ptr<FunctionSignature> signature;
  ZETASQL_RETURN_IF_ERROR(FunctionSignature::Deserialize(proto.signature(0), pools,
                                                 factory, &function_signature));

  std::vector<std::string> argument_names;
  for (const std::string& name : proto.templated_sql_function_argument_name()) {
    argument_names.push_back(name);
  }

  ZETASQL_RET_CHECK(proto.has_parse_resume_location()) << proto.DebugString();
  *result = absl::make_unique<TemplatedSQLFunction>(
      name_path, *function_signature, argument_names,
      ParseResumeLocation::FromProto(proto.parse_resume_location()));
  return absl::OkStatus();
}

absl::Status TemplatedSQLFunction::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map, FunctionProto* proto,
    bool omit_signatures) const {
  ZETASQL_RETURN_IF_ERROR(
      Function::Serialize(file_descriptor_set_map, proto, omit_signatures));
  parse_resume_location_.Serialize(proto->mutable_parse_resume_location());
  for (const std::string& name : argument_names_) {
    proto->add_templated_sql_function_argument_name(name);
  }
  return absl::OkStatus();
}

TemplatedSQLFunctionCall::TemplatedSQLFunctionCall(
    std::unique_ptr<const ResolvedExpr> expr,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        aggregate_expr_list)
    : expr_(expr.release()),
      aggregate_expression_list_(std::move(aggregate_expr_list)) {}

const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
TemplatedSQLFunctionCall::aggregate_expression_list() const {
  return aggregate_expression_list_;
}

inline void ResolvedComputedColumnFormatter(
    std::string* out,
    const std::unique_ptr<const ResolvedComputedColumn>& computed_column) {
  absl::StrAppend(out, computed_column->DebugString());
}

std::string TemplatedSQLFunctionCall::DebugString() const {
  return absl::StrCat("TemplatedSQLFunctionCall expr: ",
                      (expr_ == nullptr ? "nullptr" : expr_->DebugString()),
                      "\naggregate_expression_list:\n",
                      absl::StrJoin(aggregate_expression_list_, "\n",
                                    ResolvedComputedColumnFormatter));
}

namespace {
static bool module_initialization_complete = []() {
  Function::RegisterDeserializer(
      TemplatedSQLFunction::kTemplatedSQLFunctionGroup,
      TemplatedSQLFunction::Deserialize);
  return true;
} ();
}  // namespace

}  // namespace zetasql
