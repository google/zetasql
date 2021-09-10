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

#ifndef ZETASQL_PUBLIC_TEMPLATED_SQL_TVF_H_
#define ZETASQL_PUBLIC_TEMPLATED_SQL_TVF_H_

#include <memory>
#include <string>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/base/status.h"

// This file includes interfaces and classes related to templated SQL
// TVFs.  It includes classes to represent TemplatedSQLTVFs and their
// signature (TemplatedTVFSignature).
//
// Note: NON-templated SQL TVF objects can be found in sql_tvf.h.

namespace zetasql {

class AnalyzerOptions;
class ResolvedQueryStmt;
class TableValuedFunctionProto;

// This represents a templated table-valued function with a SQL body. The
// Resolve method of this class parses and resolves this SQL body when the
// function is called, in the context of the actual provided 'input_arguments'.
//
// The purpose of this class is to help support statements of the form
// "CREATE TABLE FUNCTION <name>(<arguments>) AS <query>", where the <arguments>
// may have templated types like "ANY TYPE" or "ANY TABLE". In this case,
// ZetaSQL cannot resolve the <query> right away and must defer this work
// until later when the function is called with concrete argument types.
class TemplatedSQLTVF : public TableValuedFunction {
 public:
  // Constructs a new templated SQL TVF named <function_name_path>, with a
  // single signature in <signature>. The <arg_name_list> should contain exactly
  // one string for each argument name in <signature>, indicating the name of
  // the argument. The <parse_resume_location> contains the source location
  // and string contents of the table function's SQL query body.
  TemplatedSQLTVF(const std::vector<std::string>& function_name_path,
                  const FunctionSignature& signature,
                  const std::vector<std::string>& arg_name_list,
                  const ParseResumeLocation& parse_resume_location,
                  TableValuedFunctionOptions tvf_options = {})
      : TableValuedFunction(function_name_path,
                            signature,
                            tvf_options),
        arg_name_list_(arg_name_list),
        parse_resume_location_(parse_resume_location) {}

  const std::vector<std::string>& GetArgumentNames() const {
    return arg_name_list_;
  }

  // If set, <resolution_catalog_> is used during the Resolve() call in order
  // to resolve the TVF expression (and overrides the <catalog> argument
  // to Resolve()).  Used for templated TVFs inside modules (which resolve
  // against the module catalog in which the TVF is defined).
  void set_resolution_catalog(Catalog* resolution_catalog) {
    resolution_catalog_ = resolution_catalog;
  }

  void set_allow_query_parameters(bool allow) {
    allow_query_parameters_ = allow;
  }

  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         TableValuedFunctionProto* proto) const override;

  static absl::Status Deserialize(
      const TableValuedFunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result);

  // Analyzes the body of this function in context of the arguments provided for
  // a specific call.
  //
  // If 'resolution_catalog_' is non-NULL, then the TVF expression is resolved
  // against 'resolution_catalog_', and the 'catalog' argument is ignored.
  // Otherwise the TVF expression is resolved against 'catalog'.
  //
  // If this analysis succeeds, returns the output schema of
  // this TVF call in 'output_tvf_call', which is guaranteed to be a
  // TemplatedSQLTVFSignature. Otherwise, if this analysis fails, this method
  // instead sets the error_* fields in 'output_tvf_call' with information
  // about the error that occurred.
  absl::Status Resolve(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& input_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* tvf_signature) const override;

  ParseResumeLocation GetParseResumeLocation() const {
    return parse_resume_location_;
  }

 private:
  // Performs some quick sanity checks on the function signature before starting
  // nested analysis.
  absl::Status CheckIsValid() const;

  // This is a helper method when parsing or analyzing the table function's
  // SQL expression body.  If 'status' is OK, also returns OK. Otherwise,
  // returns a new error forwarding any nested errors in 'status' obtained
  // from this nested parsing or analysis.
  // TODO: Remove ErrorMessageMode, once we consistently always save
  // these status objects with payload, and only produce the mode-versioned
  // status when fetched through FindXXX() calls?
  absl::Status ForwardNestedResolutionAnalysisError(
      const absl::Status& status, ErrorMessageMode mode) const;

  // Returns a new error reporting a failed expectation of the sql_body_
  // (for example, if it is a CREATE TABLE instead of a SELECT statement).
  // If 'message' is not empty, appends it to the end of the error string.
  absl::Status MakeTVFQueryAnalysisError(const std::string& message = "") const;

  // If non-NULL, this Catalog is used when the Resolve() method is called
  // to resolve the TVF expression for given arguments.
  Catalog* resolution_catalog_ = nullptr;

  // The list of names of all the function arguments, in the same order that
  // they appear in the function signature.
  std::vector<std::string> arg_name_list_;

  // Indicates the table function's original SQL query body inside the
  // CREATE TABLE FUNCTION statement that declared this function, starting
  // after the AS keyword.
  ParseResumeLocation parse_resume_location_;

  // If true, the analyzer allows query parameters within the SQL function body.
  bool allow_query_parameters_ = false;
};

// The TemplatedSQLTVF::Resolve method returns an instance of this class. It
// contains the fully-resolved query inside the function body after processing
// it in the context of all the provided input arguments.
class TemplatedSQLTVFSignature : public TVFSignature {
 public:
  // Represents a TVF call that returns 'output_schema'. Takes ownership of
  // 'resolved_templated_query'.
  TemplatedSQLTVFSignature(
      const std::vector<TVFInputArgumentType>& input_arguments,
      const TVFRelation& output_schema,
      const TVFSignatureOptions& tvf_signature_options,
      const ResolvedQueryStmt* resolved_templated_query,
      const std::vector<std::string>& arg_name_list)
      : TVFSignature(input_arguments, output_schema, tvf_signature_options),
        resolved_templated_query_(resolved_templated_query),
        arg_name_list_(arg_name_list) {}

  TemplatedSQLTVFSignature(const TemplatedSQLTVFSignature&) = delete;
  TemplatedSQLTVFSignature& operator=(const TemplatedSQLTVFSignature&) = delete;
  ~TemplatedSQLTVFSignature() override;

  // This contains the fully-resolved function body in context of the actual
  // concrete types of the provided arguments to the function call.
  const ResolvedQueryStmt* resolved_templated_query() const {
    return resolved_templated_query_;
  }

  // The list of names of all the function arguments, in the same order that
  // they appear in the function signature.
  const std::vector<std::string>& GetArgumentNames() const {
    return arg_name_list_;
  }

 private:
  const ResolvedQueryStmt* const resolved_templated_query_ = nullptr;
  const std::vector<std::string> arg_name_list_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TEMPLATED_SQL_TVF_H_
