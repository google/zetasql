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

#ifndef ZETASQL_ANALYZER_FUNCTION_RESOLVER_H_
#define ZETASQL_ANALYZER_FUNCTION_RESOLVER_H_

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/base/statusor.h"
#include "zetasql/base/status.h"

namespace zetasql {

class Coercer;
class Resolver;
class SignatureMatchResult;

// This class perfoms function resolution during ZetaSQL analysis.
// The functions here generally traverse the function expressions recursively,
// constructing and returning the ResolvedFunctionCall nodes bottom-up.
class FunctionResolver {
 public:
  FunctionResolver(Catalog* catalog, TypeFactory* type_factory,
                   Resolver* resolver);
  ~FunctionResolver() {}
  FunctionResolver(const FunctionResolver&) = delete;
  FunctionResolver& operator=(const FunctionResolver&) = delete;

  // Resolves the function call given the <function>, <arguments> expressions,
  // <expected_result_type> and creates a ResolvedFunctionCall.  No special
  // handling is done for aggregate functions - they are resolved exactly like
  // scalar functions. <is_analytic> indicates whether an OVER clause follows
  // this function call.
  // * Takes ownership of the ResolvedExprs in <arguments>.
  // * <named_arguments> is a vector of any named arguments passed into this
  //   function call along with each one's zero-based index of that argument as
  //   resolved in <arguments>. The function resolver refers to them when
  //   matching against function signatures as needed. These parse nodes
  //   should correspond to a subset of <arguments>.
  // * <expected_result_type> is optional and when specified should match the
  //   result_type of the function signature while resolving. Otherwise there is
  //   no match.
  absl::Status ResolveGeneralFunctionCall(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const Function* function, ResolvedFunctionCallBase::ErrorMode error_mode,
      bool is_analytic,
      std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
      std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
      const Type* expected_result_type,
      std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out);

  // These are the same as previous but they take a (possibly multipart)
  // function name and looks it up in the <resolver_> catalog.  Multipart
  // names are prefixed by their (nested) catalog names.
  absl::Status ResolveGeneralFunctionCall(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const std::string& function_name, bool is_analytic,
      std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
      std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
      const Type* expected_result_type,
      std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out);
  absl::Status ResolveGeneralFunctionCall(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const std::vector<std::string>& function_name_path, bool is_analytic,
      std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
      std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
      const Type* expected_result_type,
      std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out);

  // Analyzes the body of a templated SQL function in <function> in the context
  // of the arguments provided for a specific call.
  //
  // See public/templated_sql_function.h for more background
  // templated sql functions.
  //
  // <function> may provide an alternate catalog for resolution.
  // If <function>.resolution_catalog() is set, then the function expression
  // is resolved against it, otherwise this->catalog_ is used.
  //
  // If this analysis succeeds, this method compares the result type against the
  // expected type as specified in the function signature (if present). If the
  // types are not equal, uses this->coercer_ to insert a coercion if the types
  // are compatible or returns an error otherwise.
  //
  // Finally, once this check is complete, this method returns the result type
  // of this function call in <function_call_info>.
  absl::Status ResolveTemplatedSQLFunctionCall(
      const ASTNode* ast_location, const TemplatedSQLFunction& function,
      const AnalyzerOptions& analyzer_options,
      const std::vector<InputArgumentType>& actual_arguments,
      std::shared_ptr<ResolvedFunctionCallInfo>* function_call_info_out);

  // This is a helper method when parsing or analyzing the function's SQL
  // expression.  If 'status' is OK, also returns OK. Otherwise, returns a
  // new error forwarding any nested errors in 'status' obtained from the
  // nested parsing or analysis.
  static absl::Status ForwardNestedResolutionAnalysisError(
      const TemplatedSQLFunction& function, const absl::Status& status,
      ErrorMessageMode mode);

  // Returns a new error message reporting a failure parsing or analyzing the
  // SQL body. If 'message' is not empty, appends it to the end of the error
  // string.
  static absl::Status MakeFunctionExprAnalysisError(
      const TemplatedSQLFunction& function, absl::string_view message = "");

  // Converts <argument> to <target_type> and replaces <argument> with the new
  // expression if successful. If <argument> is a converted literal and
  // <set_has_explicit_type> is true, then the new ResolvedLiteral is marked as
  // having been explicitly cast (and it will thereafter be treated as a
  // non-literal with respect to coercion). <return_null_on_error> indicates
  // whether the cast should return a NULL value of the <target_type> in case of
  // cast failures, which should only be set to true when using SAFE_CAST.
  //
  // Note: <ast_location> is expected to refer to the location of <argument>,
  // and would be more appropriate as type ASTExpression, though this would
  // require refactoring some callers.
  absl::Status AddCastOrConvertLiteral(
      const ASTNode* ast_location,
      const Type* target_type,
      const ResolvedScan* scan,  // May be null
      bool set_has_explicit_type,
      bool return_null_on_error,
      std::unique_ptr<const ResolvedExpr>* argument) const;

  // Map an operator id from the parse tree to a ZetaSQL function name.
  static const std::string& UnaryOperatorToFunctionName(
      ASTUnaryExpression::Op op);
  static const std::string& BinaryOperatorToFunctionName(
      ASTBinaryExpression::Op op);

  // Returns the Coercer from <resolver_>.
  const Coercer& coercer() const;

  // Determines if the function signature matches the argument list, returning
  // a non-templated signature if true.  If <allow_argument_coercion> is TRUE
  // then function arguments can be coerced to the required signature
  // type(s), otherwise they must be an exact match.
  bool SignatureMatches(const std::vector<InputArgumentType>& input_arguments,
                        const FunctionSignature& signature,
                        bool allow_argument_coercion,
                        std::unique_ptr<FunctionSignature>* result_signature,
                        SignatureMatchResult* signature_match_result) const;

  // Perform post-processing checks on CREATE AGGREGATE FUNCTION statements at
  // initial declaration time or when the functions are called later.
  // <expr_info> and <query_info> should be valid at the time that
  // <resolved_expr> is generated. Note that <sql_function_body_location> is
  // optional, and will be used to help construct the error message if present.
  static absl::Status CheckCreateAggregateFunctionProperties(
      const ResolvedExpr& resolved_expr,
      const ASTNode* sql_function_body_location,
      const ExprResolutionInfo* expr_info,
      QueryResolutionInfo* query_info);

  // Iterates through <named_arguments> and compares them against <signature>,
  // rearranging any of <arg_locations>, <expr_args>, <input_arg_types>, and/or
  // <tvf_arg_types> to match the order of the given <named_arguments> or
  // returning an error if an invariant is not satisfied. Sets
  // <named_arguments_match_signature> to true if <named_arguments> are a match
  // for <signature> or false otherwise. In the latter case, if
  // <return_error_if_named_arguments_do_not_match_signature> is true, this
  // method will return a descriptive error message.
  //
  // The <arg_locations>, <expr_args>, <input_arg_types>, and <tvf_arg_types>
  // are all optional and will be ignored if NULL; any combination is
  // acceptable. In practice, (<expr_args>, <input_arg_types>) and
  // <tvf_arg_types> are generally mutually exclusive. If we are resolving a
  // scalar function call, <expr_args> and/or <input_arg_types> are provided.
  // Otherwise, if we are resolving a TVF call, <tvf_arg_types> are provided.
  //
  // If <signature> contains any optional arguments whose values are not
  // provided (either positionally in <expr_args> or <tvf_arg_types> or named
  // in <named_arguments>) then this method may inject suitable default values
  // in <expr_args>, <input_arg_types>, and/or <tvf_arg_types>. Currently this
  // method injects a NULL value for such missing arguments as a default policy.
  absl::Status ProcessNamedArguments(
      const std::string& function_name, const FunctionSignature& signature,
      const ASTNode* ast_location,
      const std::vector<std::pair<const ASTNamedArgument*, int>>&
          named_arguments,
      bool return_error_if_named_arguments_do_not_match_signature,
      bool* named_arguments_match_signature,
      std::vector<const ASTNode*>* arg_locations,
      std::vector<std::unique_ptr<const ResolvedExpr>>* expr_args,
      std::vector<InputArgumentType>* input_arg_types,
      std::vector<ResolvedTVFArg>* tvf_arg_types) const;

 private:
  Catalog* catalog_;           // Not owned.
  TypeFactory* type_factory_;  // Not owned.
  Resolver* resolver_;         // Not owned.

  // Returns a signature that matches the argument type list, returning
  // a concrete FunctionSignature if found.  If not found, returns NULL.
  // The caller takes ownership of the returned FunctionSignature.
  //
  // The <ast_location> and <arg_locations> identify the function call and
  // argument locations for use in error messages. The <named_arguments>
  // optionally identify a list of named arguments provided as part of the
  // function call which may be used to help identify the signature. The pair in
  // each of <named_arguments> comprises a pointer to the ASTNamedArgument
  // object that the parser produced for this named argument reference and also
  // an integer identifying the corresponding argument type by indexing into
  // <input_arguments>.
  zetasql_base::StatusOr<const FunctionSignature*> FindMatchingSignature(
      const Function* function,
      const std::vector<InputArgumentType>& input_arguments,
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const std::vector<std::pair<const ASTNamedArgument*, int>>&
          named_arguments) const;

  // Check a literal argument value against value constraints for a given
  // argument, and return an error if any are violated.
  absl::Status CheckArgumentValueConstraints(
      const ASTNode* arg_location, int idx, const Value& value,
      const FunctionArgumentType& concrete_argument,
      const std::function<std::string(int)>& BadArgErrorPrefix) const;

  // Converts <argument_literal> to <target_type> and replaces <coerced_literal>
  // with the new expression if successful. If <set_has_explicit_type> is true,
  // then the new ResolvedLiteral is marked as having been explicitly cast (and
  // it will thereafter be treated as a non-literal with respect to coercion).
  // <return_null_on_error> indicates whether the cast should return a NULL
  // value of the <target_type> in case of cast failures, which should only be
  // set to true when using SAFE_CAST.
  absl::Status ConvertLiteralToType(
      const ASTNode* ast_location, const ResolvedLiteral* argument_literal,
      const Type* target_type, const ResolvedScan* scan,
      bool set_has_explicit_type, bool return_null_on_error,
      std::unique_ptr<const ResolvedLiteral>* converted_literal) const;

  friend class FunctionResolverTest;
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_FUNCTION_RESOLVER_H_
