//
// Copyright 2019 ZetaSQL Authors
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
  // * <expected_result_type> is optional and when specified should match the
  //   result_type of the function signature while resolving. Otherwise there is
  //   no match.
  zetasql_base::Status ResolveGeneralFunctionCall(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const Function* function, ResolvedFunctionCallBase::ErrorMode error_mode,
      bool is_analytic,
      std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
      const Type* expected_result_type,
      std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out);

  // These are the same as previous but they take a (possibly multipart)
  // function name and looks it up in the <resolver_> catalog.  Multipart
  // names are prefixed by their (nested) catalog names.
  zetasql_base::Status ResolveGeneralFunctionCall(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const std::string& function_name, bool is_analytic,
      std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
      const Type* expected_result_type,
      std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out);
  zetasql_base::Status ResolveGeneralFunctionCall(
      const ASTNode* ast_location,
      const std::vector<const ASTNode*>& arg_locations,
      const std::vector<std::string>& function_name_path, bool is_analytic,
      std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
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
  zetasql_base::Status ResolveTemplatedSQLFunctionCall(
      const ASTNode* ast_location, const TemplatedSQLFunction& function,
      const AnalyzerOptions& analyzer_options,
      const std::vector<InputArgumentType>& actual_arguments,
      std::shared_ptr<ResolvedFunctionCallInfo>* function_call_info_out);

  // This is a helper method when parsing or analyzing the function's SQL
  // expression.  If 'status' is OK, also returns OK. Otherwise, returns a
  // new error forwarding any nested errors in 'status' obtained from the
  // nested parsing or analysis.
  static zetasql_base::Status ForwardNestedResolutionAnalysisError(
      const TemplatedSQLFunction& function, const zetasql_base::Status& status,
      ErrorMessageMode mode);

  // Returns a new error message reporting a failure parsing or analyzing the
  // SQL body. If 'message' is not empty, appends it to the end of the error
  // std::string.
  static zetasql_base::Status MakeFunctionExprAnalysisError(
      const TemplatedSQLFunction& function, const std::string& message = "");

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
  zetasql_base::Status AddCastOrConvertLiteral(
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
  static zetasql_base::Status CheckCreateAggregateFunctionProperties(
      const ResolvedExpr& resolved_expr,
      const ASTNode* sql_function_body_location,
      const ExprResolutionInfo* expr_info,
      QueryResolutionInfo* query_info);

 private:
  Catalog* catalog_;           // Not owned.
  TypeFactory* type_factory_;  // Not owned.
  Resolver* resolver_;         // Not owned.

  // Represents the argument types corresponding to a SignatureArgumentKind.
  // There are three possibilities:
  // 1) The object represents an untyped NULL.
  // 2) The object represents an untyped empty array.
  // 3) The object represents a list of typed arguments.
  // An object in state i and can move to state j if i < j.
  //
  // The main purpose of this class is to keep track of the types associated
  // with a templated SignatureArgumentKind in
  // CheckArgumentTypesAndCollectTemplatedArguments(). There, if we encounter a
  // typed argument for a templated SignatureArgumentKind, we add that to the
  // set. But if there are no typed arguments, then we need to know whether
  // there is an untyped NULL or empty array, because that affects the type we
  // will infer for the SignatureArgumentKind.
  class SignatureArgumentKindTypeSet {
   public:
    enum Kind { UNTYPED_NULL, UNTYPED_EMPTY_ARRAY, TYPED_ARGUMENTS };

    // Creates a set with kind UNTYPED_NULL.
    SignatureArgumentKindTypeSet() : kind_(UNTYPED_NULL) {}
    SignatureArgumentKindTypeSet(const SignatureArgumentKindTypeSet&) = delete;
    SignatureArgumentKindTypeSet& operator=(
        const SignatureArgumentKindTypeSet&) = delete;

    Kind kind() const { return kind_; }

    // Changes the set to kind UNTYPED_EMPTY_ARRAY. Cannot be called if
    // InsertTypedArgument() has already been called.
    void SetToUntypedEmptyArray() {
      DCHECK(kind_ != TYPED_ARGUMENTS);
      kind_ = UNTYPED_EMPTY_ARRAY;
    }

    // Changes the set to kind TYPED_ARGUMENTS, and adds a typed argument to
    // the set of typed arguments.
    bool InsertTypedArgument(const InputArgumentType& input_argument) {
      // Typed arguments have precedence over untyped arguments.
      DCHECK(!input_argument.is_untyped());
      kind_ = TYPED_ARGUMENTS;
      return typed_arguments_.Insert(input_argument);
    }

    // Returns the set of typed arguments corresponding to this object. Can only
    // be called if 'kind() == TYPED_ARGUMENTS'.
    const InputArgumentTypeSet& typed_arguments() const {
      DCHECK_EQ(kind_, TYPED_ARGUMENTS);
      return typed_arguments_;
    }

    std::string DebugString() const;

   private:
    Kind kind_;
    // Does not contain any untyped arguments. Only valid if 'kind_' is
    // TYPED_ARGUMENTS.
    InputArgumentTypeSet typed_arguments_;
  };

  // Maps templated arguments (ARG_TYPE_ANY_1, etc.) to a set of input argument
  // types. See CheckArgumentTypesAndCollectTemplatedArguments() for details.
  typedef std::map<SignatureArgumentKind, SignatureArgumentKindTypeSet>
      ArgKindToInputTypesMap;

  // Maps templated arguments (ARG_TYPE_ANY_1, etc.) to the
  // resolved (possibly coerced) Type each resolved to in a particular function
  // call.
  typedef std::map<SignatureArgumentKind, const Type*> ArgKindToResolvedTypeMap;

  static std::string ArgKindToInputTypesMapDebugString(
      const ArgKindToInputTypesMap& map);

  // Returns the concrete argument type for a given <function_argument_type>,
  // using the mapping from templated to concrete argument types in
  // <templated_argument_map>.  Also used for result types.
  bool GetConcreteArgument(
      const FunctionArgumentType& argument, int num_occurrences,
      const ArgKindToResolvedTypeMap& templated_argument_map,
      std::unique_ptr<FunctionArgumentType>* output_argument) const;

  // Returns a list of concrete arguments by calling GetConcreteArgument()
  // on each entry and setting num_occurrences_ with appropriate argument
  // counts.
  FunctionArgumentTypeList GetConcreteArguments(
      const std::vector<InputArgumentType>& input_arguments,
      const FunctionSignature& signature,
      int repetitions,
      int optionals,
      const ArgKindToResolvedTypeMap& templated_argument_map)
        const;

  // Returns a signature that matches the argument type list, returning
  // a concrete FunctionSignature if found.  If not found, returns NULL.
  // The caller takes ownership of the returned FunctionSignature.
  const FunctionSignature* FindMatchingSignature(
      const Function* function,
      const std::vector<InputArgumentType>& input_arguments) const;

  // Determines if the argument list count matches signature, returning the
  // number of times each repeated argument repeats and the number of
  // optional arguments present if true.
  bool SignatureArgumentCountMatches(
      const std::vector<InputArgumentType>& input_arguments,
      const FunctionSignature& signature, int* repetitions,
      int* optionals) const;

  // Determines if non-templated argument types match the signature, and returns
  // related templated argument type information.  <repetitions> identifies the
  // number of times that repeated arguments repeat.  Returns false if argument
  // types (and therefore the signature) do not match.
  //
  // Also populates <templated_argument_map> with a key for every templated
  // SignatureArgumentKind that appears in the signature (including the result
  // type) and <input_arguments>.  The corresponding value is the list of typed
  // arguments that occur for that SignatureArgumentKind. (The list may be empty
  // in the case of untyped arguments.) There is also some special handling for
  // ANY_K: if we see an argument (typed or untyped) for ARRAY_ANY_K, we act as
  // if we also saw the corresponding array element argument for ANY_K, and add
  // an entry to <templated_argument_map> even if ANY_K is not in the signature.
  bool CheckArgumentTypesAndCollectTemplatedArguments(
      const std::vector<InputArgumentType>& input_arguments,
      const FunctionSignature& signature,
      int repetitions,
      bool allow_argument_coercion,
      ArgKindToInputTypesMap* templated_argument_map,
      SignatureMatchResult* signature_match_result) const;

  // This method is only relevant for table-valued functions. It returns true in
  // 'signature_matches' if a relation input argument type matches a signature
  // argument type, and sets information in 'signature_match_result' either way.
  zetasql_base::Status CheckRelationArgumentTypes(
      int arg_idx, const InputArgumentType& input_argument,
      const FunctionArgumentType& signature_argument,
      bool allow_argument_coercion,
      SignatureMatchResult* signature_match_result,
      bool* signature_matches) const;

  // Check a literal argument value against value constraints for a given
  // argument, and return an error if any are violated.
  zetasql_base::Status CheckArgumentValueConstraints(
      const ASTNode* arg_location, int idx, const Value& value,
      const FunctionArgumentType& concrete_argument,
      const std::function<std::string(int)>& BadArgErrorPrefix) const;

  // Determines the resolved Type related to all of the templated types present
  // in a function signature. <templated_argument_map> must have been populated
  // by CheckArgumentTypesAndCollectTemplatedArguments().
  bool DetermineResolvedTypesForTemplatedArguments(
      const ArgKindToInputTypesMap& templated_argument_map,
      ArgKindToResolvedTypeMap* resolved_templated_arguments) const;

  // Converts <argument_literal> to <target_type> and replaces <coerced_literal>
  // with the new expression if successful. If <set_has_explicit_type> is true,
  // then the new ResolvedLiteral is marked as having been explicitly cast (and
  // it will thereafter be treated as a non-literal with respect to coercion).
  // <return_null_on_error> indicates whether the cast should return a NULL
  // value of the <target_type> in case of cast failures, which should only be
  // set to true when using SAFE_CAST.
  zetasql_base::Status ConvertLiteralToType(
      const ASTNode* ast_location, const ResolvedLiteral* argument_literal,
      const Type* target_type, const ResolvedScan* scan,
      bool set_has_explicit_type, bool return_null_on_error,
      std::unique_ptr<const ResolvedLiteral>* converted_literal) const;

  friend class FunctionResolverTest;
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_FUNCTION_RESOLVER_H_
