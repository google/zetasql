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

#ifndef ZETASQL_PUBLIC_FUNCTION_H_
#define ZETASQL_PUBLIC_FUNCTION_H_

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"  
#include "zetasql/public/input_argument_type.h"  
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_deserializer.h"
#include "zetasql/public/value.h"
#include "absl/base/attributes.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"

// ZetaSQL's Catalog interface class allows an implementing query engine
// to define the functions available in the engine.  The caller initially
// loads the supported ZetaSQL built-in functions into their Catalog, and
// then creates their own Functions and loads them into the Catalog.  The
// caller defines one or more named groups for their Functions.  The caller
// adds FunctionSignatures to each Function for all overloads.  Each
// FunctionSignature includes a context pointer or id that the caller can
// use for any purpose.  FunctionSignatures allow templated types, and
// optional or repeated arguments.
//
// When ZetaSQL resolves a query, it resolves the expressions by
// looking up their corresponding Catalog Functions.  Each resolved
// expression in the resolved AST includes:
//
// 1) A Function* pointer.
// 2) A concrete FunctionSignature identifying argument and result Types.
//    - If the matching FunctionSignature was originally a templated signature,
//      then this concrete signature has all argument and result types fixed.
//      Additionally, the signature includes the context pointer/id from the
//      original signature.
//
// The Functions referenced in the resolved AST may be caller-provided
// functions, or ZetaSQL built-in operators or functions.  Callers
// distinguish between them by looking at the Function's group name.
// The FunctionSignature context is used by the caller to match the
// Function to the appropriate evaluator.  For caller-provided functions,
// the context should be used as needed to select the matching evaluator.
// Only standard function call syntax (i.e., fn_name(<op1>,...)) is
// currently supported for caller-provided functions.
//
// ZetaSQL built-in functions and operators identify ZetaSQL 'standard'
// functions with well defined semantics that are expected to be common
// across all implementations.  These functions belong to the named group
// 'ZetaSQL'.  The FunctionSignature context_id_ for ZetaSQL functions
// identifies the matching FunctionSignatureId (from builtin_function.proto),
// and the caller should use that context_id_ to select the corresponding
// evaluator.  Built-in Functions for operators that do not use function call
// syntax are stored in the Catalog with special names starting with '$'.
namespace zetasql {

class AnalyzerOptions;
class Catalog;
class CycleDetector;
class Function;
class FunctionOptionsProto;
class FunctionProto;
class LanguageOptions;

// This callback signature takes an argument list as input and returns status.
// Callback implementations are intended to validate the argument list
// and return status indicating if the arguments are valid or providing a
// relevant error message.
using ArgumentConstraintsCallback = std::function<absl::Status(
    const std::vector<InputArgumentType>&, const LanguageOptions&)>;

// This callback signature takes the function signature matched by resolution,
// an argument list as input and returns status.
// Callback implementations are intended to validate the argument list
// and return status indicating if the arguments are valid with the given
// signature or providing a relevant error message.
using PostResolutionArgumentConstraintsCallback = std::function<absl::Status(
    const FunctionSignature& matched_signature,
    const std::vector<InputArgumentType>&, const LanguageOptions&)>;

// This callback signature takes a matched signature, an argument list as input
// and returns the computed output type for function call.  This is used for
// cases when the output type is dependent on argument types and/or argument
// literal values. The computed type overrides the return type stored in the
// FunctionSignature. New types should be allocated in the provided TypeFactory.
//
// This is invoked after the post_resolution_constraint callback.  If this
// returns an error, that error is used as the reason a function call isn't
// valid.
using ComputeResultTypeCallback = std::function<absl::StatusOr<const Type*>(
    Catalog*, TypeFactory*, CycleDetector*,
    const FunctionSignature&, const std::vector<InputArgumentType>&,
    const AnalyzerOptions&)>;
// The legacy signature of the ComputeResultTypeCallback. It does not take the
// matched signature as input.
using LegacyComputeResultTypeCallback =
    std::function<absl::StatusOr<const Type*>(
        Catalog*, TypeFactory*, CycleDetector*,
        const std::vector<InputArgumentType>&, const AnalyzerOptions&)>;

// This callback signature takes a list of sql representation of function
// inputs and returns the sql representation of the function call.
// Explicitly used inside GetSQL(...) to override its default behavior and can
// be used for other formatting like infix, preunary, postunary likewise.
using FunctionGetSQLCallback =
    std::function<std::string(const std::vector<std::string>&)>;

// This callback signature takes an argument list and returns a custom error
// message string to be used when no matching function signature is found for
// the function based on the argument list.
using NoMatchingSignatureCallback = std::function<std::string(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>&, ProductMode)>;

// This callback produces text containing supported function signatures. This
// text is used in the user facing messages, i.e. in errors. Example of
// text returned by such callback for LIKE operator:
// "STRING LIKE STRING; BYTES LIKE BYTES"
using SupportedSignaturesCallback =
    std::function<std::string(const LanguageOptions&, const Function&)>;

// This callback produces a prefix for bad arguments.
// An example of the standard prefix is "Argument 1 to FUNC".
using BadArgumentErrorPrefixCallback =
    std::function<std::string(const FunctionSignature&, int)>;

// Evaluates a function on the given arguments. Returns a status on error, and a
// value on success.
using FunctionEvaluator = std::function<absl::StatusOr<Value>(
    const absl::Span<const Value> arguments)>;

// Takes a concrete function signature and returns a function evaluator or an
// error if one cannot be constructed.
using FunctionEvaluatorFactory =
    std::function<absl::StatusOr<FunctionEvaluator>(const FunctionSignature&)>;

// Options that apply to a function.
// The setter methods here return a reference to *self so options can be
// constructed inline, and chained if desired.
// Example:
//     Function f(...,
//                FunctionOptions()
//                    .set_pre_resolution_argument_constraint(constraint)
//                    .set_get_sql_callback(callback))
struct FunctionOptions {
  typedef FunctionEnums::WindowOrderSupport WindowOrderSupport;
  static constexpr WindowOrderSupport ORDER_UNSUPPORTED =
      FunctionEnums::ORDER_UNSUPPORTED;
  static constexpr WindowOrderSupport ORDER_OPTIONAL =
      FunctionEnums::ORDER_OPTIONAL;
  static constexpr WindowOrderSupport ORDER_REQUIRED =
      FunctionEnums::ORDER_REQUIRED;

  FunctionOptions() {}

  // Construct FunctionOptions with support for an OVER clause.
  FunctionOptions(WindowOrderSupport window_ordering_support_in,
                  bool window_framing_support_in)
      : supports_over_clause(true),
        window_ordering_support(window_ordering_support_in),
        supports_window_framing(window_framing_support_in) {}

  static absl::Status Deserialize(
      const FunctionOptionsProto& proto,
      std::unique_ptr<FunctionOptions>* result);

  void Serialize(FunctionOptionsProto* proto) const;

  // Creates a copy of this FunctionOptions.
  FunctionOptions Copy() const {
    return *this;
  }

  FunctionOptions& set_pre_resolution_argument_constraint(
      ArgumentConstraintsCallback constraint) {
    pre_resolution_constraint = std::move(constraint);
    return *this;
  }
  FunctionOptions& set_post_resolution_argument_constraint(
      PostResolutionArgumentConstraintsCallback constraint) {
    post_resolution_constraint = std::move(constraint);
    return *this;
  }
  ABSL_DEPRECATED(
      "Please use the overload with PostResolutionArgumentConstraintsCallback")
  FunctionOptions& set_post_resolution_argument_constraint(
      ArgumentConstraintsCallback constraint) {
    post_resolution_constraint =
        [constraint = std::move(constraint)](
            const FunctionSignature& signature,
            const std::vector<InputArgumentType>& input_arguments,
            const LanguageOptions& language_options) {
          return constraint(input_arguments, language_options);
        };
    return *this;
  }
  FunctionOptions& set_compute_result_type_callback(
      ComputeResultTypeCallback callback) {
    compute_result_type_callback = std::move(callback);
    return *this;
  }
  ABSL_DEPRECATED(
      "Use the overload that takes a ComputeResultTypeCallback instead")
  FunctionOptions& set_compute_result_type_callback(
      LegacyComputeResultTypeCallback callback) {
    compute_result_type_callback =
        [callback = std::move(callback)](
            Catalog* catalog, TypeFactory* type_factory,
            CycleDetector* cycle_detector,
            const FunctionSignature& /*signature*/,
            const std::vector<InputArgumentType>& input_arguments,
            const AnalyzerOptions& analyzer_options) {
          return callback(catalog, type_factory, cycle_detector,
                          input_arguments, analyzer_options);
        };
    return *this;
  }

  FunctionOptions& set_get_sql_callback(FunctionGetSQLCallback callback) {
    get_sql_callback = std::move(callback);
    return *this;
  }

  FunctionOptions& set_no_matching_signature_callback(
      NoMatchingSignatureCallback callback) {
    no_matching_signature_callback = std::move(callback);
    return *this;
  }

  FunctionOptions& set_supported_signatures_callback(
      SupportedSignaturesCallback callback) {
    supported_signatures_callback = std::move(callback);
    return *this;
  }

  FunctionOptions& set_bad_argument_error_prefix_callback(
      BadArgumentErrorPrefixCallback callback) {
    bad_argument_error_prefix_callback = std::move(callback);
    return *this;
  }

  FunctionOptions& set_supports_over_clause(bool value) {
    supports_over_clause = value;
    return *this;
  }

  FunctionOptions& set_window_ordering_support(WindowOrderSupport value) {
    window_ordering_support = value;
    return *this;
  }

  FunctionOptions& set_supports_window_framing(bool value) {
    supports_window_framing = value;
    return *this;
  }

  FunctionOptions& set_arguments_are_coercible(bool value) {
    arguments_are_coercible = value;
    return *this;
  }
  FunctionOptions& set_is_deprecated(bool value) {
    is_deprecated = value;
    return *this;
  }
  FunctionOptions& set_alias_name(const std::string& name) {
    alias_name = name;
    return *this;
  }
  FunctionOptions& set_sql_name(const std::string& name) {
    sql_name = name;
    return *this;
  }
  FunctionOptions& set_allow_external_usage(bool value) {
    allow_external_usage = value;
    return *this;
  }
  FunctionOptions& set_volatility(FunctionEnums::Volatility value) {
    volatility = value;
    return *this;
  }
  FunctionOptions& set_supports_order_by(bool value) {
    supports_order_by = value;
    return *this;
  }
  FunctionOptions& set_supports_limit(bool value) {
    supports_limit = value;
    return *this;
  }
  FunctionOptions& set_supports_null_handling_modifier(bool value) {
    supports_null_handling_modifier = value;
    return *this;
  }
  FunctionOptions& set_supports_safe_error_mode(bool value) {
    supports_safe_error_mode = value;
    return *this;
  }
  FunctionOptions& set_supports_distinct_modifier(bool value) {
    supports_distinct_modifier = value;
    return *this;
  }
  FunctionOptions& set_supports_having_modifier(bool value) {
    supports_having_modifier = value;
    return *this;
  }
  FunctionOptions& set_supports_clamped_between_modifier(bool value) {
    supports_clamped_between_modifier = value;
    return *this;
  }
  FunctionOptions& set_uses_upper_case_sql_name(bool value) {
    uses_upper_case_sql_name = value;
    return *this;
  }

  // Add a LanguageFeature that must be enabled for this function to be enabled.
  // This is used only on built-in functions, and determines whether they will
  // be loaded in GetZetaSQLFunctions.
  FunctionOptions& add_required_language_feature(LanguageFeature feature) {
    zetasql_base::InsertIfNotPresent(&required_language_features, feature);
    return *this;
  }

  // Returns whether or not all language features required by a function is
  // enabled.
  ABSL_MUST_USE_RESULT bool check_all_required_features_are_enabled(
      const LanguageOptions::LanguageFeatureSet& enabled_features) const;

  // Sets the evaluator factory used to associate a concrete signature of this
  // function with a FunctionEvaluator. This method is used only for evaluating
  // the function using the Evaluator interface or the reference implementation
  // (see zetasql/public/evaluator.h).
  FunctionOptions& set_evaluator_factory(
      const FunctionEvaluatorFactory& evaluator_factory) {
    function_evaluator_factory = evaluator_factory;
    return *this;
  }

  // Convenience method that constructs a FunctionEvaluatorFactory that always
  // returns the given 'function_evaluator' to the caller.
  FunctionOptions& set_evaluator(const FunctionEvaluator& function_evaluator);

  // If not nullptr, the callback is invoked to obtain an evaluator for the
  // function.
  FunctionEvaluatorFactory function_evaluator_factory = nullptr;

  // If not nullptr, identifies additional constraints to check during function
  // resolution. For example, an argument must be a literal, it must be a simple
  // type, it cannot be an array, etc.
  // * Pre-resolution checks are performed against the original
  //   arguments from the parser, before signature resolution and argument type
  //   coercion are done.
  // * Post-resolution checks are performed against the coerced
  //   arguments after a function signature has been matched. In both cases, if
  //   checks fail then the query immediately fails.
  ArgumentConstraintsCallback pre_resolution_constraint = nullptr;
  PostResolutionArgumentConstraintsCallback post_resolution_constraint =
      nullptr;

  // If not nullptr, this is used after resolution to compute the result type
  // of the function, overriding the type stored in the FunctionSignature.
  ComputeResultTypeCallback compute_result_type_callback = nullptr;

  // If not nullptr, this callback is explicitly executed inside GetSQL(...)
  // to get the function's text representation in sql.
  FunctionGetSQLCallback get_sql_callback = nullptr;

  // If not nullptr, this callback is used to construct a custom error message
  // when no matching function signature is found for the arguments.
  NoMatchingSignatureCallback no_matching_signature_callback = nullptr;

  // If not nullptr, this callback is used to construct custom text containing
  // list of signatures supported by the function.
  SupportedSignaturesCallback supported_signatures_callback = nullptr;

  // If not nullptr, this callback is used to construct a custom prefix to the
  // argument error message when certain argument conditions are not met.
  BadArgumentErrorPrefixCallback bad_argument_error_prefix_callback = nullptr;

  // Indicates whether the OVER clause is supported.
  bool supports_over_clause = false;
  // Indicates the support for ORDER BY in a window definition.
  WindowOrderSupport window_ordering_support = ORDER_UNSUPPORTED;
  // Indicates whether the window framing clause is allowed.
  bool supports_window_framing = false;

  // Indicates whether function signature matching is allowed to do coercions.
  // Otherwise, signatures only match if they have exactly the same Types.
  bool arguments_are_coercible = true;

  // If true, analyzer will generate a deprecation_warning if this function
  // is called.
  bool is_deprecated = false;

  // If non-empty, defines an alias or synonym name for the function.
  // Implementations of zetasql::Catalog must expose this <alias_name>
  // via FindFunction().  For ZetaSQL builtin functions, examples of
  // currently defined aliases are POW/POWER and CEIL/CEILING.
  std::string alias_name;

  // If non-empty, defines a SQL function name to be used externally in error
  // messages.
  std::string sql_name;

  // Indicates whether this function is allowed to be used in PRODUCT_EXTERNAL
  // mode.
  bool allow_external_usage = true;

  // The volatility of a function determines how multiple executions of
  // a function are related.  Optimizers may use this property when considering
  // transformations like common subexpression elimination.
  // This is used via IsConstantExpression in
  // ../analyzer/expr_resolver_helper.h.
  FunctionEnums::Volatility volatility = FunctionEnums::IMMUTABLE;

  // Indicates whether this function supports ORDER BY in arguments (affects
  // aggregate functions only).
  bool supports_order_by = false;

  // Indicates whether this function supports LIMIT in arguments (affects
  // aggregate functions only).
  bool supports_limit = false;

  // Indicates whether this function supports IGNORE NULLS and RESPECT NULLS in
  // arguments.
  bool supports_null_handling_modifier = false;

  // Indicates whether this function supports SAFE_ERROR_MODE.
  // This is set to false for built-in operators that do not use function call
  // syntax and therefore cannot be called with the "SAFE." prefix. This is used
  // by the Random Query Generator to avoid generating impossible function
  // calls. Engines should generally never set this for non-built-in functions.
  bool supports_safe_error_mode = true;

  // Indicates whether this function supports DISTINCT in arguments
  // (affects aggregate functions only).
  bool supports_distinct_modifier = true;

  // Indicates whether this function supports HAVING in arguments
  // (affects aggregate functions only).
  bool supports_having_modifier = true;

  // Indicates whether this function supports CLAMPED BETWEEN in arguments
  // (affects aggregate functions only).
  // Must only be true for differential privacy functions.
  bool supports_clamped_between_modifier = false;

  // Indicates whether to use upper case name in SQLName() and GetSQL(), which
  // are used in (but not limited to) error messages such as
  // "No matching signature for function ...".
  //
  // Setting this option to false will not automatically convert the name to
  // lower-case.
  //
  // If <sql_name> is not empty and this option is true, SQLName() will return
  // the upper-case version of <sql_name>.
  bool uses_upper_case_sql_name = true;

  // A set of LanguageFeatures that need to be enabled for the function to be
  // loaded in GetZetaSQLFunctions.
  std::set<LanguageFeature> required_language_features;

  // Copyable.
};

// The Function interface identifies the functions available in a
// query engine.  Each Function includes a set of FunctionSignatures, where
// a signature indicates:
//
//  1) Argument and result types.
//  2) A 'context' for the signature.
//
// A Function also indicates the 'group' it belongs to.
//
// The Function also includes <function_options_> for specifying additional
// resolution requirements, if any.  Additionally, <function_options_> can
// identify a function alias/synonym that Catalogs must expose for function
// lookups by name.
class Function {
 public:
  // This is the return value of GetGroup() for ZetaSQL built-in functions.
  static const char kZetaSQLFunctionGroupName[];

  typedef FunctionEnums::Mode Mode;
  static constexpr Mode SCALAR = FunctionEnums::SCALAR;
  static constexpr Mode AGGREGATE = FunctionEnums::AGGREGATE;
  static constexpr Mode ANALYTIC = FunctionEnums::ANALYTIC;

  // Functions in the root catalog can use one of the first two constructors.
  // Functions in a nested catalog should use the constructor with the
  // <function_name_path>, identifying the full path name of the function
  // including its containing catalog names.
  //
  // These constructors perform ZETASQL_CHECK validations of basic invariants:
  // * Scalar functions cannot support the OVER clause.
  // * Analytic functions must support OVER clause.
  // * Signatures must satisfy FunctionSignature::IsValidForFunction().
  Function(absl::string_view name, absl::string_view group, Mode mode,
           FunctionOptions function_options = {});
  Function(absl::string_view name, absl::string_view group, Mode mode,
           std::vector<FunctionSignature> function_signatures,
           FunctionOptions function_options = {});

  Function(std::vector<std::string> name_path, absl::string_view group,
           Mode mode, std::vector<FunctionSignature> function_signatures,
           FunctionOptions function_options = {});

  Function(const Function&) = delete;
  Function& operator=(const Function&) = delete;
  virtual ~Function() {}

  static absl::Status Deserialize(
      const FunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory,
      std::unique_ptr<Function>* result);

  static absl::StatusOr<std::unique_ptr<Function>> Deserialize(
      const FunctionProto& proto, const TypeDeserializer& type_deserializer);

  virtual absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                                 FunctionProto* proto,
                                 bool omit_signatures = false) const;

  // Registers 'deserializer' as a function to deserialize a specific Function
  // subclass of 'group_name'. Case-sensitive. The returned function is owned by
  // the caller. This must be called at module initialization time. Dies if more
  // than one deserializer for the same 'type' is registered, or if any other
  // error occurs. For an example, please see the REGISTER_MODULE_INITIALIZER in
  // templated_sql_function.cc.
  using FunctionDeserializer = std::function<absl::Status(
      const FunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<Function>* result)>;
  static void RegisterDeserializer(const std::string& group_name,
                                   FunctionDeserializer deserializer);

  const std::string& Name() const { return function_name_path_.back(); }
  const std::vector<std::string>& FunctionNamePath() const {
    return function_name_path_;
  }

  // Returns <function_name_path_> strings joined with '.', and if
  // <include_group> is true then it is prefixed with the group name.
  const std::string FullName(bool include_group = true) const;

  // Returns an external 'SQL' name for the function, for use in error messages
  // and anywhere else appropriate.  If <function_options_> has its <sql_name>
  // set then it returns <sql_name>.  If <sql_name> is not set and Name() is
  // an internal function name (starting with '$'), then it strips off the '$'
  // and converts any '_' to ' '.  Otherwise it simply returns Name() for
  // ZetaSQL builtin functions and FullName() for non-builtin functions.
  const std::string SQLName() const;

  // Returns SQLName() prefixed with either 'operator ' or 'function ',
  // and 'aggregate ' or 'analytic ' if appropriate.  If <capitalize_qualifier>
  // is true, then the first letter of the (first) qualifier is capitalized
  // (i.e., 'Operator' vs. 'operator' and 'Analytic function' vs.
  // 'analytic function').  A function uses the 'operator ' prefix if its name
  // starts with '$'.
  const std::string QualifiedSQLName(bool capitalize_qualifier = false) const;

  // Returns the 'group' the function belongs to.
  const std::string& GetGroup() const { return group_; }

  bool IsZetaSQLBuiltin() const {
    return group_ == kZetaSQLFunctionGroupName;
  }

  // Returns whether or not this Function is a specific function
  // interface or implementation.
  template<class FunctionSubclass>
  bool Is() const {
    return dynamic_cast<const FunctionSubclass*>(this) != nullptr;
  }

  // Returns this Function as FunctionSubclass*.  Must only be used when it
  // is known that the object *is* this subclass, which can be checked using
  // Is() before calling GetAs().
  template<class FunctionSubclass>
  const FunctionSubclass* GetAs() const {
    return static_cast<const FunctionSubclass*>(this);
  }

  bool ArgumentsAreCoercible() const {
    return function_options_.arguments_are_coercible;
  }

  // Returns the number of function signatures.
  int NumSignatures() const;

  // Returns all of the function signatures.
  const std::vector<FunctionSignature>& signatures() const;

  void ResetSignatures(const std::vector<FunctionSignature>& signatures);

  // Adds a function signature to an existing function.
  void AddSignature(const FunctionSignature& signature);

  // Helper function to add function signatures with simple argument and
  // result types.
  absl::Status AddSignature(const TypeKind result_kind,
                            const std::vector<TypeKind>& input_kinds,
                            void* context, TypeFactory* factory);
  // Convenience function that returns the same Function object so that
  // calls can be chained.
  Function* AddSignatureOrDie(const TypeKind result_kind,
                              const std::vector<TypeKind>& input_kinds,
                              void* context, TypeFactory* factory);

  Mode mode() const { return mode_; }
  bool IsScalar() const { return mode_ == SCALAR; }
  bool IsAggregate() const { return mode_ == AGGREGATE; }
  bool IsAnalytic() const { return mode_ == ANALYTIC; }

  // Returns the requested FunctionSignature.  The caller does not take
  // ownership of the returned FunctionSignature.  Returns NULL if the
  // specified idx does not exist.
  const FunctionSignature* GetSignature(int idx) const;

  // Returns the function name.  If <verbose> then also returns DebugString()s
  // of all its function signatures.
  virtual std::string DebugString(bool verbose = false) const;

  // Returns SQL to perform a function call with the given SQL arguments,
  // using given FunctionSignature if provided.
  std::string GetSQL(std::vector<std::string> inputs,
                     const FunctionSignature* signature = nullptr) const;

  // Returns Status indicating whether or not any specified constraints were
  // violated. If <constraints_callback> is NULL returns OK.
  absl::Status CheckPreResolutionArgumentConstraints(
      const std::vector<InputArgumentType>& arguments,
      const LanguageOptions& language_options) const;
  absl::Status CheckPostResolutionArgumentConstraints(
      const FunctionSignature& signature,
      const std::vector<InputArgumentType>& arguments,
      const LanguageOptions& language_options) const;

  // Returns the requested callback;
  const ArgumentConstraintsCallback& PreResolutionConstraints()
      const;
  const PostResolutionArgumentConstraintsCallback& PostResolutionConstraints()
      const;
  const ComputeResultTypeCallback& GetComputeResultTypeCallback() const;
  const FunctionGetSQLCallback& GetSQLCallback() const;

  const NoMatchingSignatureCallback& GetNoMatchingSignatureCallback() const;

  // Returns a relevant (customizable) error message for the no matching
  // function signature error condition.
  const std::string GetNoMatchingFunctionSignatureErrorMessage(
      const std::vector<InputArgumentType>& arguments,
      ProductMode product_mode) const;

  // Returns a generic error message for the no matching function signature
  // error condition.
  static const std::string GetGenericNoMatchingFunctionSignatureErrorMessage(
      const std::string& qualified_function_name,
      const std::vector<InputArgumentType>& arguments,
      ProductMode product_mode);

  const SupportedSignaturesCallback& GetSupportedSignaturesCallback() const;

  // Returns a relevant (customizable) user facing text (to be used in error
  // message) listing supported function signatures. For example:
  // "DATE_DIFF(DATE, DATE, DATE_TIME_PART)"
  // "INT64 + INT64; UINT64 + UINT64; DOUBLE + DOUBLE"
  // In num_signatures returns number of signatures used to build a string.
  const std::string GetSupportedSignaturesUserFacingText(
      const LanguageOptions& language_options, int* num_signatures) const;

  const BadArgumentErrorPrefixCallback&
  GetBadArgumentErrorPrefixCallback() const;

  // Returns the factory set in <function_options_>.
  FunctionEvaluatorFactory GetFunctionEvaluatorFactory() const;

  // Returns Status indicating whether or not the constraints for the OVER
  // clause are violated:
  // 1) Scalar function cannot support the OVER clause;
  // 2) Analytic function must support the OVER clause.
  absl::Status CheckWindowSupportOptions() const;

  // Check that we don't have multiple signatures with lambda possibly matching
  // one function call. Two signatures cannot coexist if they have lambdas with
  // the same number of arguments at the same indexes. We don't have a "distinct
  // enough" concept to choose one from multiple matches signatures with lambda.
  absl::Status CheckMultipleSignatureMatchingSameFunctionCall() const;

  // Returns true if it supports the OVER clause (i.e. this function can act as
  // an analytic function). If true, the mode cannot be SCALAR.
  bool SupportsOverClause() const;

  // Returns true if ORDER BY is allowed in a window definition for this
  // function.
  bool SupportsWindowOrdering() const;

  // Returns true if ORDER BY must be specified in a window definition for this
  // function.
  bool RequiresWindowOrdering() const;

  // Returns true if window framing clause is allowed in a window definition for
  // this function.
  bool SupportsWindowFraming() const;

  // Returns true if order by is allowed in the function arguments.
  bool SupportsOrderingArguments() const;

  // Returns true if LIMIT is allowed in the function arguments.
  bool SupportsLimitArguments() const;

  // Returns true if IGNORE NULLS and RESPECT NULLS are allowed in the function
  // arguments.
  bool SupportsNullHandlingModifier() const;

  // Returns true if this function supports SAFE_ERROR_MODE.  See full comment
  // on field definition.
  bool SupportsSafeErrorMode() const;

  // Returns true if HAVING is allowed in the function arguments.
  bool SupportsHavingModifier() const;

  // Returns true if DISTINCT is allowed in the function arguments.
  bool SupportsDistinctModifier() const;

  // Returns true if CLAMPED BETWEEN is allowed in the function arguments.
  // Must only be true for differential privacy functions.
  bool SupportsClampedBetweenModifier() const;

  bool IsDeprecated() const {
    return function_options_.is_deprecated;
  }

  const FunctionOptions& function_options() const { return function_options_; }

  const std::string& alias_name() const { return function_options_.alias_name; }

 private:
  bool is_operator() const;

  std::vector<std::string> function_name_path_;
  std::string group_;
  Mode mode_;
  std::vector<FunctionSignature> function_signatures_;
  const FunctionOptions function_options_;
};

// This class contains custom information about a particular function call.
// ZetaSQL passes it to the engine in the ResolvedFunctionCall. Functions may
// introduce subclasses of this class to add custom information as needed on a
// per-function basis.
class ResolvedFunctionCallInfo {
 public:
  ResolvedFunctionCallInfo() {}
  virtual ~ResolvedFunctionCallInfo() {}
  virtual std::string DebugString() const { return "<empty>"; }

  // Returns whether or not this ResolvedFunctionCallInfo is a specific
  // table-valued function call interface or implementation.
  template <class ResolvedFunctionCallInfoSubclass>
  bool Is() const {
    return dynamic_cast<const ResolvedFunctionCallInfoSubclass*>(this) !=
           nullptr;
  }

  // Returns this ResolvedFunctionCallInfo as ResolvedFunctionCallInfo*.  Must
  // only be used when it is known that the object *is* this subclass, which can
  // be checked using Is() before calling GetAs().
  template <class ResolvedFunctionCallInfoSubclass>
  const ResolvedFunctionCallInfoSubclass* GetAs() const {
    return static_cast<const ResolvedFunctionCallInfoSubclass*>(this);
  }
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTION_H_
