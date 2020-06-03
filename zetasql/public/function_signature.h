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

#ifndef ZETASQL_PUBLIC_FUNCTION_SIGNATURE_H_
#define ZETASQL_PUBLIC_FUNCTION_SIGNATURE_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include <cstdint>
#include "absl/types/optional.h"
#include "zetasql/base/status.h"

namespace zetasql {

class FunctionArgumentType;
class FunctionArgumentTypeProto;
class FunctionSignatureOptionsProto;
class FunctionSignatureProto;
class InputArgumentType;
class LanguageOptions;
class TVFRelation;

typedef std::vector<FunctionArgumentType> FunctionArgumentTypeList;

// This class specifies options on a function argument, including
// argument cardinality.  This includes some options that are used to specify
// argument values that are illegal and should cause an analysis error.
class FunctionArgumentTypeOptions {
 public:
  typedef FunctionEnums::ProcedureArgumentMode ProcedureArgumentMode;
  typedef FunctionEnums::ArgumentCardinality ArgumentCardinality;

  FunctionArgumentTypeOptions() = default;

  explicit FunctionArgumentTypeOptions(ArgumentCardinality cardinality)
      : cardinality_(cardinality) {}

  // This constructs a set of argument type options to specify a required schema
  // for a relation argument of a table-valued function. If this required schema
  // is present, ZetaSQL enforces that column names of the passed-in relation
  // are a superset of the column names in these options (in any order), and
  // that the type of each passed-in column is equal or coercible to the type of
  // the matching required column. If either of these conditions does not hold,
  // ZetaSQL returns a descriptive error message to the user.
  //
  // If 'extra_relation_input_columns_allowed' is true, the provided relation
  // may include column names besides those specified in
  // 'relation_input_schema'.  Otherwise, ZetaSQL rejects the query if the
  // provided relation contains such extra columns. Note that if
  // 'relation_input_schema' requires a value table, this option has no effect
  // and ZetaSQL enforces that the provided relation has exactly one column.
  //
  // For more information about table-valued functions, please see
  // public/table_valued_function.h.
  FunctionArgumentTypeOptions(const TVFRelation& relation_input_schema,
                              bool extra_relation_input_columns_allowed);

  ArgumentCardinality cardinality() const { return cardinality_; }
  bool must_be_constant() const { return must_be_constant_; }
  bool must_be_non_null() const { return must_be_non_null_; }
  bool is_not_aggregate() const { return is_not_aggregate_; }
  bool must_support_equality() const { return must_support_equality_; }
  bool must_support_ordering() const { return must_support_ordering_; }
  bool must_support_grouping() const { return must_support_grouping_; }

  bool has_min_value() const { return has_min_value_; }
  bool has_max_value() const { return has_max_value_; }
  const int64_t min_value() const { return min_value_; }
  const int64_t max_value() const { return max_value_; }

  bool has_relation_input_schema() const {
    return relation_input_schema_ != nullptr;
  }

  const std::optional<int> get_resolve_descriptor_names_table_offset() const {
    return descriptor_resolution_table_offset_;
  }

  const TVFRelation& relation_input_schema() const {
    DCHECK(has_relation_input_schema());
    return *relation_input_schema_;
  }

  bool extra_relation_input_columns_allowed() const {
    return extra_relation_input_columns_allowed_;
  }
  bool has_argument_name() const { return !argument_name_.empty(); }
  const std::string& argument_name() const {
    DCHECK(has_argument_name());
    return argument_name_;
  }
  bool argument_name_is_mandatory() const {
    return argument_name_is_mandatory_;
  }
  ProcedureArgumentMode procedure_argument_mode() const {
    return procedure_argument_mode_;
  }

  FunctionArgumentTypeOptions& set_cardinality(ArgumentCardinality c) {
    cardinality_ = c;
    return *this;
  }
  FunctionArgumentTypeOptions& set_must_be_constant(bool v = true) {
    must_be_constant_ = v;
    return *this;
  }
  FunctionArgumentTypeOptions& set_must_be_non_null(bool v = true) {
    must_be_non_null_ = v;
    return *this;
  }
  FunctionArgumentTypeOptions& set_is_not_aggregate(bool v = true) {
    is_not_aggregate_ = v;
    return *this;
  }
  FunctionArgumentTypeOptions& set_must_support_equality(bool v = true) {
    must_support_equality_ = v;
    return *this;
  }
  FunctionArgumentTypeOptions& set_must_support_ordering(bool v = true) {
    must_support_ordering_ = v;
    return *this;
  }
  FunctionArgumentTypeOptions& set_must_support_grouping(bool v = true) {
    must_support_grouping_ = v;
    return *this;
  }
  FunctionArgumentTypeOptions& set_min_value(int64_t value) {
    has_min_value_ = true;
    min_value_ = value;
    return *this;
  }
  FunctionArgumentTypeOptions& set_max_value(int64_t value) {
    has_max_value_ = true;
    max_value_ = value;
    return *this;
  }
  FunctionArgumentTypeOptions& set_extra_relation_input_columns_allowed(
      bool v = true) {
    extra_relation_input_columns_allowed_ = v;
    return *this;
  }
  FunctionArgumentTypeOptions& set_argument_name(const std::string& name) {
    argument_name_ = name;
    return *this;
  }
  FunctionArgumentTypeOptions& set_argument_name_is_mandatory(bool value) {
    argument_name_is_mandatory_ = value;
    return *this;
  }
  FunctionArgumentTypeOptions& set_procedure_argument_mode(
      ProcedureArgumentMode mode) {
    procedure_argument_mode_ = mode;
    return *this;
  }
  FunctionArgumentTypeOptions& set_allow_coercion_from(
      std::function<bool(const zetasql::Type*)> allow_coercion_from) {
    allow_coercion_from_ = allow_coercion_from;
    return *this;
  }
  FunctionArgumentTypeOptions& set_resolve_descriptor_names_table_offset(
      int table_offset) {
    descriptor_resolution_table_offset_ = table_offset;
    return *this;
  }

  std::function<bool(const zetasql::Type*)> allow_coercion_from() const {
    return allow_coercion_from_;
  }

  // Return a string describing the options (not including cardinality).
  // If no options are set, this returns an empty string.
  // Otherwise, includes a leading space.
  std::string OptionsDebugString() const;

  // Get the SQL declaration for these options.
  // The result is formatted as SQL that can be included inside a function
  // signature in CREATE FUNCTION, DROP FUNCTION, etc, if possible.
  std::string GetSQLDeclaration(ProductMode product_mode) const;

  // Sets the ParseLocationRange of the argument name. Returns the modified
  // class object.
  FunctionArgumentTypeOptions& set_argument_name_parse_location(
      const ParseLocationRange& argument_name_parse_location) {
    argument_name_parse_location_ = argument_name_parse_location;
    return *this;
  }

  // Gets the ParseLocationRange of the argument name.
  absl::optional<ParseLocationRange> argument_name_parse_location() const {
    return argument_name_parse_location_;
  }

  // Sets the ParseLocationRange of the argument type. Returns the modified
  // class object.
  FunctionArgumentTypeOptions& set_argument_type_parse_location(
      const ParseLocationRange& argument_type_parse_location) {
    argument_type_parse_location_ = argument_type_parse_location;
    return *this;
  }

  // Gets the ParseLocationRange of the argument type.
  absl::optional<ParseLocationRange> argument_type_parse_location() const {
    return argument_type_parse_location_;
  }

 private:
  ArgumentCardinality cardinality_ = FunctionEnums::REQUIRED;

  // If true, this argument must be constant.
  // Currently, this means the argument must be a literal or parameter.
  // This is checked after overload resolution, so a function cannot be
  // overloaded on constant vs non-constant arguments.
  bool must_be_constant_ = false;

  // If true, this argument cannot be NULL.
  // An error will be returned if this overload is chosen and the argument
  // is a literal NULL.
  bool must_be_non_null_ = false;

  // If true, this argument is a NOT AGGREGATE argument to an aggregate
  // function.  This means that the argument must have a constant value over
  // all rows passed to the same aggregate function call.
  // Currently, this is enforced the same as must_be_constant_.
  // This is ignored for non-aggregate functions.
  bool is_not_aggregate_ = false;

  // If true, this argument must have a type with SupportsEquality().
  // This is checked after choosing a concrete signature.
  bool must_support_equality_ = false;

  // If true, this argument must have a type with SupportsOrdering().
  // This is checked after choosing a concrete signature.
  bool must_support_ordering_ = false;

  // If true, this argument must have a type with SupportsGrouping().
  bool must_support_grouping_ = false;

  bool has_min_value_ = false;
  bool has_max_value_ = false;

  // These are min or max values (inclusive) for this argument.
  // If the argument has a literal value that is outside this range, the
  // analyzer will give an error.
  int64_t min_value_ = std::numeric_limits<int64_t>::lowest();
  int64_t max_value_ = std::numeric_limits<int64_t>::max();

  // This is a list of required column names and types for a relation argument
  // to a table-valued function. This is NULL if this is a non-relation
  // argument. For more information, please refer to the comment for the
  // constructor that fills this field.
  // TODO: Rename this to 'relation_schema' since this can apply for
  // the output table of table-valued functions too.
  std::shared_ptr<const TVFRelation> relation_input_schema_;

  // If true, the provided input relation may contain extra column names besides
  // those required in 'relation_input_schema_'. Otherwise, ZetaSQL rejects
  // the query if the provided relation contains such extra columns.
  bool extra_relation_input_columns_allowed_ = true;

  // Function argument always has value NOT_SET.
  // Procedure argument is in one of the 3 modes:
  // IN: argument is used only for input to the procedure. It is also the
  //     default mode for procedure argument if no mode is specified.
  // OUT: argument is used as output of the procedure.
  // INOUT: argument is used both for input to and output from the procedure.
  ProcedureArgumentMode procedure_argument_mode_ = FunctionEnums::NOT_SET;

  // Callback to support custom argument coercion in addition to standard
  // coercion rules.
  std::function<bool(const zetasql::Type*)> allow_coercion_from_;

  // Optional user visible name for referring to the function argument by name
  // using explicit syntax: name => value. For CREATE [AGGREGATE/TABLE] FUNCTION
  // statements, this comes from the name specified for each argument in the
  // statement's function signature. In other cases, engines may assign this in
  // custom ways as needed.
  std::string argument_name_;

  // If true, and the 'argument_name_' field is non-empty, the function call
  // must refer to the argument by name only. The resolver will return an error
  // if the function call attempts to refer to the argument positionally.
  bool argument_name_is_mandatory_ = false;

  // Optional parse location range for argument name. It is populated by
  // resolver only when analyzing UDFs and TVFs. <record_parse_locations>
  // must also be set to true in the ZetaSQL analyzer options.
  absl::optional<ParseLocationRange> argument_name_parse_location_;

  // Optional parse location range for argument type. It is populated by
  // resolver only when analyzing UDFs and TVFs. <record_parse_locations>
  // must also be set to true in the ZetaSQL analyzer options.
  absl::optional<ParseLocationRange> argument_type_parse_location_;

  // Optional argument offset for descriptor argument types, which is only
  // populated for descriptor arguments whose columns should be resolved
  // from the table argument in the same tvf call at the specified argument
  // offset. The value must be the offset of an argument with table type.
  std::optional<int> descriptor_resolution_table_offset_;

  // Copyable
};

// A type for an argument or result value in a function signature.  Types
// can be fixed or templated.  Arguments can be marked as repeated (denoting
// it can occur zero or more times in a function invocation) or optional.
// Result types cannot be marked as repeated or optional.
// Type VOID is valid for the return type in Procedures and in
// ResolvedDropFunctionStmt only; VOID is not allowed as the return type for
// Functions, and is never allowed as a argument type.
// A FunctionArgumentType is concrete if it is not templated and
// num_occurrences_ indicates how many times the argument appears in a
// concrete FunctionSignature.  FunctionArgumentTypeOptions can be used to
// apply additional constraints on legal values for the argument.
class FunctionArgumentType {
 public:
  typedef FunctionEnums::ArgumentCardinality ArgumentCardinality;
  static constexpr ArgumentCardinality REQUIRED = FunctionEnums::REQUIRED;
  static constexpr ArgumentCardinality REPEATED = FunctionEnums::REPEATED;
  static constexpr ArgumentCardinality OPTIONAL = FunctionEnums::OPTIONAL;

  // Construct a templated argument of <kind>, which must not be ARG_TYPE_FIXED.
  // The num_occurrences default value (-1) indicates a non-concrete argument.
  // Concrete arguments must have this set to a non-negative number.
  FunctionArgumentType(SignatureArgumentKind kind,
                       ArgumentCardinality cardinality,
                       int num_occurrences = -1);
  FunctionArgumentType(SignatureArgumentKind kind,
                       const FunctionArgumentTypeOptions& options,
                       int num_occurrences = -1);
  FunctionArgumentType(SignatureArgumentKind kind,  // implicit; NOLINT
                       int num_occurrences = -1);
  // Construct a non-templated argument kind fixed type <type>.
  // Does not take ownership of <type>.
  FunctionArgumentType(const Type* type, ArgumentCardinality cardinality,
                       int num_occurrences = -1);
  FunctionArgumentType(const Type* type,
                       const FunctionArgumentTypeOptions& options,
                       int num_occurrences = -1);
  FunctionArgumentType(const Type* type,  // implicit; NOLINT
                       int num_occurrences = -1);

  // Construct a relation argument type for a table-valued function. This
  // argument will accept any input relation of any schema.
  static FunctionArgumentType AnyRelation() {
    return FunctionArgumentType(ARG_TYPE_RELATION);
  }

  // Construct a model argument type for a table-valued function. This argument
  // will accept any model.
  static FunctionArgumentType AnyModel() {
    return FunctionArgumentType(ARG_TYPE_MODEL);
  }

  // Constructs a connection argument type for a table-valued function. This
  // argument will accept any connection.
  static FunctionArgumentType AnyConnection() {
    return FunctionArgumentType(ARG_TYPE_CONNECTION);
  }

  // Constructs a descriptor argument type for a table-valued function. This
  // argument accepts a <table_offset> parameter to indicate if resolving column
  // names from a table parameter. <table_offset> < 0 means does not resolve
  // column names. <table_offset> >= 0 means an argument offset.
  static FunctionArgumentType AnyDescriptor(int table_offset = -1) {
    FunctionArgumentTypeOptions option =
        FunctionArgumentTypeOptions(FunctionEnums::REQUIRED);
    if (table_offset >= 0) {
      option.set_resolve_descriptor_names_table_offset(table_offset);
    }
    return FunctionArgumentType(ARG_TYPE_DESCRIPTOR, option);
  }

  // Construct a relation argument type for a table-valued function.
  //
  // This argument accepts an input relation with the names of columns in
  // 'relation_input_schema', and the type of each column of the input relation
  // must be equal or coercible to the matching column in
  // 'relation_input_schema'.
  //
  // The provided input relation may contain extra column names besides those
  // listed here if and only if 'extra_relation_input_columns_allowed' is set to
  // true. Extra columns are allowed by default here, but can be disabled if
  // needed.
  // TODO: Move the relation schema out of
  // FunctionArgumentTypeOptions and into this class.  It is analogous to
  // <type> for scalar arguments and result types.  The options should only
  // be relevant to argument types (not result types), but currently a result
  // type puts this schema into options.
  static FunctionArgumentType RelationWithSchema(
      const TVFRelation& relation_input_schema,
      bool extra_relation_input_columns_allowed) {
    return FunctionArgumentType(
        ARG_TYPE_RELATION,
        FunctionArgumentTypeOptions(relation_input_schema,
                                    extra_relation_input_columns_allowed));
  }

  ~FunctionArgumentType() {}

  // Deserialization of ParseLocationRange would refer to the filename in
  // ParseLocationRangeProto as string_view.
  // TODO Add support for storing filename as string in
  // ParseLocationPoint.
  static absl::Status Deserialize(
      const FunctionArgumentTypeProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory,
      std::unique_ptr<FunctionArgumentType>* result);

  absl::Status Serialize(
      FileDescriptorSetMap* file_descriptor_set_map,
      FunctionArgumentTypeProto* proto) const;

  const FunctionArgumentTypeOptions& options() const { return *options_; }

  bool required() const { return cardinality() == REQUIRED; }
  bool repeated() const { return cardinality() == REPEATED; }
  bool optional() const { return cardinality() == OPTIONAL; }
  ArgumentCardinality cardinality() const { return options_->cardinality(); }

  bool must_be_constant() const { return options_->must_be_constant(); }
  bool has_argument_name() const { return options_->has_argument_name(); }
  const std::string& argument_name() const { return options_->argument_name(); }

  int num_occurrences() const { return num_occurrences_; }
  void IncrementNumOccurrences() { ++num_occurrences_; }
  void set_num_occurrences(int num) { num_occurrences_ = num; }

  // Returns NULL if kind_ is not ARG_TYPE_FIXED.
  const Type* type() const { return type_; }

  SignatureArgumentKind kind() const { return kind_; }

  // Returns TRUE if kind_ is ARG_TYPE_FIXED or ARG_TYPE_RELATION and the number
  // of occurrences is greater than -1.
  bool IsConcrete() const;

  bool IsTemplated() const {
    // It is templated if it is not a fixed scalar, it is not a fixed relation,
    // and it is not a void argument.
    return kind_ != ARG_TYPE_FIXED && !IsFixedRelation() && !IsVoid();
  }

  bool IsRelation() const { return kind_ == ARG_TYPE_RELATION; }
  bool IsModel() const { return kind_ == ARG_TYPE_MODEL; }
  bool IsConnection() const { return kind_ == ARG_TYPE_CONNECTION; }
  bool IsFixedRelation() const {
    return kind_ == ARG_TYPE_RELATION &&
        options_->has_relation_input_schema();
  }
  bool IsVoid() const { return kind_ == ARG_TYPE_VOID; }

  bool IsDescriptor() const { return kind_ == ARG_TYPE_DESCRIPTOR; }
  std::optional<int> GetDescriptorResolutionTableOffset() const {
    return options_->get_resolve_descriptor_names_table_offset();
  }
  // Returns TRUE if kind_ is templated and it is related to the input kind
  // (i.e., the kinds are the same, or one is an array of the other).
  bool TemplatedKindIsRelated(SignatureArgumentKind kind) const;

  bool AllowCoercionFrom(const zetasql::Type* actual_arg_type) const {
    if (options_->allow_coercion_from() == nullptr) {
      return false;
    }
    return options_->allow_coercion_from()(actual_arg_type);
  }

  // Returns argument type name to be used in error messages.
  // This either would be a scalar short type name - DATE, INT64, BYTES etc. or
  // STRUCT, PROTO, ENUM for complex type names, or ANY when any data type is
  // allowed.
  std::string UserFacingName(ProductMode product_mode) const;

  // Returns user facing text for the argument including argument cardinality
  // (to be used in error message):
  //   - required, just argument type, e.g. INT64
  //   - optional, argument type enclosed in [], e.g. [INT64]
  //   - repeated, argument type enclosed in [] with ..., e.g. [INT64, ...]
  std::string UserFacingNameWithCardinality(ProductMode product_mode) const;

  // Checks concrete arguments to validate the number of occurrences.
  absl::Status IsValid() const;

  // If verbose is true, include FunctionOptions modifiers.
  std::string DebugString(bool verbose = false) const;

  // Get the SQL declaration for this argument, including all options.
  // The result is formatted as SQL that can be included inside a function
  // signature in CREATE FUNCTION, DROP FUNCTION, etc, if possible.
  std::string GetSQLDeclaration(ProductMode product_mode) const;

  static std::string SignatureArgumentKindToString(SignatureArgumentKind kind);

 private:
  FunctionArgumentType(
      SignatureArgumentKind kind, const Type* type,
      std::shared_ptr<const FunctionArgumentTypeOptions> options,
      int num_occurrences);

  // Returns shared options objects used in most common cases.
  static std::shared_ptr<const FunctionArgumentTypeOptions> SimpleOptions(
      ArgumentCardinality cardinality = FunctionEnums::REQUIRED);

  SignatureArgumentKind kind_;
  const Type* type_;

  // This holds the argument type options. It is a shared pointer to reduce
  // stack frame sizes when the function signatures are kept on the stack.
  std::shared_ptr<const FunctionArgumentTypeOptions> options_;

  // Indicates how many times a concrete argument occurred in a concrete
  // function signature.  REQUIRED concrete arguments must occur exactly 1
  // time, OPTIONALs can occur 0 or 1 times, and REPEATEDs can occur 0 or
  // more times.  For non-concrete arguments it is -1.
  int num_occurrences_;

  friend class FunctionSerializationTests;
  // Copyable.
};

// Returns whether the concrete argument list is valid for a matched
// FunctionSignature.
using FunctionSignatureArgumentConstraintsCallback =
    std::function<bool(const std::vector<InputArgumentType>&)>;

class FunctionSignatureOptions {
 public:
  FunctionSignatureOptions() {}

  FunctionSignatureOptions& set_constraints(
      FunctionSignatureArgumentConstraintsCallback argument_constraints) {
    constraints_ = argument_constraints;
    return *this;
  }

  // Setter/getter for whether this is a deprecated function signature. If so,
  // the analyzer will generate a deprecation warning if this signature is used
  // when the related function is called.
  FunctionSignatureOptions& set_is_deprecated(bool value) {
    is_deprecated_ = value;
    return *this;
  }
  bool is_deprecated() const { return is_deprecated_; }

  // Setters/getters for additional deprecation warnings associated with
  // this function signature. These have DeprecationWarning protos attached. The
  // analyzer will propagate these warnings to any statement that invokes this
  // function signature. (They will appear in
  // AnalyzerOutput::deprecation_warnings().)
  //
  // These warnings are typically populated for SQL UDFs, whose SQL
  // expressions use deprecated functionality. A specific example is a
  // non-templated SQL UDF whose body invokes a deprecated function signature.
  // (Templated functions are handled differently because their bodies are not
  // resolved until they are called.) We propagate the deprecation warning to
  // any statement that invokes the UDF to make it more visible, particularly
  // for cases where we are trying to assess the number of queries that use a
  // deprecated feature that we are considering removing.
  const std::vector<FreestandingDeprecationWarning>&
  additional_deprecation_warnings() const {
    return additional_deprecation_warnings_;
  }
  // Uses a template to allow both vectors and RepeatedPtrFields.
  template <class ContainerType>
  FunctionSignatureOptions& set_additional_deprecation_warnings(
      const ContainerType& warnings) {
    additional_deprecation_warnings_.clear();
    additional_deprecation_warnings_.reserve(warnings.size());
    for (const FreestandingDeprecationWarning& warning : warnings) {
      additional_deprecation_warnings_.push_back(warning);
    }
    return *this;
  }

  // Checks constraint satisfaction on the FunctionSignature.  If constraints
  // are not met then the signature is ignored during analysis.  Evaluates
  // the constraint callback if populated, and if the signature is sensitive
  // to the TimestampMode then checks that as well.
  bool CheckFunctionSignatureConstraints(
      const std::vector<InputArgumentType>& arguments) const;

  static absl::Status Deserialize(
      const FunctionSignatureOptionsProto& proto,
      std::unique_ptr<FunctionSignatureOptions>* result);

  void Serialize(FunctionSignatureOptionsProto* proto) const;

 private:
  friend class FunctionSerializationTests;

  // Validates constraints against a concrete argument list after matching
  // a FunctionSignature.  For example, this could verify that at least one
  // argument is floating point.
  FunctionSignatureArgumentConstraintsCallback constraints_;

  bool is_deprecated_ = false;
  // Stores any deprecation warnings associated with the body of a SQL function.
  std::vector<FreestandingDeprecationWarning> additional_deprecation_warnings_;

  // Copyable.
};

// FunctionSignature identifies the argument Types and other properties
// per overload of a Function (or a similar object, like a Procedure or
// TableValuedFunction).  A FunctionSignature is concrete if it
// identifies the exact number and fixed Types of its arguments and results.
// A FunctionSignature can be non-concrete, but have concrete arguments.
// A FunctionSignature can be abstract, specifying templated types and
// identifying arguments as repeated or optional.  Optional arguments must
// appear at the end of the argument list.
//
// If multiple arguments are repeated, they must be consecutive and are
// treated as if they repeat together.  To illustrate, consider the expression:
// 'CASE WHEN <bool_expr_1> THEN <expr_1>
//       WHEN <bool_expr_2> THEN <expr_2>
//       ...
//       ELSE <expr_n> END'.
//
// This expression has the following signature <arguments>:
//   arg1: <bool> repeated - WHEN
//   arg2: <any_type_1> repeated - THEN
//   arg3: <any_type_1> optional - ELSE
//   result: <any_type_1>
//
// The WHEN and THEN arguments (arg1 and arg2) repeat together and must
// occur at least once, and the ELSE is optional.  The THEN, ELSE, and
// RESULT types can be any type, but must be the same type.
//
// In order to avoid potential ambiguity, the number of optional arguments
// must be less than the number of repeated arguments.
//
// The FunctionSignature also includes <options> for specifying
// additional signature matching requirements, if any.
class FunctionSignature {
 public:
  // Does not take ownership of <context_ptr>.
  FunctionSignature(const FunctionArgumentType& result_type,
                    const FunctionArgumentTypeList& arguments,
                    void* context_ptr);

  FunctionSignature(const FunctionArgumentType& result_type,
                    const FunctionArgumentTypeList& arguments,
                    int64_t context_id);

  FunctionSignature(const FunctionArgumentType& result_type,
                    const FunctionArgumentTypeList& arguments, int64_t context_id,
                    const FunctionSignatureOptions& options);

  // Copy a FunctionSignature, assigning a new context_ptr or context_id.
  FunctionSignature(const FunctionSignature& old, void* context_ptr)
      : FunctionSignature(old) { context_ptr_ = context_ptr; }
  FunctionSignature(const FunctionSignature& old, int64_t context_id)
      : FunctionSignature(old) { context_id_ = context_id; }

  ~FunctionSignature() {}

  static absl::Status Deserialize(
      const FunctionSignatureProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory,
      std::unique_ptr<FunctionSignature>* result);

  absl::Status Serialize(
      FileDescriptorSetMap* file_descriptor_set_map,
      FunctionSignatureProto* proto) const;

  const FunctionArgumentTypeList& arguments() const {
    return arguments_;
  }

  const FunctionArgumentType& argument(int idx) const {
    return arguments_[idx];
  }

  // Returns the number of concrete arguments, with repeated and optional
  // arguments expanded.
  // Requires: HasConcreteArguments()
  int NumConcreteArguments() const {
    DCHECK(HasConcreteArguments());
    return concrete_arguments_.size();
  }

  // Returns concrete argument number <concrete_idx>.
  // Differs from argment(idx) above in that repeated and optional arguments
  // are fully expanded in a concrete signature.
  // Requires that the signature has concrete arguments.
  const FunctionArgumentType& ConcreteArgument(int concrete_idx) const {
    DCHECK(HasConcreteArguments());
    return concrete_arguments_[concrete_idx];
  }

  // Returns the Type associated with the concrete argument number
  // <concrete_idx>.
  // Requires that the signature has concrete arguments.
  const Type* ConcreteArgumentType(int concrete_idx) const {
    return ConcreteArgument(concrete_idx).type();
  }

  const FunctionArgumentType& result_type() const {
    return result_type_;
  }

  // Checks the signature result type and argument types for unsupported
  // Types given 'language_options', and returns true if found.  This
  // function considers ProductMode, TimestampMode, and enabled features
  // that affect supported types.
  bool HasUnsupportedType(
      const LanguageOptions& language_options) const;

  // The context is a union where only one of context_ptr_ and context_id_
  // is valid.  The caller distinguishes which is valid based on the
  // Function group name.
  const void* context_ptr() const { return context_ptr_; }
  int64_t context_id() const { return context_id_; }

  bool IsConcrete() const { return is_concrete_; }

  // Returns TRUE if all arguments are concrete.
  bool HasConcreteArguments() const;

  // Determines whether the argument and result types are valid.  Additionally,
  // it requires that all repeated arguments are consecutive, and all optional
  // arguments appear at the end.  There may be required arguments before
  // the repeated arguments, and there may be required arguments between the
  // repeated and optional arguments.
  absl::Status IsValid() const;

  // Checks specific invariants for the argument and return types for regular
  // function calls or table-valued function calls. The latter may use relation
  // types (returning true for FunctionArgumentType::IsRelation()) but the
  // former may not.
  absl::Status IsValidForFunction() const;
  absl::Status IsValidForTableValuedFunction() const;

  // Checks if this signature is valid for Procedure.
  // Procedure may only have fixed required arguments.
  absl::Status IsValidForProcedure() const;

  // Gets the first or last repeated argument index.  If there are no repeated
  // arguments then returns -1.
  int FirstRepeatedArgumentIndex() const;
  int LastRepeatedArgumentIndex() const;

  // Gets the number of required, repeated or optional arguments.
  int NumRequiredArguments() const;
  int NumRepeatedArguments() const { return num_repeated_arguments_; }
  int NumOptionalArguments() const { return num_optional_arguments_; }

  // Returns whether or not the constraints are satisfied.
  // If <constraints_callback> is NULL, returns true.
  bool CheckArgumentConstraints(
      const std::vector<InputArgumentType>& arguments) const;

  // If verbose is true, include FunctionOptions modifiers.
  std::string DebugString(const std::string& function_name = "",
                          bool verbose = false) const;

  // Returns a string containing the DebugString()s of all its
  // signatures.  Each signature string is prefixed with <prefix>, and
  // <separator> appears between each signature string.
  static std::string SignaturesToString(
      const std::vector<FunctionSignature>& signatures, bool verbose = false,
      const std::string& prefix = "  ", const std::string& separator = "\n");

  // Get the SQL declaration for this signature, including all options.
  // For each argument in the signature, the name will be taken from the
  // corresponding entry of <argument_names> if present.  An empty
  // <argument_names> will result in a signature with just type names.
  // The result is formatted as "(arg_name type, ...) RETURNS type", which
  // is valid to use in CREATE FUNCTION, DROP FUNCTION, etc, if possible.
  std::string GetSQLDeclaration(const std::vector<std::string>& argument_names,
                                ProductMode product_mode) const;

  bool IsDeprecated() const { return options_.is_deprecated(); }

  void SetIsDeprecated(bool value) {
    options_.set_is_deprecated(value);
  }

  const std::vector<FreestandingDeprecationWarning>&
  AdditionalDeprecationWarnings() const {
    return options_.additional_deprecation_warnings();
  }

  void SetAdditionalDeprecationWarnings(
      const std::vector<FreestandingDeprecationWarning>& warnings) {
    options_.set_additional_deprecation_warnings(warnings);
  }

  void SetConcreteResultType(const Type* type);

  // Returns true if this function signature contains any templated arguments.
  bool IsTemplated() const {
    for (const FunctionArgumentType& arg : arguments_) {
      if (arg.IsTemplated()) {
        return true;
      }
    }
    return false;
  }

 private:
  bool ComputeIsConcrete() const;
  void ComputeConcreteArgumentTypes();

  int ComputeNumRepeatedArguments() const;
  int ComputeNumOptionalArguments() const;

  FunctionArgumentTypeList arguments_;
  FunctionArgumentType result_type_;
  int num_repeated_arguments_ = -1;
  int num_optional_arguments_ = -1;

  // This union should hold enough context for the implementation
  // to map a specific function signature back to an evaluator for the
  // function.  For ZetaSQL functions, the <context_id_> is relevant.
  // For non-ZetaSQL functions, the relevant field is
  // implementation-defined.
  union {
    void* context_ptr_;
    int64_t context_id_;
  };

  // Additional constraints on the signature for it to be valid for a
  // list of concrete input arguments.
  FunctionSignatureOptions options_;

  // We precompute and materialize the list of concrete arguments because
  // we end up asking for these repeatedly.  This vector could be large if
  // functions have huge numbers of arguments, but then we probably have other
  // data structures that are proportionally large too.
  bool is_concrete_ = false;
  FunctionArgumentTypeList concrete_arguments_;

  friend class FunctionSerializationTests;
  // Copyable.
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTION_SIGNATURE_H_
