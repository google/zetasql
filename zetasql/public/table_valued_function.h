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

#ifndef ZETASQL_PUBLIC_TABLE_VALUED_FUNCTION_H_
#define ZETASQL_PUBLIC_TABLE_VALUED_FUNCTION_H_

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include <cstdint>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"

namespace zetasql {

class AnalyzerOptions;
class ResolvedExpr;
class SignatureMatchResult;
class TVFInputArgumentType;
class TVFRelationColumnProto;
class TVFRelationProto;
class TVFSignature;
class TableValuedFunctionProto;
class TableValuedFunctionOptionsProto;

// Options that apply to a table-valued function.
// The setter methods here return a reference to *self so options can be
// constructed inline, and chained if desired.
struct TableValuedFunctionOptions {
  TableValuedFunctionOptions() {}

  static absl::Status Deserialize(
      const TableValuedFunctionOptionsProto& proto,
      std::unique_ptr<TableValuedFunctionOptions>* result);

  void Serialize(TableValuedFunctionOptionsProto* proto) const;

  TableValuedFunctionOptions& set_uses_upper_case_sql_name(bool value) {
    uses_upper_case_sql_name = value;
    return *this;
  }

  // Indicates whether to use upper case name in GetSignatureUserFacingText(),
  // which is used for error messages such as
  // "No matching signature for function ...".
  bool uses_upper_case_sql_name = true;
};

// This interface describes a table-valued function (TVF) available in a query
// engine.
//
// For reference, each call to the regular Function class:
//
// * accepts value (not relation) arguments only,
// * has a fixed list of (maybe templated) signatures and the function resolver
//   selects one concrete signature,
// * returns a single value.
//
// In contrast, each TVF call:
//
// * accepts scalar or relation arguments,
// * has a single signature specifying the types of input arguments,
// * returns a stream of rows,
// * has an output table schema (column names and types, or a value table)
//   computed by a method in this class, and not described in the signature.
//
// To resolve a TVF call, the resolver:
//
// (1) gets the signature (currently, only one signature is supported)
// (2) resolves all input arguments as values or as relations based on the
//     signature
// (3) prepares a TableValuedFunction::InputArgumentList from the resolved input
//     arguments
// (4) calls TableValuedFunction::Resolve, passing the input arguments, to get
//     a TableValuedFunctionCall object with the output schema for the TVF call
// (5) fills the output name list from the column names in the output schema
// (6) returns a new ResolvedTVFScan with the resolved arguments as children
class TableValuedFunction {
 public:
  // Constructs a new TVF object with the given name and argument signature.
  //
  // Each TVF may accept value or relation arguments. The signature specifies
  // whether each argument should be a value or a relation. For a value
  // argument, the signature may specify a concrete Type or a (possibly
  // templated) SignatureArgumentKind. For relation arguments, the signature
  // should use ARG_TYPE_RELATION, and any relation will be accepted as an
  // argument.
  TableValuedFunction(const std::vector<std::string>& function_name_path,
                      const FunctionSignature& signature,
                      TableValuedFunctionOptions tvf_options = {})
      : function_name_path_(function_name_path),
        signatures_({signature}),
        tvf_options_(std::move(tvf_options)) {
    ZETASQL_CHECK_OK(signature.IsValidForTableValuedFunction());
  }
  // Table functions constructed this way should use AddSignature() to
  // add a related signature.
  explicit TableValuedFunction(
      const std::vector<std::string>& function_name_path,
      TableValuedFunctionOptions tvf_options = {})
      : function_name_path_(function_name_path),
        tvf_options_(std::move(tvf_options)) {}

  TableValuedFunction(const std::vector<std::string>& function_name_path,
                      const FunctionSignature& signature,
                      std::unique_ptr<AnonymizationInfo> anonymization_info,
                      TableValuedFunctionOptions tvf_options = {})
      : function_name_path_(function_name_path),
        signatures_({signature}),
        anonymization_info_(std::move(anonymization_info)),
        tvf_options_(std::move(tvf_options)) {
    ZETASQL_CHECK_OK(signature.IsValidForTableValuedFunction());
  }

  TableValuedFunction(const TableValuedFunction&) = delete;
  TableValuedFunction& operator=(const TableValuedFunction&) = delete;
  virtual ~TableValuedFunction() {}

  // Returns the name of this TVF.
  const std::string& Name() const { return function_name_path_.back(); }
  std::string FullName() const {
    return absl::StrJoin(function_name_path(), ".");
  }
  const std::vector<std::string>& function_name_path() const {
    return function_name_path_;
  }

  // Returns the number of function signatures.
  int64_t NumSignatures() const;

  // Returns all of the function signatures.
  // Note: For templated TVFs, the return type may or may not be concrete, and
  // if the return type is determined by the function call arguments then by
  // convention AnyRelation is returned.
  const std::vector<FunctionSignature>& signatures() const;

  // Adds a function signature to an existing table function.  TVFs currently
  // only support one signature, so an error is returned if a signature
  // already exists.
  // TODO: Support more than one signature.
  absl::Status AddSignature(const FunctionSignature& function_signature);

  // Returns the requested FunctionSignature.  The caller does not take
  // ownership of the returned FunctionSignature.  Returns NULL if the
  // specified idx does not exist.
  // TODO: Consider making this return a const reference instead.
  // This signature should be consistent with Function::GetSignature(), so
  // if we change it here we should also change it there.  Should it return
  // Status, in case <idx> is out of range (rather than ZETASQL_CHECK failing)?
  const FunctionSignature* GetSignature(int64_t idx) const;

  // Returns user facing text (to be used in error messages) listing function
  // signatures.  Note that there is no way to create table function signatures
  // that are deprecated or have unsupported types, so this returns user facing
  // text for all signatures.
  virtual std::string GetSupportedSignaturesUserFacingText(
      const LanguageOptions& language_options) const;

  virtual std::string DebugString() const;

  // Returns an error message for a table-valued function call named
  // 'tvf_name_string' with 'tvf_catalog_entry' that did not match the
  // function signature (identified by 'signature_idx') with 'input_arg_types'.
  // 'signature_match_result' should contain the result of a previous call to
  // FunctionResolver::SignatureMatches and 'language_options' should contain
  // the language options for the query.
  std::string GetTVFSignatureErrorMessage(
      const std::string& tvf_name_string,
      const std::vector<InputArgumentType>& input_arg_types, int signature_idx,
      const SignatureMatchResult& signature_match_result,
      const LanguageOptions& language_options) const;

  // Serializes this table-valued function to a protocol buffer. Subclasses may
  // override this to add more information as needed.
  virtual absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                                 TableValuedFunctionProto* proto) const;

  // Deserializes a table-valued function from a protocol buffer.
  // The specific steps taken to perform the deserialization depend on the
  // 'type' field of 'proto'. An associated deserializer for this 'type' must
  // already exist by this time from a previous call to RegisterDeserializer.
  static absl::Status Deserialize(
      const TableValuedFunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result);

  // Registers 'deserializer' as a function to deserialize a specific
  // TableValuedFunction subclass of 'type'. The returned TVF is owned by the
  // caller. This must be called at module initialization time. Dies if more
  // than one deserializer for the same 'type' is registered, or if any other
  // error occurs. For an example, please see the REGISTER_MODULE_INITIALIZER in
  // table_valued_function.cc.
  using TVFDeserializer = std::function<absl::Status(
      const TableValuedFunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result)>;
  static void RegisterDeserializer(FunctionEnums::TableValuedFunctionType type,
                                   TVFDeserializer deserializer);

  // Determines if this object is of the <TableFunctionSubclass>.
  template <class TableValuedFunctionSubclass>
  bool Is() const {
    return dynamic_cast<const TableValuedFunctionSubclass*>(this) != nullptr;
  }

  // Returns this TableValuedFunction as TableValuedFunctionSubclass*.  Must
  // only be used when it is known that the object *is* this subclass, which can
  // be checked using Is() before calling GetAs().
  template <class TableValuedFunctionSubclass>
  TableValuedFunctionSubclass* GetAs() {
    return static_cast<TableValuedFunctionSubclass*>(this);
  }
  template <class TableValuedFunctionSubclass>
  const TableValuedFunctionSubclass* GetAs() const {
    return static_cast<const TableValuedFunctionSubclass*>(this);
  }

  // Sets the <anonymization_info_> with the specified <userid_column_name_path>
  // (overwriting any previous anonymization info).  An error is returned if
  // the named column is ambiguous or does not exist in this table valued
  // function.
  //
  // Setting the AnonymizationInfo defines this table valued function as
  // supporting anonymization semantics and returning sensitive private data.
  absl::Status SetUserIdColumnNamePath(
      absl::Span<const std::string> userid_column_name_path);

  // Returns anonymization info for a table valued function, including a column
  // reference that indicates the userid column for anonymization purposes.
  std::optional<const AnonymizationInfo> anonymization_info() const {
    if (anonymization_info_ != nullptr) {
      return *anonymization_info_;
    }
    return std::nullopt;
  }

  // The Resolve method determines the output schema of a particular call to
  // this TVF based on the input arguments provided in the query.
  //
  // ZetaSQL provides information about the number and types of these
  // arguments in 'actual_arguments'.  ZetaSQL also includes the concrete
  // version of signature_ in 'concrete_signature' providing the resolved type
  // of templated arguments and occurrence counts of optional or repeated
  // arguments (with concrete relation schemas identified in the
  // FunctionArgumentTypeOptions).
  //
  // Validating that the 'actual_arguments' match the 'concrete_signature'
  // generally happens in two phases.  First, the Resolver performs initial
  // matching which primarily covers non-templated arguments (for both
  // table and scalar arguments).  The initial matching also covers templated
  // arguments, verifying that they are the proper general type (i.e., if the
  // function takes ANY TABLE then the related argument must be a table
  // argument and not a scalar argument).  Second, this Resolve() method is
  // invoked which may do additional validation (primarily for templated
  // arguments).  If the arguments are incompatible then this method returns
  // a descriptive error message indicating the nature of the failure.
  //
  // Otherwise, this method fills 'output_tvf_call' to indicate the result
  // schema of the table returned by this TVF call.
  //
  // This method accepts a Catalog and TypeFactory for possible use when
  // computing the output schema. It is important to note that ZetaSQL
  // provides these items when each function is resolved, not when it is
  // declared. Therefore the engine may add new types, tables, or functions
  // in-between these two times and they will be available for lookup here.
  virtual absl::Status Resolve(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& actual_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* output_tvf_signature) const = 0;

  const TableValuedFunctionOptions& tvf_options() const {
    return tvf_options_;
  }

 protected:
  // Returns user facing text (to be used in error messages) for the
  // specified table function <signature>. For example:
  //   tvf_name(DOUBLE, TABLE<col_name1 BOOL, col_name2 STRING>)
  //
  // The <language_options> identifies the product mode (INTERNAL vs. EXTERNAL),
  // affecting the resulting Type names (i.e., DOUBLE vs. FLOAT8).
  std::string GetSignatureUserFacingText(
      const FunctionSignature& signature,
      const LanguageOptions& language_options) const;

  // This is the name of this TVF.
  const std::vector<std::string> function_name_path_;

  // The signatures describe the input arguments that this TVF accepts.
  // Currently, only one signature is supported.
  std::vector<FunctionSignature> signatures_;

  // The AnonymizationInfo related to a TVF. See
  // (broken link) for further details.
  std::unique_ptr<AnonymizationInfo> anonymization_info_;

  TableValuedFunctionOptions tvf_options_ = {};
};

// Represents a column for some TVF input argument types (e.g. TVFRelation and
// TVFModelArgument).
struct TVFSchemaColumn {
  TVFSchemaColumn(const std::string& name_in, const Type* type_in,
                  bool is_pseudo_column_in = false)
      : name(name_in), type(type_in), is_pseudo_column(is_pseudo_column_in) {}

  // Serializes this TVFRelation column to a protocol buffer.
  absl::StatusOr<TVFRelationColumnProto> ToProto(
      FileDescriptorSetMap* file_descriptor_set_map) const;

  // Deserializes a TVFSchema column from a protocol buffer.
  // ParseLocationRangeProto stores the filename that will become string_view
  // when deserialized. Returned TVFSchemaColumn references a string owned by
  // 'proto', and therefore 'proto' must outlive the returned value.
  // TODO Add support for storing filename as string in
  // ParseLocationPoint.
  static absl::StatusOr<TVFSchemaColumn> FromProto(
      const TVFRelationColumnProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory);

  std::string DebugString(bool is_for_value_table) const {
    // Prevent concatenating value column name.
    if (!is_for_value_table || is_pseudo_column) {
      return absl::StrCat(name, " ", type->DebugString());
    }
    return type->DebugString();
  }

  std::string name;
  const Type* type = nullptr;
  bool is_pseudo_column;
  // Parse location ranges for the TVFSchema column name and type. As of now
  // they are populated if and only if this TVF definition comes from a
  // CREATE TABLE FUNCTION statement and record_parse_locations is true in
  // AnalyzerOptions.
  absl::optional<ParseLocationRange> name_parse_location_range;
  absl::optional<ParseLocationRange> type_parse_location_range;
};

// To support ZETASQL_RET_CHECK_EQ.
bool operator==(const TVFSchemaColumn& a, const TVFSchemaColumn& b);
inline std::ostream& operator<<(std::ostream& out,
                                const TVFSchemaColumn& column);

// This represents a relation passed as an input argument to a TVF, or returned
// from a TVF. It either contains a list of columns, where each column contains
// a name and a type, or the relation may be a value table. For the value table
// case, there should be exactly one column, with an empty name. Note that each
// column name is a ZetaSQL IdString and so it must outlive the TVFRelation
// that contains it.
// TODO: Give this class a better name that suggests it is the schema
// of a table-valued argument or return value. The word 'relation' implies that
// it might contain an entire table, which is untrue.
class TVFRelation {
 public:
  using Column = TVFSchemaColumn;
  using ColumnList = std::vector<Column>;

  // Creates a new TVFRelation with a fixed list of columns.
  explicit TVFRelation(ColumnList columns)
      : columns_(std::move(columns)), is_value_table_(false) {}

  // Creates a new value-table TVFRelation with a single column of 'type' with
  // no name.
  //
  // A value table will act like each row is a single unnamed value with some
  // type rather than acting like each row is a vector of named columns.
  static TVFRelation ValueTable(const Type* type) {
    TVFRelation result = TVFRelation({Column("", type)});
    result.is_value_table_ = true;
    return result;
  }

  // Creates a new value-table TVFRelation with at least one column, and the
  // first column (column 0) is treated as the value of the row. Additional
  // columns may be present and must be pseudo-columns.
  static absl::StatusOr<TVFRelation> ValueTable(
      const Type* type, const ColumnList& pseudo_columns) {
    ColumnList columns;
    columns.reserve(pseudo_columns.size() + 1);
    columns.emplace_back("", type);
    for (const Column& column : pseudo_columns) {
      ZETASQL_RET_CHECK(column.is_pseudo_column);
      columns.push_back(column);
    }
    TVFRelation result = TVFRelation(std::move(columns));
    result.is_value_table_ = true;
    return result;
  }

  // Creates a new value-table TVFRelation with the provided column.
  static TVFRelation ValueTable(const Column& column) {
    TVFRelation result = TVFRelation({column});
    result.is_value_table_ = true;
    return result;
  }

  const ColumnList& columns() const { return columns_; }
  const Column& column(int i) const { return columns_[i]; }
  int num_columns() const { return columns_.size(); }
  bool is_value_table() const { return is_value_table_; }

  std::string GetSQLDeclaration(ProductMode product_mode) const;
  std::string DebugString() const;

  // Serializes this relation to a proto and deserializes it back again. This is
  // useful when serializing the ZetaSQL catalog.
  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         TVFRelationProto* proto) const;
  static absl::StatusOr<TVFRelation> Deserialize(
      const TVFRelationProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory);

 private:
  ColumnList columns_;
  bool is_value_table_;
};

bool operator == (const TVFRelation& a, const TVFRelation& b);

inline std::ostream& operator<<(std::ostream& out,
                                const TVFRelation& relation) {
  out << relation.DebugString();
  return out;
}

// This represents a model passed as an input argument to a TVF. It contains a
// pointer to the model object in the catalog.
class TVFModelArgument {
 public:
  // Creates a new TVFModelArgument using a model catalog object.
  // Does not take ownership of <model>, which must outlive this class.
  explicit TVFModelArgument(const Model* model) : model_(model) {}

  const Model* model() const { return model_; }
  std::string GetSQLDeclaration(ProductMode product_mode) const;
  std::string DebugString() const;

  // TODO: Implement serialize and deserialize.

 private:
  const Model* model_;  // Not owned.
};

// This represents a connection passed as an input argument to a TVF. It
// contains a pointer to the connection object in the catalog.
class TVFConnectionArgument {
 public:
  // Creates a new TVFConnectionArgument using a connection catalog object.
  // Does not take ownership of <connection>, which must outlive this class.
  explicit TVFConnectionArgument(const Connection* connection)
      : connection_(connection) {}

  const Connection* connection() const { return connection_; }
  std::string GetSQLDeclaration(ProductMode product_mode) const;
  std::string DebugString() const;

 private:
  const Connection* connection_;  // Not owned.
};

// This represents a descriptor passed as an input argument to a TVF. It
// contains a list of column names.
class TVFDescriptorArgument {
 public:
  explicit TVFDescriptorArgument(std::vector<std::string> column_names)
      : column_names_(std::move(column_names)) {}

  std::vector<std::string> column_names() const { return column_names_; }
  std::string DebugString() const;

 private:
  std::vector<std::string> column_names_;
};

// This represents one input argument to a call to a TVF.
// Each such call includes zero or more input arguments.
// Each input argument may be either scalar or relation.
class TVFInputArgumentType {
 public:
  // Creates a scalar input argument with a single type.
  explicit TVFInputArgumentType(const InputArgumentType& input_arg_type)
      : kind_(TVFInputArgumentTypeKind::SCALAR),
        scalar_arg_type_(input_arg_type.type()) {
    if (input_arg_type.literal_value() != nullptr) {
      scalar_arg_value_.reset(new Value(*input_arg_type.literal_value()));
    }
  }

  // Creates a relation input argument of several columns, where each column
  // contains a name and a type. If the input argument is a value table, the
  // vector should have one element with an empty name.
  explicit TVFInputArgumentType(const TVFRelation& relation)
      : kind_(TVFInputArgumentTypeKind::RELATION), relation_(relation) {}

  // Creates a model argument.
  explicit TVFInputArgumentType(const TVFModelArgument& model)
      : kind_(TVFInputArgumentTypeKind::MODEL), model_(model) {}

  // Creates a connection argument.
  explicit TVFInputArgumentType(const TVFConnectionArgument& connection)
      : kind_(TVFInputArgumentTypeKind::CONNECTION), connection_(connection) {}

  explicit TVFInputArgumentType(
      const TVFDescriptorArgument& descriptor_argument)
      : kind_(TVFInputArgumentTypeKind::DESCRIPTOR),
        descriptor_argument_(descriptor_argument) {}

  bool is_scalar() const { return kind_ == TVFInputArgumentTypeKind::SCALAR; }
  bool is_relation() const {
    return kind_ == TVFInputArgumentTypeKind::RELATION;
  }
  bool is_model() const { return kind_ == TVFInputArgumentTypeKind::MODEL; }
  bool is_connection() const {
    return kind_ == TVFInputArgumentTypeKind::CONNECTION;
  }
  bool is_descriptor() const {
    return kind_ == TVFInputArgumentTypeKind::DESCRIPTOR;
  }
  absl::StatusOr<InputArgumentType> GetScalarArgType() const {
    ZETASQL_RET_CHECK(kind_ == TVFInputArgumentTypeKind::SCALAR);
    if (scalar_arg_value_ != nullptr) {
      return InputArgumentType(*scalar_arg_value_);
    } else {
      return InputArgumentType(scalar_arg_type_);
    }
  }

  // Returns the resolved expression for scalar arguments, if present. This is
  // optional, and is not owned or serialized. If present, this class assumes
  // that it is consistent with scalar_arg_type_ and scalar_arg_value_.
  const ResolvedExpr* scalar_expr() const {
    ZETASQL_DCHECK(is_scalar());
    return scalar_expr_;
  }
  void set_scalar_expr(const ResolvedExpr* expr) {
    ZETASQL_DCHECK(is_scalar());
    scalar_expr_ = expr;
  }

  // TODO: Rename to GetRelation and return StatusOr to keep
  // consistent with GetScalarArgType above.
  const TVFRelation& relation() const {
    ZETASQL_DCHECK(is_relation());
    return relation_;
  }
  const TVFModelArgument& model() const {
    ZETASQL_DCHECK(is_model());
    return model_;
  }
  const TVFConnectionArgument& connection() const {
    ZETASQL_DCHECK(is_connection());
    return connection_;
  }

  const TVFDescriptorArgument& descriptor_argument() const {
    ZETASQL_DCHECK(is_descriptor());
    return descriptor_argument_;
  }

  std::string DebugString() const {
    if (kind_ == TVFInputArgumentTypeKind::RELATION) {
      return relation_.DebugString();
    } else if (kind_ == TVFInputArgumentTypeKind::MODEL) {
      return model_.DebugString();
    } else if (kind_ == TVFInputArgumentTypeKind::CONNECTION) {
      return connection_.DebugString();
    } else if (kind_ == TVFInputArgumentTypeKind::DESCRIPTOR) {
      return descriptor_argument_.DebugString();
    } else if (scalar_arg_value_ != nullptr) {
      return InputArgumentType(*scalar_arg_value_).DebugString();
    } else {
      return InputArgumentType(scalar_arg_type_).DebugString();
    }
  }

 private:
  enum class TVFInputArgumentTypeKind {
    UNKNOWN,
    CONNECTION,
    MODEL,
    RELATION,
    SCALAR,
    DESCRIPTOR
  };
  // Defines whether this is a relation, scalar argument or a model.
  const TVFInputArgumentTypeKind kind_;

  // TODO: Refactor and use absl::optional instead of having multiple
  // member variables.
  // Only one of the following is defined, based on kind_.
  const TVFRelation relation_ = TVFRelation({});
  const TVFModelArgument model_ = TVFModelArgument(nullptr);
  const TVFConnectionArgument connection_ = TVFConnectionArgument(nullptr);
  const TVFDescriptorArgument descriptor_argument_ =
      TVFDescriptorArgument(std::vector<std::string>());
  const Type* scalar_arg_type_ = nullptr;

  // This is the literal value for 'scalar_arg_type_', if applicable. We store
  // it here separately because the InputArgumentType class does not own its
  // Value, but instead holds a pointer to an external Value. The Value provided
  // in the class constructor does not live as long as this class instance does,
  // thus the need to keep a separate owned copy here as a field.
  std::shared_ptr<Value> scalar_arg_value_;

  // This is the resolved expression for scalar arguments. This is optional,
  // and is not owned or serialized.
  const ResolvedExpr* scalar_expr_ = nullptr;

  // Copyable.
};

struct TVFSignatureOptions {
  // Deprecation warnings associated with the body of a SQL TVF.
  std::vector<FreestandingDeprecationWarning> additional_deprecation_warnings;
};

// This class contains information about a specific resolved TVF call. It
// includes the input arguments passed into the TVF call and also its output
// schema (including whether it is a value table). Engines may also subclass
// this to include more information if needed.
class TVFSignature {
 public:
  // Represents a TVF call that returns 'output_schema'.
  TVFSignature(const std::vector<TVFInputArgumentType>& input_arguments,
               const TVFRelation& result_schema,
               const TVFSignatureOptions& options = {})
      : input_arguments_(input_arguments),
        result_schema_(result_schema),
        options_(options) {}

  TVFSignature(const TVFSignature&) = delete;
  TVFSignature& operator=(const TVFSignature&) = delete;
  virtual ~TVFSignature() {}

  const std::vector<TVFInputArgumentType>& input_arguments() const {
    return input_arguments_;
  }
  const TVFInputArgumentType& argument(int idx) const {
    return input_arguments_[idx];
  }
  const TVFRelation& result_schema() const { return result_schema_; }
  const TVFSignatureOptions& options() const { return options_; }

  virtual std::string DebugString(bool verbose) const {
    std::vector<std::string> arg_debug_strings;
    arg_debug_strings.reserve(input_arguments_.size());
    for (const TVFInputArgumentType& input_argument : input_arguments_) {
      arg_debug_strings.push_back(input_argument.DebugString());
    }
    std::string ret = absl::StrCat("(", absl::StrJoin(arg_debug_strings, ", "),
                                   ") -> ", result_schema_.DebugString());
    if (verbose) {
      const std::string deprecation_warnings_debug_string =
          DeprecationWarningsToDebugString(
              options_.additional_deprecation_warnings);
      if (!deprecation_warnings_debug_string.empty()) {
        absl::StrAppend(&ret, " ", deprecation_warnings_debug_string);
      }
    }
    return ret;
  }

  // Returns AnonymizationInfo related to a resolved call of this TVF.
  // For further details, see:
  //
  // (broken link).
  //
  // This method only returns AnonymizationInfo for TVFs that produce private
  // user data and that support anonymization queries.
  std::optional<const AnonymizationInfo> GetAnonymizationInfo() const {
    return anonymization_info_ == nullptr
               ? std::nullopt
               : std::optional<const zetasql::AnonymizationInfo>(
                     *anonymization_info_);
  }
  void SetAnonymizationInfo(
      std::unique_ptr<AnonymizationInfo> anonymization_info) {
    anonymization_info_ = std::move(anonymization_info);
  }
  bool SupportsAnonymization() const {
    return GetAnonymizationInfo().has_value();
  }

  std::string DebugString() const { return DebugString(/*verbose=*/false); }

  // Returns whether or not this TVFCall is a specific table-valued function
  // call interface or implementation.
  template <class TVFCallSubclass>
  bool Is() const {
    return dynamic_cast<const TVFCallSubclass*>(this) != nullptr;
  }

  // Returns this TVFCall as TVFCall*.  Must only be used when it is known that
  // the object *is* this subclass, which can be checked using Is() before
  // calling GetAs().
  template <class TVFCallSubclass>
  TVFCallSubclass* GetAs() {
    return static_cast<TVFCallSubclass*>(this);
  }
  template <class TVFCallSubclass>
  const TVFCallSubclass* GetAs() const {
    return static_cast<const TVFCallSubclass*>(this);
  }

 private:
  // Returns the input arguments passed into this TVF call.
  const std::vector<TVFInputArgumentType> input_arguments_;

  // Returns the output schema returned by this TVF call.
  const TVFRelation result_schema_;

  const TVFSignatureOptions options_;

  // The AnonymizationInfo related to a resolved call of this TVF. See
  // (broken link) for further details.
  std::unique_ptr<AnonymizationInfo> anonymization_info_;
};

// This represents a TVF that always returns a relation with the same fixed
// output schema.
class FixedOutputSchemaTVF : public TableValuedFunction {
 public:
  // Constructs a new TVF object with the given name and fixed output schema.
  FixedOutputSchemaTVF(const std::vector<std::string>& function_name_path,
                       const FunctionSignature& signature,
                       const TVFRelation& result_schema,
                       TableValuedFunctionOptions tvf_options = {})
      : TableValuedFunction(function_name_path, signature, tvf_options),
        result_schema_(result_schema) {}

  // Constructs a new TVF object with the given name, anonymization info and
  // fixed output schema.
  FixedOutputSchemaTVF(const std::vector<std::string>& function_name_path,
                       const FunctionSignature& signature,
                       std::unique_ptr<AnonymizationInfo> anonymization_info,
                       const TVFRelation& result_schema,
                       TableValuedFunctionOptions tvf_options = {})
      : TableValuedFunction(function_name_path, signature,
                            std::move(anonymization_info), tvf_options),
        result_schema_(result_schema) {}

  FixedOutputSchemaTVF(const FixedOutputSchemaTVF&) = delete;
  FixedOutputSchemaTVF& operator=(const FixedOutputSchemaTVF&) = delete;
  ~FixedOutputSchemaTVF() override {}

  const TVFRelation& result_schema() const { return result_schema_; }

  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         TableValuedFunctionProto* proto) const override;

  static absl::Status Deserialize(
      const TableValuedFunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result);

  // Returns the fixed output schema set in the constructor.
  absl::Status Resolve(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& actual_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* tvf_signature) const override;

 private:
  // This is the fixed output schema of the TVF.
  TVFRelation result_schema_;
};

// This represents a TVF that accepts a relation for its first argument. The TVF
// returns a relation with the same output schema as this input relation. The
// TVF may also accept additional arguments as defined by the signature passed
// to the constructor.
class ForwardInputSchemaToOutputSchemaTVF : public TableValuedFunction {
 public:
  // Constructs a new instance of this TVF with name 'function_name_path'.
  // 'signature' specifies the number and types of arguments that the TVF
  // accepts. This signature must have at least one argument and the first
  // argument must be a relation, or otherwise the Resolve method returns an
  // error.
  ForwardInputSchemaToOutputSchemaTVF(
      const std::vector<std::string>& function_name_path,
      const FunctionSignature& signature,
      TableValuedFunctionOptions tvf_options = {})
      : TableValuedFunction(function_name_path, signature, tvf_options) {
    ZETASQL_CHECK_OK(CheckIsValid());
  }

  ForwardInputSchemaToOutputSchemaTVF(
      const ForwardInputSchemaToOutputSchemaTVF&) = delete;
  ForwardInputSchemaToOutputSchemaTVF& operator=(
      const ForwardInputSchemaToOutputSchemaTVF&) = delete;
  ~ForwardInputSchemaToOutputSchemaTVF() override {}

  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         TableValuedFunctionProto* proto) const override;

  static absl::Status Deserialize(
      const TableValuedFunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result);

  absl::Status Resolve(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& actual_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* output_tvf_signature) const override;

 private:
  // Performs some quick sanity checks on the function signature.
  absl::Status CheckIsValid() const;
};

// This represents a TVF that accepts a relation for its first (templated)
// argument. The TVF returns a relation with a schema that is constructed by
// copying the schema of input relation and appending <extra_columns_> to the
// schema. The TVF may also accept additional arguments as defined by the
// signature passed to the constructor.
class ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF
    : public TableValuedFunction {
 public:
  ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
      const std::vector<std::string>& function_name_path,
      const FunctionSignature& signature,
      const std::vector<TVFSchemaColumn>& extra_columns,
      TableValuedFunctionOptions tvf_options = {})
      : TableValuedFunction(function_name_path, signature, tvf_options),
        extra_columns_(extra_columns) {
    ZETASQL_CHECK_OK(IsValidForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
        signature.IsTemplated(), extra_columns));
  }

  ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
      const ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF&) = delete;
  ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF& operator=(
      const ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF&) = delete;
  ~ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF() override {}

  absl::Status Resolve(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& actual_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* output_tvf_signature) const override;

  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         TableValuedFunctionProto* proto) const override;

  static absl::Status Deserialize(
      const TableValuedFunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result);

  // This method checks if <extra_columns> is valid. Specifically, it checks:
  //   a. if extra column name is empty.
  //   b. if extra column name is duplicated.
  //   c. if input table is non-templated, which is invalid usage for this
  //   tvf.
  //   d. if extra column is pseudo column, which is invalid usage for this tvf.
  absl::Status IsValidForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
      bool isTemplated,
      const std::vector<TVFSchemaColumn>& extra_columns) const;

 private:
  const std::vector<TVFSchemaColumn> extra_columns_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TABLE_VALUED_FUNCTION_H_
