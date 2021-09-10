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

#ifndef ZETASQL_PUBLIC_INPUT_ARGUMENT_TYPE_H_
#define ZETASQL_PUBLIC_INPUT_ARGUMENT_TYPE_H_

#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/types/optional.h"

namespace zetasql {

class TVFConnectionArgument;
class TVFModelArgument;
class TVFRelation;

// Attributes of a function argument when a function is actually called.
// Identifies the argument's data type and its literal value if appropriate.
//
// This is mainly an internal class used while resolving function calls.
// This is exposed publicly because some of the FunctionOptions callbacks
// use it to customize resolution for particular Functions.
class InputArgumentType {
 public:
  // Same as InputArgumentType::UntypedNull(). Consider using the latter.
  InputArgumentType() : category_(kUntypedNull), type_(types::Int64Type()) {}

  // Constructor for literal arguments. <literal_value> cannot be nullptr.
  // A Value can be either a NULL or non-NULL Value of any ZetaSQL Type.
  // The <literal_value> is not owned and must outlive all referencing
  // InputArgumentTypes. A true <is_default_argument_value> indicates that the
  // related function call argument was unspecified by the user, and
  // <literal_value> was injected as the default value for the unspecified
  // argument.
  explicit InputArgumentType(const Value& literal_value,
                             bool is_default_argument_value = false);

  // Constructor for non-literal and parameter arguments.
  explicit InputArgumentType(const Type* type,
                             bool is_query_parameter = false);

  // Constructor for STRUCT arguments that can have a mix of literal and
  // non-literal fields.  If a STRUCT argument is a literal or has all
  // non-literal fields, then the previous constructors can be used.
  InputArgumentType(const StructType* type,
                    const std::vector<InputArgumentType>& field_types)
      : category_(kTypedExpression), type_(type), field_types_(field_types) {}

  ~InputArgumentType() {}

  // This may return nullptr (such as for for lambda).
  const Type* type() const { return type_; }

  const std::vector<InputArgumentType>& field_types() const {
    return field_types_;
  }
  size_t field_types_size() const { return field_types_.size(); }
  const InputArgumentType& field_type(int i) const {
    return field_types_.at(i);
  }

  // Check for or get a literal value.
  // WARNING: Untyped literals ("NULL" or "[]") are not included here.
  // They must be checked for separately using is_untyped() methods.
  const Value* literal_value() const {
    return literal_value_.has_value() ? &literal_value_.value() : nullptr;
  }
  bool is_literal() const { return literal_value_.has_value(); }
  bool is_literal_null() const {
    return literal_value_.has_value() && literal_value_.value().is_null();
  }
  bool is_literal_empty_array() const {
    return literal_value_.has_value() &&
           literal_value_.value().is_empty_array();
  }

  // Check for an untyped literal value ("NULL" or "[]").
  // WARNING: These are distinct argument types not included in is_literal().
  bool is_untyped() const {
    return category_ == kUntypedNull || category_ == kUntypedParameter ||
           category_ == kUntypedEmptyArray;
  }
  bool is_untyped_null() const { return category_ == kUntypedNull; }
  bool is_untyped_empty_array() const {
    return category_ == kUntypedEmptyArray;
  }

  // Check for either literal (typed) or untyped NULL or [].
  bool is_null() const {
    return is_literal_null() || is_untyped_null();
  }
  bool is_empty_array() const {
    return is_literal_empty_array() || is_untyped_empty_array();
  }

  // Indicates whether or not the argument is a SQL query parameter, whose
  // value is unknown at resolution time but which is constant for the
  // query execution.  Some functions require one or more arguments to
  // be either a literal or parameter (for example, the separator argument
  // to STRING_AGG).
  bool is_query_parameter() const {
    return category_ == kTypedParameter || category_ == kUntypedParameter;
  }

  bool is_untyped_query_parameter() const {
    return category_ == kUntypedParameter;
  }
  bool is_relation() const { return category_ == kRelation; }
  bool is_model() const { return category_ == kModel; }
  bool is_connection() const { return category_ == kConnection; }
  bool is_lambda() const { return category_ == kLambda; }

  bool is_default_argument_value() const {
    return is_default_argument_value_;
  }

  // Argument type name to be used in user facing text (i.e. error messages).
  std::string UserFacingName(ProductMode product_mode) const;

  // If <verbose>, then NULLs are identified as untyped if appropriate,
  // and parameters are identified.
  // External error messaging should not be verbose.
  std::string DebugString(bool verbose = false) const;

  // Returns a comma-separated string in vector order.  If <verbose>, then
  // the generated string for each argument is also verbose.
  static std::string ArgumentsToString(
      const std::vector<InputArgumentType>& arguments,
      ProductMode product_mode = PRODUCT_INTERNAL);

  // Represents an argument type for untyped NULL that is coercible to any other
  // type. By convention, <type_> defaults to INT64.
  static InputArgumentType UntypedNull() {
    return InputArgumentType(kUntypedNull, types::Int64Type());
  }

  // Represents an argument type for untyped empty array that is coercible to
  // any other array type. By convention, <type_> defaults to ARRAY<INT64>.
  static InputArgumentType UntypedEmptyArray() {
    return InputArgumentType(kUntypedEmptyArray,
                             types::Int64ArrayType());
  }

  // Represents an argument type for untyped query parameters that is coercible
  // to any other type. <type_> defaults to INT64.
  static InputArgumentType UntypedQueryParameter() {
    return InputArgumentType(kUntypedParameter, types::Int64Type());
  }

  // Constructor for relation arguments. Only for use when analyzing
  // table-valued functions. For more information, see table_valued_function.h.
  // 'relation_input_schema' specifies the schema for the provided input
  // relation when function is called.
  static InputArgumentType RelationInputArgumentType(
      const TVFRelation& relation_input_schema);

  // Constructor for model arguments. Only for use when analyzing
  // table-valued functions. 'model_arg' specifies the model object for the
  // provided input. For more information about model argument,
  // see table_valued_function.h.
  static InputArgumentType ModelInputArgumentType(
      const TVFModelArgument& model_arg);

  // Constructor for connection arguments. Only for use when analyzing
  // table-valued functions. 'connection_arg' specifies the connection object
  // for the provided input. For more information about connection argument, see
  // table_valued_function.h.
  static InputArgumentType ConnectionInputArgumentType(
      const TVFConnectionArgument& connection_arg);

  // Constructor for descriptor argument. Only for use when analyzing
  // table-valued functions. For more information about descriptor
  // argument, see table_valued_function.h.
  static InputArgumentType DescriptorInputArgumentType();

  // Constructor for lambda arguments. Only for use when analyzing lambda
  // arguments.
  static InputArgumentType LambdaInputArgumentType();

  bool has_relation_input_schema() const {
    return relation_input_schema_ != nullptr;
  }
  const TVFRelation& relation_input_schema() const {
    ZETASQL_DCHECK(relation_input_schema_ != nullptr);
    return *relation_input_schema_;
  }

  // Determines equality/inequality of two InputArgumentTypes, considering Type
  // equality via Type::Equals() and whether they are literal or NULL.
  // The comparison between non-null literal InputArgumentTypes does not
  // consider their Values so they are considered equal here.
  bool operator==(const InputArgumentType& type) const;
  bool operator!=(const InputArgumentType& type) const;

 private:
  enum Category {
    kTypedExpression,  // non-literal, non-parameter
    kTypedLiteral,
    kTypedParameter,
    kUntypedParameter,
    kUntypedNull,
    kUntypedEmptyArray,
    kRelation,
    kModel,
    kConnection,
    kDescriptor,
    kLambda,
  };

  explicit InputArgumentType(Category category, const Type* type)
      : category_(category), type_(type) {}

  Category category_ = kUntypedNull;
  const Type* type_ = nullptr;
  absl::optional<Value> literal_value_;  // only set for kTypedLiteral.

  // True if this InputArgumentType was constructed from a default function
  // argument value.
  bool is_default_argument_value_ = false;

  // Populated only for STRUCT type arguments. Stores the InputArgumentType of
  // the struct fields (in the same order). We need this for STRUCT coercion
  // where we need to check field-by-field whether 'from_struct' field is
  // castable/coercible to the corresponding 'to_struct' field type.
  std::vector<InputArgumentType> field_types_;

  // This is only non-NULL for table-valued functions. It holds a list of
  // provided column names and types for a relation argument. This is a shared
  // pointer only because the InputArgumentType is copyable and there is only
  // need for one TVFRelation instance to exist.
  std::shared_ptr<const TVFRelation> relation_input_schema_;

  // This is only non-NULL for table-valued functions. It holds the model
  // argument. This is a shared pointer only because the InputArgumentType is
  // copyable and there is only need for one TVFModelArgument instance to exist.
  std::shared_ptr<const TVFModelArgument> model_arg_;

  // This is only non-NULL for table-valued functions. It holds the connection
  // argument. This is a shared pointer only because the InputArgumentType is
  // copyable and there is only need for one TVFConnectionArgument instance to
  // exist.
  std::shared_ptr<const TVFConnectionArgument> connection_arg_;
  // Copyable.
};

// Only hashes the type kind, not the type itself (so two different enums will
// hash the same).  WARNING - this could perform poorly if there are many
// different arguments of the same non-simple type kind (ENUM, PROTO, ARRAY,
// or STRUCT).  This is potentially possible for instance for a large IN-list
// of one of these types (though ENUM would be currently ok).  TODO:
// Address this if/when we support IN-list of non-simple types, which may
// require updating the InputArgumentType::operator==() function.
struct InputArgumentTypeLossyHasher {
  size_t operator()(const InputArgumentType& type) const {
    return absl::HashOf(type.type() != nullptr ? type.type()->kind() : -2,
                        type.is_literal(), type.is_untyped(),
                        type.is_query_parameter(), type.is_literal_null());
  }
};

// Less function for InputArgumentType.  It is currently used to provide
// deterministic ordering for a set of InputArgumentTypes.  Arguments of the
// same TypeKind sort together, and within a TypeKind the non-literals sort
// before literals, which sort before nulls.
struct InputArgumentTypeLess {
  bool operator()(const InputArgumentType& type1,
                  const InputArgumentType& type2) const;
};

// Set container for InputArgumentType which has a special property where we
// can fetch the dominant argument.
//
// The dominant argument is the single argument that most determines the
// supertype.
// 1. If an argument is Inserted with <set_dominant> then that argument
//    is set as dominant.
// 2. Otherwise, if any non-simple typed arguments (other than an untyped
//    empty array) exist then the first non-simple-typed argument is dominant.
// 3. Otherwise, the first argument that isn't an untyped NULL or empty
//    array is dominant.
// 4. Otherwise, there is no dominant argument.
// The dominant argument will always be the first argument of its class so that
// supertyping prefers the type and field names from the leftmost argument.
class InputArgumentTypeSet {
 public:
  InputArgumentTypeSet() {}
  InputArgumentTypeSet(const InputArgumentTypeSet&) = delete;
  InputArgumentTypeSet& operator=(const InputArgumentTypeSet&) = delete;
  ~InputArgumentTypeSet() {}

  const InputArgumentType* dominant_argument() const {
    return dominant_argument_.get();
  }

  // Return the set of unique InputArgumentTypes.
  // Order is arbitrary.
  const std::vector<InputArgumentType>& arguments() const {
    return arguments_vector_;
  }

  bool empty() const { return arguments_vector_.empty(); }

  // Insert a new InputArgumentType, updating <dominant_argument_> if
  // appropriate.  <set_dominant> will force the argument to be dominant.
  // Returns true if argument was inserted; false means it was already present.
  bool Insert(const InputArgumentType& argument, bool set_dominant = false);

  void clear();

  // Returns a comma-separated string surrounded by {}, where the arguments
  // are ordered by InputArgumentTypeLess.  If <verbose>, then untyped NULL
  // and parameter arguments are identified as such.
  std::string ToString(bool verbose = false) const;

 private:
  // We store the set of unique arguments in a vector rather than a hash_set
  // so we can iterate over it quickly, and clear it quickly.
  // Many callers call arguments() just so they can iterate through them,
  // and this is very slow in a hash_set.
  std::vector<InputArgumentType> arguments_vector_;

  // If the set of arguments gets large, we'll also start storing a hash_set of
  // arguments so we can do membership check in Insert in sub-linear time.
  const int kMaxSizeBeforeMakingHashSet = 4;
  using ArgumentsHashSet =
      absl::flat_hash_set<InputArgumentType, InputArgumentTypeLossyHasher>;
  std::unique_ptr<ArgumentsHashSet> arguments_set_;

  // The dominant argument, according to the definition above.
  std::unique_ptr<const InputArgumentType> dominant_argument_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_INPUT_ARGUMENT_TYPE_H_
