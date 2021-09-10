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

#include "zetasql/public/input_argument_type.h"

#include <algorithm>

#include "zetasql/base/logging.h"
#include "zetasql/public/table_valued_function.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

// Determine equivalence class of the input argument type, for ordering
// purposes via InputArgumentTypeLess.  Typed and untyped nulls are
// considered in the same class, non-null literals are in their own class,
// and non-literals and parameters are in the same class.
static int InputArgumentTypeEquivalenceClass(const InputArgumentType& type) {
  if (type.is_untyped() || type.is_literal_null()) {
    return 2;
  }
  if (type.is_literal()) {
    return 1;
  }
  // Non-literals (including parameters).
  return 0;
}

bool InputArgumentTypeLess::operator()(
    const InputArgumentType& type1, const InputArgumentType& type2) const {
  // If arguments have different type kinds, then order by kind.
  if (type1.type() != nullptr && type2.type() != nullptr &&
      type1.type()->kind() != type2.type()->kind()) {
    return type1.type()->kind() < type2.type()->kind();
  }

  // For same type kinds, generally order non-literals (including parameters)
  // before literals, and literals before nulls (with untyped nulls last).
  // We determine the equivalence classes of the InputArgumentTypes and
  // order between them.
  int type1_class = InputArgumentTypeEquivalenceClass(type1);
  int type2_class = InputArgumentTypeEquivalenceClass(type2);
  if (type1_class == type2_class) {
    // They are in the same equivalence class.  For complex types we still
    // need to handle the case of different distinct types for the same type
    // kind (for example, two different struct literals, etc.).  We order
    // between them via DebugString();
    if (type1.type() != nullptr && !type1.type()->IsSimpleType()) {
      return type1.DebugString() < type2.DebugString();
    }
  }
  return type1_class < type2_class;
}

bool InputArgumentType::operator==(const InputArgumentType& rhs) const {
  const bool types_equal =
      (type_ == nullptr && rhs.type_ == nullptr) ||
      (type_ != nullptr && rhs.type_ != nullptr && type_->Equals(rhs.type_));
  return category_ == rhs.category_ && types_equal &&
         is_literal_null() == rhs.is_literal_null() &&
         is_literal_empty_array() == rhs.is_literal_empty_array();
}

bool InputArgumentType::operator!=(const InputArgumentType& type) const {
  return !(*this == type);
}

InputArgumentType::InputArgumentType(const Value& literal_value,
                                     bool is_default_argument_value)
    : category_(kTypedLiteral),
      type_(literal_value.type()),
      literal_value_(literal_value),
      is_default_argument_value_(is_default_argument_value) {
  if (literal_value.type()->IsStruct()) {
    if (literal_value.is_null()) {
      // This is a NULL struct, so its field InputArgumentTypes are the
      // struct's field types.
      for (const StructType::StructField& struct_field :
           literal_value.type()->AsStruct()->fields()) {
        field_types_.push_back(InputArgumentType(struct_field.type));
      }
    } else {
      for (const Value& field_value : literal_value.fields()) {
        field_types_.push_back(InputArgumentType(field_value));
      }
    }
  }
}

InputArgumentType::InputArgumentType(const Type* type, bool is_query_parameter)
    : category_(is_query_parameter ? kTypedParameter : kTypedExpression),
      type_(type) {
  ZETASQL_DCHECK(type != nullptr);
  if (type->IsStruct()) {
    for (const StructType::StructField& struct_field :
             type->AsStruct()->fields()) {
      // The struct itself might be a parameter, but its fields should not
      // coerce like parameters.
      field_types_.push_back(InputArgumentType(struct_field.type,
                                               false /* is_query_parameter */));
    }
  }
}

std::string InputArgumentType::UserFacingName(ProductMode product_mode) const {
  if (is_untyped_null()) {
    return "NULL";
  } else if (is_untyped_empty_array()) {
    return "[]";
  } else if (is_relation()) {
    std::vector<std::string> type_strings;
    type_strings.reserve(relation_input_schema().num_columns());
    for (const TVFRelation::Column& column :
         relation_input_schema().columns()) {
      type_strings.push_back(column.type->ShortTypeName(product_mode));
      if (!relation_input_schema().is_value_table()) {
        type_strings.back() =
            absl::StrCat(column.name, " ", type_strings.back());
      }
    }
    return absl::StrCat("TABLE<", absl::StrJoin(type_strings, ", "), ">");
  } else if (is_model()) {
    return "MODEL";
  } else if (is_connection()) {
    return "CONNECTION";
  } else if (is_lambda()) {
    return "LAMBDA";
  }
  if (type() == nullptr) {
    return DebugString(false);
  }
  return type()->ShortTypeName(product_mode);
}

std::string InputArgumentType::DebugString(bool verbose) const {
  if (is_lambda()) {
    return "LAMBDA";
  }

  std::string prefix;
  if (is_untyped_null()) {
    absl::StrAppend(&prefix, verbose ? "untyped" : "", "NULL");
    return prefix;
  } else if (is_untyped_empty_array()) {
    absl::StrAppend(&prefix, verbose ? "untyped" : "", "empty array");
    return prefix;
  } else if (literal_value_.has_value()) {
    if (literal_value_.value().is_null()) {
      absl::StrAppend(&prefix, "null ");
    } else if (type()->IsSimpleType()) {
      absl::StrAppend(&prefix, "literal ");
    }
  } else if (verbose && is_query_parameter()) {
    if (is_untyped()) {
      absl::StrAppend(&prefix, "untyped ");
    }
    absl::StrAppend(&prefix, "parameter ");
  }
  if (type_ == nullptr) {
    return prefix;
  } else {
    return absl::StrCat(prefix, type_->DebugString());
  }
}

// static
std::string InputArgumentType::ArgumentsToString(
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  constexpr int kMaxArgumentsStringLength = 1024;
  std::string arguments_string;
  bool first = true;
  for (const InputArgumentType& argument : arguments) {
    absl::StrAppend(&arguments_string, (first ? "" : ", "),
                    argument.UserFacingName(product_mode));
    if (arguments_string.size() > kMaxArgumentsStringLength) {
      constexpr absl::string_view kEllipses = "...";
      arguments_string.resize(kMaxArgumentsStringLength - kEllipses.size());
      arguments_string.append(std::string(kEllipses));
      break;
    }
    first = false;
  }
  return arguments_string;
}

// static
InputArgumentType InputArgumentType::RelationInputArgumentType(
    const TVFRelation& relation_input_schema) {
  InputArgumentType type;
  type.category_ = kRelation;
  type.relation_input_schema_.reset(new TVFRelation(relation_input_schema));
  return type;
}

InputArgumentType InputArgumentType::ModelInputArgumentType(
    const TVFModelArgument& model_arg) {
  InputArgumentType type;
  type.category_ = kModel;
  type.model_arg_.reset(new TVFModelArgument(model_arg));
  return type;
}

InputArgumentType InputArgumentType::ConnectionInputArgumentType(
    const TVFConnectionArgument& connection_arg) {
  InputArgumentType type;
  type.category_ = kConnection;
  type.connection_arg_.reset(new TVFConnectionArgument(connection_arg));
  return type;
}

InputArgumentType InputArgumentType::DescriptorInputArgumentType() {
  InputArgumentType type;
  type.category_ = kDescriptor;
  return type;
}

InputArgumentType InputArgumentType::LambdaInputArgumentType() {
  InputArgumentType type;
  type.category_ = kLambda;
  type.type_ = nullptr;
  return type;
}

bool InputArgumentTypeSet::Insert(
    const InputArgumentType& argument, bool set_dominant) {
  if (set_dominant) {
    dominant_argument_ = absl::make_unique<InputArgumentType>(argument);
  } else if (dominant_argument_ != nullptr &&
             dominant_argument_->type() != nullptr &&
             dominant_argument_->type()->IsSimpleType() &&
             argument.type() != nullptr && !argument.type()->IsSimpleType() &&
             !argument.is_untyped_empty_array()) {
    dominant_argument_ = absl::make_unique<InputArgumentType>(argument);
  } else if (dominant_argument_ == nullptr && !argument.is_untyped()) {
    dominant_argument_ = absl::make_unique<InputArgumentType>(argument);
  }

  if (arguments_set_ != nullptr) {
    const bool inserted =
        zetasql_base::InsertIfNotPresent(arguments_set_.get(), argument);
    if (inserted) {
      arguments_vector_.push_back(argument);
    }
    ZETASQL_DCHECK_EQ(arguments_set_->size(), arguments_vector_.size());
    return inserted;
  } else {
    for (const InputArgumentType& arg : arguments_vector_) {
      if (arg == argument) return false;
    }
    // It didn't exist already, so we'll insert it.
    arguments_vector_.push_back(argument);

    // If the vector is big enough that we don't want to do linear scans,
    // start storing the hash_set too.  We still keep the bucket_count low
    // because we still don't expect these hash_sets to usually get large.
    if (arguments_vector_.size() > kMaxSizeBeforeMakingHashSet) {
      arguments_set_ = absl::make_unique<ArgumentsHashSet>(
          arguments_vector_.begin(), arguments_vector_.end(),
          10 /* bucket_count */);
    }
    return true;
  }
}

void InputArgumentTypeSet::clear() {
  arguments_set_.reset();
  arguments_vector_.clear();
  dominant_argument_.reset();
}

std::string InputArgumentTypeSet::ToString(bool verbose) const {
  std::vector<InputArgumentType> argument_list = arguments_vector_;
  std::sort(argument_list.begin(), argument_list.end(),
            InputArgumentTypeLess());
  std::string dominant_argument;
  if (verbose) {
    absl::StrAppend(&dominant_argument, ": dominant_argument(",
                    (dominant_argument_ != nullptr
                         ? dominant_argument_->DebugString(verbose)
                         : "null"),
                    ")");
  }
  return absl::StrCat("{", InputArgumentType::ArgumentsToString(argument_list),
                      "}", dominant_argument);
}

}  // namespace zetasql
