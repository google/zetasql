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

// Implementations of the various ValueExprs.

#include <algorithm>
#include <array>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/proto_value_conversion.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/parameters.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include <cstdint>
#include "zetasql/base/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

using zetasql::values::Bool;
using zetasql::values::Int64;

namespace zetasql {

// -------------------------------------------------------
// ValueExpr
// -------------------------------------------------------

ValueExpr::~ValueExpr() {}

// -------------------------------------------------------
// TableAsArrayExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<TableAsArrayExpr>> TableAsArrayExpr::Create(
    const std::string& table_name, const ArrayType* type) {
  return absl::WrapUnique(new TableAsArrayExpr(table_name, type));
}

absl::Status TableAsArrayExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> /* params_schemas */) {
  // Eval() ignores the parameters.
  return absl::OkStatus();
}

bool TableAsArrayExpr::Eval(absl::Span<const TupleData* const> /* params */,
                            EvaluationContext* context,
                            VirtualTupleSlot* result,
                            absl::Status* status) const {
  const Value& array = context->GetTableAsArray(table_name());
  if (!array.is_valid()) {
    *status = zetasql_base::OutOfRangeErrorBuilder()
              << "Table not populated with array: " << table_name();
    return false;
  } else if (!output_type()->Equals(array.type())) {
    *status = zetasql_base::OutOfRangeErrorBuilder()
              << "Type of populated table (as array) " << table_name()
              << " deviates from the "
              << "type reported in the catalog.\n"
              << "Actual: " << array.type()->DebugString() << "\n"
              << "Expected: " << output_type()->DebugString();
    return false;
  }

  result->SetValue(array);
  return true;
}

std::string TableAsArrayExpr::DebugInternal(const std::string& indent,
                                            bool verbose) const {
  return absl::StrCat("TableAsArrayExpr(", table_name(), ")");
}

TableAsArrayExpr::TableAsArrayExpr(const std::string& table_name,
                                   const ArrayType* type)
    : ValueExpr(type), table_name_(table_name) {}

// -------------------------------------------------------
// NewStructExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<NewStructExpr>> NewStructExpr::Create(
    const StructType* type, std::vector<std::unique_ptr<ExprArg>> args) {
  ZETASQL_RET_CHECK_EQ(type->num_fields(), args.size());
  for (int i = 0; i < args.size(); i++) {
    ZETASQL_RET_CHECK(args[i]->node()->AsValueExpr() != nullptr);
    ZETASQL_RET_CHECK(type->field(i).type->Equals(args[i]->node()->output_type()));
    ZETASQL_RET_CHECK(!args[i]->has_variable());
  }
  return absl::WrapUnique(new NewStructExpr(type, std::move(args)));
}

absl::Status NewStructExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  for (ExprArg* arg : mutable_field_list()) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(params_schemas));
  }
  return absl::OkStatus();
}

bool NewStructExpr::Eval(absl::Span<const TupleData* const> params,
                         EvaluationContext* context, VirtualTupleSlot* result,
                         absl::Status* status) const {
  int64_t values_size = 0;
  std::vector<Value> values(field_list().size());
  for (int i = 0; i < field_list().size(); ++i) {
    Value* field = &values[i];
    std::shared_ptr<TupleSlot::SharedProtoState> field_shared_state;
    VirtualTupleSlot field_result(field, &field_shared_state);
    if (!field_list()[i]->value_expr()->Eval(params, context, &field_result,
                                             status)) {
      return false;
    }
    values_size += field->physical_byte_size();
    if (values_size >= context->options().max_value_byte_size) {
      *status = zetasql_base::OutOfRangeErrorBuilder()
                << "Cannot construct struct Value larger than "
                << context->options().max_value_byte_size << " bytes";
      return false;
    }
  }
  result->SetValue(
      Value::UnsafeStruct(output_type()->AsStruct(), std::move(values)));
  return true;
}

std::string NewStructExpr::DebugInternal(const std::string& indent,
                                         bool verbose) const {
  std::string indent_child = indent + kIndentFork;
  std::string result = absl::StrCat("NewStructExpr(", indent_child,
                                    "type: ", output_type()->DebugString());
  if (!field_list().empty()) {
    int i = 0;
    for (auto ch : field_list()) {
      absl::StrAppend(&result, ",", indent_child, i, " ",
                      output_type()->AsStruct()->field(i).name, ": ",
                      ch->DebugInternal(indent + kIndentSpace, verbose));
      ++i;
    }
  }
  absl::StrAppend(&result, ")");
  return result;
}

NewStructExpr::NewStructExpr(const StructType* type,
                             std::vector<std::unique_ptr<ExprArg>> args)
    : ValueExpr(type) {
  SetArgs<ExprArg>(kField, std::move(args));
}

absl::Span<const ExprArg* const> NewStructExpr::field_list() const {
  return GetArgs<ExprArg>(kField);
}

absl::Span<ExprArg* const> NewStructExpr::mutable_field_list() {
  return GetMutableArgs<ExprArg>(kField);
}

// -------------------------------------------------------
// NewArrayExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<NewArrayExpr>> NewArrayExpr::Create(
    const ArrayType* array_type,
    std::vector<std::unique_ptr<ValueExpr>> elements) {
  for (const auto& e : elements) {
    ZETASQL_RET_CHECK(array_type->element_type()->Equals(e->output_type()));
  }
  return absl::WrapUnique(new NewArrayExpr(array_type, std::move(elements)));
}

absl::Status NewArrayExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  for (ExprArg* arg : mutable_elements()) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(params_schemas));
  }
  return absl::OkStatus();
}

bool NewArrayExpr::Eval(absl::Span<const TupleData* const> params,
                        EvaluationContext* context, VirtualTupleSlot* result,
                        absl::Status* status) const {
  int64_t values_size = 0;
  std::vector<Value> values(elements().size());
  for (int i = 0; i < elements().size(); ++i) {
    Value* element = &values[i];
    std::shared_ptr<TupleSlot::SharedProtoState> element_shared_state;
    VirtualTupleSlot element_result(element, &element_shared_state);
    if (!elements()[i]->value_expr()->Eval(params, context, &element_result,
                                           status)) {
      return false;
    }
    values_size += element->physical_byte_size();
    if (values_size >= context->options().max_value_byte_size) {
      *status = zetasql_base::OutOfRangeErrorBuilder()
                << "Cannot construct array Value larger than "
                << context->options().max_value_byte_size << " bytes";
      return false;
    }
  }
  result->SetValue(
      Value::UnsafeArray(output_type()->AsArray(), std::move(values)));
  return true;
}

std::string NewArrayExpr::DebugInternal(const std::string& indent,
                                        bool verbose) const {
  std::string indent_child = indent + kIndentSpace;
  std::vector<std::string> fstr;
  for (auto ch : elements()) {
    fstr.push_back(ch->DebugInternal(indent_child, verbose));
  }
  return verbose
             ? absl::StrCat("NewArrayExpr(", indent_child,
                            "type: ", output_type()->DebugString(), ",",
                            indent_child,
                            absl::StrJoin(fstr, "," + indent_child), ")")
             : absl::StrCat("NewArrayExpr(", absl::StrJoin(fstr, ", "), ")");
}

NewArrayExpr::NewArrayExpr(const ArrayType* array_type,
                           std::vector<std::unique_ptr<ValueExpr>> elements)
    : ValueExpr(array_type) {
  std::vector<std::unique_ptr<ExprArg>> args;
  args.reserve(elements.size());
  for (auto& e : elements) {
    args.push_back(absl::make_unique<ExprArg>(std::move(e)));
  }
  SetArgs<ExprArg>(kElement, std::move(args));
}

absl::Span<const ExprArg* const> NewArrayExpr::elements() const {
  return GetArgs<ExprArg>(kElement);
}

absl::Span<ExprArg* const> NewArrayExpr::mutable_elements() {
  return GetMutableArgs<ExprArg>(kElement);
}

// -------------------------------------------------------
// ArrayNestExpr
// -------------------------------------------------------

std::string ArrayNestExpr::DebugInternal(const std::string& indent,
                                         bool verbose) const {
  return absl::StrCat(
      "ArrayNestExpr(is_with_table=", is_with_table_,
      ArgDebugString({"element", "input"}, {k1, k1}, indent, verbose), ")");
}

::zetasql_base::StatusOr<std::unique_ptr<ArrayNestExpr>> ArrayNestExpr::Create(
    const ArrayType* array_type, std::unique_ptr<ValueExpr> element,
    std::unique_ptr<RelationalOp> input, bool is_with_table) {
  return absl::WrapUnique(new ArrayNestExpr(array_type, std::move(element),
                                            std::move(input), is_with_table));
}

absl::Status ArrayNestExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));
  const std::unique_ptr<const TupleSchema> input_schema =
      input()->CreateOutputSchema();
  return mutable_element()->SetSchemasForEvaluation(
      ConcatSpans(params_schemas, {input_schema.get()}));
}

bool ArrayNestExpr::Eval(absl::Span<const TupleData* const> params,
                         EvaluationContext* context, VirtualTupleSlot* result,
                         absl::Status* status) const {
  auto status_or_iter =
      input()->CreateIterator(params, /*num_extra_slots=*/0, context);
  if (!status_or_iter.ok()) {
    *status = status_or_iter.status();
    return false;
  }
  std::unique_ptr<TupleIterator> iter = std::move(status_or_iter).value();

  const bool iter_originally_preserved_order = iter->PreservesOrder();
  // We disable reordering when nesting a relation in an array for backwards
  // compatibility with the text-based reference implementation compliance
  // tests. Another advantage is that it effectively turns off scrambling for
  // simple [prepare_database] statements in the compliance tests, which makes
  // the results easier to read.
  *status = iter->DisableReordering();
  if (!status->ok()) return false;

  // For WITH tables, the array represents multiple rows, so we must track the
  // memory usage with a MemoryAccountant. For non-WITH tables, we simply ensure
  // that the array is not too large.
  MemoryAccountant* accountant = nullptr;
  if (is_with_table_) {
    accountant = context->memory_accountant();
  }
  std::vector<Value> output;
  std::function<void()> return_bytes = [accountant, &output]() {
    if (accountant != nullptr) {
      for (const Value& value : output) {
        accountant->ReturnBytes(value.physical_byte_size());
      }
    }
  };
  // If we fail early, return the accumulated bytes.
  auto cleanup = zetasql_base::MakeCleanup(return_bytes);
  int64_t output_byte_size = 0;  // Valid if 'is_with_table_' is false.
  while (true) {
    const TupleData* tuple = iter->Next();
    if (tuple == nullptr) {
      *status = iter->Status();
      if (!status->ok()) return false;
      break;
    }

    TupleSlot slot;
    if (!element()->EvalSimple(ConcatSpans(params, {tuple}), context, &slot,
                               status)) {
      return false;
    }
    Value& value = *slot.mutable_value();

    // Check for memory usage. See the comment for 'accountant' above.
    if (accountant == nullptr) {
      output_byte_size += value.physical_byte_size();
      if (output_byte_size > context->options().max_value_byte_size) {
        *status = zetasql_base::OutOfRangeErrorBuilder()
                  << "Cannot construct array Value larger than "
                  << context->options().max_value_byte_size << " bytes";
        return false;
      }
    } else {
      if (!accountant->RequestBytes(value.physical_byte_size(), status)) {
        return false;
      }
    }

    output.push_back(std::move(value));
  }

  // Free the memory. Ideally we would not do this here and instead do it when
  // the TupleIterator that stores the WITH table is deleted. But we have no way
  // to easily hold a MemoryAccountant reservation after this method returns. So
  // as a hack, we free the memory here and re-reserve it in the LetOp/LetExpr
  // that ends up holding the WITH table.
  return_bytes();
  result->SetValue(InternalValue::ArrayNotChecked(
      output_type()->AsArray(), iter_originally_preserved_order,
      std::move(output)));  // Invalidates 'output' (and therefore 'cleanup').
  output.clear();           // Nothing for 'cleanup' to do.
  return true;
}

ArrayNestExpr::ArrayNestExpr(const ArrayType* array_type,
                             std::unique_ptr<ValueExpr> element,
                             std::unique_ptr<RelationalOp> input,
                             bool is_with_table)
    : ValueExpr(array_type), is_with_table_(is_with_table) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
  SetArg(kElement, absl::make_unique<ExprArg>(std::move(element)));
}

const ValueExpr* ArrayNestExpr::element() const {
  return GetArg(kElement)->node()->AsValueExpr();
}

ValueExpr* ArrayNestExpr::mutable_element() {
  return GetMutableArg(kElement)->mutable_node()->AsMutableValueExpr();
}

const RelationalOp* ArrayNestExpr::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* ArrayNestExpr::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// DerefExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<DerefExpr>> DerefExpr::Create(
    const VariableId& name, const Type* type) {
  return absl::WrapUnique(new DerefExpr(name, type));
}

absl::Status DerefExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  int first_schema_idx = -1;
  int first_slot = -1;
  for (int i = 0; i < params_schemas.size(); ++i) {
    const TupleSchema* schema = params_schemas[i];
    absl::optional<int> slot = schema->FindIndexForVariable(name_);
    if (slot.has_value()) {
      ZETASQL_RET_CHECK_EQ(first_slot, -1) << "Duplicate name detected: " << name_;
      first_slot = slot.value();
      first_schema_idx = i;
    }
  }
  ZETASQL_RET_CHECK_GE(first_slot, 0) << "Missing name: " << name_;
  idx_in_params_ = first_schema_idx;
  slot_ = first_slot;
  return absl::OkStatus();
}

bool DerefExpr::Eval(absl::Span<const TupleData* const> params,
                     EvaluationContext* context, VirtualTupleSlot* result,
                     absl::Status* /* status */) const {
  DCHECK(idx_in_params_ >= 0 && slot_ >= 0)
      << "You forgot to call SetSchemasForEvaluation() " << name_;
  result->CopyFromSlot(params[idx_in_params_]->slot(slot_));
  return true;
}

std::string DerefExpr::DebugInternal(const std::string& indent,
                                     bool verbose) const {
  return verbose ? absl::StrCat("DerefExpr(", name().ToString(), ")")
                 : absl::StrCat("$", name().ToString());
}

DerefExpr::DerefExpr(const VariableId& name, const Type* type)
    : ValueExpr(type), name_(name) {}

// -------------------------------------------------------
// ConstExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<ConstExpr>> ConstExpr::Create(
    const Value& value) {
  return absl::WrapUnique(new ConstExpr(value));
}

absl::Status ConstExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> /* params_schemas */) {
  // Eval() ignores the parameters.
  return absl::OkStatus();
}

bool ConstExpr::Eval(absl::Span<const TupleData* const> /* params */,
                     EvaluationContext* context, VirtualTupleSlot* result,
                     absl::Status* /* status */) const {
  result->CopyFromSlot(slot_);
  return true;
}

std::string ConstExpr::DebugInternal(const std::string& indent,
                                     bool verbose) const {
  return absl::StrCat("ConstExpr(", value().DebugString(verbose), ")");
}

ConstExpr::ConstExpr(const Value& value) : ValueExpr(value.type()) {
  slot_.SetValue(value);
}

// -------------------------------------------------------
// FieldValueExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<FieldValueExpr>> FieldValueExpr::Create(
    int field_index, std::unique_ptr<ValueExpr> expr) {
  return absl::WrapUnique(new FieldValueExpr(field_index, std::move(expr)));
}

absl::Status FieldValueExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return mutable_input()->SetSchemasForEvaluation(params_schemas);
}

bool FieldValueExpr::Eval(absl::Span<const TupleData* const> params,
                          EvaluationContext* context, VirtualTupleSlot* result,
                          absl::Status* status) const {
  TupleSlot struct_slot;
  if (!input()->EvalSimple(params, context, &struct_slot, status)) {
    return false;
  }
  const Value& struct_value = struct_slot.value();
  Value field_value = struct_value.is_null()
                          ? Value::Null(output_type())
                          : struct_value.field(field_index());
  result->SetValueAndMaybeSharedProtoState(
      std::move(field_value), struct_slot.mutable_shared_proto_state());
  return true;
}

std::string FieldValueExpr::DebugInternal(const std::string& indent,
                                          bool verbose) const {
  return absl::StrCat("FieldValueExpr(", field_index(), ":", field_name(), ", ",
                      input()->DebugInternal(indent, verbose), ")");
}

FieldValueExpr::FieldValueExpr(int field_index, std::unique_ptr<ValueExpr> expr)
    : ValueExpr(expr->output_type()->AsStruct()->field(field_index).type),
      field_index_(field_index) {
  SetArg(kStruct, absl::make_unique<ExprArg>(std::move(expr)));
}

std::string FieldValueExpr::field_name() const {
  return GetArg(kStruct)
      ->value_expr()
      ->output_type()
      ->AsStruct()
      ->field(field_index_)
      .name;
}

const ValueExpr* FieldValueExpr::input() const {
  return GetArg(kStruct)->node()->AsValueExpr();
}

ValueExpr* FieldValueExpr::mutable_input() {
  return GetMutableArg(kStruct)->mutable_node()->AsMutableValueExpr();
}

// -------------------------------------------------------
// ProtoFieldReader
// -------------------------------------------------------

bool ProtoFieldReader::GetFieldValue(const TupleSlot& proto_slot,
                                     EvaluationContext* context,
                                     Value* field_value,
                                     absl::Status* status) const {
  context->set_last_get_field_value_call_read_fields_from_proto(this, false);

  const Value& proto_value = proto_slot.value();
  if (proto_value.is_null()) {
    if (access_info_.field_info.get_has_bit) {
      if (access_info_.return_default_value_when_unset) {
        *status = zetasql_base::InternalErrorBuilder()
                  << "ProtoFieldAccessInfo.return_default_value_when_unset "
                  << "must be false if field_info->get_has_bit is true";
        return false;
      }
      *field_value = Value::NullBool();
      return true;
    }
    if (access_info_.return_default_value_when_unset) {
      *field_value = access_info_.field_info.default_value;
      return true;
    }
    *field_value = Value::Null(access_info_.field_info.type);
    return true;
  }

  ProtoFieldValueMapKey value_map_key;
  value_map_key.proto_rep = InternalValue::GetProtoRep(proto_value);
  value_map_key.registry = registry_;

  // We store the ProtoFieldValueList in 'shared_state' if
  // EvaluationOptions::store_proto_field_value_maps is true. Otherwise,
  // 'value_list_owner' owns the ProtoFieldValueList.
  std::unique_ptr<ProtoFieldValueList> value_list_owner;
  std::shared_ptr<TupleSlot::SharedProtoState>& shared_state =
      *proto_slot.mutable_shared_proto_state();
  const std::unique_ptr<ProtoFieldValueList>* existing_value_list =
      shared_state->has_value()
          ? FindOrNull(shared_state->value(), value_map_key)
          : nullptr;
  const ProtoFieldValueList* value_list =
      existing_value_list == nullptr ? nullptr : existing_value_list->get();
  if (value_list == nullptr) {
    context->set_last_get_field_value_call_read_fields_from_proto(this, true);
    context->set_num_proto_deserializations(
        context->num_proto_deserializations() + 1);

    std::vector<const ProtoFieldInfo*> field_infos;
    field_infos.reserve(registry_->GetRegisteredFields().size());
    for (const ProtoFieldAccessInfo* access_info :
         registry_->GetRegisteredFields()) {
      field_infos.push_back(&access_info->field_info);
    }

    value_list_owner = absl::make_unique<ProtoFieldValueList>();
    value_list = value_list_owner.get();

    const absl::Status read_status = ReadProtoFields(
        field_infos, proto_value.ToCord(), value_list_owner.get());
    if (!read_status.ok()) {
      *status = read_status;
      return false;
    }

    // Store 'value_list' in 'proto_slot' if
    // EvaluationOptions::store_proto_field_value_maps is true.
    if (context->options().store_proto_field_value_maps) {
      if (!shared_state->has_value()) {
        *shared_state = ProtoFieldValueMap();
      }
      (shared_state->value())[value_map_key] = std::move(value_list_owner);
    }
  }

  if (access_info_registry_id_ >= value_list->size()) {
    *status = zetasql_base::InternalErrorBuilder() << "Corrupt ProtoFieldValueList";
    return false;
  }
  const ::zetasql_base::StatusOr<Value>& value =
      (*value_list)[access_info_registry_id_];

  if (!value.ok()) {
    *status = value.status();
    return false;
  }

  *field_value = value.value();
  return true;
}

// -------------------------------------------------------
// GetProtoFieldExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<GetProtoFieldExpr>> GetProtoFieldExpr::Create(
    std::unique_ptr<ValueExpr> proto_expr,
    const ProtoFieldReader* field_reader) {
  return absl::WrapUnique(
      new GetProtoFieldExpr(std::move(proto_expr), field_reader));
}

absl::Status GetProtoFieldExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return mutable_proto_expr()->SetSchemasForEvaluation(params_schemas);
}

bool GetProtoFieldExpr::Eval(absl::Span<const TupleData* const> params,
                             EvaluationContext* context,
                             VirtualTupleSlot* result,
                             absl::Status* status) const {
  TupleSlot proto_slot;
  if (!proto_expr()->EvalSimple(params, context, &proto_slot, status)) {
    return false;
  }
  if (!field_reader_->GetFieldValue(proto_slot, context,
                                    result->mutable_value(), status)) {
    return false;
  }
  result->MaybeUpdateSharedProtoStateAfterSettingValue(
      proto_slot.mutable_shared_proto_state());
  return true;
}

GetProtoFieldExpr::GetProtoFieldExpr(std::unique_ptr<ValueExpr> proto_expr,
                                     const ProtoFieldReader* field_reader)
    : ValueExpr(field_reader->access_info().field_info.type),
      field_reader_(field_reader) {
  SetArg(kProtoExpr, absl::make_unique<ExprArg>(std::move(proto_expr)));
}

const ValueExpr* GetProtoFieldExpr::proto_expr() const {
  return GetArg(kProtoExpr)->node()->AsValueExpr();
}

ValueExpr* GetProtoFieldExpr::mutable_proto_expr() {
  return GetMutableArg(kProtoExpr)->mutable_node()->AsMutableValueExpr();
}

std::string GetProtoFieldExpr::DebugInternal(const std::string& indent,
                                             bool verbose) const {
  const ProtoFieldInfo& field_info = field_reader_->access_info().field_info;
  return absl::StrCat(
      "GetProtoFieldExpr(", (field_info.get_has_bit ? "has_" : ""),
      field_info.descriptor->name(), ", ", proto_expr()->DebugString(),
      " [fid=", field_reader_->id(), " rid=", field_reader_->registry_id(),
      "])");
}

// -------------------------------------------------------
// SingleValueExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<SingleValueExpr>> SingleValueExpr::Create(
    std::unique_ptr<ValueExpr> value, std::unique_ptr<RelationalOp> input) {
  return absl::WrapUnique(
      new SingleValueExpr(std::move(value), std::move(input)));
}

absl::Status SingleValueExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));
  const std::unique_ptr<const TupleSchema> input_schema =
      input()->CreateOutputSchema();
  return mutable_value()->SetSchemasForEvaluation(
      ConcatSpans(params_schemas, {input_schema.get()}));
}

bool SingleValueExpr::Eval(absl::Span<const TupleData* const> params,
                           EvaluationContext* context, VirtualTupleSlot* result,
                           absl::Status* status) const {
  auto status_or_iter =
      input()->CreateIterator(params, /*num_extra_slots=*/0, context);
  if (!status_or_iter.ok()) {
    *status = status_or_iter.status();
    return false;
  }
  std::unique_ptr<TupleIterator> iter = std::move(status_or_iter).value();

  const TupleData* tuple = iter->Next();
  if (tuple == nullptr) {
    *status = iter->Status();
    if (!status->ok()) return false;

    result->SetValue(Value::Null(output_type()));
    return true;
  }

  if (!value()->Eval(ConcatSpans(params, {tuple}), context, result, status)) {
    return false;
  }

  tuple = iter->Next();
  if (tuple == nullptr) {
    *status = iter->Status();
    if (!status->ok()) return false;

    return true;
  }

  *status = zetasql_base::OutOfRangeErrorBuilder() << "More than one element";
  return false;
}

std::string SingleValueExpr::DebugInternal(const std::string& indent,
                                           bool verbose) const {
  return absl::StrCat(
      "SingleValueExpr(",
      ArgDebugString({"value", "input"}, {k1, k1}, indent, verbose), ")");
}

SingleValueExpr::SingleValueExpr(std::unique_ptr<ValueExpr> value,
                                 std::unique_ptr<RelationalOp> input)
    : ValueExpr(value->output_type()) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
  SetArg(kValue, absl::make_unique<ExprArg>(std::move(value)));
}

const RelationalOp* SingleValueExpr::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* SingleValueExpr::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

const ValueExpr* SingleValueExpr::value() const {
  return GetArg(kValue)->node()->AsValueExpr();
}

ValueExpr* SingleValueExpr::mutable_value() {
  return GetMutableArg(kValue)->mutable_node()->AsMutableValueExpr();
}

// -------------------------------------------------------
// ExistsExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<ExistsExpr>> ExistsExpr::Create(
    std::unique_ptr<RelationalOp> input) {
  return absl::WrapUnique(new ExistsExpr(std::move(input)));
}

absl::Status ExistsExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return mutable_input()->SetSchemasForEvaluation(params_schemas);
}

bool ExistsExpr::Eval(absl::Span<const TupleData* const> params,
                      EvaluationContext* context, VirtualTupleSlot* result,
                      absl::Status* status) const {
  auto status_or_iter =
      input()->CreateIterator(params, /*num_extra_slots=*/0, context);
  if (!status_or_iter.ok()) {
    *status = status_or_iter.status();
    return false;
  }
  std::unique_ptr<TupleIterator> iter = std::move(status_or_iter).value();

  const TupleData* tuple = iter->Next();
  if (tuple == nullptr) {
    *status = iter->Status();
    if (!status->ok()) return false;

    result->SetValue(Bool(false));
    return true;
  }

  result->SetValue(Bool(true));
  return true;
}

std::string ExistsExpr::DebugInternal(const std::string& indent,
                                      bool verbose) const {
  return absl::StrCat("ExistsExpr(",
                      ArgDebugString({"input"}, {k1}, indent, verbose), ")");
}

ExistsExpr::ExistsExpr(std::unique_ptr<RelationalOp> input)
    : ValueExpr(types::BoolType()) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
}

const RelationalOp* ExistsExpr::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* ExistsExpr::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// ScalarFunctionCallExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<ScalarFunctionCallExpr>>
ScalarFunctionCallExpr::Create(
    std::unique_ptr<const ScalarFunctionBody> function,
    std::vector<std::unique_ptr<ValueExpr>> exprs,
    ResolvedFunctionCallBase::ErrorMode error_mode) {
  ZETASQL_RET_CHECK(function != nullptr);
  return absl::WrapUnique(new ScalarFunctionCallExpr(
      std::move(function), std::move(exprs), error_mode));
}

absl::Status ScalarFunctionCallExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  absl::Span<AlgebraArg* const> args = GetMutableArgs();
  for (AlgebraArg* arg : args) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(params_schemas));
  }
  return absl::OkStatus();
}

bool ScalarFunctionCallExpr::Eval(absl::Span<const TupleData* const> params,
                                  EvaluationContext* context,
                                  VirtualTupleSlot* result,
                                  absl::Status* status) const {
  const auto& args = GetArgs();
  std::vector<Value> call_args(args.size());
  for (int i = 0; i < args.size(); i++) {
    std::shared_ptr<TupleSlot::SharedProtoState> arg_shared_state;
    VirtualTupleSlot arg_result(&call_args[i], &arg_shared_state);
    if (!args[i]->value_expr()->Eval(params, context, &arg_result, status)) {
      return false;
    }
  }

  if (!function_->Eval(call_args, context, result->mutable_value(), status)) {
    if (ShouldSuppressError(*status, error_mode_)) {
      *status = absl::OkStatus();
      result->SetValue(Value::Null(output_type()));
      return true;
    }
    return false;
  }
  result->MaybeResetSharedProtoState();
  return true;
}

std::string ScalarFunctionCallExpr::DebugInternal(const std::string& indent,
                                                  bool verbose) const {
  std::vector<std::string> sarg;
  for (auto arg : GetArgs()) {
    std::string indent_child = indent + kIndentSpace;
    sarg.push_back(arg->value_expr()->DebugInternal(indent_child, verbose));
  }
  return absl::StrCat(function()->debug_name(), "(", absl::StrJoin(sarg, ", "),
                      ")");
}

ScalarFunctionCallExpr::ScalarFunctionCallExpr(
    std::unique_ptr<const ScalarFunctionBody> function,
    std::vector<std::unique_ptr<ValueExpr>> exprs,
    ResolvedFunctionCallBase::ErrorMode error_mode)
    : ValueExpr(function->output_type()),
      function_(std::move(function)),
      error_mode_(error_mode) {
  std::vector<std::unique_ptr<ExprArg>> args;
  args.reserve(exprs.size());
  for (auto& e : exprs) {
    args.push_back(absl::make_unique<ExprArg>(std::move(e)));
  }
  SetArgs<ExprArg>(kArgument, std::move(args));
}

// -------------------------------------------------------
// AggregateFunctionCallExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<AggregateFunctionCallExpr>>
AggregateFunctionCallExpr::Create(
    std::unique_ptr<const AggregateFunctionBody> function,
    std::vector<std::unique_ptr<ValueExpr>> exprs) {
  ZETASQL_RET_CHECK(function != nullptr);
  return absl::WrapUnique(
      new AggregateFunctionCallExpr(std::move(function), std::move(exprs)));
}

absl::Status AggregateFunctionCallExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  absl::Span<AlgebraArg* const> args = GetMutableArgs();
  for (AlgebraArg* arg : args) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(params_schemas));
  }
  return absl::OkStatus();
}

std::string AggregateFunctionCallExpr::DebugInternal(const std::string& indent,
                                                     bool verbose) const {
  std::vector<std::string> sarg;
  for (auto arg : GetArgs()) {
    std::string indent_child = indent + kIndentSpace;
    sarg.push_back(arg->value_expr()->DebugInternal(indent_child, verbose));
  }
  return absl::StrCat(function()->debug_name(), "(", absl::StrJoin(sarg, ", "),
                      ")");
}

AggregateFunctionCallExpr::AggregateFunctionCallExpr(
    std::unique_ptr<const AggregateFunctionBody> function,
    std::vector<std::unique_ptr<ValueExpr>> exprs)
    : ValueExpr(function->output_type()), function_(std::move(function)) {
  std::vector<std::unique_ptr<ExprArg>> args;
  args.reserve(exprs.size());
  for (auto& e : exprs) {
    args.push_back(absl::make_unique<ExprArg>(std::move(e)));
  }
  SetArgs<ExprArg>(kArgument, std::move(args));
}

bool AggregateFunctionCallExpr::Eval(absl::Span<const TupleData* const> params,
                                     EvaluationContext* context,
                                     VirtualTupleSlot* result,
                                     absl::Status* status) const {
  *status = ::zetasql_base::InternalErrorBuilder()
            << "Use AggregateArg to evaluate an aggregate function";
  return false;
}

// -------------------------------------------------------
// AnalyticFunctionCallExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<AnalyticFunctionCallExpr>>
AnalyticFunctionCallExpr::Create(
    std::unique_ptr<const AnalyticFunctionBody> function,
    std::vector<std::unique_ptr<ValueExpr>> non_const_arguments,
    std::vector<std::unique_ptr<ValueExpr>> const_arguments) {
  ZETASQL_RET_CHECK(function != nullptr);
  return absl::WrapUnique(new AnalyticFunctionCallExpr(
      std::move(function), std::move(non_const_arguments),
      std::move(const_arguments)));
}

absl::Status AnalyticFunctionCallExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  for (ExprArg* arg : mutable_non_const_arguments()) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(params_schemas));
  }
  for (ExprArg* arg : mutable_const_arguments()) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(params_schemas));
  }
  return absl::OkStatus();
}

std::string AnalyticFunctionCallExpr::DebugInternal(const std::string& indent,
                                                    bool verbose) const {
  std::vector<std::string> arg_strings;
  for (auto arg : GetArgs()) {
    std::string indent_child = indent + kIndentSpace;
    arg_strings.push_back(
        arg->value_expr()->DebugInternal(indent_child, verbose));
  }
  return absl::StrCat(function()->debug_name(), "(",
                      absl::StrJoin(arg_strings, ", "), ")");
}

absl::Span<const ExprArg* const> AnalyticFunctionCallExpr::non_const_arguments()
    const {
  return GetArgs<ExprArg>(kNonConstArgument);
}

absl::Span<ExprArg* const>
AnalyticFunctionCallExpr::mutable_non_const_arguments() {
  return GetMutableArgs<ExprArg>(kNonConstArgument);
}

absl::Span<const ExprArg* const> AnalyticFunctionCallExpr::const_arguments()
    const {
  return GetArgs<ExprArg>(kConstArgument);
}

absl::Span<ExprArg* const> AnalyticFunctionCallExpr::mutable_const_arguments() {
  return GetMutableArgs<ExprArg>(kConstArgument);
}

AnalyticFunctionCallExpr::AnalyticFunctionCallExpr(
    std::unique_ptr<const AnalyticFunctionBody> function,
    std::vector<std::unique_ptr<ValueExpr>> non_const_arguments,
    std::vector<std::unique_ptr<ValueExpr>> const_arguments)
    : ValueExpr(function->output_type()), function_(std::move(function)) {
  std::vector<std::unique_ptr<ExprArg>> non_const_expr_args;
  non_const_expr_args.reserve(non_const_arguments.size());
  for (auto& non_const_argument : non_const_arguments) {
    non_const_expr_args.push_back(
        absl::make_unique<ExprArg>(std::move(non_const_argument)));
  }
  SetArgs<ExprArg>(kNonConstArgument, std::move(non_const_expr_args));

  std::vector<std::unique_ptr<ExprArg>> const_expr_args;
  const_expr_args.reserve(const_arguments.size());
  for (auto& const_argument : const_arguments) {
    const_expr_args.push_back(
        absl::make_unique<ExprArg>(std::move(const_argument)));
  }
  SetArgs<ExprArg>(kConstArgument, std::move(const_expr_args));
}

bool AnalyticFunctionCallExpr::Eval(absl::Span<const TupleData* const> params,
                                    EvaluationContext* context,
                                    VirtualTupleSlot* result,
                                    absl::Status* status) const {
  *status =
      ::zetasql_base::InternalErrorBuilder()
      << "Use NonAggregateAnalyticArg::Eval to evaluate an analytic function";
  return false;
}

// -------------------------------------------------------
// IfExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<IfExpr>> IfExpr::Create(
    std::unique_ptr<ValueExpr> condition, std::unique_ptr<ValueExpr> true_value,
    std::unique_ptr<ValueExpr> false_value) {
  ZETASQL_RET_CHECK(true_value->output_type()->Equals(false_value->output_type()));
  return absl::WrapUnique(new IfExpr(
      std::move(condition), std::move(true_value), std::move(false_value)));
}

absl::Status IfExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_join_expr()->SetSchemasForEvaluation(params_schemas));
  ZETASQL_RETURN_IF_ERROR(
      mutable_true_value()->SetSchemasForEvaluation(params_schemas));
  return mutable_false_value()->SetSchemasForEvaluation(params_schemas);
}

bool IfExpr::Eval(absl::Span<const TupleData* const> params,
                  EvaluationContext* context, VirtualTupleSlot* result,
                  absl::Status* status) const {
  TupleSlot slot;
  if (!join_expr()->EvalSimple(params, context, &slot, status)) return false;
  if (slot.value() == Bool(true)) {
    return true_value()->Eval(params, context, result, status);
  } else {
    return false_value()->Eval(params, context, result, status);
  }
}

std::string IfExpr::DebugInternal(const std::string& indent,
                                  bool verbose) const {
  return absl::StrCat("IfExpr(",
                      ArgDebugString({"condition", "true_value", "false_value"},
                                     {k1, k1, k1}, indent, verbose),
                      ")");
}

IfExpr::IfExpr(std::unique_ptr<ValueExpr> condition,
               std::unique_ptr<ValueExpr> true_value,
               std::unique_ptr<ValueExpr> false_value)
    : ValueExpr(true_value->output_type()) {
  SetArg(kCondition, absl::make_unique<ExprArg>(std::move(condition)));
  SetArg(kTrueValue, absl::make_unique<ExprArg>(std::move(true_value)));
  SetArg(kFalseValue, absl::make_unique<ExprArg>(std::move(false_value)));
}

const ValueExpr* IfExpr::join_expr() const {
  return GetArg(kCondition)->node()->AsValueExpr();
}

ValueExpr* IfExpr::mutable_join_expr() {
  return GetMutableArg(kCondition)->mutable_node()->AsMutableValueExpr();
}

const ValueExpr* IfExpr::true_value() const {
  return GetArg(kTrueValue)->node()->AsValueExpr();
}

ValueExpr* IfExpr::mutable_true_value() {
  return GetMutableArg(kTrueValue)->mutable_node()->AsMutableValueExpr();
}

const ValueExpr* IfExpr::false_value() const {
  return GetArg(kFalseValue)->node()->AsValueExpr();
}

ValueExpr* IfExpr::mutable_false_value() {
  return GetMutableArg(kFalseValue)->mutable_node()->AsMutableValueExpr();
}

// -------------------------------------------------------
// LetExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<LetExpr>> LetExpr::Create(
    std::vector<std::unique_ptr<ExprArg>> assign,
    std::unique_ptr<ValueExpr> body) {
  return absl::WrapUnique(new LetExpr(std::move(assign), std::move(body)));
}

absl::Status LetExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  // Initialize 'schema_ptrs' with 'params_schemas', then extend 'schema_ptrs'
  // with new schemas owned by 'new_schemas'.
  std::vector<std::unique_ptr<const TupleSchema>> new_schemas;
  new_schemas.reserve(assign().size());

  std::vector<const TupleSchema*> schema_ptrs;
  schema_ptrs.reserve(params_schemas.size() + assign().size());
  schema_ptrs.insert(schema_ptrs.end(), params_schemas.begin(),
                     params_schemas.end());

  for (ExprArg* arg : mutable_assign()) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(schema_ptrs));

    auto new_schema = absl::make_unique<TupleSchema>(
        std::vector<VariableId>{arg->variable()});
    schema_ptrs.push_back(new_schema.get());
    new_schemas.push_back(std::move(new_schema));
  }

  return mutable_body()->SetSchemasForEvaluation(schema_ptrs);
}

bool LetExpr::Eval(absl::Span<const TupleData* const> params,
                   EvaluationContext* context, VirtualTupleSlot* result,
                   absl::Status* status) const {
  // Initialize 'data_ptrs' with 'params', then extend 'data_ptrs' with new
  // TupleDatas owned by 'new_datas'.  We use a TupleDeque in case one of the
  // parameters represents multiple rows (e.g., an array corresponding to a WITH
  // table).
  auto new_datas =
      absl::make_unique<TupleDataDeque>(context->memory_accountant());

  std::vector<const TupleData*> data_ptrs;
  data_ptrs.reserve(params.size() + assign().size());
  data_ptrs.insert(data_ptrs.end(), params.begin(), params.end());

  for (const ExprArg* arg : assign()) {
    auto new_data = absl::make_unique<TupleData>(/*num_slots=*/1);

    if (!arg->value_expr()->EvalSimple(data_ptrs, context,
                                       new_data->mutable_slot(0), status)) {
      return false;
    }

    data_ptrs.push_back(new_data.get());
    if (!new_datas->PushBack(std::move(new_data), status)) {
      return false;
    }
  }
  return body()->Eval(data_ptrs, context, result, status);
}

std::string LetExpr::DebugInternal(const std::string& indent,
                                   bool verbose) const {
  return absl::StrCat(
      "LetExpr(", ArgDebugString({"assign", "body"}, {kN, k1}, indent, verbose),
      ")");
}

LetExpr::LetExpr(std::vector<std::unique_ptr<ExprArg>> assign,
                 std::unique_ptr<ValueExpr> body)
    : ValueExpr(body->output_type()) {
  SetArgs<ExprArg>(kAssign, std::move(assign));
  SetArg(kBody, absl::make_unique<ExprArg>(std::move(body)));
}

absl::Span<const ExprArg* const> LetExpr::assign() const {
  return GetArgs<ExprArg>(kAssign);
}

absl::Span<ExprArg* const> LetExpr::mutable_assign() {
  return GetMutableArgs<ExprArg>(kAssign);
}

const ValueExpr* LetExpr::body() const {
  return GetArg(kBody)->node()->AsValueExpr();
}

ValueExpr* LetExpr::mutable_body() {
  return GetMutableArg(kBody)->mutable_node()->AsMutableValueExpr();
}

// -------------------------------------------------------
// DMLValueExpr
// -------------------------------------------------------

// This is a little gross, but implementing this class as an algebra node is
// kind of a hack anyway. One way to make this method better would be to print
// out scan_op_->DebugString() and somehow match up the ResolvedExprs with the
// corresponding ValueExpr::DebugString() in 'resolved_expr_map_'. But that
// doesn't seem to be worth the effort.
std::string DMLValueExpr::DebugInternal(const std::string& /*indent*/,
                                        bool verbose) const {
  if (verbose) {
    const std::vector<std::string> lines =
        absl::StrSplit(resolved_node_->DebugString(), '\n');
    std::string ret;
    for (const std::string& line : lines) {
      absl::StrAppend(&ret, line, "\n");
    }
    return ret;
  } else {
    return ResolvedNodeKind_Name(resolved_node_->node_kind());
  }
}

DMLValueExpr::DMLValueExpr(
    const Table* table, const ArrayType* table_array_type,
    const StructType* primary_key_type, const StructType* dml_output_type,
    const ResolvedNode* resolved_node, const ResolvedColumnList* column_list,
    std::unique_ptr<const ColumnToVariableMapping> column_to_variable_mapping,
    std::unique_ptr<const ResolvedScanMap> resolved_scan_map,
    std::unique_ptr<const ResolvedExprMap> resolved_expr_map)
    : ValueExpr(dml_output_type),
      table_(table),
      table_array_type_(table_array_type),
      primary_key_type_(primary_key_type),
      dml_output_type_(dml_output_type),
      resolved_node_(resolved_node),
      column_list_(column_list),
      column_to_variable_mapping_(std::move(column_to_variable_mapping)),
      resolved_scan_map_(std::move(resolved_scan_map)),
      resolved_expr_map_(std::move(resolved_expr_map)) {}

::zetasql_base::StatusOr<RelationalOp*> DMLValueExpr::LookupResolvedScan(
    const ResolvedScan* resolved_scan) const {
  const std::unique_ptr<RelationalOp>* relational_op =
      zetasql_base::FindOrNull(*resolved_scan_map_, resolved_scan);
  ZETASQL_RET_CHECK(relational_op != nullptr);
  return relational_op->get();
}

::zetasql_base::StatusOr<ValueExpr*> DMLValueExpr::LookupResolvedExpr(
    const ResolvedExpr* resolved_expr) const {
  const std::unique_ptr<ValueExpr>* value_expr =
      zetasql_base::FindOrNull(*resolved_expr_map_, resolved_expr);
  ZETASQL_RET_CHECK(value_expr != nullptr);
  return value_expr->get();
}

// Convenience helper to make ValueExpr::Eval() easier to call (at the cost of
// some performance, which doesn't matter for DML ValueExprs since they are just
// for compliance testing).
static ::zetasql_base::StatusOr<Value> EvalExpr(
    const ValueExpr& value_expr, absl::Span<const TupleData* const> params,
    EvaluationContext* context) {
  TupleSlot slot;
  absl::Status status;
  if (!value_expr.EvalSimple(params, context, &slot, &status)) {
    return status;
  }
  return slot.value();
}

absl::Status DMLValueExpr::VerifyNumRowsModified(
    const ResolvedAssertRowsModified* assert_rows_modified,
    absl::Span<const TupleData* const> params, int64_t actual_num_rows_modified,
    EvaluationContext* context, bool print_array_elements) const {
  if (assert_rows_modified != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* rows_modified,
                     LookupResolvedExpr(assert_rows_modified->rows()));
    ZETASQL_ASSIGN_OR_RETURN(const Value expected_rows_modified,
                     EvalExpr(*rows_modified, params, context));

    ZETASQL_RET_CHECK(expected_rows_modified.type()->IsInt64());
    if (expected_rows_modified.is_null()) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "ASSERT_ROWS_MODIFIED must have a non-NULL argument";
    }
    const int64_t expected = expected_rows_modified.int64_value();
    if (expected != actual_num_rows_modified) {
      const absl::string_view things_modified =
          print_array_elements ? "array elements" : "rows";
      return zetasql_base::OutOfRangeErrorBuilder()
             << "ASSERT_ROWS_MODIFIED expected " << expected << " "
             << things_modified << " modified, but found "
             << actual_num_rows_modified;
    }
  }

  return absl::OkStatus();
}

::zetasql_base::StatusOr<std::vector<Value>>
DMLValueExpr::GetScannedTupleAsColumnValues(
    const ResolvedColumnList& column_list, const Tuple& t) const {
  std::vector<Value> values;
  for (const ResolvedColumn& column : column_list) {
    ZETASQL_ASSIGN_OR_RETURN(const Value value, GetColumnValue(column, t));
    values.push_back(value);
  }
  return values;
}

::zetasql_base::StatusOr<Value> DMLValueExpr::GetColumnValue(
    const ResolvedColumn& column, const Tuple& t) const {
  ZETASQL_ASSIGN_OR_RETURN(
      const VariableId variable_id,
      column_to_variable_mapping_->LookupVariableNameForColumn(&column));
  absl::optional<int> slot = t.schema->FindIndexForVariable(variable_id);
  ZETASQL_RET_CHECK(slot.has_value()) << variable_id;
  return t.data->slot(slot.value()).value();
}

absl::Status DMLValueExpr::PopulatePrimaryKeyRowMap(
    const std::vector<std::vector<Value>>& original_rows,
    absl::string_view duplicate_primary_key_error_prefix,
    EvaluationContext* context, PrimaryKeyRowMap* row_map,
    bool* has_primary_key) const {
  ZETASQL_ASSIGN_OR_RETURN(const absl::optional<std::vector<int>> primary_key_indexes,
                   GetPrimaryKeyColumnIndexes(context));
  *has_primary_key = primary_key_indexes.has_value();
  for (int64_t row_number = 0; row_number < original_rows.size(); ++row_number) {
    // It is expensive to call this for every row, but this code is only used
    // for compliance testing, so it's ok.
    ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());

    const std::vector<Value>& row_values = original_rows[row_number];

    RowNumberAndValues row_number_and_values;
    row_number_and_values.row_number = row_number;
    row_number_and_values.values = row_values;

    ZETASQL_ASSIGN_OR_RETURN(const Value primary_key,
                     GetPrimaryKeyOrRowNumber(row_number_and_values, context));
    auto insert_result =
        row_map->insert(std::make_pair(primary_key, row_number_and_values));
    if (!insert_result.second) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << duplicate_primary_key_error_prefix << " ("
             << primary_key.ShortDebugString() << ")";
    }
  }

  return absl::OkStatus();
}

::zetasql_base::StatusOr<Value> DMLValueExpr::GetPrimaryKeyOrRowNumber(
    const RowNumberAndValues& row_number_and_values, EvaluationContext* context,
    bool* has_primary_key) const {
  ZETASQL_ASSIGN_OR_RETURN(const absl::optional<std::vector<int>> primary_key_indexes,
                   GetPrimaryKeyColumnIndexes(context));
  if (!primary_key_indexes.has_value()) {
    return Value::Int64(row_number_and_values.row_number);
  }
  // For emulated primary keys, use the emulated value (value of the first
  // column) directly instead of making a Struct from the value. This avoids
  // breaking tests depending on query plans. We may later remove this special
  // case and update the tests.
  if (context->options().emulate_primary_keys) {
    ZETASQL_RET_CHECK_EQ(primary_key_indexes->size(), 1);
    ZETASQL_RET_CHECK_EQ((*primary_key_indexes)[0], 0);
    const Value& value = row_number_and_values.values[0];
    ZETASQL_RET_CHECK(value.is_valid());
    return value;
  }
  std::vector<Value> key_column_values;
  for (int index : *primary_key_indexes) {
    const Value& value = row_number_and_values.values[index];
    ZETASQL_RET_CHECK(value.is_valid());
    key_column_values.push_back(value);
  }
  return Value::Struct(primary_key_type_, key_column_values);
}

zetasql_base::StatusOr<absl::optional<std::vector<int>>>
DMLValueExpr::GetPrimaryKeyColumnIndexes(EvaluationContext* context) const {
  if (is_value_table()) {
    return absl::optional<std::vector<int>>();
  }

  // The algebrizer can opt out of using primary key from the catalog.
  if (primary_key_type_ == nullptr) {
    return context->options().emulate_primary_keys
               ? absl::make_optional(std::vector<int>{0})
               : absl::optional<std::vector<int>>();
  }
  ZETASQL_RET_CHECK(!context->options().emulate_primary_keys)
      << "Cannot emulate primary key while using the primary key set in Table";
  return table_->PrimaryKey();
}

::zetasql_base::StatusOr<Value> DMLValueExpr::GetDMLOutputValue(
    int64_t num_rows_modified,
    const std::vector<std::vector<Value>>& dml_output_rows,
    EvaluationContext* context) const {
  for (const std::vector<Value>& dml_output_row : dml_output_rows) {
    for (const Value& value : dml_output_row) {
      ZETASQL_RET_CHECK(value.is_valid());
    }
  }

  std::vector<Value> dml_output_values;
  for (const std::vector<Value>& dml_output_row : dml_output_rows) {
    // It is expensive to call this for every row, but this code is only used
    // for compliance testing, so it's ok.
    ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());

    ZETASQL_RET_CHECK_EQ(dml_output_row.size(), column_list_->size());
    if (is_value_table()) {
      ZETASQL_RET_CHECK_EQ(1, dml_output_row.size());
      dml_output_values.push_back(dml_output_row[0]);
    } else {
      const Type* element_type = table_array_type_->element_type();
      ZETASQL_RET_CHECK(element_type->IsStruct());
      const StructType* table_row_type = element_type->AsStruct();
      dml_output_values.push_back(
          Value::Struct(table_row_type, dml_output_row));
    }
  }

  // Table rows are not ordered.
  const Value dml_output_row_array = InternalValue::ArrayNotChecked(
      table_array_type_, InternalValue::kIgnoresOrder,
      std::move(dml_output_values));
  return Value::Struct(dml_output_type_,
                       std::array<Value, 2>{Value::Int64(num_rows_modified),
                                            dml_output_row_array});
}

// Evaluates 'op' on 'params', then populates 'schema' and 'datas' with the
// corresponding TupleSchema and TupleDatas.
static absl::Status EvalRelationalOp(
    const RelationalOp& op, absl::Span<const TupleData* const> params,
    EvaluationContext* context, std::unique_ptr<TupleSchema>* schema,
    std::vector<std::unique_ptr<TupleData>>* datas) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> iter,
                   op.CreateIterator(params, /*num_extra_slots=*/0, context));
  *schema = absl::make_unique<TupleSchema>(iter->Schema().variables());
  // We disable reordering when iterating over relations when processing DML
  // statements for backwardws compatibility with the text-based reference
  // implementation compliance tests. As another advantage, this effectively
  // disables scrambling for simple statements, which makes the tests easier to
  // understand.
  ZETASQL_RETURN_IF_ERROR(iter->DisableReordering());
  while (true) {
    const TupleData* data = iter->Next();
    if (data == nullptr) {
      ZETASQL_RETURN_IF_ERROR(iter->Status());
      break;
    }
    datas->push_back(absl::make_unique<TupleData>(*data));
  }
  return absl::OkStatus();
}

// -------------------------------------------------------
// DMLDeleteValueExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<DMLDeleteValueExpr>>
DMLDeleteValueExpr::Create(
    const Table* table, const ArrayType* table_array_type,
    const StructType* primary_key_type, const StructType* dml_output_type,
    const ResolvedDeleteStmt* resolved_node,
    const ResolvedColumnList* column_list,
    std::unique_ptr<const ColumnToVariableMapping> column_to_variable_mapping,
    std::unique_ptr<const ResolvedScanMap> resolved_scan_map,
    std::unique_ptr<const ResolvedExprMap> resolved_expr_map) {
  return absl::WrapUnique(new DMLDeleteValueExpr(
      table, table_array_type, primary_key_type, dml_output_type, resolved_node,
      column_list, std::move(column_to_variable_mapping),
      std::move(resolved_scan_map), std::move(resolved_expr_map)));
}

absl::Status DMLDeleteValueExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  if (stmt()->assert_rows_modified() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ValueExpr * rows,
        LookupResolvedExpr(stmt()->assert_rows_modified()->rows()));
    ZETASQL_RETURN_IF_ERROR(rows->SetSchemasForEvaluation(params_schemas));
  }

  ZETASQL_ASSIGN_OR_RETURN(RelationalOp * scan,
                   LookupResolvedScan(stmt()->table_scan()));
  ZETASQL_RETURN_IF_ERROR(scan->SetSchemasForEvaluation(params_schemas));
  const std::unique_ptr<const TupleSchema> scan_schema =
      scan->CreateOutputSchema();

  ZETASQL_ASSIGN_OR_RETURN(ValueExpr * where_expr,
                   LookupResolvedExpr(stmt()->where_expr()));
  return where_expr->SetSchemasForEvaluation(
      ConcatSpans(params_schemas, {scan_schema.get()}));
}

::zetasql_base::StatusOr<Value> DMLDeleteValueExpr::Eval(
    absl::Span<const TupleData* const> params,
    EvaluationContext* context) const {
  ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* where_expr,
                   LookupResolvedExpr(stmt()->where_expr()));

  int64_t num_rows_deleted = 0;
  std::vector<std::vector<Value>> dml_output_rows;

  ZETASQL_ASSIGN_OR_RETURN(const RelationalOp* relational_op,
                   LookupResolvedScan(stmt()->table_scan()));

  std::unique_ptr<TupleSchema> tuple_schema;
  std::vector<std::unique_ptr<TupleData>> tuple_datas;
  ZETASQL_RETURN_IF_ERROR(EvalRelationalOp(*relational_op, params, context,
                                   &tuple_schema, &tuple_datas));
  for (const std::unique_ptr<TupleData>& tuple_data : tuple_datas) {
    // It is expensive to call this for every row, but this code is only used
    // for compliance testing, so it's ok.
    ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());

    const Tuple tuple(tuple_schema.get(), tuple_data.get());
    ZETASQL_ASSIGN_OR_RETURN(std::vector<Value> tuple_as_values,
                     GetScannedTupleAsColumnValues(*column_list_, tuple));

    // The WHERE clause can reference column values and statement parameters.
    ZETASQL_ASSIGN_OR_RETURN(
        const Value where_value,
        EvalExpr(*where_expr, ConcatSpans(params, {tuple.data}), context));
    const bool deleted = (where_value == Bool(true));
    if (deleted) {
      ++num_rows_deleted;
      if (!context->options().return_all_rows_for_dml) {
        dml_output_rows.push_back(tuple_as_values);
      }
    } else {
      // In all_rows mode,the output contains the remaining rows.
      if (context->options().return_all_rows_for_dml) {
        dml_output_rows.push_back(tuple_as_values);
      }
    }
  }

  ZETASQL_RETURN_IF_ERROR(VerifyNumRowsModified(stmt()->assert_rows_modified(), params,
                                        num_rows_deleted, context));

  ZETASQL_RETURN_IF_ERROR(resolved_node_->CheckFieldsAccessed());
  return GetDMLOutputValue(num_rows_deleted, dml_output_rows, context);
}

DMLDeleteValueExpr::DMLDeleteValueExpr(
    const Table* table, const ArrayType* table_array_type,
    const StructType* primary_key_type, const StructType* dml_output_type,
    const ResolvedDeleteStmt* resolved_node,
    const ResolvedColumnList* column_list,
    std::unique_ptr<const ColumnToVariableMapping> column_to_variable_mapping,
    std::unique_ptr<const ResolvedScanMap> resolved_scan_map,
    std::unique_ptr<const ResolvedExprMap> resolved_expr_map)
    : DMLValueExpr(table, table_array_type, primary_key_type, dml_output_type,
                   resolved_node, column_list,
                   std::move(column_to_variable_mapping),
                   std::move(resolved_scan_map), std::move(resolved_expr_map)) {
}

// -------------------------------------------------------
// DMLUpdateValueExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<DMLUpdateValueExpr>>
DMLUpdateValueExpr::Create(
    const Table* table, const ArrayType* table_array_type,
    const StructType* primary_key_type, const StructType* dml_output_type,
    const ResolvedUpdateStmt* resolved_node,
    const ResolvedColumnList* column_list,
    std::unique_ptr<const ColumnToVariableMapping> column_to_variable_mapping,
    std::unique_ptr<const ResolvedScanMap> resolved_scan_map,
    std::unique_ptr<const ResolvedExprMap> resolved_expr_map) {
  return absl::WrapUnique(new DMLUpdateValueExpr(
      table, table_array_type, primary_key_type, dml_output_type, resolved_node,
      column_list, std::move(column_to_variable_mapping),
      std::move(resolved_scan_map), std::move(resolved_expr_map)));
}

absl::Status DMLUpdateValueExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_ASSIGN_OR_RETURN(RelationalOp * table_scan,
                   LookupResolvedScan(stmt()->table_scan()));
  ZETASQL_RETURN_IF_ERROR(table_scan->SetSchemasForEvaluation(params_schemas));
  const std::unique_ptr<const TupleSchema> table_scan_schema =
      table_scan->CreateOutputSchema();

  std::unique_ptr<const TupleSchema> from_scan_schema;
  if (stmt()->from_scan() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(RelationalOp * from,
                     LookupResolvedScan(stmt()->from_scan()));
    ZETASQL_RETURN_IF_ERROR(from->SetSchemasForEvaluation(params_schemas));
    from_scan_schema = from->CreateOutputSchema();
  }

  std::vector<const TupleSchema*> joined_schemas =
      ConcatSpans(params_schemas, {table_scan_schema.get()});
  if (from_scan_schema != nullptr) {
    joined_schemas =
        ConcatSpans(absl::Span<const TupleSchema* const>(joined_schemas),
                    {from_scan_schema.get()});
  }

  ZETASQL_ASSIGN_OR_RETURN(ValueExpr * where_expr,
                   LookupResolvedExpr(stmt()->where_expr()));
  ZETASQL_RETURN_IF_ERROR(where_expr->SetSchemasForEvaluation(joined_schemas));

  for (const std::unique_ptr<const ResolvedUpdateItem>& update_item :
       stmt()->update_item_list()) {
    ZETASQL_RETURN_IF_ERROR(
        SetSchemasForEvaluationOfUpdateItem(update_item.get(), joined_schemas));
  }

  if (stmt()->assert_rows_modified() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ValueExpr * rows,
        LookupResolvedExpr(stmt()->assert_rows_modified()->rows()));
    ZETASQL_RETURN_IF_ERROR(rows->SetSchemasForEvaluation(params_schemas));
  }

  return absl::OkStatus();
}

::zetasql_base::StatusOr<Value> DMLUpdateValueExpr::Eval(
    absl::Span<const TupleData* const> params,
    EvaluationContext* context) const {
  // Schema of tuples from the from scan. NULL if there is no from scan.
  std::unique_ptr<TupleSchema> from_schema;
  // Consists of one tuple per row of the table in the from scan. NULL if there
  // is no from scan.
  std::unique_ptr<std::vector<std::unique_ptr<TupleData>>> from_tuples;

  if (stmt()->from_scan() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(const RelationalOp* relational_op,
                     LookupResolvedScan(stmt()->from_scan()));

    std::unique_ptr<TupleSchema> from_schema;
    from_tuples = absl::make_unique<std::vector<std::unique_ptr<TupleData>>>();
    ZETASQL_RETURN_IF_ERROR(EvalRelationalOp(*relational_op, params, context,
                                     &from_schema, from_tuples.get()));
  }

  ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* where_expr,
                   LookupResolvedExpr(stmt()->where_expr()));

  int64_t num_rows_modified = 0;
  std::vector<std::vector<Value>> dml_output_rows;

  ZETASQL_ASSIGN_OR_RETURN(const RelationalOp* relational_op,
                   LookupResolvedScan(stmt()->table_scan()));

  std::unique_ptr<TupleSchema> tuple_schema;
  std::vector<std::unique_ptr<TupleData>> tuples;
  ZETASQL_RETURN_IF_ERROR(EvalRelationalOp(*relational_op, params, context,
                                   &tuple_schema, &tuples));
  for (const std::unique_ptr<TupleData>& tuple_data : tuples) {
    // It is expensive to call this for every row, but this code is only used
    // for compliance testing, so it's ok.
    ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());

    const Tuple tuple(tuple_schema.get(), tuple_data.get());
    std::vector<const TupleData*> joined_tuple_datas;
    ZETASQL_RETURN_IF_ERROR(GetJoinedTupleDatas(params, tuple_data.get(),
                                        from_tuples.get(), where_expr, context,
                                        &joined_tuple_datas));
    if (joined_tuple_datas.empty()) {
      ZETASQL_ASSIGN_OR_RETURN(const std::vector<Value> dml_output_row,
                       GetScannedTupleAsColumnValues(*column_list_, tuple));
      if (context->options().return_all_rows_for_dml) {
        dml_output_rows.push_back(dml_output_row);
      }
      continue;
    }

    ++num_rows_modified;

    UpdateMap update_map;
    for (const std::unique_ptr<const ResolvedUpdateItem>& update_item :
         stmt()->update_item_list()) {
      ResolvedColumn update_column, update_target_column;
      std::vector<UpdatePathComponent> prefix_components;
      ZETASQL_RETURN_IF_ERROR(AddToUpdateMap(
          update_item.get(), joined_tuple_datas, context, &update_column,
          &update_target_column, &prefix_components, &update_map));
    }

    ZETASQL_ASSIGN_OR_RETURN(std::vector<Value> dml_output_row,
                     GetDMLOutputRow(tuple, update_map, context));
    dml_output_rows.push_back(dml_output_row);
  }

  // Verify that there are no duplicate primary keys in the modified table.
  absl::string_view duplicate_primary_key_error_prefix =
      "Modification resulted in duplicate primary key";
  PrimaryKeyRowMap row_map;
  bool has_primary_key;
  ZETASQL_RETURN_IF_ERROR(PopulatePrimaryKeyRowMap(
      dml_output_rows, duplicate_primary_key_error_prefix, context, &row_map,
      &has_primary_key));

  ZETASQL_RETURN_IF_ERROR(VerifyNumRowsModified(stmt()->assert_rows_modified(), params,
                                        num_rows_modified, context));

  ZETASQL_RETURN_IF_ERROR(resolved_node_->CheckFieldsAccessed());
  return GetDMLOutputValue(num_rows_modified, dml_output_rows, context);
}

::zetasql_base::StatusOr<Value> DMLUpdateValueExpr::UpdateNode::GetNewValue(
    const Value& original_value) const {
  if (is_leaf()) return leaf_value();

  switch (original_value.type_kind()) {
    case TYPE_STRUCT: {
      if (original_value.is_null()) {
        return zetasql_base::OutOfRangeErrorBuilder()
               << "Cannot set field of NULL "
               << original_value.type()->TypeName(
                      ProductMode::PRODUCT_EXTERNAL);
      }

      std::vector<Value> new_fields = original_value.fields();
      for (const auto& entry : child_map()) {
        const UpdatePathComponent& component = entry.first;
        const UpdateNode& update_node = *entry.second;

        ZETASQL_RET_CHECK(component.kind() == UpdatePathComponent::Kind::STRUCT_FIELD)
            << "Unexpected non-struct UpdatePathComponent::Kind in "
            << "GetNewValue(): "
            << UpdatePathComponent::GetKindString(component.kind());
        const int64_t field_idx = component.struct_field_index();

        ZETASQL_ASSIGN_OR_RETURN(const Value field_value,
                         update_node.GetNewValue(new_fields[field_idx]));
        new_fields[field_idx] = field_value;
      }
      return Value::Struct(original_value.type()->AsStruct(), new_fields);
    }
    case TYPE_PROTO:
      return GetNewProtoValue(original_value);
    case TYPE_ARRAY: {
      if (original_value.is_null()) {
        return zetasql_base::OutOfRangeErrorBuilder()
               << "Cannot use [] to modify a NULL array of type "
               << original_value.type()->TypeName(
                      ProductMode::PRODUCT_EXTERNAL);
      }

      std::vector<Value> new_elements = original_value.elements();
      for (const auto& entry : child_map()) {
        const UpdatePathComponent& component = entry.first;
        const UpdateNode& update_node = *entry.second;

        ZETASQL_RET_CHECK(component.kind() == UpdatePathComponent::Kind::ARRAY_OFFSET)
            << "Unexpected non-struct UpdatePathComponent::Kind in "
            << "GetNewValue(): "
            << UpdatePathComponent::GetKindString(component.kind());
        const int64_t offset = component.array_offset();

        if (offset < 0 || offset >= new_elements.size()) {
          return zetasql_base::OutOfRangeErrorBuilder()
                 << "Cannot SET array offset " << offset << " of an "
                 << original_value.type()->TypeName(
                        ProductMode::PRODUCT_EXTERNAL)
                 << " of size " << new_elements.size();
        }

        ZETASQL_ASSIGN_OR_RETURN(const Value element_value,
                         update_node.GetNewValue(new_elements[offset]));
        new_elements[offset] = element_value;
      }
      return Value::Array(original_value.type()->AsArray(), new_elements);
    }
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "Unexpected type kind for GetNewValue() on an internal "
          << "UpdateNode: " << TypeKind_Name(original_value.type_kind());
  }
}

::zetasql_base::StatusOr<Value> DMLUpdateValueExpr::UpdateNode::GetNewProtoValue(
    const Value& original_value) const {
  ZETASQL_RET_CHECK_EQ(original_value.type_kind(), TYPE_PROTO);

  if (original_value.is_null()) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "Cannot set field of NULL "
           << original_value.type()->TypeName(ProductMode::PRODUCT_EXTERNAL);
  }

  // TODO: Serializing and deserializing the protos over and over seems
  // wasteful, but this code is only for compliance tests, so this is fine.
  TypeFactory type_factory;
  google::protobuf::DynamicMessageFactory message_factory;
  std::unique_ptr<google::protobuf::Message> new_message =
      absl::WrapUnique(original_value.ToMessage(&message_factory));
  for (const auto& entry : child_map()) {
    const UpdatePathComponent& component = entry.first;
    const UpdateNode& update_node = *entry.second;

    ZETASQL_RET_CHECK(component.kind() == UpdatePathComponent::Kind::PROTO_FIELD)
        << "Unexpected non-proto UpdatePathComponent::Kind in "
        << "GetNewProtoValue(): "
        << UpdatePathComponent::GetKindString(component.kind());
    const google::protobuf::FieldDescriptor* field_descriptor =
        component.proto_field_descriptor();

    const Type* field_type;
    ZETASQL_RETURN_IF_ERROR(
        type_factory.GetProtoFieldType(field_descriptor, &field_type));

    // Read the original value of the field (as an array if it is repeated).
    Value original_field_value;
    ZETASQL_RETURN_IF_ERROR(ProtoFieldToValue(*new_message, field_descriptor,
                                      /*index=*/-1, field_type,
                                      /*use_wire_format_annotations=*/false,
                                      &original_field_value));

    // Compute the new value of the field.
    ZETASQL_ASSIGN_OR_RETURN(const Value new_field_value,
                     update_node.GetNewValue(original_field_value));

    // Overwrite the value of the field in 'new_message'.
    if (field_descriptor->is_required() && new_field_value.is_null()) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "Cannot clear required proto field "
             << field_descriptor->full_name();
    }
    ZETASQL_RET_CHECK_EQ(field_descriptor->is_repeated(),
                 new_field_value.type_kind() == TYPE_ARRAY);
    if (field_descriptor->is_repeated() && !new_field_value.is_null()) {
      for (const Value& value : new_field_value.elements()) {
        if (value.is_null()) {
          return zetasql_base::OutOfRangeErrorBuilder()
                 << "Cannot store a NULL element in repeated proto field "
                 << field_descriptor->full_name();
        }
      }
    }
    new_message->GetReflection()->ClearField(new_message.get(),
                                             field_descriptor);
    ZETASQL_RETURN_IF_ERROR(
        MergeValueToProtoField(new_field_value, field_descriptor,
                               /*use_wire_format_annotations=*/false,
                               &message_factory, new_message.get()));
  }
  return Value::Proto(original_value.type()->AsProto(),
                      absl::Cord(new_message->SerializeAsString()));
}

DMLUpdateValueExpr::DMLUpdateValueExpr(
    const Table* table, const ArrayType* table_array_type,
    const StructType* primary_key_type, const StructType* dml_output_type,
    const ResolvedUpdateStmt* resolved_node,
    const ResolvedColumnList* column_list,
    std::unique_ptr<const ColumnToVariableMapping> column_to_variable_mapping,
    std::unique_ptr<const ResolvedScanMap> resolved_scan_map,
    std::unique_ptr<const ResolvedExprMap> resolved_expr_map)
    : DMLValueExpr(table, table_array_type, primary_key_type, dml_output_type,
                   resolved_node, column_list,
                   std::move(column_to_variable_mapping),
                   std::move(resolved_scan_map), std::move(resolved_expr_map)) {
}

absl::Status DMLUpdateValueExpr::SetSchemasForEvaluationOfUpdateItem(
    const ResolvedUpdateItem* update_item,
    absl::Span<const TupleSchema* const> params_schemas) {
  for (const std::unique_ptr<const ResolvedUpdateArrayItem>& update_array_item :
       update_item->array_update_list()) {
    ZETASQL_ASSIGN_OR_RETURN(ValueExpr * offset_expr,
                     LookupResolvedExpr(update_array_item->offset()));
    ZETASQL_RETURN_IF_ERROR(offset_expr->SetSchemasForEvaluation(params_schemas));
    ZETASQL_RETURN_IF_ERROR(SetSchemasForEvaluationOfUpdateItem(
        update_array_item->update_item(), params_schemas));
  }

  if (update_item->array_update_list().empty()) {
    if (update_item->set_value() != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(ValueExpr * leaf_value_expr,
                       LookupResolvedExpr(update_item->set_value()->value()));
      return leaf_value_expr->SetSchemasForEvaluation(params_schemas);
    }

    ZETASQL_RET_CHECK(!update_item->delete_list().empty() ||
              !update_item->update_list().empty() ||
              !update_item->insert_list().empty());

    ZETASQL_ASSIGN_OR_RETURN(ValueExpr * update_target_expr,
                     LookupResolvedExpr(update_item->target()));
    ZETASQL_RETURN_IF_ERROR(
        update_target_expr->SetSchemasForEvaluation(params_schemas));

    const ResolvedColumn& element_column =
        update_item->element_column()->column();

    for (const std::unique_ptr<const ResolvedDeleteStmt>& nested_delete :
         update_item->delete_list()) {
      ZETASQL_RETURN_IF_ERROR(SetSchemasForEvaluationOfNestedDelete(
          nested_delete.get(), element_column, params_schemas));
    }
    for (const std::unique_ptr<const ResolvedUpdateStmt>& nested_update :
         update_item->update_list()) {
      ZETASQL_RETURN_IF_ERROR(SetSchemasForEvaluationOfNestedUpdate(
          nested_update.get(), element_column, params_schemas));
    }
    for (const std::unique_ptr<const ResolvedInsertStmt>& nested_insert :
         update_item->insert_list()) {
      ZETASQL_RETURN_IF_ERROR(SetSchemasForEvaluationOfNestedInsert(nested_insert.get(),
                                                            params_schemas));
    }
  }

  return absl::OkStatus();
}

absl::Status DMLUpdateValueExpr::SetSchemasForEvaluationOfNestedDelete(
    const ResolvedDeleteStmt* nested_delete,
    const ResolvedColumn& element_column,
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_ASSIGN_OR_RETURN(const VariableId element_column_variable_id,
                   column_to_variable_mapping_->LookupVariableNameForColumn(
                       &element_column));

  std::vector<VariableId> new_variables;
  new_variables.push_back(element_column_variable_id);

  if (nested_delete->array_offset_column() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(const VariableId array_offset_column_variable_id,
                     column_to_variable_mapping_->LookupVariableNameForColumn(
                         &nested_delete->array_offset_column()->column()));
    new_variables.push_back(array_offset_column_variable_id);
  }
  const TupleSchema new_schema(new_variables);

  ZETASQL_ASSIGN_OR_RETURN(ValueExpr * where_expr,
                   LookupResolvedExpr(nested_delete->where_expr()));
  ZETASQL_RETURN_IF_ERROR(where_expr->SetSchemasForEvaluation(
      ConcatSpans(params_schemas, {&new_schema})));

  if (nested_delete->assert_rows_modified() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ValueExpr * rows_expr,
        LookupResolvedExpr(nested_delete->assert_rows_modified()->rows()));
    ZETASQL_RETURN_IF_ERROR(rows_expr->SetSchemasForEvaluation(params_schemas));
  }

  return absl::OkStatus();
}

absl::Status DMLUpdateValueExpr::SetSchemasForEvaluationOfNestedUpdate(
    const ResolvedUpdateStmt* nested_update,
    const ResolvedColumn& element_column,
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_ASSIGN_OR_RETURN(const VariableId element_column_variable_id,
                   column_to_variable_mapping_->LookupVariableNameForColumn(
                       &element_column));
  std::vector<VariableId> new_variables;
  new_variables.push_back(element_column_variable_id);

  if (nested_update->array_offset_column() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(const VariableId array_offset_column_variable_id,
                     column_to_variable_mapping_->LookupVariableNameForColumn(
                         &nested_update->array_offset_column()->column()));
    new_variables.push_back(array_offset_column_variable_id);
  }
  const TupleSchema new_schema(new_variables);
  const std::vector<const TupleSchema*> new_params_schemas =
      ConcatSpans(params_schemas, {&new_schema});

  for (const std::unique_ptr<const ResolvedUpdateItem>& update_item :
       nested_update->update_item_list()) {
    ZETASQL_RETURN_IF_ERROR(SetSchemasForEvaluationOfUpdateItem(update_item.get(),
                                                        new_params_schemas));
  }

  ZETASQL_ASSIGN_OR_RETURN(ValueExpr * where_expr,
                   LookupResolvedExpr(nested_update->where_expr()));
  ZETASQL_RETURN_IF_ERROR(where_expr->SetSchemasForEvaluation(new_params_schemas));

  if (nested_update->assert_rows_modified() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ValueExpr * rows_expr,
        LookupResolvedExpr(nested_update->assert_rows_modified()->rows()));
    ZETASQL_RETURN_IF_ERROR(rows_expr->SetSchemasForEvaluation(params_schemas));
  }

  return absl::OkStatus();
}

absl::Status DMLUpdateValueExpr::SetSchemasForEvaluationOfNestedInsert(
    const ResolvedInsertStmt* nested_insert,
    absl::Span<const TupleSchema* const> params_schemas) {
  if (nested_insert->query() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(RelationalOp * query,
                     LookupResolvedScan(nested_insert->query()));
    ZETASQL_RETURN_IF_ERROR(query->SetSchemasForEvaluation(params_schemas));
  } else {
    for (const std::unique_ptr<const ResolvedInsertRow>& insert_row :
         nested_insert->row_list()) {
      for (const std::unique_ptr<const ResolvedDMLValue>& dml_value :
           insert_row->value_list()) {
        ZETASQL_ASSIGN_OR_RETURN(ValueExpr * value_expr,
                         LookupResolvedExpr(dml_value->value()));
        ZETASQL_RETURN_IF_ERROR(value_expr->SetSchemasForEvaluation(params_schemas));
      }
    }
  }

  if (nested_insert->assert_rows_modified() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ValueExpr * rows_expr,
        LookupResolvedExpr(nested_insert->assert_rows_modified()->rows()));
    ZETASQL_RETURN_IF_ERROR(rows_expr->SetSchemasForEvaluation(params_schemas));
  }

  return absl::OkStatus();
}

absl::Status DMLUpdateValueExpr::GetJoinedTupleDatas(
    absl::Span<const TupleData* const> params, const TupleData* left_tuple,
    const std::vector<std::unique_ptr<TupleData>>* right_tuples,
    const ValueExpr* where_expr, EvaluationContext* context,
    std::vector<const TupleData*>* joined_tuple_datas) const {
  joined_tuple_datas->clear();

  if (right_tuples == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        const Value where_value,
        EvalExpr(*where_expr, ConcatSpans(params, {left_tuple}), context));
    if (where_value == Bool(true)) {
      *joined_tuple_datas = ConcatSpans(params, {left_tuple});
    }
    return absl::OkStatus();
  }

  for (const std::unique_ptr<TupleData>& right_tuple : *right_tuples) {
    const std::vector<const TupleData*> candidate_joined_tuple_datas =
        ConcatSpans(params, {left_tuple, right_tuple.get()});
    ZETASQL_ASSIGN_OR_RETURN(
        const Value where_value,
        EvalExpr(*where_expr, candidate_joined_tuple_datas, context));
    if (where_value == Bool(true)) {
      if (!joined_tuple_datas->empty()) {
        return zetasql_base::OutOfRangeErrorBuilder()
               << "UPDATE with join requires that each row of the table being "
               << "updated correspond with at most one joined row that matches "
               << "the WHERE clause";
      }

      *joined_tuple_datas = candidate_joined_tuple_datas;
    }
  }

  return absl::OkStatus();
}

absl::Status DMLUpdateValueExpr::AddToUpdateMap(
    const ResolvedUpdateItem* update_item,
    absl::Span<const TupleData* const> tuples_for_row,
    EvaluationContext* context, ResolvedColumn* update_column,
    ResolvedColumn* update_target_column,
    std::vector<UpdatePathComponent>* prefix_components,
    UpdateMap* update_map) const {
  // Extract components from 'update_item->target()' and append them to
  // 'prefix_components' for the duration of this function call.
  std::vector<UpdatePathComponent> new_components;
  ZETASQL_RETURN_IF_ERROR(PopulateUpdatePathComponents(
      update_item->target(), update_target_column, &new_components));
  if (prefix_components->empty()) {
    *update_column = *update_target_column;
  }
  prefix_components->insert(prefix_components->end(), new_components.begin(),
                            new_components.end());
  auto new_components_cleanup =
      zetasql_base::MakeCleanup([prefix_components, &new_components] {
        for (int i = 0; i < new_components.size(); ++i) {
          prefix_components->pop_back();
        }
      });

  // Iterate over each ResolvedUpdateArrayItem (if there are any) and recurse
  // for each one.
  std::unordered_set<int64_t> used_array_offsets;
  for (const std::unique_ptr<const ResolvedUpdateArrayItem>& update_array_item :
       update_item->array_update_list()) {
    ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* offset_expr,
                     LookupResolvedExpr(update_array_item->offset()));

    ZETASQL_ASSIGN_OR_RETURN(const Value offset_value,
                     EvalExpr(*offset_expr, tuples_for_row, context));
    ZETASQL_RET_CHECK_EQ(offset_value.type_kind(), TYPE_INT64);
    if (offset_value.is_null()) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "Cannot SET a NULL offset of an "
             << update_item->target()->type()->TypeName(
                    ProductMode::PRODUCT_EXTERNAL);
    }
    const int64_t offset_int64 = offset_value.int64_value();

    if (!zetasql_base::InsertIfNotPresent(&used_array_offsets, offset_int64)) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "Cannot perform multiple updates to offset " << offset_int64
             << " of an "
             << update_item->target()->type()->TypeName(
                    ProductMode::PRODUCT_EXTERNAL);
    }

    prefix_components->emplace_back(/*is_struct_field_index=*/false,
                                    offset_int64);
    auto cleanup = zetasql_base::MakeCleanup(
        [prefix_components] { prefix_components->pop_back(); });

    ResolvedColumn update_array_target_column;
    ZETASQL_RETURN_IF_ERROR(AddToUpdateMap(update_array_item->update_item(),
                                   tuples_for_row, context, update_column,
                                   &update_array_target_column,
                                   prefix_components, update_map));
    ZETASQL_RET_CHECK(update_array_target_column ==
              update_item->element_column()->column());
  }

  // If there are no ResolvedUpdateArrayItem children, then create the path of
  // UpdateNodes corresponding to the chain of
  // ResolvedUpdateItem->ResolvedUpdateArrayItem->...->ResolvedUpdateItem nodes
  // that ends at 'update_item'.
  if (update_item->array_update_list().empty()) {
    const bool first_update_node_is_leaf = prefix_components->empty();
    auto emplace_result = update_map->emplace(
        *update_column,
        absl::make_unique<UpdateNode>(first_update_node_is_leaf));
    UpdateNode& first_update_node = *emplace_result.first->second;
    // If this fails, the analyzer allowed conflicting updates.
    ZETASQL_RET_CHECK_EQ(first_update_node_is_leaf, first_update_node.is_leaf());

    ZETASQL_ASSIGN_OR_RETURN(const Value leaf_value,
                     GetLeafValue(update_item, tuples_for_row, context));
    ZETASQL_RETURN_IF_ERROR(AddToUpdateNode(prefix_components->begin(),
                                    prefix_components->end(), leaf_value,
                                    &first_update_node));
  }

  return absl::OkStatus();
}

absl::Status DMLUpdateValueExpr::PopulateUpdatePathComponents(
    const ResolvedExpr* update_target, ResolvedColumn* column,
    std::vector<UpdatePathComponent>* components) const {
  switch (update_target->node_kind()) {
    case RESOLVED_COLUMN_REF:
      *column = update_target->GetAs<ResolvedColumnRef>()->column();
      return absl::OkStatus();
    case RESOLVED_GET_STRUCT_FIELD: {
      const auto* get_struct_field =
          update_target->GetAs<ResolvedGetStructField>();
      ZETASQL_RETURN_IF_ERROR(PopulateUpdatePathComponents(get_struct_field->expr(),
                                                   column, components));
      components->emplace_back(/*is_struct_field_index=*/true,
                               get_struct_field->field_idx());
      return absl::OkStatus();
    }
    case RESOLVED_GET_PROTO_FIELD: {
      const auto* get_proto_field =
          update_target->GetAs<ResolvedGetProtoField>();
      ZETASQL_RETURN_IF_ERROR(PopulateUpdatePathComponents(get_proto_field->expr(),
                                                   column, components));
      components->emplace_back(get_proto_field->field_descriptor());
      return absl::OkStatus();
    }
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "Unsupported node kind in PopulateUpdatePathComponents(): "
          << ResolvedNodeKind_Name(update_target->node_kind());
  }
}

absl::Status DMLUpdateValueExpr::AddToUpdateNode(
    std::vector<UpdatePathComponent>::const_iterator start_component,
    std::vector<UpdatePathComponent>::const_iterator end_component,
    const Value& leaf_value, UpdateNode* update_node) const {
  ZETASQL_RET_CHECK_EQ(update_node->is_leaf(), start_component == end_component);
  if (update_node->is_leaf()) {
    *update_node->mutable_leaf_value() = leaf_value;
    return absl::OkStatus();
  }

  UpdateNode::ChildMap* child_map = update_node->mutable_child_map();
  const UpdatePathComponent& next_component = *start_component;
  ++start_component;

  // Sanity check that we aren't trying to add two different
  // UpdatePathComponent::Kinds to 'child_map'.
  if (!child_map->empty()) {
    const UpdatePathComponent::Kind expected_kind =
        child_map->begin()->first.kind();
    ZETASQL_RET_CHECK(next_component.kind() == expected_kind)
        << "AddToUpdateNode() expected UpdatePathComponent::Kind "
        << UpdatePathComponent::GetKindString(expected_kind) << ", but found "
        << UpdatePathComponent::GetKindString(next_component.kind());
  }

  // Get the UpdateNode child corresponding to 'next_component', adding it to
  // 'child_map' if necessary.
  const bool is_leaf = (start_component == end_component);
  auto emplace_result = child_map->emplace(
      next_component, absl::make_unique<UpdateNode>(is_leaf));
  UpdateNode& next_update_node = *emplace_result.first->second;
  // If this fails, the analyzer allowed conflicting updates.
  ZETASQL_RET_CHECK_EQ(is_leaf, next_update_node.is_leaf());

  return AddToUpdateNode(start_component, end_component, leaf_value,
                         &next_update_node);
}

::zetasql_base::StatusOr<Value> DMLUpdateValueExpr::GetLeafValue(
    const ResolvedUpdateItem* update_item,
    absl::Span<const TupleData* const> tuples_for_row,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK(update_item->array_update_list().empty());

  if (update_item->set_value() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* leaf_value_expr,
                     LookupResolvedExpr(update_item->set_value()->value()));
    return EvalExpr(*leaf_value_expr, tuples_for_row, context);
  }

  ZETASQL_RET_CHECK(!update_item->delete_list().empty() ||
            !update_item->update_list().empty() ||
            !update_item->insert_list().empty());

  ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* update_target_expr,
                   LookupResolvedExpr(update_item->target()));

  ZETASQL_ASSIGN_OR_RETURN(const Value original_value,
                   EvalExpr(*update_target_expr, tuples_for_row, context));
  ZETASQL_RET_CHECK(original_value.type()->IsArray());
  if (original_value.is_null()) {
    absl::string_view statement_type =
        !update_item->delete_list().empty()
            ? "DELETE"
            : (!update_item->update_list().empty() ? "UPDATE" : "INSERT");
    return zetasql_base::OutOfRangeErrorBuilder()
           << "Cannot execute a nested " << statement_type
           << " statement on a NULL array value";
  }
  const std::vector<Value>& original_elements = original_value.elements();

  const ResolvedColumn& element_column =
      update_item->element_column()->column();
  std::vector<UpdatedElement> updated_elements(original_elements.size());
  for (const std::unique_ptr<const ResolvedDeleteStmt>& nested_delete :
       update_item->delete_list()) {
    ZETASQL_RETURN_IF_ERROR(ProcessNestedDelete(nested_delete.get(), tuples_for_row,
                                        element_column, original_elements,
                                        context, &updated_elements));
  }
  for (const std::unique_ptr<const ResolvedUpdateStmt>& nested_update :
       update_item->update_list()) {
    ZETASQL_RETURN_IF_ERROR(ProcessNestedUpdate(nested_update.get(), tuples_for_row,
                                        element_column, original_elements,
                                        context, &updated_elements));
  }
  for (const std::unique_ptr<const ResolvedInsertStmt>& nested_insert :
       update_item->insert_list()) {
    ZETASQL_RETURN_IF_ERROR(ProcessNestedInsert(nested_insert.get(), tuples_for_row,
                                        original_elements, context,
                                        &updated_elements));
  }

  std::vector<Value> new_elements;
  ZETASQL_RET_CHECK_GE(updated_elements.size(), original_elements.size());
  for (int i = 0; i < updated_elements.size(); ++i) {
    const UpdatedElement& updated_element = updated_elements[i];
    switch (updated_element.kind()) {
      case UpdatedElement::Kind::UNMODIFIED:
        new_elements.push_back(original_elements[i]);
        break;
      case UpdatedElement::Kind::DELETED:
        // Nothing to do.
        break;
      case UpdatedElement::Kind::MODIFIED:
        new_elements.push_back(updated_element.new_value());
        break;
    }
  }

  return Value::Array(original_value.type()->AsArray(), new_elements);
}

::zetasql_base::StatusOr<std::vector<Value>> DMLUpdateValueExpr::GetDMLOutputRow(
    const Tuple& tuple, const UpdateMap& update_map,
    EvaluationContext* context) const {
  absl::flat_hash_set<int> key_index_set;
  ZETASQL_ASSIGN_OR_RETURN(const absl::optional<std::vector<int>> key_indexes,
                   GetPrimaryKeyColumnIndexes(context));
  if (key_indexes.has_value()) {
    key_index_set.insert(key_indexes->begin(), key_indexes->end());
  }

  std::vector<Value> dml_output_row;
  for (int i = 0; i < column_list_->size(); ++i) {
    const ResolvedColumn& column = (*column_list_)[i];

    ZETASQL_ASSIGN_OR_RETURN(const Value original_value, GetColumnValue(column, tuple));

    const std::unique_ptr<UpdateNode>* update_node_or_null =
        zetasql_base::FindOrNull(update_map, column);
    if (update_node_or_null == nullptr) {
      // 'column' was not modified by the statement.
      dml_output_row.push_back(original_value);
    } else {
      ZETASQL_ASSIGN_OR_RETURN(const Value new_value,
                       (*update_node_or_null)->GetNewValue(original_value));
      if (key_index_set.contains(i)) {
        // Attempting to modify a primary key column.
        const LanguageOptions& language_options = context->GetLanguageOptions();
        if (language_options.LanguageFeatureEnabled(
                FEATURE_DISALLOW_PRIMARY_KEY_UPDATES)) {
          return zetasql_base::OutOfRangeErrorBuilder()
                 << "Cannot modify a primary key column with UPDATE";
        }
        if (new_value.is_null() && language_options.LanguageFeatureEnabled(
                                       FEATURE_DISALLOW_NULL_PRIMARY_KEYS)) {
          return zetasql_base::OutOfRangeErrorBuilder()
                 << "Cannot set a primary key column to NULL with UPDATE";
        }
      }

      dml_output_row.push_back(new_value);
    }
  }

  return dml_output_row;
}

absl::Status DMLUpdateValueExpr::ProcessNestedDelete(
    const ResolvedDeleteStmt* nested_delete,
    absl::Span<const TupleData* const> tuples_for_row,
    const ResolvedColumn& element_column,
    const std::vector<Value>& original_elements, EvaluationContext* context,
    std::vector<UpdatedElement>* new_elements) const {
  ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* where_expr,
                   LookupResolvedExpr(nested_delete->where_expr()));

  int64_t num_values_deleted = 0;
  ZETASQL_RET_CHECK_EQ(original_elements.size(), new_elements->size());
  for (int i = 0; i < original_elements.size(); ++i) {
    const Value& original_value = original_elements[i];
    UpdatedElement& updated_element = (*new_elements)[i];
    switch (updated_element.kind()) {
      case UpdatedElement::Kind::UNMODIFIED: {
        // As in SetSchemasForEvaluationOfNestedDelete(), first we put the
        // original value, then maybe the offset.
        std::vector<Value> new_values_for_where = {original_value};
        if (nested_delete->array_offset_column() != nullptr) {
          const Value array_offset_value = values::Int64(i);
          new_values_for_where.push_back(array_offset_value);
        }
        const TupleData extra_data_for_where =
            CreateTupleDataFromValues(new_values_for_where);

        ZETASQL_ASSIGN_OR_RETURN(
            const Value where_value,
            EvalExpr(*where_expr,
                     ConcatSpans(tuples_for_row, {&extra_data_for_where}),
                     context));
        if (where_value == Bool(true)) {
          updated_element.delete_value();
          ++num_values_deleted;
        }
        break;
      }
      case UpdatedElement::Kind::DELETED:
        // Nothing to do. (This can happen if we apply two nested deletes to the
        // same array.)
        break;
      case UpdatedElement::Kind::MODIFIED:
        ZETASQL_RET_CHECK_FAIL()
            << "Unexpected MODIFIED UpdatedElement in ProcessNestedDelete()";
    }
  }

  return VerifyNumRowsModified(nested_delete->assert_rows_modified(),
                               tuples_for_row, num_values_deleted, context,
                               /*print_array_elements=*/true);
}

absl::Status DMLUpdateValueExpr::ProcessNestedUpdate(
    const ResolvedUpdateStmt* nested_update,
    absl::Span<const TupleData* const> tuples_for_row,
    const ResolvedColumn& element_column,
    const std::vector<Value>& original_elements, EvaluationContext* context,
    std::vector<UpdatedElement>* new_elements) const {
  ZETASQL_ASSIGN_OR_RETURN(const VariableId element_column_variable_id,
                   column_to_variable_mapping_->LookupVariableNameForColumn(
                       &element_column));

  absl::optional<VariableId> array_offset_column_variable_id;
  if (nested_update->array_offset_column() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(array_offset_column_variable_id,
                     column_to_variable_mapping_->LookupVariableNameForColumn(
                         &nested_update->array_offset_column()->column()));
  }

  ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* where_expr,
                   LookupResolvedExpr(nested_update->where_expr()));

  int64_t num_values_modified = 0;
  ZETASQL_RET_CHECK_EQ(original_elements.size(), new_elements->size());
  for (int i = 0; i < original_elements.size(); ++i) {
    const Value& original_value = original_elements[i];
    UpdatedElement& updated_element = (*new_elements)[i];

    // As in SetSchemasForEvaluationOfNestedUpdate(), first we put the
    // original value, then maybe the offset.
    std::vector<Value> values_for_element = {original_value};
    if (nested_update->array_offset_column() != nullptr) {
      const Value array_offset_value = values::Int64(i);
      values_for_element.push_back(array_offset_value);
    }
    const TupleData data_for_element =
        CreateTupleDataFromValues(values_for_element);
    const std::vector<const TupleData*> tuples_with_element =
        ConcatSpans(tuples_for_row, {&data_for_element});

    ZETASQL_ASSIGN_OR_RETURN(const Value where_value,
                     EvalExpr(*where_expr, tuples_with_element, context));
    if (where_value != Bool(true)) continue;

    switch (updated_element.kind()) {
      case UpdatedElement::Kind::UNMODIFIED: {
        UpdateMap update_map;
        for (const std::unique_ptr<const ResolvedUpdateItem>& update_item :
             nested_update->update_item_list()) {
          ResolvedColumn update_column, update_target_column;
          std::vector<UpdatePathComponent> prefix_components;
          ZETASQL_RETURN_IF_ERROR(AddToUpdateMap(
              update_item.get(), tuples_with_element, context, &update_column,
              &update_target_column, &prefix_components, &update_map));
        }

        // All of the ResolvedUpdateItems in a nested UPDATE modify
        // 'element_column'.
        ZETASQL_RET_CHECK_EQ(update_map.size(), 1);
        const auto& update_map_entry = *update_map.begin();
        ZETASQL_RET_CHECK(update_map_entry.first == element_column);
        const UpdateNode& update_node = *update_map_entry.second;

        ZETASQL_ASSIGN_OR_RETURN(const Value new_value,
                         update_node.GetNewValue(original_value));
        updated_element.set_new_value(new_value);

        ++num_values_modified;
        break;
      }
      case UpdatedElement::Kind::DELETED:
        // Nothing to do.
        break;
      case UpdatedElement::Kind::MODIFIED:
        return zetasql_base::OutOfRangeErrorBuilder()
               << "Attempted to modify an array element with multiple nested "
               << "UPDATE statements";
    }
  }

  return VerifyNumRowsModified(nested_update->assert_rows_modified(),
                               tuples_for_row, num_values_modified, context,
                               /*print_array_elements=*/true);
}

absl::Status DMLUpdateValueExpr::ProcessNestedInsert(
    const ResolvedInsertStmt* nested_insert,
    absl::Span<const TupleData* const> tuples_for_row,
    const std::vector<Value>& original_elements, EvaluationContext* context,
    std::vector<UpdatedElement>* new_elements) const {
  const int64_t original_size_of_new_elements = new_elements->size();

  ZETASQL_RET_CHECK_NE(nested_insert->query() == nullptr,
               nested_insert->row_list().empty());
  if (nested_insert->query() != nullptr) {
    ZETASQL_RET_CHECK_EQ(nested_insert->query_output_column_list().size(), 1);
    ZETASQL_ASSIGN_OR_RETURN(const VariableId query_output_variable_id,
                     column_to_variable_mapping_->LookupVariableNameForColumn(
                         &nested_insert->query_output_column_list()[0]));

    ZETASQL_ASSIGN_OR_RETURN(const RelationalOp* relational_op,
                     LookupResolvedScan(nested_insert->query()));

    std::unique_ptr<TupleSchema> tuple_schema;
    std::vector<std::unique_ptr<TupleData>> tuples;
    ZETASQL_RETURN_IF_ERROR(EvalRelationalOp(*relational_op, tuples_for_row, context,
                                     &tuple_schema, &tuples));

    const absl::optional<int> opt_query_output_variable_slot =
        tuple_schema->FindIndexForVariable(query_output_variable_id);
    ZETASQL_RET_CHECK(opt_query_output_variable_slot.has_value());
    const int query_output_variable_slot =
        opt_query_output_variable_slot.value();

    for (const std::unique_ptr<TupleData>& query_tuple : tuples) {
      // It is expensive to call this for every row, but this code is only used
      // for compliance testing, so it's ok.
      ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());
      const Value& new_value =
          query_tuple->slot(query_output_variable_slot).value();

      UpdatedElement new_element;
      new_element.set_new_value(new_value);
      new_elements->push_back(new_element);
    }
  } else {
    for (const std::unique_ptr<const ResolvedInsertRow>& insert_row :
         nested_insert->row_list()) {
      for (const std::unique_ptr<const ResolvedDMLValue>& dml_value :
           insert_row->value_list()) {
        ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* value_expr,
                         LookupResolvedExpr(dml_value->value()));

        ZETASQL_ASSIGN_OR_RETURN(const Value new_value,
                         EvalExpr(*value_expr, tuples_for_row, context));

        UpdatedElement new_element;
        new_element.set_new_value(new_value);
        new_elements->push_back(new_element);
      }
    }
  }

  const int64_t num_values_inserted =
      new_elements->size() - original_size_of_new_elements;
  return VerifyNumRowsModified(nested_insert->assert_rows_modified(),
                               tuples_for_row, num_values_inserted, context,
                               /*print_array_elements=*/true);
}

// -------------------------------------------------------
// DMLInsertValueExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<DMLInsertValueExpr>>
DMLInsertValueExpr::Create(
    const Table* table, const ArrayType* table_array_type,
    const StructType* primary_key_type, const StructType* dml_output_type,
    const ResolvedInsertStmt* resolved_node,
    const ResolvedColumnList* column_list,
    std::unique_ptr<const ColumnToVariableMapping> column_to_variable_mapping,
    std::unique_ptr<const ResolvedScanMap> resolved_scan_map,
    std::unique_ptr<const ResolvedExprMap> resolved_expr_map) {
  return absl::WrapUnique(new DMLInsertValueExpr(
      table, table_array_type, primary_key_type, dml_output_type, resolved_node,
      column_list, std::move(column_to_variable_mapping),
      std::move(resolved_scan_map), std::move(resolved_expr_map)));
}

absl::Status DMLInsertValueExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  if (stmt()->query() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(RelationalOp * query, LookupResolvedScan(stmt()->query()));
    ZETASQL_RETURN_IF_ERROR(query->SetSchemasForEvaluation(params_schemas));
  } else {
    for (const std::unique_ptr<const ResolvedInsertRow>& row :
         stmt()->row_list()) {
      for (const std::unique_ptr<const ResolvedDMLValue>& value :
           row->value_list()) {
        ZETASQL_ASSIGN_OR_RETURN(ValueExpr * expr, LookupResolvedExpr(value->value()));
        ZETASQL_RETURN_IF_ERROR(expr->SetSchemasForEvaluation(params_schemas));
      }
    }
  }

  ZETASQL_ASSIGN_OR_RETURN(RelationalOp * scan,
                   LookupResolvedScan(stmt()->table_scan()));
  ZETASQL_RETURN_IF_ERROR(scan->SetSchemasForEvaluation(params_schemas));

  if (stmt()->assert_rows_modified() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ValueExpr * rows,
        LookupResolvedExpr(stmt()->assert_rows_modified()->rows()));
    ZETASQL_RETURN_IF_ERROR(rows->SetSchemasForEvaluation(params_schemas));
  }

  return absl::OkStatus();
}

::zetasql_base::StatusOr<Value> DMLInsertValueExpr::Eval(
    absl::Span<const TupleData* const> params,
    EvaluationContext* context) const {
  InsertColumnMap insert_column_map;
  ZETASQL_RETURN_IF_ERROR(PopulateInsertColumnMap(&insert_column_map));

  std::vector<std::vector<Value>> rows_to_insert;
  ZETASQL_RETURN_IF_ERROR(PopulateRowsToInsert(insert_column_map, params, context,
                                       &rows_to_insert));

  std::vector<std::vector<Value>> original_rows;
  ZETASQL_RETURN_IF_ERROR(PopulateRowsInOriginalTable(params, context, &original_rows));

  absl::string_view duplicate_primary_key_error_prefix =
      "Found two rows with primary key";

  // We currently store all old rows into `row_map` even in the case where we
  // are only returning new rows. This is because we have to do the error
  // checking that we do not cause a primary key collision. A future
  // optimization might do this checking without materializing the entire
  // updated PrimaryKeyRowMap in this case.
  PrimaryKeyRowMap row_map;
  bool has_primary_key;
  // Duplicate primary keys in the original table can only result from a problem
  // with the input table.
  ZETASQL_RETURN_IF_ERROR(PopulatePrimaryKeyRowMap(
      original_rows, duplicate_primary_key_error_prefix, context, &row_map,
      &has_primary_key));
  if (!has_primary_key &&
      stmt()->insert_mode() != ResolvedInsertStmt::OR_ERROR) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "INSERT " << stmt()->GetInsertModeString()
           << " is not allowed because the table does not have a primary key";
  }

  ZETASQL_ASSIGN_OR_RETURN(
      int64_t num_rows_modified,
      InsertRows(insert_column_map, rows_to_insert, context, &row_map));

  ZETASQL_RETURN_IF_ERROR(VerifyNumRowsModified(stmt()->assert_rows_modified(), params,
                                        num_rows_modified, context));

  ZETASQL_RETURN_IF_ERROR(resolved_node_->CheckFieldsAccessed());

  return context->options().return_all_rows_for_dml
             ? GetDMLOutputValue(num_rows_modified, row_map, context)
             : DMLValueExpr::GetDMLOutputValue(num_rows_modified,
                                               rows_to_insert, context);
}

DMLInsertValueExpr::DMLInsertValueExpr(
    const Table* table, const ArrayType* table_array_type,
    const StructType* primary_key_type, const StructType* dml_output_type,
    const ResolvedInsertStmt* resolved_node,
    const ResolvedColumnList* column_list,
    std::unique_ptr<const ColumnToVariableMapping> column_to_variable_mapping,
    std::unique_ptr<const ResolvedScanMap> resolved_scan_map,
    std::unique_ptr<const ResolvedExprMap> resolved_expr_map)
    : DMLValueExpr(table, table_array_type, primary_key_type, dml_output_type,
                   resolved_node, column_list,
                   std::move(column_to_variable_mapping),
                   std::move(resolved_scan_map), std::move(resolved_expr_map)) {
}

absl::Status DMLInsertValueExpr::PopulateInsertColumnMap(
    InsertColumnMap* insert_column_map) const {
  const std::vector<ResolvedColumn>& insert_column_list =
      stmt()->insert_column_list();
  // Populate 'insert_column_map', leaving InsertColumnOffsets.column_offset
  // unset for now.
  for (int i = 0; i < insert_column_list.size(); ++i) {
    const ResolvedColumn& insert_column = insert_column_list[i];

    InsertColumnOffsets offsets;
    offsets.insert_column_offset = i;
    // 'offsets.column_offset' is populated below.

    ZETASQL_RET_CHECK(insert_column_map->emplace(insert_column, offsets).second);
  }

  // Populate InsertColumnOffsets.column_offset.
  int num_column_offsets_set = 0;
  for (int i = 0; i < column_list_->size(); ++i) {
    const ResolvedColumn& column = (*column_list_)[i];
    InsertColumnOffsets* offsets = zetasql_base::FindOrNull(*insert_column_map, column);
    if (offsets != nullptr) {
      ZETASQL_RET_CHECK_EQ(-1, offsets->column_offset);
      offsets->column_offset = i;
      ++num_column_offsets_set;
    }
  }
  ZETASQL_RET_CHECK_EQ(insert_column_list.size(), num_column_offsets_set);
  ZETASQL_RET_CHECK_EQ(insert_column_list.size(), insert_column_map->size());

  return absl::OkStatus();
}

absl::Status DMLInsertValueExpr::PopulateRowsToInsert(
    const InsertColumnMap& insert_column_map,
    absl::Span<const TupleData* const> params, EvaluationContext* context,
    std::vector<std::vector<Value>>* rows_to_insert) const {
  // One element for each row being inserted, storing only the columns being
  // inserted.
  std::vector<std::vector<Value>> columns_to_insert;
  ZETASQL_RETURN_IF_ERROR(PopulateColumnsToInsert(insert_column_map, params, context,
                                          &columns_to_insert));

  for (const std::vector<Value>& columns_to_insert_for_row :
       columns_to_insert) {
    std::vector<Value> row_to_insert;

    for (int i = 0; i < column_list_->size(); ++i) {
      const ResolvedColumn& column = (*column_list_)[i];

      const InsertColumnOffsets* insert_column_offsets =
          zetasql_base::FindOrNull(insert_column_map, column);
      if (insert_column_offsets == nullptr) {
        // Fill in NULL for any values that are not being inserted.
        row_to_insert.push_back(Value::Null(column.type()));
      } else {
        ZETASQL_RET_CHECK_EQ(i, insert_column_offsets->column_offset);
        const int insert_column_offset =
            insert_column_offsets->insert_column_offset;
        row_to_insert.push_back(
            columns_to_insert_for_row[insert_column_offset]);
      }
    }

    rows_to_insert->push_back(row_to_insert);
  }

  return absl::OkStatus();
}

absl::Status DMLInsertValueExpr::PopulateColumnsToInsert(
    const InsertColumnMap& insert_column_map,
    absl::Span<const TupleData* const> params, EvaluationContext* context,
    std::vector<std::vector<Value>>* columns_to_insert) const {
  if (stmt()->query() != nullptr) {
    const ResolvedScan* query = stmt()->query();

    ZETASQL_ASSIGN_OR_RETURN(const RelationalOp* relational_op,
                     LookupResolvedScan(query));

    std::unique_ptr<TupleSchema> tuple_schema;
    std::vector<std::unique_ptr<TupleData>> tuples;
    ZETASQL_RETURN_IF_ERROR(EvalRelationalOp(*relational_op, params, context,
                                     &tuple_schema, &tuples));

    for (const std::unique_ptr<TupleData>& tuple : tuples) {
      // It is expensive to call this for every row, but this code is only used
      // for compliance testing, so it's ok.
      ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());
      ZETASQL_ASSIGN_OR_RETURN(const std::vector<Value> columns_to_insert_for_row,
                       GetScannedTupleAsColumnValues(
                           stmt()->query_output_column_list(),
                           Tuple(tuple_schema.get(), tuple.get())));
      columns_to_insert->push_back(columns_to_insert_for_row);
    }
  } else {
    for (const std::unique_ptr<const ResolvedInsertRow>& resolved_insert_row :
         stmt()->row_list()) {
      // It is expensive to call this for every row, but this code is only used
      // for compliance testing, so it's ok.
      ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());

      const std::vector<std::unique_ptr<const ResolvedDMLValue>>& dml_values =
          resolved_insert_row->value_list();
      ZETASQL_RET_CHECK_EQ(dml_values.size(), insert_column_map.size());

      std::vector<Value> columns_to_insert_for_row;
      for (const std::unique_ptr<const ResolvedDMLValue>& dml_value :
           dml_values) {
        ZETASQL_ASSIGN_OR_RETURN(const ValueExpr* value_expr,
                         LookupResolvedExpr(dml_value->value()));
        ZETASQL_ASSIGN_OR_RETURN(const Value value,
                         EvalExpr(*value_expr, params, context));
        columns_to_insert_for_row.push_back(value);
      }

      columns_to_insert->push_back(columns_to_insert_for_row);
    }
  }
  return absl::OkStatus();
}

absl::Status DMLInsertValueExpr::PopulateRowsInOriginalTable(
    absl::Span<const TupleData* const> params, EvaluationContext* context,
    std::vector<std::vector<Value>>* original_rows) const {
  ZETASQL_ASSIGN_OR_RETURN(const RelationalOp* relational_op,
                   LookupResolvedScan(stmt()->table_scan()));

  std::unique_ptr<TupleSchema> tuple_schema;
  std::vector<std::unique_ptr<TupleData>> tuples;
  ZETASQL_RETURN_IF_ERROR(EvalRelationalOp(*relational_op, params, context,
                                   &tuple_schema, &tuples));

  for (const std::unique_ptr<TupleData>& tuple : tuples) {
    // It is expensive to call this for every row, but this code is only used
    // for compliance testing, so it's ok.
    ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());
    ZETASQL_ASSIGN_OR_RETURN(
        const std::vector<Value> column_values,
        GetScannedTupleAsColumnValues(*column_list_,
                                      Tuple(tuple_schema.get(), tuple.get())));
    original_rows->push_back(column_values);
  }

  return absl::OkStatus();
}

::zetasql_base::StatusOr<int64_t> DMLInsertValueExpr::InsertRows(
    const InsertColumnMap& insert_column_map,
    const std::vector<std::vector<Value>>& rows_to_insert,
    EvaluationContext* context, PrimaryKeyRowMap* row_map) const {
  std::unordered_set<Value, ValueHasher> modified_primary_keys;
  const int64_t max_original_row_number = row_map->size() - 1;
  bool found_primary_key_collision = false;
  for (int i = 0; i < rows_to_insert.size(); ++i) {
    // It is expensive to call this for every row, but this code is only used
    // for compliance testing, so it's ok.
    ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());

    const std::vector<Value>& row_to_insert = rows_to_insert[i];

    RowNumberAndValues row_number_and_values;
    row_number_and_values.values = row_to_insert;
    // The only use of this row number is as the primary key if the table does
    // not have a real primary key, so we set it to the next row number.
    row_number_and_values.row_number = row_map->size();

    ZETASQL_ASSIGN_OR_RETURN(const Value primary_key,
                     GetPrimaryKeyOrRowNumber(row_number_and_values, context));
    if (context->GetLanguageOptions().LanguageFeatureEnabled(
            FEATURE_DISALLOW_NULL_PRIMARY_KEYS)) {
      bool primary_key_has_null;
      if (primary_key_type_ == nullptr) {
        primary_key_has_null = primary_key.is_null();
      } else {
        ZETASQL_RET_CHECK(primary_key.type()->IsStruct());
        ZETASQL_RET_CHECK(!primary_key.is_null());
        primary_key_has_null = std::any_of(
            primary_key.fields().begin(), primary_key.fields().end(),
            [](const Value& v) { return v.is_null(); });
      }
      if (primary_key_has_null) {
        // Ideally this logic would be in the analyzer, but the analyzer cannot
        // determine whether an expression is NULL. So the reference
        // implementation must respect this feature for the sake of compliance
        // testing other engines.
        return zetasql_base::OutOfRangeErrorBuilder()
               << "Cannot INSERT a NULL value into a primary key column";
      }
    }

    auto insert_result =
        row_map->insert(std::make_pair(primary_key, row_number_and_values));
    if (insert_result.second) {
      // The row was successfully inserted.
      ZETASQL_RET_CHECK(modified_primary_keys.insert(primary_key).second);
    } else {
      // The primary key of the new row is in the table, possibly corresponding
      // to a row that was previously inserted.
      RowNumberAndValues& old_row = insert_result.first->second;
      found_primary_key_collision = true;
      switch (stmt()->insert_mode()) {
        case ResolvedInsertStmt::OR_ERROR: {
          const std::string row_indent = "    ";
          return zetasql_base::OutOfRangeErrorBuilder()
                 << "Failed to insert row with primary key ("
                 << primary_key.ShortDebugString() << ")"
                 << " due to previously "
                 << (old_row.row_number <= max_original_row_number ? "existing"
                                                                   : "inserted")
                 << " row";
        }
        case ResolvedInsertStmt::OR_IGNORE:
          // Skip this row.
          break;
        case ResolvedInsertStmt::OR_REPLACE:
          // Replace the old row with the new row, using the same primary key.
          old_row.values = row_to_insert;
          modified_primary_keys.insert(primary_key);
          break;
        case ResolvedInsertStmt::OR_UPDATE: {
          // Update the old row according to the new row, using the same primary
          // key. Unlike OR_REPLACE, here we only change the columns being
          // inserted.
          ZETASQL_RET_CHECK_EQ(old_row.values.size(), row_to_insert.size());
          for (const auto& elt : insert_column_map) {
            const int column_offset = elt.second.column_offset;
            old_row.values[column_offset] = row_to_insert[column_offset];
          }
          modified_primary_keys.insert(primary_key);
          break;
        }
        default:
          ZETASQL_RET_CHECK_FAIL() << "Unsupported insert mode "
                           << ResolvedInsertStmtEnums_InsertMode_Name(
                                  stmt()->insert_mode());
      }
    }
  }

  if (!found_primary_key_collision) {
    // Dummy access of the insert mode. It does not matter in this case, but we
    // require that all fields in the resolved AST are explicitly accessed at
    // some point.
    stmt()->insert_mode();
  }

  return modified_primary_keys.size();
}

::zetasql_base::StatusOr<Value> DMLInsertValueExpr::GetDMLOutputValue(
    int64_t num_rows_modified, const PrimaryKeyRowMap& row_map,
    EvaluationContext* context) const {
  std::vector<std::vector<Value>> dml_output_rows(row_map.size());
  for (const auto& elt : row_map) {
    // It is expensive to call this for every row, but this code is only used
    // for compliance testing, so it's ok.
    ZETASQL_RETURN_IF_ERROR(context->VerifyNotAborted());

    const int64_t row_number = elt.second.row_number;
    dml_output_rows[row_number] = elt.second.values;
  }

  return DMLValueExpr::GetDMLOutputValue(num_rows_modified, dml_output_rows,
                                         context);
}

// -------------------------------------------------------
// RootExpr
// -------------------------------------------------------

::zetasql_base::StatusOr<std::unique_ptr<RootExpr>> RootExpr::Create(
    std::unique_ptr<ValueExpr> value_expr,
    std::unique_ptr<RootData> root_data) {
  return absl::WrapUnique(
      new RootExpr(std::move(value_expr), std::move(root_data)));
}

absl::Status RootExpr::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return mutable_value_expr()->SetSchemasForEvaluation(params_schemas);
}

bool RootExpr::Eval(absl::Span<const TupleData* const> params,
                    EvaluationContext* context, VirtualTupleSlot* result,
                    absl::Status* status) const {
  return value_expr()->Eval(params, context, result, status);
}

std::string RootExpr::DebugInternal(const std::string& indent,
                                    bool verbose) const {
  return absl::StrCat("RootExpr(", value_expr()->DebugInternal(indent, verbose),
                      ")");
}

RootExpr::RootExpr(std::unique_ptr<ValueExpr> value_expr,
                   std::unique_ptr<RootData> root_data)
    : ValueExpr(value_expr->output_type()), root_data_(std::move(root_data)) {
  SetArg(kValueExpr, absl::make_unique<ExprArg>(std::move(value_expr)));
}

const ValueExpr* RootExpr::value_expr() const {
  return GetArg(kValueExpr)->node()->AsValueExpr();
}

ValueExpr* RootExpr::mutable_value_expr() {
  return GetMutableArg(kValueExpr)->mutable_node()->AsMutableValueExpr();
}

}  // namespace zetasql
