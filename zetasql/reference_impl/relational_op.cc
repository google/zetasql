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

// This file contains implementations for relational operators that don't
// warrant their own files.

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/tuple_comparator.h"
#include "zetasql/reference_impl/variable_id.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

using zetasql::types::EmptyStructType;
using zetasql::values::Bool;
using zetasql::values::Int64;

namespace zetasql {

// -------------------------------------------------------
// RelationalArg
// -------------------------------------------------------

RelationalArg::RelationalArg(std::unique_ptr<RelationalOp> op)
    : AlgebraArg(VariableId(), std::move(op)) {}

RelationalArg::~RelationalArg() {}

RelationalOp::~RelationalOp() {}

// -------------------------------------------------------
// RelationalOp
// -------------------------------------------------------

absl::Status RelationalOp::set_is_order_preserving(bool is_order_preserving) {
  ZETASQL_RET_CHECK(!is_order_preserving || may_preserve_order())
      << "Operator cannot preserve order";
  is_order_preserving_ = is_order_preserving;
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<TupleIterator>> RelationalOp::Eval(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  // Copy params and pass ownershp of the copy to the value capture of a lambda.
  const std::vector<std::shared_ptr<const TupleData>> params_copies =
      DeepCopyTupleDatas(params);
  PassThroughTupleIterator::IteratorFactory iterator_factory =
      [this, params_copies, num_extra_slots, context]() {
        return CreateIterator(StripSharedPtrs(params_copies), num_extra_slots,
                              context);
      };
  const std::unique_ptr<const TupleSchema> schema = CreateOutputSchema();
  PassThroughTupleIterator::DebugStringFactory debug_string_factory = [this]() {
    return IteratorDebugString();
  };
  std::unique_ptr<TupleIterator> iter =
      absl::make_unique<PassThroughTupleIterator>(iterator_factory, *schema,
                                                  debug_string_factory);
  return iter;
}

absl::StatusOr<std::unique_ptr<TupleIterator>> RelationalOp::MaybeReorder(
    std::unique_ptr<TupleIterator> iter, EvaluationContext* context) const {
  if (context->options().scramble_undefined_orderings) {
    iter = absl::make_unique<ReorderingTupleIterator>(std::move(iter));
  }
  return iter;
}

// -------------------------------------------------------
// InArrayColumnFilterArg
// -------------------------------------------------------

absl::StatusOr<std::unique_ptr<InArrayColumnFilterArg>>
InArrayColumnFilterArg::Create(const VariableId& variable, int column_idx,
                               std::unique_ptr<ValueExpr> array) {
  return absl::WrapUnique(
      new InArrayColumnFilterArg(variable, column_idx, std::move(array)));
}

absl::Status InArrayColumnFilterArg::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return array_->SetSchemasForEvaluation(params_schemas);
}

absl::StatusOr<std::unique_ptr<ColumnFilter>> InArrayColumnFilterArg::Eval(
    absl::Span<const TupleData* const> params,
    EvaluationContext* context) const {
  TupleSlot array;
  absl::Status status;
  if (!array_->EvalSimple(params, context, &array, &status)) {
    return status;
  }

  std::vector<Value> values;
  if (!array.value().is_null()) {
    values.reserve(array.value().elements().size());
    for (const Value& value : array.value().elements()) {
      // Check for NULL or NaN.
      if (value.SqlEquals(value) == values::True()) {
        values.push_back(value);
      }
    }
  }

  return absl::make_unique<ColumnFilter>(values);
}

std::string InArrayColumnFilterArg::DebugInternal(const std::string& indent,
                                                  bool verbose) const {
  return absl::StrCat("InArrayColumnFilterArg($", variable_.ToString(),
                      ", column_idx: ", column_idx(),
                      ", array: ", array_->DebugInternal(indent, verbose), ")");
}

InArrayColumnFilterArg::InArrayColumnFilterArg(const VariableId& variable,
                                               int column_idx,
                                               std::unique_ptr<ValueExpr> array)
    : ColumnFilterArg(column_idx),
      variable_(variable),
      array_(std::move(array)) {}

// -------------------------------------------------------
// InListColumnFilterArg
// -------------------------------------------------------

absl::StatusOr<std::unique_ptr<InListColumnFilterArg>>
InListColumnFilterArg::Create(
    const VariableId& variable, int column_idx,
    std::vector<std::unique_ptr<ValueExpr>> elements) {
  return absl::WrapUnique(
      new InListColumnFilterArg(variable, column_idx, std::move(elements)));
}

absl::Status InListColumnFilterArg::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  for (std::unique_ptr<ValueExpr>& element : elements_) {
    ZETASQL_RETURN_IF_ERROR(element->SetSchemasForEvaluation(params_schemas));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<ColumnFilter>> InListColumnFilterArg::Eval(
    absl::Span<const TupleData* const> params,
    EvaluationContext* context) const {
  std::vector<Value> elements(elements_.size());
  for (int i = 0; i < elements_.size(); ++i) {
    std::shared_ptr<TupleSlot::SharedProtoState> shared_state;
    VirtualTupleSlot result(&elements[i], &shared_state);
    absl::Status status;
    if (!elements_[i]->Eval(params, context, &result, &status)) {
      return status;
    }
  }

  std::vector<Value> values;
  values.reserve(elements.size());
  for (const Value& value : elements) {
    // Check for NULL and NaN.
    if (value.SqlEquals(value) == values::True()) {
      values.push_back(value);
    }
  }

  return absl::make_unique<ColumnFilter>(values);
}

std::string InListColumnFilterArg::DebugInternal(const std::string& indent,
                                                 bool verbose) const {
  std::vector<std::string> element_strs;
  element_strs.reserve(elements_.size());
  for (const std::unique_ptr<ValueExpr>& element : elements_) {
    element_strs.push_back(element->DebugInternal(indent, verbose));
  }
  return absl::StrCat("InListColumnFilterArg($", variable_.ToString(),
                      ", column_idx: ", column_idx(), ", elements: (",
                      absl::StrJoin(element_strs, ", "), "))");
}

InListColumnFilterArg::InListColumnFilterArg(
    const VariableId& variable, int column_idx,
    std::vector<std::unique_ptr<ValueExpr>> elements)
    : ColumnFilterArg(column_idx),
      variable_(variable),
      elements_(std::move(elements)) {}

// -------------------------------------------------------
// HalfUnboundedColumnFilterArg
// -------------------------------------------------------

absl::StatusOr<std::unique_ptr<HalfUnboundedColumnFilterArg>>
HalfUnboundedColumnFilterArg::Create(const VariableId& variable, int column_idx,
                                     Kind kind,
                                     std::unique_ptr<ValueExpr> arg) {
  return absl::WrapUnique(new HalfUnboundedColumnFilterArg(
      variable, column_idx, kind, std::move(arg)));
}

absl::Status HalfUnboundedColumnFilterArg::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return arg_->SetSchemasForEvaluation(params_schemas);
}

absl::StatusOr<std::unique_ptr<ColumnFilter>>
HalfUnboundedColumnFilterArg::Eval(absl::Span<const TupleData* const> params,
                                   EvaluationContext* context) const {
  TupleSlot arg;
  absl::Status status;
  if (!arg_->EvalSimple(params, context, &arg, &status)) {
    return status;
  }

  // Check for NULL and NaN.
  if (arg.value().SqlEquals(arg.value()) == values::True()) {
    Value lower_bound;
    Value upper_bound;
    switch (kind_) {
      case kLE:
        upper_bound = arg.value();
        break;
      case kGE:
        lower_bound = arg.value();
        break;
    }
    return absl::make_unique<ColumnFilter>(lower_bound, upper_bound);
  } else {
    // Return something that can't be matched.
    return absl::make_unique<ColumnFilter>(std::vector<Value>());
  }
}

std::string HalfUnboundedColumnFilterArg::DebugInternal(
    const std::string& indent, bool verbose) const {
  std::string comparator;
  switch (kind_) {
    case kLE:
      comparator = "<=";
      break;
    case kGE:
      comparator = ">=";
      break;
  }

  return absl::StrCat("HalfUnboundedColumnFilterArg($", variable_.ToString(),
                      ", column_idx: ", column_idx(), ", filter: ", comparator,
                      " ", arg_->DebugInternal(indent, verbose), ")");
}

HalfUnboundedColumnFilterArg::HalfUnboundedColumnFilterArg(
    const VariableId& variable, int column_idx, Kind kind,
    std::unique_ptr<ValueExpr> arg)
    : ColumnFilterArg(column_idx),
      variable_(variable),
      kind_(kind),
      arg_(std::move(arg)) {}

// -------------------------------------------------------
// EvaluatorTableScanOp
// -------------------------------------------------------

std::string EvaluatorTableScanOp::GetIteratorDebugString(
    absl::string_view table_name) {
  return absl::StrCat("EvaluatorTableTupleIterator(", table_name, ")");
}

absl::StatusOr<std::unique_ptr<EvaluatorTableScanOp>>
EvaluatorTableScanOp::Create(
    const Table* table, const std::string& alias,
    absl::Span<const int> column_idxs,
    absl::Span<const std::string> column_names,
    absl::Span<const VariableId> variables,
    std::vector<std::unique_ptr<ColumnFilterArg>> and_filters,
    std::unique_ptr<ValueExpr> read_time) {
  return absl::WrapUnique(new EvaluatorTableScanOp(
      table, alias, column_idxs, column_names, variables,
      std::move(and_filters), std::move(read_time)));
}

absl::StatusOr<std::unique_ptr<ColumnFilter>>
EvaluatorTableScanOp::IntersectColumnFilters(
    const std::vector<std::unique_ptr<ColumnFilter>>& filters) {
  // Invariant: a Value that matches all the ColumnFilters in entry.second is
  // in the range ['lower_bound', 'upper_bound'] and in 'in_set'. We
  // represent +/- infinity with invalid 'lower_bound'/'upper_bound'. We
  // represent an 'in_set' consisting of all values with absl::nullopt.
  Value lower_bound;
  Value upper_bound;
  absl::optional<absl::flat_hash_set<Value>> in_set;

  for (const std::unique_ptr<ColumnFilter>& filter : filters) {
    // Intersect 'filter' with the state we have for its kind.
    switch (filter->kind()) {
      case ColumnFilter::kRange:
        // Tighten the upper and lower bounds.
        if (!lower_bound.is_valid() ||
            (filter->lower_bound().is_valid() &&
             lower_bound.SqlLessThan(filter->lower_bound()) ==
                 values::True())) {
          lower_bound = filter->lower_bound();
        }
        if (!upper_bound.is_valid() || (filter->upper_bound().is_valid() &&
                                        filter->upper_bound().SqlLessThan(
                                            upper_bound) == values::True())) {
          upper_bound = filter->upper_bound();
        }

        // Verify that the intersection is valid.
        if (lower_bound.is_valid() && upper_bound.is_valid() &&
            upper_bound.SqlLessThan(lower_bound) == values::True()) {
          // Nothing matches.
          lower_bound = Value();
          upper_bound = Value();
          in_set = absl::flat_hash_set<Value>();
          break;
        }
        break;
      case ColumnFilter::kInList: {
        absl::flat_hash_set<Value> new_in_set(filter->in_list().begin(),
                                              filter->in_list().end());
        if (!in_set.has_value()) {
          in_set = std::move(new_in_set);
        } else {
          absl::flat_hash_set<Value> old_in_set = std::move(in_set.value());
          in_set.value().clear();

          const auto* set1 = &old_in_set;
          const auto* set2 = &new_in_set;
          if (set2->size() < set1->size()) {
            std::swap(set1, set2);
          }
          for (const Value& value : *set1) {
            if (set2->contains(value)) {
              ZETASQL_RET_CHECK(in_set.value().insert(value).second);
            }
          }
        }
        break;
      }
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unexpected ColumnFilter::Kind " << filter->kind()
                         << " in EvaluatorTableScanOp::CreateIterator()";
    }
  }

  // Take the intersection of the range represented by
  // 'lower_bound'/'upper_bound' and the elements in 'in_set'.
  if (in_set.has_value()) {
    for (auto i = in_set.value().begin(); i != in_set.value().end();) {
      auto current = i;
      ++i;

      const Value& value = *current;
      if ((lower_bound.is_valid() &&
           value.SqlLessThan(lower_bound) == values::True()) ||
          (upper_bound.is_valid() &&
           upper_bound.SqlLessThan(value) == values::True())) {
        in_set.value().erase(current);
      }
    }

    std::vector<Value> in_list(in_set.value().begin(), in_set.value().end());
    // Making the output deterministic makes the code easier to use and
    // test. Using SQL comparison handles cases where there are type differences
    // (e.g., comparing INT64 and UINT64).
    std::sort(in_list.begin(), in_list.end(),
              [](const Value& v1, const Value& v2) {
                return v1.SqlLessThan(v2) == values::True();
              });
    return absl::make_unique<ColumnFilter>(in_list);
  } else {
    return absl::make_unique<ColumnFilter>(lower_bound, upper_bound);
  }
}

absl::Status EvaluatorTableScanOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  for (std::unique_ptr<ColumnFilterArg>& filter : and_filters_) {
    ZETASQL_RETURN_IF_ERROR(filter->SetSchemasForEvaluation(params_schemas));
  }

  if (read_time_ != nullptr) {
    ZETASQL_RETURN_IF_ERROR(read_time_->SetSchemasForEvaluation(params_schemas));
  }

  return absl::OkStatus();
}

namespace {
class EvaluatorTableTupleIterator : public TupleIterator {
 public:
  EvaluatorTableTupleIterator(
      const std::string& name, std::unique_ptr<TupleSchema> schema,
      int num_extra_slots, EvaluationContext* context,
      std::unique_ptr<EvaluatorTableIterator> evaluator_table_iter)
      : name_(name),
        schema_(std::move(schema)),
        context_(context),
        evaluator_table_iter_(std::move(evaluator_table_iter)),
        current_(schema_->num_variables() + num_extra_slots) {
    context_->RegisterCancelCallback(
        [this] { return evaluator_table_iter_->Cancel(); });
  }

  EvaluatorTableTupleIterator(const EvaluatorTableTupleIterator&) = delete;
  EvaluatorTableTupleIterator& operator=(const EvaluatorTableTupleIterator&) =
      delete;

  const TupleSchema& Schema() const override { return *schema_; }

  TupleData* Next() override {
    if (!called_next_) {
      evaluator_table_iter_->SetDeadline(
          context_->GetStatementEvaluationDeadline());
      called_next_ = true;
    }
    if (!evaluator_table_iter_->NextRow()) {
      status_ = evaluator_table_iter_->Status();
      return nullptr;
    }

    if (schema_->num_variables() != evaluator_table_iter_->NumColumns()) {
      status_ = zetasql_base::InternalErrorBuilder()
                << "EvaluatorTableTupleIterator::Next() found wrong number of "
                << "columns: " << current_.num_slots() << " vs. "
                << evaluator_table_iter_->NumColumns();
      return nullptr;
    }

    for (int i = 0; i < schema_->num_variables(); ++i) {
      current_.mutable_slot(i)->SetValue(evaluator_table_iter_->GetValue(i));
    }
    return &current_;
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return EvaluatorTableScanOp::GetIteratorDebugString(name_);
  }

 private:
  const std::string name_;
  const std::unique_ptr<TupleSchema> schema_;
  EvaluationContext* context_;
  bool called_next_ = false;
  std::unique_ptr<EvaluatorTableIterator> evaluator_table_iter_;
  TupleData current_;
  absl::Status status_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>>
EvaluatorTableScanOp::CreateIterator(absl::Span<const TupleData* const> params,
                                     int num_extra_slots,
                                     EvaluationContext* context) const {
  absl::optional<absl::Time> read_time;
  if (read_time_ != nullptr) {
    std::shared_ptr<TupleSlot::SharedProtoState> shared_state;
    Value time_value;
    VirtualTupleSlot result(&time_value, &shared_state);
    absl::Status status;
    if (!read_time_->Eval(params, context, &result, &status)) {
      return status;
    }

    // The resolver should have already verified that the FOR SYSTEM TIME AS OF
    // expression is a timestamp.
    ZETASQL_RET_CHECK(time_value.type()->IsTimestamp());
    read_time = time_value.ToTime();
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<EvaluatorTableIterator> evaluator_table_iter,
                   table_->CreateEvaluatorTableIterator(column_idxs_));
  if (read_time.has_value()) {
    ZETASQL_RETURN_IF_ERROR(evaluator_table_iter->SetReadTime(read_time.value()));
  }

  absl::flat_hash_map<int, std::vector<std::unique_ptr<ColumnFilter>>>
      filter_list_map;
  for (const std::unique_ptr<ColumnFilterArg>& arg : and_filters_) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ColumnFilter> filter,
                     arg->Eval(params, context));
    filter_list_map[arg->column_idx()].push_back(std::move(filter));
  }

  absl::flat_hash_map<int, std::unique_ptr<ColumnFilter>> filter_map;
  for (const auto& entry : filter_list_map) {
    const int column_idx = entry.first;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ColumnFilter> filter,
                     IntersectColumnFilters(entry.second));
    ZETASQL_RET_CHECK(filter_map.emplace(column_idx, std::move(filter)).second);
  }

  ZETASQL_RETURN_IF_ERROR(
      evaluator_table_iter->SetColumnFilterMap(std::move(filter_map)));

  std::unique_ptr<TupleIterator> tuple_iter =
      absl::make_unique<EvaluatorTableTupleIterator>(
          table_->Name(), CreateOutputSchema(), num_extra_slots, context,
          std::move(evaluator_table_iter));
  return MaybeReorder(std::move(tuple_iter), context);
}

std::unique_ptr<TupleSchema> EvaluatorTableScanOp::CreateOutputSchema() const {
  return absl::make_unique<TupleSchema>(variables_);
}

std::string EvaluatorTableScanOp::IteratorDebugString() const {
  return GetIteratorDebugString(table_->Name());
}

std::string EvaluatorTableScanOp::DebugInternal(const std::string& indent,
                                                bool verbose) const {
  const std::string indent_input = absl::StrCat(indent, kIndentFork);

  std::vector<std::string> column_strings;
  ZETASQL_CHECK_EQ(column_names_.size(), column_idxs_.size());
  column_strings.reserve(column_names_.size());
  for (int i = 0; i < column_names_.size(); ++i) {
    column_strings.push_back(
        absl::StrCat(column_names_[i], "#", column_idxs_[i]));
  }

  std::vector<std::string> filter_strings;
  filter_strings.reserve(and_filters_.size());
  for (const std::unique_ptr<ColumnFilterArg>& filter : and_filters_) {
    filter_strings.push_back(filter->DebugInternal(indent_input, verbose));
  }

  return absl::StrCat(
      "EvaluatorTableScanOp(", column_names_.empty() ? "" : indent_input,
      absl::StrJoin(column_strings, indent_input),
      filter_strings.empty() ? "" : indent_input,
      absl::StrJoin(filter_strings, indent_input), indent_input,
      "table: ", table_->Name(),
      alias_.empty() ? "" : absl::StrCat(indent_input, "alias: ", alias_), ")");
}

EvaluatorTableScanOp::EvaluatorTableScanOp(
    const Table* table, const std::string& alias,
    absl::Span<const int> column_idxs,
    absl::Span<const std::string> column_names,
    absl::Span<const VariableId> variables,
    std::vector<std::unique_ptr<ColumnFilterArg>> and_filters,
    std::unique_ptr<ValueExpr> read_time)
    : table_(table),
      alias_(alias),
      column_idxs_(column_idxs.begin(), column_idxs.end()),
      column_names_(column_names.begin(), column_names.end()),
      variables_(variables.begin(), variables.end()),
      and_filters_(std::move(and_filters)),
      read_time_(std::move(read_time)) {}

// -------------------------------------------------------
// LetOp
// -------------------------------------------------------

std::string LetOp::GetIteratorDebugString(
    absl::string_view input_debug_string) {
  return absl::StrCat("LetOpTupleIterator(", input_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<LetOp>> LetOp::Create(
    std::vector<std::unique_ptr<ExprArg>> assign,
    std::vector<std::unique_ptr<CppValueArg>> cpp_assign,
    std::unique_ptr<RelationalOp> body) {
  return absl::WrapUnique(
      new LetOp(std::move(assign), std::move(cpp_assign), std::move(body)));
}

absl::Status LetOp::SetSchemasForEvaluation(
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

namespace {

// Class that owns the lifetime of a collection of C++ variable values in the
// EvaluationContext. The Removes variable mappings in the destructor.
class CppValueHolder {
 public:
  explicit CppValueHolder(EvaluationContext* context) : context_(context) {}

  // This class is not copyable or moveable
  CppValueHolder(const CppValueHolder&) = delete;
  CppValueHolder(CppValueHolder&&) = delete;
  CppValueHolder& operator=(const CppValueHolder&) = delete;
  CppValueHolder& operator=(CppValueHolder&&) = delete;

  ~CppValueHolder() {
    for (const VariableId& var : variables_) {
      context_->ClearCppValue(var);
    }
  }

  // Registers a VariableId->CppValue mapping with the EvaluationContext,
  // which will be removed in the CppValueHolder objects's destructor.
  absl::Status AddVariable(VariableId variable,
                           std::unique_ptr<CppValueBase> value) {
    ZETASQL_RET_CHECK(context_->SetCppValueIfNotPresent(variable, std::move(value)))
        << "Variable " << variable << " already holds a C++ value";
    variables_.push_back(variable);
    return absl::OkStatus();
  }

 private:
  EvaluationContext* context_;
  std::vector<VariableId> variables_;
};

// Wrapper that owns 'params' while 'iter' uses them.
class LetOpTupleIterator : public TupleIterator {
 public:
  // 'deque' tracks the memory required by 'params'. There is no harm in having
  // copied the Values because the big ones are internally reference counted.
  LetOpTupleIterator(std::unique_ptr<TupleDataDeque> deque,
                     absl::Span<const std::shared_ptr<const TupleData>> params,
                     std::unique_ptr<TupleIterator> iter,
                     std::unique_ptr<CppValueHolder> cpp_values)
      : deque_(std::move(deque)),
        params_(params.begin(), params.end()),
        iter_(std::move(iter)),
        cpp_values_(std::move(cpp_values)) {}

  LetOpTupleIterator(const LetOpTupleIterator&) = delete;
  LetOpTupleIterator& operator=(const LetOpTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return iter_->Schema(); }

  TupleData* Next() override { return iter_->Next(); }

  absl::Status Status() const override { return iter_->Status(); }

  bool PreservesOrder() const override { return iter_->PreservesOrder(); }

  absl::Status DisableReordering() override {
    return iter_->DisableReordering();
  }

  std::string DebugString() const override {
    return LetOp::GetIteratorDebugString(iter_->DebugString());
  }

 private:
  const std::unique_ptr<TupleDataDeque> deque_;
  const std::vector<std::shared_ptr<const TupleData>> params_;
  std::unique_ptr<TupleIterator> iter_;
  const std::unique_ptr<CppValueHolder> cpp_values_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> LetOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  // Initialize 'all_params' with 'params', then extend 'all_params' with new
  // TupleDatas owned by 'new_params'. We use a TupleDeque in case one of the
  // parameters represents multiple rows (e.g., an array corresponding to a WITH
  // table).
  auto new_params =
      absl::make_unique<TupleDataDeque>(context->memory_accountant());

  std::vector<const TupleData*> all_params;
  all_params.reserve(params.size() + assign().size());
  all_params.insert(all_params.end(), params.begin(), params.end());

  absl::Status status;
  for (const ExprArg* a : assign()) {
    auto new_data = absl::make_unique<TupleData>(/*num_slots=*/1);
    if (!a->value_expr()->EvalSimple(all_params, context,
                                     new_data->mutable_slot(0), &status)) {
      return status;
    }

    all_params.push_back(new_data.get());
    if (!new_params->PushBack(std::move(new_data), &status)) {
      return status;
    }
  }

  auto cpp_values = absl::make_unique<CppValueHolder>(context);
  for (const CppValueArg* a : cpp_assign()) {
    ZETASQL_RETURN_IF_ERROR(
        cpp_values->AddVariable(a->variable(), a->CreateValue(context)));
  }

  std::vector<std::shared_ptr<const TupleData>> all_params_copies =
      DeepCopyTupleDatas(all_params);
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> iter,
                   body()->CreateIterator(StripSharedPtrs(all_params_copies),
                                          num_extra_slots, context));
  iter = absl::make_unique<LetOpTupleIterator>(
      std::move(new_params), all_params_copies, std::move(iter),
      std::move(cpp_values));
  return iter;
}

std::unique_ptr<TupleSchema> LetOp::CreateOutputSchema() const {
  return body()->CreateOutputSchema();
}

std::string LetOp::IteratorDebugString() const {
  return GetIteratorDebugString(body()->IteratorDebugString());
}

std::string LetOp::DebugInternal(const std::string& indent,
                                 bool verbose) const {
  return absl::StrCat("LetOp(",
                      ArgDebugString({"assign", "cpp_assign", "body"},
                                     {kN, kN, k1}, indent, verbose),
                      ")");
}

LetOp::LetOp(std::vector<std::unique_ptr<ExprArg>> assign,
             std::vector<std::unique_ptr<CppValueArg>> cpp_assign,
             std::unique_ptr<RelationalOp> body) {
  SetArgs<ExprArg>(kAssign, std::move(assign));
  SetArgs<CppValueArg>(kCppAssign, std::move(cpp_assign));
  SetArg(kBody, absl::make_unique<RelationalArg>(std::move(body)));
}

absl::Span<const ExprArg* const> LetOp::assign() const {
  return GetArgs<ExprArg>(kAssign);
}

absl::Span<ExprArg* const> LetOp::mutable_assign() {
  return GetMutableArgs<ExprArg>(kAssign);
}

absl::Span<const CppValueArg* const> LetOp::cpp_assign() const {
  return GetArgs<CppValueArg>(kCppAssign);
}

const RelationalOp* LetOp::body() const {
  return GetArg(kBody)->node()->AsRelationalOp();
}

RelationalOp* LetOp::mutable_body() {
  return GetMutableArg(kBody)->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// SortOp
// -------------------------------------------------------

std::string SortOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("SortTupleIterator(", input_iter_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<SortOp>> SortOp::Create(
    std::vector<std::unique_ptr<KeyArg>> keys,
    std::vector<std::unique_ptr<ExprArg>> values,
    std::unique_ptr<ValueExpr> limit, std::unique_ptr<ValueExpr> offset,
    std::unique_ptr<RelationalOp> input, bool is_order_preserving,
    bool is_stable_sort) {
  ZETASQL_RET_CHECK_EQ(limit == nullptr, offset == nullptr);
  if (is_stable_sort) {
    ZETASQL_RET_CHECK(limit == nullptr);
    ZETASQL_RET_CHECK(is_order_preserving);
  }
  ZETASQL_RET_CHECK(!is_stable_sort || is_order_preserving);
  // Don't check whether the key type supports ordering here. Do that in the
  // algebrizer instead. For example, the algebrize doesn't allow ORDER BY
  // <STRUCT>, but it does support PARTITION BY <STRUCT> in an analytic function
  // call, and that is implemented doing a sort by struct.
  auto op = absl::WrapUnique(new SortOp(std::move(keys), std::move(values),
                                        std::move(limit), std::move(offset),
                                        std::move(input), is_stable_sort));
  ZETASQL_RETURN_IF_ERROR(op->set_is_order_preserving(is_order_preserving));
  return op;
}

absl::Status SortOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  if (has_limit_) {
    ZETASQL_RETURN_IF_ERROR(mutable_limit()->SetSchemasForEvaluation(params_schemas));
  }
  if (has_offset_) {
    ZETASQL_RETURN_IF_ERROR(mutable_offset()->SetSchemasForEvaluation(params_schemas));
  }

  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));

  const std::unique_ptr<const TupleSchema> input_schema =
      input()->CreateOutputSchema();
  for (KeyArg* key : mutable_keys()) {
    ZETASQL_RETURN_IF_ERROR(key->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {input_schema.get()})));

    ValueExpr* collation = key->mutable_collation();
    if (collation != nullptr) {
      ZETASQL_RETURN_IF_ERROR(collation->SetSchemasForEvaluation(params_schemas));
    }
  }
  for (ExprArg* value : mutable_values()) {
    ZETASQL_RETURN_IF_ERROR(value->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {input_schema.get()})));
  }
  return absl::OkStatus();
}

namespace {
// Takes a list of tuples sorted by 'comparator'. If DisableReordering() is
// called before Next(), returns them in order. Otherwise, scrambles the order
// of tuples that are equal with respect to 'comparator'.
class SortTupleIterator : public TupleIterator {
 public:
  SortTupleIterator(std::unique_ptr<TupleIterator> input_iter_for_debug_string,
                    std::unique_ptr<const TupleSchema> schema,
                    std::unique_ptr<TupleComparator> comparator,
                    std::unique_ptr<TupleDataDeque> tuples,
                    EvaluationContext* context)
      : input_iter_for_debug_string_(std::move(input_iter_for_debug_string)),
        schema_(std::move(schema)),
        comparator_(std::move(comparator)),
        tuples_(std::move(tuples)),
        context_(context) {}

  SortTupleIterator(const SortTupleIterator&) = delete;
  SortTupleIterator& operator=(const SortTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return *schema_; }

  TupleData* Next() override {
    if (num_next_calls_ %
            absl::GetFlag(
                FLAGS_zetasql_call_verify_not_aborted_rows_period) ==
        0) {
      status_ = context_->VerifyNotAborted();
      if (!status_.ok()) {
        return nullptr;
      }
    }
    if (enable_reordering_ && num_next_calls_ == 0) {
      status_ = ReorderTuplesWithSameKey();
      if (!status_.ok()) {
        return nullptr;
      }
    }
    ++num_next_calls_;

    if (tuples_->IsEmpty()) return nullptr;

    current_ = tuples_->PopFront();
    return current_.get();
  }

  absl::Status Status() const override { return status_; }

  bool PreservesOrder() const override { return !enable_reordering_; }

  absl::Status DisableReordering() override {
    ZETASQL_RET_CHECK_EQ(num_next_calls_, 0)
        << "DisableReordering() cannot be called after Next()";
    enable_reordering_ = false;
    return absl::OkStatus();
  }

  std::string DebugString() const override {
    return SortOp::GetIteratorDebugString(
        input_iter_for_debug_string_->DebugString());
  }

 private:
  // Iterates over 'tuples_' and scrambles the order of tuples with the same
  // key.
  absl::Status ReorderTuplesWithSameKey() {
    // Scramble the sorted order.
    std::vector<std::unique_ptr<TupleData>> tuples;
    tuples.reserve(tuples_->GetSize());
    while (!tuples_->IsEmpty()) {
      tuples.push_back(tuples_->PopFront());
    }
    std::vector<int> scrambled_idxs;
    scrambled_idxs.reserve(tuples.size());
    for (int start_idx = 0; start_idx < tuples.size();) {
      const TupleData& start_tuple = *tuples[start_idx];
      int equal_length = 1;
      while (start_idx + equal_length < tuples.size()) {
        const int tuple_idx = start_idx + equal_length;
        const TupleData& tuple = *tuples[tuple_idx];
        const bool start_equals_tuple = !(*comparator_)(start_tuple, tuple) &&
                                        !(*comparator_)(tuple, start_tuple);
        if (!start_equals_tuple) {
          break;
        }
        ++equal_length;
      }
      // This is similar shuffling logic to ReorderingTupleIterator. It is
      // needed for backwards compatibility with the text-based reference
      // implementation compliance tests.
      for (int range_idx = 0; range_idx < equal_length; ++range_idx) {
        // Iterates over odd indexes, then even indexes. Example for 5 tuples:
        // 0 -> 1  // [0 .. size/2) is mapped to odd indexes
        // 1 -> 3
        // 2 -> 0  // [size/2 .. size) is mapped to even indexes
        // 3 -> 2
        // 4 -> 4
        const int half_size = equal_length / 2;
        const int scrambled_range_idx = range_idx < half_size
                                            ? (range_idx * 2 + 1)
                                            : 2 * (range_idx - half_size);
        scrambled_idxs.push_back(start_idx + scrambled_range_idx);
      }
      start_idx += equal_length;
    }

    ZETASQL_RET_CHECK(tuples_->IsEmpty());
    absl::Status status;
    for (int idx : scrambled_idxs) {
      if (!tuples_->PushBack(std::move(tuples[idx]), &status)) {
        return status;
      }
    }
    return absl::OkStatus();
  }

  // We store a TupleIterator instead of the debug string to avoid having to
  // compute the debug string unnecessarily.
  const std::unique_ptr<TupleIterator> input_iter_for_debug_string_;
  const std::unique_ptr<const TupleSchema> schema_;
  const std::unique_ptr<TupleComparator> comparator_;
  std::unique_ptr<TupleDataDeque> tuples_;
  int64_t num_next_calls_ = 0;
  std::unique_ptr<TupleData> current_;
  EvaluationContext* context_;
  bool enable_reordering_ = true;
  absl::Status status_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> SortOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  Value limit_value;   // Invalid if no limit set.
  Value offset_value;  // Always valid, but meaningless if no limit set.

  if (has_limit()) {
    TupleSlot slot;
    absl::Status status;
    if (!limit()->EvalSimple(params, context, &slot, &status)) {
      return status;
    }
    limit_value = std::move(*slot.mutable_value());
  }

  if (has_offset()) {
    TupleSlot slot;
    absl::Status status;
    if (!offset()->EvalSimple(params, context, &slot, &status)) {
      return status;
    }
    offset_value = std::move(*slot.mutable_value());
  } else {
    offset_value = Value::Int64(0);
  }

  struct LimitOffset {
    LimitOffset(int64_t limit_in, int64_t offset_in)
        : limit(limit_in), offset(offset_in) {}

    int64_t limit;
    int64_t offset;
  };
  absl::optional<LimitOffset> limit_offset;
  if (limit_value.is_valid()) {
    if (limit_value.is_null() || offset_value.is_null()) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "Limit requires non-null count and offset";
    }
    if (limit_value.int64_value() < 0 || offset_value.int64_value() < 0) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "Limit requires non-negative count and offset";
    }
    limit_offset =
        LimitOffset(limit_value.int64_value(), offset_value.int64_value());
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleIterator> input_iter,
      input()->CreateIterator(params, /*num_extra_slots=*/0, context));

  std::vector<int> slots_for_keys;
  slots_for_keys.reserve(keys().size());
  for (int i = 0; i < keys().size(); ++i) {
    slots_for_keys.push_back(i);
  }
  std::vector<int> slots_for_values;
  slots_for_values.reserve(values().size());
  for (int i = 0; i < values().size(); ++i) {
    slots_for_values.push_back(keys().size() + i);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleComparator> comparator,
      TupleComparator::Create(keys(), slots_for_keys, params, context));

  // If 'limit_offset' is set, 'top_n_outputs' contains the top
  // 'limit_offset.limit + limit_offset.offset' rows. Otherwise, 'outputs'
  // contains all the rows.
  auto top_n_outputs = absl::make_unique<TupleDataOrderedQueue>(
      *comparator, context->memory_accountant());
  auto outputs =
      absl::make_unique<TupleDataDeque>(context->memory_accountant());
  absl::Status status;
  while (true) {
    const TupleData* next_input = input_iter->Next();
    if (next_input == nullptr) {
      ZETASQL_RETURN_IF_ERROR(input_iter->Status());
      break;
    }

    const std::vector<const TupleData*> params_and_input_tuple =
        ConcatSpans(params, {next_input});

    auto next_output = absl::make_unique<TupleData>(
        keys().size() + values().size() + num_extra_slots);
    for (int i = 0; i < keys().size(); ++i) {
      TupleSlot* slot = next_output->mutable_slot(i);
      if (!keys()[i]->value_expr()->EvalSimple(params_and_input_tuple, context,
                                               slot, &status)) {
        return status;
      }
    }
    for (int i = 0; i < values().size(); ++i) {
      TupleSlot* slot = next_output->mutable_slot(keys().size() + i);
      if (!values()[i]->value_expr()->EvalSimple(params_and_input_tuple,
                                                 context, slot, &status)) {
        return status;
      }
    }

    if (limit_offset.has_value()) {
      if (!top_n_outputs->Insert(std::move(next_output), &status)) {
        return status;
      }
      if (top_n_outputs->GetSize() - limit_offset->limit >
          limit_offset->offset) {
        top_n_outputs->PopBack();
      }
    } else {
      if (!outputs->PushBack(std::move(next_output), &status)) {
        return status;
      }
    }
  }

  // If there is a limit set, drop the first 'offset' entries from
  // 'top_n_outputs' and dump the rest into 'outputs'.
  bool is_uniquely_ordered;
  if (limit_offset.has_value()) {
    ZETASQL_RET_CHECK(outputs->IsEmpty());
    for (int i = 0; i < limit_offset->offset && !top_n_outputs->IsEmpty();
         ++i) {
      top_n_outputs->PopFront();
    }
    while (!top_n_outputs->IsEmpty()) {
      if (!outputs->PushBack(top_n_outputs->PopFront(), &status)) {
        return status;
      }
    }
    // This is safe because 'limit_offset' is only set as an optimization, and
    // is not set for compliance or random query tests. If that changes, this
    // will cause spurious test failures due to asserting that things are in an
    // order that is not actually required. This is better than silently
    // ignoring failures.
    is_uniquely_ordered = true;
  } else {
    ZETASQL_RET_CHECK(top_n_outputs->IsEmpty());
    outputs->Sort(*comparator,
                  context->options().always_use_stable_sort || is_stable_sort_);
    const std::vector<const TupleData*> output_ptrs = outputs->GetTuplePtrs();
    is_uniquely_ordered =
        comparator->IsUniquelyOrdered(output_ptrs, slots_for_values);
  }
  // We are done with 'top_n_outputs'. Deallocate it and crash if we ever
  // try to access it again.
  top_n_outputs.reset();

  std::unique_ptr<TupleIterator> iter = absl::make_unique<SortTupleIterator>(
      std::move(input_iter), CreateOutputSchema(), std::move(comparator),
      std::move(outputs), context);
  const bool scramble_undefined_orderings =
      context->options().scramble_undefined_orderings;
  if (!scramble_undefined_orderings || is_uniquely_ordered || is_stable_sort_) {
    // Disable SortOpTupleIterator's scrambling of tuples with the same order by
    // key. If 'is_uniquely_ordered' is true, this reordering wouldn't affect
    // the order of tuples returned anyway, but explicitly turning off the
    // reordering signifies to consumers of the iterator that it preserves
    // order.
    ZETASQL_RETURN_IF_ERROR(iter->DisableReordering());
  }

  if (scramble_undefined_orderings && !is_order_preserving()) {
    // This can happen for an order by operator that does not guarantee ordered
    // output, such as at the top level of a subquery with an order by. In this
    // case, for backwards compatibility, we disable the scrambling of tuples
    // with equal keys in SortOpTupleIterator and just wrap the entire iterator
    // in a ReorderingTupleIterator.
    ZETASQL_RETURN_IF_ERROR(iter->DisableReordering());
    iter = absl::make_unique<ReorderingTupleIterator>(std::move(iter));
  }

  return iter;
}

std::unique_ptr<TupleSchema> SortOp::CreateOutputSchema() const {
  std::vector<VariableId> vars;
  vars.reserve(keys().size() + values().size());
  for (const KeyArg* key : keys()) {
    vars.push_back(key->variable());
  }
  for (const ExprArg* value : values()) {
    vars.push_back(value->variable());
  }
  return absl::make_unique<TupleSchema>(vars);
}

std::string SortOp::IteratorDebugString() const {
  return GetIteratorDebugString(input()->IteratorDebugString());
}

std::string SortOp::DebugInternal(const std::string& indent,
                                  bool verbose) const {
  return absl::StrCat(
      "SortOp(", is_order_preserving() ? "ordered" : "unordered",
      ArgDebugString(
          {"keys", "values", "limit", "offset", "input"},
          {kN, kN, has_limit() ? k1 : k0, has_offset() ? k1 : k0, k1}, indent,
          verbose),
      ")");
}

SortOp::SortOp(std::vector<std::unique_ptr<KeyArg>> keys,
               std::vector<std::unique_ptr<ExprArg>> values,
               std::unique_ptr<ValueExpr> limit,
               std::unique_ptr<ValueExpr> offset,
               std::unique_ptr<RelationalOp> input, bool is_stable_sort)
    : has_limit_(limit != nullptr),
      has_offset_(offset != nullptr),
      is_stable_sort_(is_stable_sort) {
  SetArgs<KeyArg>(kKey, std::move(keys));
  SetArgs<ExprArg>(kValue, std::move(values));
  if (has_limit_) {
    SetArg(kLimit, absl::make_unique<ExprArg>(std::move(limit)));
  } else {
    SetArgs(kLimit, std::vector<std::unique_ptr<ExprArg>>{});
  }
  if (has_offset_) {
    SetArg(kOffset, absl::make_unique<ExprArg>(std::move(offset)));
  } else {
    SetArgs(kOffset, std::vector<std::unique_ptr<ExprArg>>{});
  }
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
}

absl::Span<const KeyArg* const> SortOp::keys() const {
  return GetArgs<KeyArg>(kKey);
}

absl::Span<KeyArg* const> SortOp::mutable_keys() {
  return GetMutableArgs<KeyArg>(kKey);
}

absl::Span<const ExprArg* const> SortOp::values() const {
  return GetArgs<ExprArg>(kValue);
}

absl::Span<ExprArg* const> SortOp::mutable_values() {
  return GetMutableArgs<ExprArg>(kValue);
}

const ValueExpr* SortOp::limit() const {
  return GetArg(kLimit)->node()->AsValueExpr();
}

ValueExpr* SortOp::mutable_limit() {
  return GetMutableArg(kLimit)->mutable_node()->AsMutableValueExpr();
}

const ValueExpr* SortOp::offset() const {
  return GetArg(kOffset)->node()->AsValueExpr();
}

ValueExpr* SortOp::mutable_offset() {
  return GetMutableArg(kOffset)->mutable_node()->AsMutableValueExpr();
}

const RelationalOp* SortOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* SortOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// ComputeOp
// -------------------------------------------------------

std::string ComputeOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("ComputeTupleIterator(", input_iter_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<ComputeOp>> ComputeOp::Create(
    std::vector<std::unique_ptr<ExprArg>> map,
    std::unique_ptr<RelationalOp> input) {
  return absl::WrapUnique(new ComputeOp(std::move(map), std::move(input)));
}

absl::Status ComputeOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));
  const std::unique_ptr<const TupleSchema> input_schema =
      input()->CreateOutputSchema();

  // map()[i] is evaluated with tuples corresponding to 'params_schemas', plus
  // one more tuple correspoding to the variables from 'input_schema' and
  // map()[0],...,map[i - 1].
  std::vector<VariableId> vars = input_schema->variables();
  vars.reserve(map().size());
  for (ExprArg* arg : mutable_map()) {
    auto new_schema = absl::make_unique<const TupleSchema>(vars);
    ZETASQL_RETURN_IF_ERROR(arg->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {new_schema.get()})));
    vars.push_back(arg->variable());
  }

  return absl::OkStatus();
}

namespace {
// Iterator corresponding to a ComputeOp. To return a tuple, it reads a tuple
// from an underlying iterator and augments it with the result of evaluating a
// list of ExprArgs. Each ExprArg is allowed to depend on the variables from the
// previous ExprArgs, the tuple from the underlying iterator, and some
// parameters passed into the constructor.
class ComputeTupleIterator : public TupleIterator {
 public:
  ComputeTupleIterator(absl::Span<const TupleData* const> params,
                       absl::Span<const ExprArg* const> expr_args,
                       std::unique_ptr<TupleIterator> iter,
                       std::unique_ptr<TupleSchema> output_schema,
                       EvaluationContext* context)
      : expr_args_(expr_args.begin(), expr_args.end()),
        params_(params.begin(), params.end()),
        iter_(std::move(iter)),
        output_schema_(std::move(output_schema)),
        context_(context) {}

  ComputeTupleIterator(const ComputeTupleIterator&) = delete;
  ComputeTupleIterator& operator=(const ComputeTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return *output_schema_; }

  TupleData* Next() override {
    TupleData* current = iter_->Next();
    if (current == nullptr) {
      status_ = iter_->Status();
      return nullptr;
    }

    if (current->num_slots() < Schema().num_variables()) {
      status_ = zetasql_base::InternalErrorBuilder()
                << "ComputeTupleIterator::Next() found " << current->num_slots()
                << " slots but expected at least " << Schema().num_variables();
      return nullptr;
    }

    for (int i = 0; i < expr_args_.size(); ++i) {
      TupleSlot* slot =
          current->mutable_slot(iter_->Schema().num_variables() + i);
      absl::Status status;
      if (!expr_args_[i]->value_expr()->EvalSimple(
              ConcatSpans(absl::Span<const TupleData* const>(params_),
                          {current}),
              context_, slot, &status)) {
        status_ = status;
        return nullptr;
      }
    }

    return current;
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return ComputeOp::GetIteratorDebugString(iter_->DebugString());
  }

 private:
  const std::vector<const ExprArg*> expr_args_;
  const std::vector<const TupleData*> params_;

  std::unique_ptr<TupleIterator> iter_;
  std::unique_ptr<TupleSchema> output_schema_;
  absl::Status status_;
  EvaluationContext* context_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> ComputeOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleIterator> iter,
      input()->CreateIterator(params, num_extra_slots + map().size(), context));
  iter = absl::make_unique<ComputeTupleIterator>(params, map(), std::move(iter),
                                                 CreateOutputSchema(), context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> ComputeOp::CreateOutputSchema() const {
  std::unique_ptr<TupleSchema> input_schema = input()->CreateOutputSchema();
  std::vector<VariableId> variables = input_schema->variables();
  variables.reserve(variables.size() + map().size());
  for (const ExprArg* arg : map()) {
    variables.push_back(arg->variable());
  }
  return absl::make_unique<TupleSchema>(variables);
}

std::string ComputeOp::IteratorDebugString() const {
  return GetIteratorDebugString(input()->IteratorDebugString());
}

std::string ComputeOp::DebugInternal(const std::string& indent,
                                     bool verbose) const {
  return absl::StrCat(
      "ComputeOp(", ArgDebugString({"map", "input"}, {kN, k1}, indent, verbose),
      ")");
}

ComputeOp::ComputeOp(std::vector<std::unique_ptr<ExprArg>> map,
                     std::unique_ptr<RelationalOp> input) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
  SetArgs<ExprArg>(kMap, std::move(map));
}

absl::Span<const ExprArg* const> ComputeOp::map() const {
  return GetArgs<ExprArg>(kMap);
}

absl::Span<ExprArg* const> ComputeOp::mutable_map() {
  return GetMutableArgs<ExprArg>(kMap);
}

const RelationalOp* ComputeOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* ComputeOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// FilterOp
// -------------------------------------------------------

std::string FilterOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("FilterTupleIterator(", input_iter_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<FilterOp>> FilterOp::Create(
    std::unique_ptr<ValueExpr> predicate, std::unique_ptr<RelationalOp> input) {
  return absl::WrapUnique(new FilterOp(std::move(predicate), std::move(input)));
}

absl::Status FilterOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));

  const std::unique_ptr<const TupleSchema> input_schema =
      input()->CreateOutputSchema();
  return mutable_predicate()->SetSchemasForEvaluation(
      ConcatSpans(params_schemas, {input_schema.get()}));
}

namespace {
// Filters out all tuples from an underlying iterator that don't match a
// predicate.
class FilterTupleIterator : public TupleIterator {
 public:
  FilterTupleIterator(absl::Span<const TupleData* const> params,
                      const ValueExpr* predicate,
                      std::unique_ptr<TupleIterator> iter,
                      EvaluationContext* context)
      : predicate_(predicate),
        params_(params.begin(), params.end()),
        iter_(std::move(iter)),
        context_(context) {}

  FilterTupleIterator(const FilterTupleIterator&) = delete;
  FilterTupleIterator& operator=(const FilterTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return iter_->Schema(); }

  TupleData* Next() override {
    while (true) {
      TupleData* current = iter_->Next();
      if (current == nullptr) {
        status_ = iter_->Status();
        return nullptr;
      }

      TupleSlot slot;
      absl::Status status;
      if (!predicate_->EvalSimple(
              ConcatSpans(absl::Span<const TupleData* const>(params_),
                          {current}),
              context_, &slot, &status)) {
        status_ = status;
        return nullptr;
      }
      if (slot.value() == Bool(true)) {
        return current;
      }
    }
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return FilterOp::GetIteratorDebugString(iter_->DebugString());
  }

 private:
  const ValueExpr* predicate_;
  const std::vector<const TupleData*> params_;
  std::unique_ptr<TupleIterator> iter_;
  absl::Status status_;
  EvaluationContext* context_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> FilterOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> iter,
                   input()->CreateIterator(params, num_extra_slots, context));
  iter = absl::make_unique<FilterTupleIterator>(params, predicate(),
                                                std::move(iter), context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> FilterOp::CreateOutputSchema() const {
  return input()->CreateOutputSchema();
}

std::string FilterOp::IteratorDebugString() const {
  return GetIteratorDebugString(input()->IteratorDebugString());
}

std::string FilterOp::DebugInternal(const std::string& indent,
                                    bool verbose) const {
  return absl::StrCat(
      "FilterOp(",
      ArgDebugString({"condition", "input"}, {k1, k1}, indent, verbose), ")");
}

FilterOp::FilterOp(std::unique_ptr<ValueExpr> predicate,
                   std::unique_ptr<RelationalOp> input) {
  SetArg(kPredicate, absl::make_unique<ExprArg>(std::move(predicate)));
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
}

const ValueExpr* FilterOp::predicate() const {
  return GetArg(kPredicate)->node()->AsValueExpr();
}

ValueExpr* FilterOp::mutable_predicate() {
  return GetMutableArg(kPredicate)->mutable_node()->AsMutableValueExpr();
}

const RelationalOp* FilterOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* FilterOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// LimitOp
// -------------------------------------------------------

std::string LimitOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("LimitTupleIterator(", input_iter_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<LimitOp>> LimitOp::Create(
    std::unique_ptr<ValueExpr> row_count, std::unique_ptr<ValueExpr> offset,
    std::unique_ptr<RelationalOp> input, bool is_order_preserving) {
  ZETASQL_RET_CHECK(row_count->output_type()->IsInt64());
  ZETASQL_RET_CHECK(offset->output_type()->IsInt64());

  auto op = absl::WrapUnique(
      new LimitOp(std::move(row_count), std::move(offset), std::move(input)));
  ZETASQL_RETURN_IF_ERROR(op->set_is_order_preserving(is_order_preserving));
  return op;
}

absl::Status LimitOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_row_count()->SetSchemasForEvaluation(params_schemas));
  ZETASQL_RETURN_IF_ERROR(mutable_offset()->SetSchemasForEvaluation(params_schemas));
  return mutable_input()->SetSchemasForEvaluation(params_schemas);
}

namespace {
// Skips the first 'offset' tuples from 'iter' and then returns the next 'count'
// tuples.
class LimitTupleIterator : public TupleIterator {
 public:
  LimitTupleIterator(int64_t count, int64_t offset, EvaluationContext* context,
                     std::unique_ptr<TupleIterator> iter)
      : count_(count),
        offset_(offset),
        context_(context),
        iter_(std::move(iter)) {}

  LimitTupleIterator(const LimitTupleIterator&) = delete;
  LimitTupleIterator& operator=(const LimitTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return iter_->Schema(); }

  TupleData* Next() override {
    // Skip the first 'offset_' tuples from 'iter_'.
    while (next_iter_row_number_ < offset_) {
      TupleData* current = iter_->Next();
      if (current == nullptr) {
        Finish(iter_->Status());
        return nullptr;
      }
      ++next_iter_row_number_;
    }

    // Don't return more than 'count_' tuples from 'iter_'.
    if (next_iter_row_number_ >= offset_ + count_) {
      Finish(absl::nullopt);
      return nullptr;
    }

    TupleData* current = iter_->Next();
    if (current == nullptr) {
      Finish(iter_->Status());
      return nullptr;
    }
    ++next_iter_row_number_;

    return current;
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return LimitOp::GetIteratorDebugString(iter_->DebugString());
  }

 private:
  // Update 'status_' and 'context_' to indicate that the iterator is done. If
  // 'iter_' is done, 'iter_status' contains its status.
  void Finish(absl::optional<absl::Status> iter_status) {
    if (iter_status.has_value()) {
      status_ = iter_status.value();
    }
    // The ZetaSQL behavior is non-deterministic if the underlying iterator
    // does not preserve order, there is more than one input tuple, there is at
    // least one output tuple, and not every input tuple is output.
    const bool has_output = next_iter_row_number_ > offset_;
    const bool output_everything = offset_ == 0 && iter_status.has_value();
    if (!iter_->PreservesOrder() && has_output && !output_everything) {
      // Read at least two rows from 'iter_' if possible, so that we can
      // determine if the input has more than one row.
      while (next_iter_row_number_ <= 1 && !iter_status.has_value()) {
        const TupleData* current = iter_->Next();
        if (current == nullptr) {
          status_ = iter_->Status();
          if (!status_.ok()) return;
          iter_status = status_;
          break;
        }
        ++next_iter_row_number_;
      }
      if (next_iter_row_number_ >= 2) {
        context_->SetNonDeterministicOutput();
      }
    }
  }

  const int64_t count_;
  const int64_t offset_;
  EvaluationContext* context_;
  std::unique_ptr<TupleIterator> iter_;
  // The row number of the next tuple returned by iter_->Next().
  int64_t next_iter_row_number_ = 0;
  absl::Status status_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> LimitOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  TupleSlot count_slot;
  absl::Status status;
  if (!row_count()->EvalSimple(params, context, &count_slot, &status))
    return status;
  const Value& count = count_slot.value();

  TupleSlot offset_slot;
  if (!offset()->EvalSimple(params, context, &offset_slot, &status))
    return status;
  const Value& offset_value = offset_slot.value();

  if (count.is_null() || offset_value.is_null()) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "Limit requires non-null count and offset";
  }
  if (count.int64_value() < 0 || offset_value.int64_value() < 0) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "Limit requires non-negative count and offset";
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> iter,
                   input()->CreateIterator(params, num_extra_slots, context));
  const bool underlying_iter_preserves_order = iter->PreservesOrder();

  iter = absl::make_unique<LimitTupleIterator>(count.int64_value(),
                                               offset_value.int64_value(),
                                               context, std::move(iter));
  // Scramble the output if the scrambling is enabled and either the underlying
  // iterator scrambles or this operator does not preserve order.
  if (context->options().scramble_undefined_orderings &&
      !(underlying_iter_preserves_order && is_order_preserving())) {
    iter = absl::make_unique<ReorderingTupleIterator>(std::move(iter));
  }
  return iter;
}

std::unique_ptr<TupleSchema> LimitOp::CreateOutputSchema() const {
  return input()->CreateOutputSchema();
}

std::string LimitOp::IteratorDebugString() const {
  return GetIteratorDebugString(input()->IteratorDebugString());
}

std::string LimitOp::DebugInternal(const std::string& indent,
                                   bool verbose) const {
  return absl::StrCat("LimitOp(",
                      is_order_preserving() ? "ordered" : "unordered",
                      ArgDebugString({"row_count", "offset", "input"},
                                     {k1, k1, k1}, indent, verbose),
                      ")");
}

LimitOp::LimitOp(std::unique_ptr<ValueExpr> row_count,
                 std::unique_ptr<ValueExpr> offset,
                 std::unique_ptr<RelationalOp> input) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
  SetArg(kRowCount, absl::make_unique<ExprArg>(std::move(row_count)));
  SetArg(kOffset, absl::make_unique<ExprArg>(std::move(offset)));
}

const ValueExpr* LimitOp::row_count() const {
  return GetArg(kRowCount)->node()->AsValueExpr();
}

ValueExpr* LimitOp::mutable_row_count() {
  return GetMutableArg(kRowCount)->mutable_node()->AsMutableValueExpr();
}

const ValueExpr* LimitOp::offset() const {
  return GetArg(kOffset)->node()->AsValueExpr();
}

ValueExpr* LimitOp::mutable_offset() {
  return GetMutableArg(kOffset)->mutable_node()->AsMutableValueExpr();
}

const RelationalOp* LimitOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* LimitOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// SampleScanOp
// -------------------------------------------------------

std::string SampleScanOp::GetIteratorDebugString(
    absl::string_view input_iter_debug_string) {
  return absl::StrCat("SampleScanTupleIterator(", input_iter_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<SampleScanOp>> SampleScanOp::Create(
    Method method, std::unique_ptr<ValueExpr> size,
    std::unique_ptr<ValueExpr> repeatable, std::unique_ptr<RelationalOp> input,
    std::vector<std::unique_ptr<ValueExpr>> partition_key,
    const VariableId& sample_weight) {
  ZETASQL_RET_CHECK(repeatable == nullptr || repeatable->output_type()->IsInt64());

  return absl::WrapUnique(
      new SampleScanOp(std::move(size), std::move(repeatable), std::move(input),
                       method, std::move(partition_key), sample_weight));
}

absl::Status SampleScanOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_size()->SetSchemasForEvaluation(params_schemas));
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));
  if (has_repeatable()) {
    ZETASQL_RETURN_IF_ERROR(
        mutable_repeatable()->SetSchemasForEvaluation(params_schemas));
  }

  std::unique_ptr<TupleSchema> input_schema = input()->CreateOutputSchema();
  auto key_part_params = ConcatSpans(params_schemas, {input_schema.get()});
  for (ExprArg* arg : mutable_partition_key()) {
    ZETASQL_RETURN_IF_ERROR(
        arg->mutable_value_expr()->SetSchemasForEvaluation(key_part_params));
  }
  return absl::OkStatus();
}

namespace {
class SampleScanTupleIteratorBase : public TupleIterator {
 public:
  SampleScanTupleIteratorBase(absl::optional<int64_t> seed,
                              EvaluationContext* context,
                              std::unique_ptr<TupleIterator> iter,
                              std::unique_ptr<TupleSchema> schema,
                              const VariableId& weight)
      : bitgen_(MakeBitgen(seed)),
        context_(context),
        iter_(std::move(iter)),
        output_schema_(std::move(schema)),
        include_weight_(weight.is_valid()) {}

  SampleScanTupleIteratorBase(const SampleScanTupleIteratorBase&) = delete;
  SampleScanTupleIteratorBase& operator=(const SampleScanTupleIteratorBase&) =
      delete;

  const TupleSchema& Schema() const override { return *output_schema_; }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return SampleScanOp::GetIteratorDebugString(iter_->DebugString());
  }

  TupleData* Next() final {
    absl::StatusOr<TupleData*> status_or_next = NextInternal();
    if (status_or_next.ok()) {
      return status_or_next.value();
    }
    status_ = status_or_next.status();
    return nullptr;
  }

  // Derived clases implement Next with this safer interface.
  virtual absl::StatusOr<TupleData*> NextInternal() = 0;

 protected:
  // Update 'context_' to indicate that input iterator is done.
  absl::Status Finish() {
    if (!seed_.has_value() && saw_row_) {
      context_->SetNonDeterministicOutput();
    }
    return iter_->Status();
  }

  absl::Status SetWeight(double weight, TupleData* current) {
    if (current->num_slots() < Schema().num_variables()) {
      return zetasql_base::InternalErrorBuilder()
             << "ComputeTupleIterator::Next() found " << current->num_slots()
             << " slots but expected at least " << Schema().num_variables();
    }
    if (include_weight_) {
      current->mutable_slot(iter_->Schema().num_variables())
          ->SetValue(Value::Double(weight));
    }
    return absl::OkStatus();
  }

  // Build a bitgen instance, optionally with the given seed.
  absl::BitGen MakeBitgen(absl::optional<int64_t> seed) {
    if (seed.has_value()) {
      return absl::BitGen(std::seed_seq{*seed});
    }
    return absl::BitGen();
  }

  absl::BitGen bitgen_;
  // If set, bitgen_ was populated using this value.
  const absl::optional<int64_t> seed_;
  EvaluationContext* context_;
  std::unique_ptr<TupleIterator> iter_;
  std::unique_ptr<TupleSchema> output_schema_;
  // If true, add a column with the weight measure,
  const bool include_weight_;

  // If true, iter_ yielded at least one row.
  bool saw_row_ = false;

 private:
  // Output status, copied from iter_.
  absl::Status status_;
};

class BernoulliSampleScanTupleIterator : public SampleScanTupleIteratorBase {
 public:
  BernoulliSampleScanTupleIterator(double percent, absl::optional<int64_t> seed,
                                   EvaluationContext* context,
                                   std::unique_ptr<TupleIterator> iter,
                                   std::unique_ptr<TupleSchema> schema,
                                   const VariableId& weight)
      : SampleScanTupleIteratorBase(seed, context, std::move(iter),
                                    std::move(schema), weight),
        percent_(percent),
        weight_(1.0 / percent_) {}

  BernoulliSampleScanTupleIterator(const BernoulliSampleScanTupleIterator&) =
      delete;
  BernoulliSampleScanTupleIterator& operator=(
      const BernoulliSampleScanTupleIterator&) = delete;

  absl::StatusOr<TupleData*> NextInternal() override {
    // Randomly elide values
    while (!absl::Bernoulli(bitgen_, percent_)) {
      TupleData* current = iter_->Next();
      if (current == nullptr) {
        ZETASQL_RETURN_IF_ERROR(Finish());
        return nullptr;
      }
    }

    TupleData* current = iter_->Next();
    if (current == nullptr) {
      ZETASQL_RETURN_IF_ERROR(Finish());
      return nullptr;
    }
    ZETASQL_RETURN_IF_ERROR(SetWeight(weight_, current));

    saw_row_ = true;
    return current;
  }

 private:
  const double percent_;
  const double weight_;
};

class ReservoirSampleScanTupleIterator : public SampleScanTupleIteratorBase {
 public:
  ReservoirSampleScanTupleIterator(
      int64_t reservoir_size, absl::optional<int64_t> seed,
      EvaluationContext* context, absl::Span<const TupleData* const> params,
      std::unique_ptr<TupleIterator> iter, std::unique_ptr<TupleSchema> schema,
      absl::Span<const KeyArg* const> partition_key, const VariableId& weight)
      : SampleScanTupleIteratorBase(seed, context, std::move(iter),
                                    std::move(schema), weight),
        reservoir_size_(reservoir_size),
        params_(params),
        partition_key_(partition_key) {}

  ReservoirSampleScanTupleIterator(const ReservoirSampleScanTupleIterator&) =
      delete;
  ReservoirSampleScanTupleIterator& operator=(
      const ReservoirSampleScanTupleIterator&) = delete;

  absl::StatusOr<TupleData*> NextInternal() override {
    if (!built_reservoir_) {
      built_reservoir_ = true;
      ZETASQL_RETURN_IF_ERROR(BuildReservoirState());
    }

    if (reservoir_next_ >= reservoir_output_.size()) {
      return nullptr;
    }
    return &reservoir_output_[reservoir_next_++];
  }

 private:
  struct ScoredEntry {
    uint32_t score;
    TupleData tuple;

    bool operator<(const ScoredEntry& other) const {
      return score < other.score;
    }
  };

  struct Reservoir {
    // For a reservoir of size N, 'entries' is the top N scoring candidates seen
    // thus far.
    std::priority_queue<ScoredEntry> entries;
    int64_t num_candidates_considered;
  };

  // Consume all tuples in 'input_' and do a reservoir sample on them. The
  // output tuples are stored in 'reservoir_output_'. If 'input_' signaled an
  // error, 'reservoir_output_' is empty and 'status_' contains the error from
  // 'input_'.
  absl::Status BuildReservoirState() {
    // Nothing to do if the output requests no rows.
    if (reservoir_size_ == 0) {
      return Finish();
    }

    // Consume all input, assigning each input tuple a random integral
    // identifier. Within the tuple's partition, only keep that tuple in the
    // output if the random integer is among the K most extreme values.
    ZETASQL_ASSIGN_OR_RETURN(auto comp, MakeTupleComparator());
    // We use a std::map here not because we need order but because it allows
    // us to re-use the comparitor class. The reference implementation inserts
    // shuffles when appropriate to remove incidentally created orders.
    std::map<TupleData, Reservoir, TupleComparator> reservoir_map(*comp);
    while (auto tuple = iter_->Next()) {
      auto score = absl::Uniform<uint32_t>(bitgen_);
      ZETASQL_ASSIGN_OR_RETURN(TupleData partition_key, ComputePartitionKey(*tuple));
      Reservoir& partition = reservoir_map[partition_key];

      if (partition.entries.size() < reservoir_size_) {
        // Append to the output since there are not yet K values.
        partition.entries.push({score, *tuple});
      } else {
        // Push an entry onto the heap and then remove the 'largest' element
        // from the heap. It is not important if it is the largest or smallest
        // value, we just want the K most extreme values.
        partition.entries.push({score, *tuple});
        partition.entries.pop();
      }

      partition.num_candidates_considered++;
      saw_row_ = true;
    }
    ZETASQL_RETURN_IF_ERROR(Finish());

    // Move the tuples into reservoir_output_ so they can be yielded via calls
    // to Next().
    for (auto& [key, res] : reservoir_map) {
      double weight =
          (1.0 * res.num_candidates_considered) / res.entries.size();
      while (!res.entries.empty()) {
        reservoir_output_.push_back(std::move(res.entries.top().tuple));
        res.entries.pop();
        ZETASQL_RETURN_IF_ERROR(SetWeight(weight, &reservoir_output_.back()));
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<TupleComparator>> MakeTupleComparator() {
    std::vector<int> compare_slots;
    compare_slots.reserve(partition_key_.size());
    for (int i = 0; i < partition_key_.size(); ++i) {
      compare_slots.emplace_back(i);
    }
    return TupleComparator::Create(partition_key_, compare_slots,
                                   /*params=*/{}, context_);
  }

  absl::StatusOr<TupleData> ComputePartitionKey(const TupleData& input_row) {
    TupleData result(static_cast<int>(partition_key_.size()));
    for (int i = 0; i < partition_key_.size(); ++i) {
      absl::Status s;
      if (!partition_key_[i]->value_expr()->EvalSimple(
              ConcatSpans(absl::Span<const TupleData* const>(params_),
                          {&input_row}),
              context_, result.mutable_slot(i), &s)) {
        return s;
      }
    }
    return result;
  }

  // This is the number of rows that the sample will select for each reservoir.
  const int64_t reservoir_size_;

  // Params are used to evaluate partition key expressions.
  absl::Span<const TupleData* const> params_;
  // For stratified resevoir samples, partition_key_ defines which reservoir
  // each row is considered for.
  absl::Span<const KeyArg* const> partition_key_;

  // If true, reservoir_output_ is populated. reservoir_input_ might specify a
  // length of zero which means a populated reservoir_output_ could be empty.
  bool built_reservoir_ = false;
  // The next item in reservoir_output_ to yield.
  int reservoir_next_ = 0;
  // The sampled tuples from the input via reservoir sampling.
  std::vector<TupleData> reservoir_output_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> SampleScanOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  absl::Status status;

  TupleSlot size_slot;
  if (!size()->EvalSimple(params, context, &size_slot, &status)) {
    return status;
  }
  const Value& size = size_slot.value();

  if (size.is_null()) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "SampleScan requires non-null size";
  }

  // Get the seed from REPEATABLE if there was a REPEATABLE input. absl::nullopt
  // will cause the iterator to generate a seed.
  absl::optional<int64_t> seed;
  if (has_repeatable()) {
    TupleSlot repeatable_slot;
    if (!repeatable()->EvalSimple(params, context, &repeatable_slot, &status)) {
      return status;
    }
    const Value& repeatable = repeatable_slot.value();
    if (repeatable.is_null()) {
      return zetasql_base::OutOfRangeErrorBuilder() << "REPEATABLE must not be null";
    }
    if (repeatable.ToInt64() < 0) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "REPEATABLE must not be negative";
    }

    seed = repeatable.ToInt64();
  }

  if (has_weight()) {
    // The input iterator needs to allocate an extra tuple slot for weight.
    num_extra_slots++;
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> iter,
                   input()->CreateIterator(params, num_extra_slots, context));
  const bool underlying_iter_preserves_order = iter->PreservesOrder();

  if (method_ == Method::kReservoirRows) {
    if (size.int64_value() < 0) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "SampleScan requires non-negative size";
    }
    iter = absl::make_unique<ReservoirSampleScanTupleIterator>(
        size.int64_value(), seed, context, params, std::move(iter),
        CreateOutputSchema(), partition_key(), weight());
  }
  if (method_ == Method::kBernoulliPercent) {
    if (size.is_null()) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "PERCENT value must not be null";
    }
    double value = size.ToDouble();
    if (value < 0 || value > 100) {
      return zetasql_base::OutOfRangeErrorBuilder()
             << "PERCENT value must be in the range [0, 100]";
    }
    iter = absl::make_unique<BernoulliSampleScanTupleIterator>(
        value / 100.0, seed, context, std::move(iter), CreateOutputSchema(),
        weight());
  }

  // Scramble the output if the scrambling is enabled and either the underlying
  // iterator scrambles or this operator does not preserve order.
  if (context->options().scramble_undefined_orderings &&
      !(underlying_iter_preserves_order && is_order_preserving())) {
    iter = absl::make_unique<ReorderingTupleIterator>(std::move(iter));
  }
  return iter;
}

std::unique_ptr<TupleSchema> SampleScanOp::CreateOutputSchema() const {
  const auto& input_schema = input()->CreateOutputSchema();
  std::vector<VariableId> vars = input_schema->variables();
  if (has_weight()) {
    vars.push_back(weight());
  }
  return absl::make_unique<TupleSchema>(vars);
}

std::string SampleScanOp::IteratorDebugString() const {
  return GetIteratorDebugString(input()->IteratorDebugString());
}

std::string SampleScanOp::DebugInternal(const std::string& indent,
                                        bool verbose) const {
  std::string result = "SampleScanOp(";
  switch (method_) {
    case Method::kBernoulliPercent:
      absl::StrAppend(&result, "BERNOULLI");
      break;
    case Method::kReservoirRows:
      absl::StrAppend(&result, "RESERVOIR");
      break;
  }
  absl::StrAppend(&result, ", ");
  absl::StrAppend(&result, is_order_preserving() ? "ordered" : "unordered");
  absl::StrAppend(
      &result, ArgDebugString({"input", "size", "repeatable", "partition_by"},
                              {k1, k1, kOpt, kNOpt}, indent, verbose,
                              /*more_children=*/has_weight()));
  if (has_weight()) {
    absl::StrAppend(&result, indent, kIndentFork,
                    GetArg(kWeight)->DebugString(), " := weight");
  }
  absl::StrAppend(&result, ")");
  return result;
}

SampleScanOp::SampleScanOp(
    std::unique_ptr<ValueExpr> size, std::unique_ptr<ValueExpr> repeatable,
    std::unique_ptr<RelationalOp> input, Method method,
    std::vector<std::unique_ptr<ValueExpr>> partition_key,
    const VariableId& sample_weight)
    : method_(method) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
  SetArg(kSize, absl::make_unique<ExprArg>(std::move(size)));
  if (repeatable) {
    SetArg(kRepeatable, absl::make_unique<ExprArg>(std::move(repeatable)));
  } else {
    SetArg(kRepeatable, nullptr);
  }
  std::vector<std::unique_ptr<KeyArg>> partition_key_args;
  partition_key_args.reserve(partition_key.size());
  for (auto& key_part : partition_key) {
    partition_key_args.emplace_back(
        absl::make_unique<KeyArg>(std::move(key_part)));
  }
  SetArgs<KeyArg>(kPartitionKey, std::move(partition_key_args));
  std::unique_ptr<ExprArg> weight_arg;
  if (sample_weight.is_valid()) {
    weight_arg = absl::make_unique<ExprArg>(sample_weight, types::DoubleType());
  }
  SetArg(kWeight, std::move(weight_arg));
}

const RelationalOp* SampleScanOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* SampleScanOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}

const ValueExpr* SampleScanOp::size() const {
  return GetArg(kSize)->node()->AsValueExpr();
}

ValueExpr* SampleScanOp::mutable_size() {
  return GetMutableArg(kSize)->mutable_node()->AsMutableValueExpr();
}

bool SampleScanOp::has_repeatable() const {
  return GetArg(kRepeatable) != nullptr;
}

const ValueExpr* SampleScanOp::repeatable() const {
  return GetArg(kRepeatable)->node()->AsValueExpr();
}

absl::Span<const KeyArg* const> SampleScanOp::partition_key() const {
  return GetArgs<KeyArg>(kPartitionKey);
}

absl::Span<KeyArg* const> SampleScanOp::mutable_partition_key() {
  return GetMutableArgs<KeyArg>(kPartitionKey);
}

bool SampleScanOp::has_weight() const { return GetArg(kWeight) != nullptr; }

const VariableId& SampleScanOp::weight() const {
  static const VariableId* empty = new VariableId();
  return has_weight() ? GetArg(kWeight)->variable() : *empty;
}

ValueExpr* SampleScanOp::mutable_repeatable() {
  return GetMutableArg(kRepeatable)->mutable_node()->AsMutableValueExpr();
}

// -------------------------------------------------------
// EnumerateOp
// -------------------------------------------------------

std::string EnumerateOp::GetIteratorDebugString(
    absl::string_view count_debug_string) {
  return absl::StrCat("EnumerateTupleIterator(", count_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<EnumerateOp>> EnumerateOp::Create(
    std::unique_ptr<ValueExpr> row_count) {
  ZETASQL_RET_CHECK(row_count->output_type()->IsInt64());
  return absl::WrapUnique(new EnumerateOp(std::move(row_count)));
}

absl::Status EnumerateOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return mutable_row_count()->SetSchemasForEvaluation(params_schemas);
}

namespace {
// Outputs 'count' empty tuples (with extra slots as requested).
class EnumerateTupleIterator : public TupleIterator {
 public:
  EnumerateTupleIterator(int64_t count, int num_extra_slots,
                         EvaluationContext* context)
      : count_(count), schema_({}), context_(context), data_(num_extra_slots) {}

  EnumerateTupleIterator(const EnumerateTupleIterator&) = delete;
  EnumerateTupleIterator& operator=(const EnumerateTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return schema_; }

  TupleData* Next() override {
    if (num_tuples_returned_ %
            absl::GetFlag(
                FLAGS_zetasql_call_verify_not_aborted_rows_period) ==
        0) {
      absl::Status status = context_->VerifyNotAborted();
      if (!status.ok()) {
        status_ = status;
        return nullptr;
      }
    }

    if (num_tuples_returned_ >= count_) return nullptr;
    ++num_tuples_returned_;
    return &data_;
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return EnumerateOp::GetIteratorDebugString(absl::StrCat(count_));
  }

 private:
  const int64_t count_;
  const TupleSchema schema_;
  EvaluationContext* context_;
  TupleData data_;
  int64_t num_tuples_returned_ = 0;
  absl::Status status_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> EnumerateOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  TupleSlot count_slot;
  absl::Status status;
  if (!row_count()->EvalSimple(params, context, &count_slot, &status))
    return status;
  const Value& count = count_slot.value();
  if (count.is_null()) {
    return zetasql_base::OutOfRangeErrorBuilder()
           << "Enumerate requires non-null count";
  }
  std::unique_ptr<TupleIterator> iter =
      absl::make_unique<EnumerateTupleIterator>(count.int64_value(),
                                                num_extra_slots, context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> EnumerateOp::CreateOutputSchema() const {
  return absl::make_unique<TupleSchema>(std::vector<VariableId>());
}

std::string EnumerateOp::IteratorDebugString() const {
  return GetIteratorDebugString("<count>");
}

std::string EnumerateOp::DebugInternal(const std::string& indent,
                                       bool verbose) const {
  return absl::StrCat("EnumerateOp(",
                      row_count()->DebugInternal(indent, verbose), ")");
}

EnumerateOp::EnumerateOp(std::unique_ptr<ValueExpr> row_count) {
  SetArg(kRowCount, absl::make_unique<ExprArg>(std::move(row_count)));
}

const ValueExpr* EnumerateOp::row_count() const {
  return GetArg(kRowCount)->node()->AsValueExpr();
}

ValueExpr* EnumerateOp::mutable_row_count() {
  return GetMutableArg(kRowCount)->mutable_node()->AsMutableValueExpr();
}

// -------------------------------------------------------
// JoinOp
// -------------------------------------------------------

const std::string& JoinOp::JoinKindToString(JoinOp::JoinKind kind) {
  static auto* join_names = new std::map<JoinOp::JoinKind, std::string>{
      {JoinOp::kInnerJoin, "INNER"},
      {JoinOp::kLeftOuterJoin, "LEFT OUTER"},
      {JoinOp::kRightOuterJoin, "RIGHT OUTER"},
      {JoinOp::kFullOuterJoin, "FULL OUTER"},
      {JoinOp::kCrossApply, "CROSS APPLY"},
      {JoinOp::kOuterApply, "OUTER APPLY"}};
  return (*join_names)[kind];
}

absl::StatusOr<std::unique_ptr<JoinOp>> JoinOp::Create(
    JoinKind kind, std::vector<HashJoinEqualityExprs> equality_exprs,
    std::unique_ptr<ValueExpr> remaining_condition,
    std::unique_ptr<RelationalOp> left, std::unique_ptr<RelationalOp> right,
    std::vector<std::unique_ptr<ExprArg>> left_outputs,
    std::vector<std::unique_ptr<ExprArg>> right_outputs) {
  if (!equality_exprs.empty()) {
    ZETASQL_RET_CHECK(kind != kCrossApply && kind != kOuterApply)
        << JoinKindToString(kind)
        << " does not support hash join equality expressions";
  }
  std::vector<std::unique_ptr<ExprArg>> hash_join_equality_left_exprs;
  hash_join_equality_left_exprs.reserve(equality_exprs.size());
  std::vector<std::unique_ptr<ExprArg>> hash_join_equality_right_exprs;
  hash_join_equality_right_exprs.reserve(equality_exprs.size());
  for (HashJoinEqualityExprs& exprs : equality_exprs) {
    hash_join_equality_left_exprs.push_back(std::move(exprs.left_expr));
    hash_join_equality_right_exprs.push_back(std::move(exprs.right_expr));
  }

  if (kind != kRightOuterJoin && kind != kFullOuterJoin) {
    ZETASQL_RET_CHECK(left_outputs.empty())
        << "Left outputs require right outer or full outer join";
  }
  if (kind != kLeftOuterJoin && kind != kFullOuterJoin && kind != kCrossApply &&
      kind != kOuterApply) {
    ZETASQL_RET_CHECK(right_outputs.empty())
        << "Right outputs require left outer or full join";
  }

  return absl::WrapUnique(new JoinOp(
      kind, std::move(hash_join_equality_left_exprs),
      std::move(hash_join_equality_right_exprs), std::move(remaining_condition),
      std::move(left), std::move(right), std::move(left_outputs),
      std::move(right_outputs)));
}

absl::Status JoinOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(
      mutable_left_input()->SetSchemasForEvaluation(params_schemas));

  const std::unique_ptr<const TupleSchema> left_schema =
      left_input()->CreateOutputSchema();
  const std::unique_ptr<const TupleSchema> right_schema =
      right_input()->CreateOutputSchema();

  switch (join_kind_) {
    case kInnerJoin:
    case kLeftOuterJoin:
    case kRightOuterJoin:
    case kFullOuterJoin:
      // Uncorrelated right-hand side.
      ZETASQL_RETURN_IF_ERROR(
          mutable_right_input()->SetSchemasForEvaluation(params_schemas));
      break;
    case kCrossApply:
    case kOuterApply:
      // Correlated right-hand side.
      ZETASQL_RETURN_IF_ERROR(mutable_right_input()->SetSchemasForEvaluation(
          ConcatSpans(params_schemas, {left_schema.get()})));
      break;
  }

  for (ExprArg* left_expr : mutable_hash_join_equality_left_exprs()) {
    ZETASQL_RETURN_IF_ERROR(left_expr->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {left_schema.get()})));
  }

  for (ExprArg* right_expr : mutable_hash_join_equality_right_exprs()) {
    ZETASQL_RETURN_IF_ERROR(right_expr->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {right_schema.get()})));
  }

  for (ExprArg* left_output : mutable_left_outputs()) {
    ZETASQL_RETURN_IF_ERROR(left_output->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {left_schema.get()})));
  }

  for (ExprArg* right_output : mutable_right_outputs()) {
    ZETASQL_RETURN_IF_ERROR(right_output->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {right_schema.get()})));
  }

  ZETASQL_RETURN_IF_ERROR(mutable_remaining_join_expr()->SetSchemasForEvaluation(
      ConcatSpans(params_schemas, {left_schema.get(), right_schema.get()})));

  return absl::OkStatus();
}

namespace {

// Interface representing the right-hand input side of a join.
class RightInputForJoin {
 public:
  virtual ~RightInputForJoin() {}

  // Returns true if this input depends on the left-hand side. An example is an
  // array join like:
  //  SELECT key, val FROM TestTable tt, tt.KitchenSink.repeated_int32_val val;
  virtual bool IsCorrelated() const = 0;

  // Returns the schema of the right-hand side. Must not be called before
  // ResetForLeftInput().
  virtual const TupleSchema& Schema() const = 0;

  // Resets the input for a particular left tuple.
  //
  // 'left_input' may be NULL if IsCorrelated() returns false. In that
  // case, the right input does not depend on the left-hand side.
  virtual absl::Status ResetForLeftInput(const Tuple* left_input) = 0;

  // Returns the number of tuples on this side of the join that the last call to
  // ResetForLeftInput() identified as possibly joining with the current left
  // tuple. Guaranteed to be the entire right-hand side if IsCorrelated() is
  // false and the current left tuple is NULL. Must not be called before
  // ResetForLeftInput().
  virtual int64_t GetNumMatchingTuples() const = 0;

  // Returns the corresponding tuple from the potential right-hand side matches
  // to the left tuple last passed to ResetForLeftInput(). Must not be called
  // before ResetForLeftInput(). 'index' must be in [0, NumTuples()). The
  // returned reference is only valid until the next call to a non-const method.
  virtual const TupleData& GetMatchingTuple(int64_t index) const = 0;

  // Records that GetMatchingTuple(index) joined with some left tuple. Has the
  // same requirements as GetMatchingTuple(). Also requires that IsCorrelated()
  // is false.
  virtual absl::Status RecordMatchingTupleJoined(int64_t index) = 0;

  // Returns true if GetMatchingTuple(index) joined with some left tuple. Has
  // the same requirements as RecordMatchingTupleJoined().
  virtual absl::StatusOr<bool> DidMatchingTupleJoin(int64_t index) const = 0;

  // A human-readable debug string representing the right-hand side.
  virtual std::string DebugString() const = 0;
};

// Tracks a right-hand side tuple and whether it has joined with anything yet.
struct RightTupleAndJoinedBit {
  const TupleData* tuple = nullptr;  // Not owned.
  bool joined = false;
};

// Returns a vector of RightTupleAndJoinedBits corresponding to 'tuples'.
static std::vector<RightTupleAndJoinedBit> WrapWithJoinedBits(
    const std::vector<const TupleData*>& tuples) {
  std::vector<RightTupleAndJoinedBit> ret;
  ret.reserve(tuples.size());
  for (const TupleData* tuple : tuples) {
    RightTupleAndJoinedBit t;
    t.tuple = tuple;
    ret.push_back(std::move(t));
  }
  return ret;
}

// Represents the right-hand input side of a join whose right-hand side cannot
// depend on the left-hand side.
class UncorrelatedRightInput : public RightInputForJoin {
 public:
  UncorrelatedRightInput(std::unique_ptr<TupleSchema> schema,
                         std::unique_ptr<TupleDataDeque> tuples,
                         std::unique_ptr<TupleIterator> iter_for_debug_string)
      : schema_(std::move(schema)),
        iter_for_debug_string_(std::move(iter_for_debug_string)),
        tuples_(std::move(tuples)),
        tuples_and_bits_(WrapWithJoinedBits(tuples_->GetTuplePtrs())) {}

  UncorrelatedRightInput(const UncorrelatedRightInput&) = delete;
  UncorrelatedRightInput& operator=(const UncorrelatedRightInput&) = delete;

  ~UncorrelatedRightInput() override {}

  bool IsCorrelated() const override { return false; }

  const TupleSchema& Schema() const override { return *schema_; }

  absl::Status ResetForLeftInput(const Tuple* /* left_input */) override {
    return absl::OkStatus();
  }

  int64_t GetNumMatchingTuples() const override {
    return tuples_and_bits_.size();
  }

  const TupleData& GetMatchingTuple(int64_t index) const override {
    return *tuples_and_bits_[index].tuple;
  }

  absl::Status RecordMatchingTupleJoined(int64_t index) override {
    tuples_and_bits_[index].joined = true;
    return absl::OkStatus();
  }

  absl::StatusOr<bool> DidMatchingTupleJoin(int64_t index) const override {
    return tuples_and_bits_[index].joined;
  }

  std::string DebugString() const override {
    return iter_for_debug_string_->DebugString();
  }

 private:
  const std::unique_ptr<TupleSchema> schema_;
  // We store a TupleIterator instead of the debug string to avoid computing the
  // debug string unnecessarily.
  const std::unique_ptr<TupleIterator> iter_for_debug_string_;

  std::unique_ptr<TupleDataDeque> tuples_;
  // TupleDatas owned by 'tuples_'.
  std::vector<RightTupleAndJoinedBit> tuples_and_bits_;
};

class UncorrelatedHashedRightInput : public RightInputForJoin {
 public:
  static absl::StatusOr<std::unique_ptr<UncorrelatedHashedRightInput>> Create(
      absl::Span<const TupleData* const> params,
      absl::Span<const ExprArg* const> left_equality_exprs,
      absl::Span<const ExprArg* const> right_equality_exprs,
      std::unique_ptr<TupleSchema> schema,
      std::unique_ptr<TupleDataDeque> right_tuples,
      std::unique_ptr<TupleIterator> iter_for_debug_string,
      EvaluationContext* context) {
    ZETASQL_RET_CHECK_EQ(left_equality_exprs.size(), right_equality_exprs.size());

    std::vector<RightTupleAndJoinedBit> right_tuples_and_bits =
        WrapWithJoinedBits(right_tuples->GetTuplePtrs());

    auto right_tuple_map = absl::make_unique<RightTupleMap>();
    for (auto& tuple_and_bit : right_tuples_and_bits) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleData> key,
                       CreateTupleMapKey(params, *tuple_and_bit.tuple,
                                         right_equality_exprs, context));
      (*right_tuple_map)[*key].push_back(&tuple_and_bit);
    }
    return absl::WrapUnique(new UncorrelatedHashedRightInput(
        params, left_equality_exprs, std::move(schema), std::move(right_tuples),
        std::move(right_tuples_and_bits), std::move(right_tuple_map),
        std::move(iter_for_debug_string), context));
  }

  bool IsCorrelated() const override { return false; }

  const TupleSchema& Schema() const override { return *schema_; }

  absl::Status ResetForLeftInput(const Tuple* left_input) override {
    if (left_input == nullptr) {
      matching_right_tuple_list_ = absl::nullopt;
    } else {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleData> key,
                       CreateTupleMapKey(params_, *left_input->data,
                                         left_equality_exprs_, context_));
      const auto it = right_tuple_map_->find(*key);
      if (it == right_tuple_map_->end()) {
        // No matching tuples.
        matching_right_tuple_list_ = nullptr;
      } else {
        const TupleData& other_key = it->first;
        matching_right_tuple_list_ = &it->second;
        // We have to compare 'key' against 'other_key', because TupleData::==()
        // uses Value::Equals(), which is more permissive than SQL equality. In
        // particular, SQL specifies that the result of NULL = NULL is NULL and
        // that the result of NaN = NaN is false, but NULL.Equals(NULL) and
        // NaN.Equals(NaN) are both true.
        ZETASQL_RET_CHECK_EQ(key->num_slots(), other_key.num_slots());
        absl::Status status;
        for (int i = 0; i < key->num_slots(); ++i) {
          const ComparisonFunction equals_function(FunctionKind::kEqual,
                                                   types::BoolType());
          Value equals_result;
          if (!equals_function.Eval(
                  params_, {key->slot(i).value(), other_key.slot(i).value()},
                  context_, &equals_result, &status)) {
            return status;
          }
          if (equals_result != values::Bool(true)) {
            matching_right_tuple_list_ = nullptr;
            break;
          }
        }
      }
    }
    return absl::OkStatus();
  }

  int64_t GetNumMatchingTuples() const override {
    if (!matching_right_tuple_list_.has_value()) {
      // No value -> iterate over everything.
      return right_tuples_and_bits_.size();
    }
    if (matching_right_tuple_list_ == nullptr) {
      // NULL value -> no matches.
      return 0;
    }
    return matching_right_tuple_list_.value()->size();
  }

  const TupleData& GetMatchingTuple(int64_t index) const override {
    if (!matching_right_tuple_list_.has_value()) {
      // No value -> iterate over everything.
      return *right_tuples_and_bits_[index].tuple;
    }
    // Otherwise iterate over the list.
    return *(*matching_right_tuple_list_.value())[index]->tuple;
  }

  absl::Status RecordMatchingTupleJoined(int64_t index) override {
    if (!matching_right_tuple_list_.has_value()) {
      // No value -> iterate over everything.
      right_tuples_and_bits_[index].joined = true;
    } else {
      // Otherwise iterate over the list.
      (*matching_right_tuple_list_.value())[index]->joined = true;
    }
    return absl::OkStatus();
  }

  absl::StatusOr<bool> DidMatchingTupleJoin(int64_t index) const override {
    if (!matching_right_tuple_list_.has_value()) {
      // No value -> iterate over everything.
      return right_tuples_and_bits_[index].joined;
    } else {
      // Otherwise iterate over the list.
      return (*matching_right_tuple_list_.value())[index]->joined;
    }
  }

  std::string DebugString() const override {
    return iter_for_debug_string_->DebugString();
  }

 private:
  using RightTupleList = std::vector<RightTupleAndJoinedBit*>;
  // Maps the values of the right-hand side join expressions to the
  // corresponding right tuples.
  using RightTupleMap = absl::flat_hash_map<TupleData, RightTupleList>;

  UncorrelatedHashedRightInput(
      absl::Span<const TupleData* const> params,
      absl::Span<const ExprArg* const> left_equality_exprs,
      std::unique_ptr<TupleSchema> schema,
      std::unique_ptr<TupleDataDeque> right_tuples,
      // The TupleDatas in here are owned by 'right_tuples'.
      std::vector<RightTupleAndJoinedBit> right_tuples_and_bits,
      std::unique_ptr<RightTupleMap> right_tuple_map,
      std::unique_ptr<TupleIterator> iter_for_debug_string,
      EvaluationContext* context)
      : params_(params.begin(), params.end()),
        left_equality_exprs_(left_equality_exprs.begin(),
                             left_equality_exprs.end()),
        schema_(std::move(schema)),
        right_tuples_(std::move(right_tuples)),
        right_tuples_and_bits_(std::move(right_tuples_and_bits)),
        right_tuple_map_(std::move(right_tuple_map)),
        iter_for_debug_string_(std::move(iter_for_debug_string)),
        context_(context) {}

  UncorrelatedHashedRightInput(const UncorrelatedHashedRightInput&) = delete;
  UncorrelatedHashedRightInput& operator=(const UncorrelatedHashedRightInput&) =
      delete;

  // Returns the TupleMap key corresponding to 'row' and 'args'.
  static absl::StatusOr<std::unique_ptr<TupleData>> CreateTupleMapKey(
      absl::Span<const TupleData* const> params, const TupleData& row,
      absl::Span<const ExprArg* const> args, EvaluationContext* context) {
    auto key = absl::make_unique<TupleData>(args.size());
    for (int i = 0; i < args.size(); ++i) {
      const ExprArg* arg = args[i];
      TupleSlot* slot = key->mutable_slot(i);
      absl::Status status;
      if (!arg->value_expr()->EvalSimple(ConcatSpans(params, {&row}), context,
                                         slot, &status)) {
        return status;
      }
      // Represent non-negative INT64 values with UINT64 values to support
      // equalities of the form INT64 = UINT64 (or UINT64 = INT64).
      if (slot->value().type_kind() == TYPE_INT64 && !slot->value().is_null()) {
        const int64_t int64_value = slot->value().int64_value();
        if (int64_value >= 0) {
          slot->SetValue(values::Uint64(static_cast<uint64_t>(int64_value)));
        }
      }
    }
    return key;
  }

  const std::vector<const TupleData*> params_;
  const std::vector<const ExprArg*> left_equality_exprs_;
  const std::unique_ptr<TupleSchema> schema_;

  std::unique_ptr<TupleDataDeque> right_tuples_;
  // The TupleDatas in here are owned by 'right_tuples_'.
  std::vector<RightTupleAndJoinedBit> right_tuples_and_bits_;
  std::unique_ptr<RightTupleMap> right_tuple_map_;
  // The TupleList in 'right_tuple_map_' corresponding to the current left
  // tuple. NULL indicates there are no corresponding tuples. No value indicates
  // that left tuple in the last call to ResetForLeftInput() was NULL and
  // therefore GetNumMatchingTuples()/etc. should iterate over everything.
  absl::optional<RightTupleList*> matching_right_tuple_list_ = nullptr;

  // We store a TupleIterator instead of the debug string to avoid computing the
  // debug string unnecessarily.
  const std::unique_ptr<TupleIterator> iter_for_debug_string_;

  EvaluationContext* context_;
};

// Reads the input tuples from 'op' and populates them in 'tuples'. If
// 'iter_for_debug_string' is non-NULL, populates it with the iterator. (We pass
// around the iterator instead of the debug string to avoid computing the debug
// string unnecessarily.)
absl::Status ExtractFromRelationalOp(
    const RelationalOp* op, absl::Span<const TupleData* const> params,
    EvaluationContext* context, TupleDataDeque* tuples,
    std::unique_ptr<TupleIterator>* iter_for_debug_string) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> iter,
                   op->CreateIterator(params, /*num_extra_slots=*/0, context));
  tuples->Clear();
  absl::Status status;
  while (true) {
    TupleData* tuple = iter->Next();
    if (tuple == nullptr) {
      ZETASQL_RETURN_IF_ERROR(iter->Status());
      break;
    }
    if (!tuples->PushBack(absl::make_unique<TupleData>(*tuple), &status)) {
      return status;
    }
  }

  if (iter_for_debug_string != nullptr) {
    *iter_for_debug_string = std::move(iter);
  }

  return absl::OkStatus();
}

// Represents the right-hand input side of a join whose right-hand side can
// depend on the left-hand side (i.e., cross apply and outer apply).
class CorrelatedRightInput : public RightInputForJoin {
 public:
  CorrelatedRightInput(const RelationalOp* input_op,
                       absl::Span<const TupleData* const> params,
                       EvaluationContext* context)
      : params_(params.begin(), params.end()),
        input_op_(input_op),
        context_(context),
        tuples_(context_->memory_accountant()) {}

  CorrelatedRightInput(const CorrelatedRightInput&) = delete;
  CorrelatedRightInput& operator=(const CorrelatedRightInput&) = delete;

  ~CorrelatedRightInput() override {}

  bool IsCorrelated() const override { return true; }

  const TupleSchema& Schema() const override { return *schema_; }

  absl::Status ResetForLeftInput(const Tuple* left_input) override {
    // 'left_input' cannot be NULL because IsCorrelated() returns true.
    ZETASQL_RET_CHECK(left_input != nullptr);

    if (schema_ == nullptr) {
      schema_ = input_op_->CreateOutputSchema();
    }

    const std::vector<const TupleData*> new_params = ConcatSpans(
        absl::Span<const TupleData* const>(params_), {left_input->data});

    ZETASQL_RETURN_IF_ERROR(ExtractFromRelationalOp(input_op_, new_params, context_,
                                            &tuples_,
                                            /*iter_for_debug_string=*/nullptr));
    tuple_ptrs_ = tuples_.GetTuplePtrs();
    return absl::OkStatus();
  }

  int64_t GetNumMatchingTuples() const override { return tuple_ptrs_.size(); }

  const TupleData& GetMatchingTuple(int64_t index) const override {
    return *tuple_ptrs_[index];
  }

  absl::Status RecordMatchingTupleJoined(int64_t index) override {
    ZETASQL_RET_CHECK_FAIL() << "RecordMatchingTupleJoined() cannot be called because "
                     << "IsCorrelated() returns true";
  }

  absl::StatusOr<bool> DidMatchingTupleJoin(int64_t index) const override {
    ZETASQL_RET_CHECK_FAIL() << "DidMatchingTupleJoin() cannot be called because "
                     << "IsCorrelated() returns true";
  }

  std::string DebugString() const override {
    // The right-hand side depends on the left-hand side, so it isn't possible
    // to obtain a single TupleIterator representing the right-hand side. Thus,
    // we have to resort to using RelationalOp::IteratorDebugString() instead of
    // TupleIterator::DebugString().
    return input_op_->IteratorDebugString();
  }

 private:
  const std::vector<const TupleData*> params_;
  const RelationalOp* input_op_;
  EvaluationContext* context_;

  std::unique_ptr<TupleSchema> schema_;
  TupleDataDeque tuples_;
  // Owned by 'tuples_'.
  std::vector<const TupleData*> tuple_ptrs_;
};

// Takes left tuples, right tuples, and an arbitrary join predicate, and outputs
// the joined tuples that match the join predicate.
class JoinTupleIterator : public TupleIterator {
 public:
  using JoinKind = JoinOp::JoinKind;

  JoinTupleIterator(JoinKind join_kind,
                    absl::Span<const TupleData* const> params,
                    const ValueExpr* join_expr,
                    std::unique_ptr<TupleIterator> left_iter,
                    absl::Span<const ExprArg* const> left_outputs,
                    std::unique_ptr<RightInputForJoin> right_input,
                    absl::Span<const ExprArg* const> right_outputs,
                    std::unique_ptr<TupleSchema> output_schema,
                    int num_extra_slots, EvaluationContext* context)
      : join_kind_(join_kind),
        params_(params.begin(), params.end()),
        join_expr_(join_expr),
        left_iter_(std::move(left_iter)),
        left_outputs_(left_outputs.begin(), left_outputs.end()),
        right_input_(std::move(right_input)),
        right_outputs_(right_outputs.begin(), right_outputs.end()),
        output_schema_(std::move(output_schema)),
        context_(context) {
    output_tuple_.AddSlots(output_schema_->num_variables() + num_extra_slots);
  }

  JoinTupleIterator(const JoinTupleIterator&) = delete;
  JoinTupleIterator& operator=(const JoinTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return *output_schema_; }

  // 1) First we iterate over all the left tuples.
  // 1a) For each left tuple, we iterate over all the right tuples and output
  //     joined tuples that match the join condition.
  // 1b) When we are done considering all the right tuples, we may have to
  //     output another tuple obtained by right-padding NULLs onto the left
  //     tuple.
  // 2) When we are done iterating over all the left tuples, we may have to loop
  //    over all the right tuples.
  // 2a) For each right tuple, we may have to left-pad NULLs.
  TupleData* Next() override {
    if (!left_padding_right_tuples_ && !next_left_tuple_.has_value() &&
        next_right_tuple_idx_ == 0) {
      const absl::Status init_status = InitializeJoinCandidates();
      if (!init_status.ok()) {
        status_ = init_status;
        return nullptr;
      }
    }

    while (true) {
      if (done_) {
        // Stop storing the right side in memory to free up the memory for other
        // operators. We only bother to do this on success because if there are
        // errors the query processing will stop anyway.
        right_input_.reset();
        return nullptr;
      }

      std::unique_ptr<Tuple> left_tuple;
      if (next_left_tuple_.value() != nullptr) {
        left_tuple = absl::make_unique<Tuple>(&left_iter_->Schema(),
                                              next_left_tuple_.value());
      }

      std::unique_ptr<Tuple> right_tuple;
      if (next_right_tuple_idx_ >= 0) {
        const TupleData& data =
            right_input_->GetMatchingTuple(next_right_tuple_idx_);
        right_tuple = absl::make_unique<Tuple>(&right_input_->Schema(), &data);
      }

      const absl::StatusOr<bool> status_or_joined =
          JoinTuples(left_tuple.get(), right_tuple.get());
      if (!status_or_joined.ok()) {
        status_ = status_or_joined.status();
        return nullptr;
      }
      const bool joined = status_or_joined.value();

      if (!left_padding_right_tuples_ && next_right_tuple_idx_ >= 0 && joined) {
        left_tuple_joined_ = true;

        if (!right_input_->IsCorrelated()) {
          absl::Status joined_status =
              right_input_->RecordMatchingTupleJoined(next_right_tuple_idx_);
          if (!joined_status.ok()) {
            status_ = joined_status;
            return nullptr;
          }
        }
      }

      const absl::Status advance_status = Advance();
      if (!advance_status.ok()) {
        status_ = advance_status;
        return nullptr;
      }

      if (joined) {
        return &output_tuple_;
      }
    }
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return JoinOp::GetIteratorDebugString(join_kind_, left_iter_->DebugString(),
                                          right_input_->DebugString());
  }

 private:
  // Updates the private variables to point to the first candidate join tuples.
  absl::Status InitializeJoinCandidates() {
    ZETASQL_RET_CHECK(!next_left_tuple_.has_value());
    next_left_tuple_ = left_iter_->Next();
    if (next_left_tuple_ == nullptr) {
      ZETASQL_RETURN_IF_ERROR(left_iter_->Status());

      // There are no left tuples, so we are done unless we are doing a right
      // join or full outer join, in which case we need to load the right-hand
      // side and emit right tuples that are left-padded with NULLs.
      if (join_kind_ == JoinKind::kRightOuterJoin ||
          join_kind_ == JoinKind::kFullOuterJoin) {
        ZETASQL_ASSIGN_OR_RETURN(
            std::ignore /* done_with_left_tuple */,
            InitializeRightTuplesForLeftTuple(/*left_tuple=*/nullptr));
        return FinishJoiningLeftAndRightTuples();
      }
      done_ = true;
      return absl::OkStatus();
    }

    ZETASQL_ASSIGN_OR_RETURN(const bool done_with_left_tuple,
                     InitializeRightTuplesForCurrentLeftTuple());
    if (done_with_left_tuple) {
      // We are done with the first left tuple, so move on to the next one.
      return AdvanceToNextLeftTupleWithJoinCandidates();
    }
    return absl::OkStatus();
  }

  // Advances the private variables for the next candidate join tuples.
  absl::Status Advance() {
    ZETASQL_RET_CHECK(!done_);

    if (left_padding_right_tuples_) {
      return AdvanceToNextRightTupleForLeftPadding();
    }

    if (next_right_tuple_idx_ == -1) {
      // We tried to join the current left tuple with right tuples, but found
      // no joining tuples. Then we right padded the tuple with NULLs and
      // output it. Advance to the next left tuple.
      ZETASQL_RET_CHECK(join_kind_ == JoinKind::kLeftOuterJoin ||
                join_kind_ == JoinKind::kOuterApply ||
                join_kind_ == JoinKind::kFullOuterJoin)
          << JoinOp::JoinKindToString(join_kind_);
      return AdvanceToNextLeftTupleWithJoinCandidates();
    }

    // Advance to the next right tuple.
    ++next_right_tuple_idx_;
    if (next_right_tuple_idx_ < right_input_->GetNumMatchingTuples()) {
      return absl::OkStatus();
    }
    ZETASQL_ASSIGN_OR_RETURN(const bool done_with_left_tuple,
                     FinishRightTuplesForCurrentLeftTuple());
    ZETASQL_RET_CHECK_EQ(done_with_left_tuple, next_right_tuple_idx_ != -1)
        << next_right_tuple_idx_;
    if (!done_with_left_tuple) {
      return absl::OkStatus();
    }
    // We are out of right tuples for this left tuple, and there is no need to
    // pad the left tuple with NULLs. Advance to the next left tuple and reset
    // to the beginning of the right tuples.
    return AdvanceToNextLeftTupleWithJoinCandidates();
  }

  // Repeatedly increments 'next_left_tuple_idx_' and performs corresponding
  // updates to the other private variables until either reaching a left tuple
  // for which there are right tuples, or reaching a left tuple for which there
  // are no right tuples that requires padding the left tuple NULLs. If there
  // are no more left tuples, either sets 'done_' to true or
  // 'left_padding_right_tuples_' to true and 'next_right_tuple_idx_' to 0,
  // depending on 'join_kind_'.
  //
  // Requires that we are not left-padding right tuples.
  absl::Status AdvanceToNextLeftTupleWithJoinCandidates() {
    ZETASQL_RET_CHECK(!left_padding_right_tuples_);

    while (true) {
      left_tuple_joined_ = false;
      next_left_tuple_ = left_iter_->Next();
      if (next_left_tuple_ == nullptr) {
        ZETASQL_RETURN_IF_ERROR(left_iter_->Status());
        // We have finished trying to join left tuples with right tuples.
        return FinishJoiningLeftAndRightTuples();
      }

      ZETASQL_ASSIGN_OR_RETURN(const bool done_with_left_tuple,
                       InitializeRightTuplesForCurrentLeftTuple());
      if (!done_with_left_tuple) {
        // The next output should be the current left tuple padded with NULLs.
        return absl::OkStatus();
      }
    }
  }

  // Resets the right-hand side based on the given value of the left-hand side,
  // which may be NULL. If there are no right-hand side tuples, updates the
  // private variables to reflect whether we are done with the left tuple or
  // whether we have to emit an output tuple that is right-padded with
  // NULLs. Returns true if we are done with the left tuple.
  absl::StatusOr<bool> InitializeRightTuplesForLeftTuple(
      const Tuple* left_tuple) {
    next_right_tuple_idx_ = 0;
    ZETASQL_RETURN_IF_ERROR(right_input_->ResetForLeftInput(left_tuple));
    if (right_input_->GetNumMatchingTuples() == 0) {
      ZETASQL_ASSIGN_OR_RETURN(const bool done_with_left_tuple,
                       FinishRightTuplesForCurrentLeftTuple());
      ZETASQL_RET_CHECK_EQ(done_with_left_tuple, next_right_tuple_idx_ != -1)
          << next_right_tuple_idx_;
      return done_with_left_tuple;
    }
    // There are right tuples left, so we are not done with 'left_tuple'.
    return false;
  }

  // Resets the right-hand side based on the current value of the left-hand
  // side. Returns true if there are right tuples for the current left tuple.
  absl::StatusOr<bool> InitializeRightTuplesForCurrentLeftTuple() {
    const Tuple left_tuple(&left_iter_->Schema(), next_left_tuple_.value());
    return InitializeRightTuplesForLeftTuple(&left_tuple);
  }

  // Updates the private variables to reflect that we are done considering right
  // tuples for a particular left tuple. The next join candidate to consider
  // either involves the next left tuple or the current left tuple padded with
  // NULLs. Returns true if we are done with the current tuple.
  absl::StatusOr<bool> FinishRightTuplesForCurrentLeftTuple() {
    ZETASQL_RET_CHECK(!left_padding_right_tuples_);
    ZETASQL_RET_CHECK(next_left_tuple_.has_value());
    ZETASQL_RET_CHECK_EQ(next_right_tuple_idx_, right_input_->GetNumMatchingTuples());
    if (left_tuple_joined_) {
      return true;  // Don't pad with NULLs.
    }
    // We are done trying to join a particular left tuple, but we never
    // found a right tuple to join it with. Consider padding it with
    // NULLs.
    switch (join_kind_) {
      case JoinKind::kInnerJoin:
      case JoinKind::kRightOuterJoin:
      case JoinKind::kCrossApply:
        return true;  // Don't pad with NULLs.
      case JoinKind::kLeftOuterJoin:
      case JoinKind::kOuterApply:
      case JoinKind::kFullOuterJoin:
        next_right_tuple_idx_ = -1;
        return false;  // Pad with NULLs.
    }
  }

  // If the join kind is not right outer or full outer join, simply sets 'done_'
  // to true. Otherwise, looks for the first right tuple that did not join and
  // sets 'left_padding_right_tuples_' to true.
  absl::Status FinishJoiningLeftAndRightTuples() {
    ZETASQL_RET_CHECK(!left_padding_right_tuples_);
    ZETASQL_RET_CHECK(next_left_tuple_.has_value());
    ZETASQL_RET_CHECK(next_left_tuple_.value() == nullptr);
    switch (join_kind_) {
      case JoinKind::kInnerJoin:
      case JoinKind::kLeftOuterJoin:
      case JoinKind::kCrossApply:
      case JoinKind::kOuterApply:
        done_ = true;
        return absl::OkStatus();
      case JoinKind::kRightOuterJoin:
      case JoinKind::kFullOuterJoin:
        // For right outer and full outer joins, now we have to left-pad
        // non-joining right tuples with NULLs.
        left_padding_right_tuples_ = true;
        ZETASQL_RETURN_IF_ERROR(
            right_input_->ResetForLeftInput(/*left_input=*/nullptr));
        next_right_tuple_idx_ = 0;
        if (next_right_tuple_idx_ == right_input_->GetNumMatchingTuples()) {
          done_ = true;
          return absl::OkStatus();
        }
        ZETASQL_ASSIGN_OR_RETURN(
            const bool right_tuple_joined,
            right_input_->DidMatchingTupleJoin(next_right_tuple_idx_));
        if (!right_tuple_joined) {
          return absl::OkStatus();
        }
        return AdvanceToNextRightTupleForLeftPadding();
    }
  }

  // Advances the next right tuple to the first one that did not join with any
  // left tuple. Requires 'left_padding_right_tuples_' to be true.
  absl::Status AdvanceToNextRightTupleForLeftPadding() {
    ZETASQL_RET_CHECK(left_padding_right_tuples_);
    ZETASQL_RET_CHECK(join_kind_ == JoinKind::kRightOuterJoin ||
              join_kind_ == JoinKind::kFullOuterJoin)
        << JoinOp::JoinKindToString(join_kind_);
    ZETASQL_RET_CHECK(next_left_tuple_.has_value());
    ZETASQL_RET_CHECK(next_left_tuple_.value() == nullptr);
    ZETASQL_RET_CHECK_GE(next_right_tuple_idx_, 0);
    for (++next_right_tuple_idx_;
         next_right_tuple_idx_ < right_input_->GetNumMatchingTuples();
         ++next_right_tuple_idx_) {
      ZETASQL_ASSIGN_OR_RETURN(
          const bool right_tuple_joined,
          right_input_->DidMatchingTupleJoin(next_right_tuple_idx_));
      if (!right_tuple_joined) {
        return absl::OkStatus();
      }
    }
    done_ = true;
    return absl::OkStatus();
  }

  // Does the following:
  // - If 'left_input' and 'right_input' are non-NULL, evaluates 'join_expr' on
  //   'left_input', 'right_input', and 'params'. If the result is not
  //   Bool(true), returns false. Otherwise, returns true and populates
  //   'output_tuple_' as described in the header comment for JoinOp
  //   (based on 'join_kind_').
  //
  // - If exactly one of 'left_input' and 'right_input' are NULL, always returns
  //   true. Also populates 'output_tuple_' as above, but using NULLs for the
  //   values for either 'left_outputs_' or 'right_outputs_' (depending on
  //   whether 'left_input' or 'right_input' is NULL). Returns an error if
  //   'join_kind' mis not compatible with this behavior as described in the
  //   header comment for JoinOp.
  //
  // - If both of 'left_input' and 'right_input' are NULL, returns an error.
  //   Also returns an error if 'output_tuple_' does not have enough slots for
  //   'join_kind_'.
  absl::StatusOr<bool> JoinTuples(const Tuple* left_input,
                                  const Tuple* right_input) {
    if (num_join_tuples_calls_ %
            absl::GetFlag(
                FLAGS_zetasql_call_verify_not_aborted_rows_period) ==
        0) {
      ZETASQL_RETURN_IF_ERROR(context_->VerifyNotAborted());
    }
    ++num_join_tuples_calls_;

    ZETASQL_RET_CHECK(left_input != nullptr || right_input != nullptr);
    if (left_input != nullptr && right_input != nullptr) {
      TupleSlot slot;
      absl::Status status;
      if (!join_expr_->EvalSimple(
              ConcatSpans(absl::Span<const TupleData* const>(params_),
                          {left_input->data, right_input->data}),
              context_, &slot, &status)) {
        return status;
      }
      if (slot.value() != Bool(true)) {
        return false;
      }
    }

    int next_slot_idx = 0;
    // Copy the left input to the output for everything except right outer and
    // full outer join.
    switch (join_kind_) {
      case JoinKind::kRightOuterJoin:
      case JoinKind::kFullOuterJoin:
        break;
      case JoinKind::kInnerJoin:
      case JoinKind::kCrossApply:
      case JoinKind::kOuterApply:
      case JoinKind::kLeftOuterJoin:
        ZETASQL_RET_CHECK(left_input != nullptr);
        ZETASQL_RET_CHECK_GE(output_tuple_.num_slots(),
                     left_input->schema->num_variables());
        for (int i = 0; i < left_input->schema->num_variables(); ++i) {
          *output_tuple_.mutable_slot(i) = left_input->data->slot(i);
        }
        next_slot_idx = left_input->schema->num_variables();
        break;
    }

    // Compute the left outputs and add them to the output, or pad with NULLs.
    ZETASQL_RET_CHECK_GE(output_tuple_.num_slots(),
                 next_slot_idx + left_outputs_.size());
    if (left_input == nullptr) {
      for (int i = 0; i < left_outputs_.size(); ++i) {
        output_tuple_.mutable_slot(next_slot_idx + i)
            ->SetValue(Value::Null(left_outputs_[i]->type()));
      }
    } else {
      for (int i = 0; i < left_outputs_.size(); ++i) {
        const ExprArg* arg = left_outputs_[i];

        TupleSlot* slot = output_tuple_.mutable_slot(next_slot_idx + i);
        absl::Status status;
        if (!arg->value_expr()->EvalSimple(
                ConcatSpans(absl::Span<const TupleData* const>(params_),
                            {left_input->data}),
                context_, slot, &status)) {
          return status;
        }
      }
    }
    next_slot_idx += left_outputs_.size();

    // Copy the right input to the output for inner join (and cross apply) and
    // right outer join.
    switch (join_kind_) {
      case JoinKind::kFullOuterJoin:
      case JoinKind::kLeftOuterJoin:
      case JoinKind::kOuterApply:
        break;
      case JoinKind::kInnerJoin:
      case JoinKind::kRightOuterJoin:
      case JoinKind::kCrossApply:
        ZETASQL_RET_CHECK(right_input != nullptr);
        ZETASQL_RET_CHECK_GE(output_tuple_.num_slots(),
                     next_slot_idx + right_input->schema->num_variables());
        for (int i = 0; i < right_input->schema->num_variables(); ++i) {
          *output_tuple_.mutable_slot(next_slot_idx + i) =
              right_input->data->slot(i);
        }
        next_slot_idx += right_input->schema->num_variables();
    }

    // Compute the right outputs and add them to the output, or pad with NULLs.
    ZETASQL_RET_CHECK_GE(output_tuple_.num_slots(),
                 next_slot_idx + right_outputs_.size());
    if (right_input == nullptr) {
      for (int i = 0; i < right_outputs_.size(); ++i) {
        output_tuple_.mutable_slot(next_slot_idx + i)
            ->SetValue(Value::Null(right_outputs_[i]->type()));
      }
    } else {
      for (int i = 0; i < right_outputs_.size(); ++i) {
        const ExprArg* arg = right_outputs_[i];

        TupleSlot* slot = output_tuple_.mutable_slot(next_slot_idx + i);
        absl::Status status;
        if (!arg->value_expr()->EvalSimple(
                ConcatSpans(absl::Span<const TupleData* const>(params_),
                            {right_input->data}),
                context_, slot, &status)) {
          return status;
        }
      }
    }
    next_slot_idx += right_outputs_.size();

    return true;
  }

  const JoinKind join_kind_;
  const std::vector<const TupleData*> params_;
  const ValueExpr* join_expr_;
  std::unique_ptr<TupleIterator> left_iter_;
  const std::vector<const ExprArg*> left_outputs_;

  std::unique_ptr<RightInputForJoin> right_input_;
  const std::vector<const ExprArg*> right_outputs_;

  std::unique_ptr<const TupleSchema> output_schema_;

  bool done_ = false;
  // The next left tuple to consider. Unset means uninitialized. NULL means
  // there are no more left tuples.
  absl::optional<const TupleData*> next_left_tuple_;
  // The next right tuple to consider. May be -1 to indicate right-padding with
  // NULLs.
  int64_t next_right_tuple_idx_ = 0;
  // If true, we are no longer iterating over left/right tuples trying to join
  // them. Instead, we are now iterating over right tuples looking for those
  // that did not join with any left tuples.
  bool left_padding_right_tuples_ = false;
  // If true, 'left_tuples_[next_left_tuple_idx_]' has joined with some right
  // tuple. Invalid if 'left_padding_right_tuples_' is true.
  bool left_tuple_joined_ = false;

  TupleData output_tuple_;

  absl::Status status_;

  EvaluationContext* context_;
  // The number of calls to JoinTuples(). Used to call
  // context_->VerifyNotAborted() periodicially.
  int64_t num_join_tuples_calls_ = 0;
};

}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> JoinOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  std::unique_ptr<RightInputForJoin> right_hand_side;
  switch (join_kind_) {
    case kInnerJoin:
    case kLeftOuterJoin:
    case kRightOuterJoin:
    case kFullOuterJoin: {
      auto tuples =
          absl::make_unique<TupleDataDeque>(context->memory_accountant());
      std::unique_ptr<TupleIterator> iter_for_right_debug_string;
      ZETASQL_RETURN_IF_ERROR(ExtractFromRelationalOp(right_input(), params, context,
                                              tuples.get(),
                                              &iter_for_right_debug_string));
      if (hash_join_equality_left_exprs().empty()) {
        right_hand_side = absl::make_unique<UncorrelatedRightInput>(
            right_input()->CreateOutputSchema(), std::move(tuples),
            std::move(iter_for_right_debug_string));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(
            right_hand_side,
            UncorrelatedHashedRightInput::Create(
                params, hash_join_equality_left_exprs(),
                hash_join_equality_right_exprs(),
                right_input()->CreateOutputSchema(), std::move(tuples),
                std::move(iter_for_right_debug_string), context));
      }
      break;
    }
    case kCrossApply:
    case kOuterApply: {
      right_hand_side = absl::make_unique<CorrelatedRightInput>(
          right_input(), params, context);
      break;
    }
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleIterator> left_iter,
      left_input()->CreateIterator(params, /*num_extra_slots=*/0, context));

  std::unique_ptr<TupleIterator> iter = absl::make_unique<JoinTupleIterator>(
      join_kind_, params, remaining_join_expr(), std::move(left_iter),
      left_outputs(), std::move(right_hand_side), right_outputs(),
      CreateOutputSchema(), num_extra_slots, context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> JoinOp::CreateOutputSchema() const {
  const std::unique_ptr<TupleSchema> left_schema =
      left_input()->CreateOutputSchema();
  const std::unique_ptr<TupleSchema> right_schema =
      right_input()->CreateOutputSchema();

  std::vector<VariableId> output_variables;
  output_variables.reserve(
      left_schema->num_variables() + left_outputs().size() +
      right_schema->num_variables() + right_outputs().size());

  // Left inputs are appended to the output for everything except right outer
  // and full outer join.
  switch (join_kind_) {
    case JoinOp::kRightOuterJoin:
    case JoinOp::kFullOuterJoin:
      break;
    case JoinOp::kInnerJoin:
    case JoinOp::kLeftOuterJoin:
    case JoinOp::kCrossApply:
    case JoinOp::kOuterApply:
      output_variables.insert(output_variables.end(),
                              left_schema->variables().begin(),
                              left_schema->variables().end());
      break;
  }

  // Left outputs are always present in the output.
  for (const ExprArg* left_output : left_outputs()) {
    output_variables.push_back(left_output->variable());
  }

  // Right inputs are appended to the output for inner join (and cross apply)
  // and right outer join.
  switch (join_kind_) {
    case JoinOp::kInnerJoin:
    case JoinOp::kRightOuterJoin:
    case JoinOp::kCrossApply:
      output_variables.insert(output_variables.end(),
                              right_schema->variables().begin(),
                              right_schema->variables().end());
      break;
    case JoinOp::kLeftOuterJoin:
    case JoinOp::kFullOuterJoin:
    case JoinOp::kOuterApply:
      break;
  }

  // Right outputs are always present in the output.
  for (const ExprArg* right_output : right_outputs()) {
    output_variables.push_back(right_output->variable());
  }

  return absl::make_unique<TupleSchema>(output_variables);
}

std::string JoinOp::GetIteratorDebugString(
    JoinKind join_kind, absl::string_view left_input_debug_string,
    absl::string_view right_input_debug_string) {
  const std::string join_string = JoinOp::JoinKindToString(join_kind);
  return absl::StrCat("JoinTupleIterator(", join_string,
                      ", left=", left_input_debug_string,
                      ", right=", right_input_debug_string, ")");
}

std::string JoinOp::IteratorDebugString() const {
  return GetIteratorDebugString(join_kind_, left_input()->IteratorDebugString(),
                                right_input()->IteratorDebugString());
}

std::string JoinOp::DebugInternal(const std::string& indent,
                                  bool verbose) const {
  static std::vector<std::string>* arg_names =
      new std::vector<std::string>{"left_outputs",
                                   "right_outputs",
                                   "hash_join_equality_left_exprs",
                                   "hash_join_equality_right_exprs",
                                   "remaining_condition",
                                   "left_input",
                                   "right_input"};
  const ArgPrintMode left_output_mode =
      (join_kind_ == kRightOuterJoin || join_kind_ == kFullOuterJoin) ? kN : k0;
  const ArgPrintMode right_output_mode =
      (join_kind_ == kInnerJoin || join_kind_ == kCrossApply) ? k0 : kN;
  return absl::StrCat(
      "JoinOp(", JoinKindToString(join_kind_),
      ArgDebugString(*arg_names,
                     {left_output_mode, right_output_mode, kN, kN, k1, k1, k1},
                     indent, verbose),
      ")");
}

JoinOp::JoinOp(
    JoinKind kind,
    std::vector<std::unique_ptr<ExprArg>> hash_join_equality_left_exprs,
    std::vector<std::unique_ptr<ExprArg>> hash_join_equality_right_exprs,
    std::unique_ptr<ValueExpr> remaining_condition,
    std::unique_ptr<RelationalOp> left, std::unique_ptr<RelationalOp> right,
    std::vector<std::unique_ptr<ExprArg>> left_outputs,
    std::vector<std::unique_ptr<ExprArg>> right_outputs)
    : join_kind_(kind) {
  SetArgs<ExprArg>(kLeftOutput, std::move(left_outputs));
  SetArgs<ExprArg>(kRightOutput, std::move(right_outputs));
  SetArgs<ExprArg>(kHashJoinEqualityLeftExprs,
                   std::move(hash_join_equality_left_exprs));
  SetArgs<ExprArg>(kHashJoinEqualityRightExprs,
                   std::move(hash_join_equality_right_exprs));
  SetArg(kRemainingCondition,
         absl::make_unique<ExprArg>(std::move(remaining_condition)));
  SetArg(kLeftInput, absl::make_unique<RelationalArg>(std::move(left)));
  SetArg(kRightInput, absl::make_unique<RelationalArg>(std::move(right)));
}

absl::Span<const ExprArg* const> JoinOp::hash_join_equality_left_exprs() const {
  return GetArgs<ExprArg>(kHashJoinEqualityLeftExprs);
}

absl::Span<ExprArg* const> JoinOp::mutable_hash_join_equality_left_exprs() {
  return GetMutableArgs<ExprArg>(kHashJoinEqualityLeftExprs);
}

absl::Span<const ExprArg* const> JoinOp::hash_join_equality_right_exprs()
    const {
  return GetArgs<ExprArg>(kHashJoinEqualityRightExprs);
}

absl::Span<ExprArg* const> JoinOp::mutable_hash_join_equality_right_exprs() {
  return GetMutableArgs<ExprArg>(kHashJoinEqualityRightExprs);
}

const ValueExpr* JoinOp::remaining_join_expr() const {
  return GetArg(kRemainingCondition)->node()->AsValueExpr();
}

ValueExpr* JoinOp::mutable_remaining_join_expr() {
  return GetMutableArg(kRemainingCondition)
      ->mutable_node()
      ->AsMutableValueExpr();
}

const RelationalOp* JoinOp::left_input() const {
  return GetArg(kLeftInput)->node()->AsRelationalOp();
}

RelationalOp* JoinOp::mutable_left_input() {
  return GetMutableArg(kLeftInput)->mutable_node()->AsMutableRelationalOp();
}

const RelationalOp* JoinOp::right_input() const {
  return GetArg(kRightInput)->node()->AsRelationalOp();
}

RelationalOp* JoinOp::mutable_right_input() {
  return GetMutableArg(kRightInput)->mutable_node()->AsMutableRelationalOp();
}

absl::Span<const ExprArg* const> JoinOp::left_outputs() const {
  return GetArgs<ExprArg>(kLeftOutput);
}

absl::Span<ExprArg* const> JoinOp::mutable_left_outputs() {
  return GetMutableArgs<ExprArg>(kLeftOutput);
}

absl::Span<const ExprArg* const> JoinOp::right_outputs() const {
  return GetArgs<ExprArg>(kRightOutput);
}

absl::Span<ExprArg* const> JoinOp::mutable_right_outputs() {
  return GetMutableArgs<ExprArg>(kRightOutput);
}

// -------------------------------------------------------
// ArrayScanOp
// -------------------------------------------------------

std::string ArrayScanOp::FieldArg::DebugInternal(const std::string& indent,
                                                 bool verbose) const {
  return absl::StrCat(ExprArg::DebugInternal(indent, verbose), " := field[",
                      field_index(), "]");
}

std::string ArrayScanOp::GetIteratorDebugString(
    absl::string_view array_debug_string) {
  return absl::StrCat("ArrayScanTupleIterator(", array_debug_string, ")");
}

absl::StatusOr<std::unique_ptr<ArrayScanOp>> ArrayScanOp::Create(
    const VariableId& element, const VariableId& position,
    absl::Span<const std::pair<VariableId, int>> fields,
    std::unique_ptr<ValueExpr> array) {
  ZETASQL_RET_CHECK(array->output_type()->IsArray());
  return absl::WrapUnique(
      new ArrayScanOp(element, position, fields, std::move(array)));
}

absl::Status ArrayScanOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return mutable_array_expr()->SetSchemasForEvaluation(params_schemas);
}

namespace {
// Returns one tuple per element of 'array_value'.
// - If 'element' is valid, the tuple includes a variable containing the array
//   element.
// - If 'position' is valid, the tuple includes a variable containing the
//   zero-based element position.
// - For each element in 'field_list', the tuple contains a variable containing
//   the corresponding field of the array element (which must be a struct if
//   'field_list' is non-empty). This functionality is useful for scanning an
//   table represented as an array (e.g., in the compliance tests).
class ArrayScanTupleIterator : public TupleIterator {
 public:
  ArrayScanTupleIterator(
      const Value& array_value, const VariableId& element,
      const VariableId& position,
      absl::Span<const ArrayScanOp::FieldArg* const> field_list,
      std::unique_ptr<TupleSchema> schema, int num_extra_slots,
      EvaluationContext* context)
      : array_value_(array_value),
        schema_(std::move(schema)),
        include_element_(element.is_valid()),
        include_position_(position.is_valid()),
        field_list_(field_list.begin(), field_list.end()),
        current_(schema_->num_variables() + num_extra_slots),
        context_(context) {
    context_->RegisterCancelCallback([this] { return Cancel(); });
  }

  ArrayScanTupleIterator(const ArrayScanTupleIterator&) = delete;
  ArrayScanTupleIterator& operator=(const ArrayScanTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return *schema_; }

  TupleData* Next() override {
    // Scanning a NULL array results in no output.
    if (array_value_.is_null()) {
      return nullptr;
    }

    if (next_element_idx_ == array_value_.num_elements()) {
      // Done iterating over the array. The output is non-deterministic if it
      // includes the position but the array is unordered and has more than one
      // element.
      if (include_position_ &&
          (InternalValue::GetOrderKind(array_value_) ==
           InternalValue::kIgnoresOrder) &&
          array_value_.num_elements() > 1) {
        context_->SetNonDeterministicOutput();
      }

      return nullptr;
    }

    if (cancelled_) {
      status_ = zetasql_base::CancelledErrorBuilder()
                << "ArrayScanTupleIterator was cancelled";
      return nullptr;
    }

    const Value& element = array_value_.element(next_element_idx_);
    for (int i = 0; i < field_list_.size(); ++i) {
      current_.mutable_slot(i)->SetValue(
          element.field(field_list_[i]->field_index()));
    }
    int next_value_idx = field_list_.size();
    if (include_element_) {
      current_.mutable_slot(next_value_idx)->SetValue(element);

      ++next_value_idx;
    }
    if (include_position_) {
      current_.mutable_slot(next_value_idx)->SetValue(Int64(next_element_idx_));
    }
    ++next_element_idx_;

    return &current_;
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return ArrayScanOp::GetIteratorDebugString(array_value_.DebugString());
  }

  absl::Status Cancel() {
    cancelled_ = true;
    return absl::OkStatus();
  }

 private:
  const Value array_value_;
  const std::unique_ptr<TupleSchema> schema_;
  const bool include_element_;
  const bool include_position_;
  const std::vector<const ArrayScanOp::FieldArg*> field_list_;
  TupleData current_;
  int next_element_idx_ = 0;
  bool cancelled_ = false;
  absl::Status status_;
  EvaluationContext* context_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> ArrayScanOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  TupleSlot array_slot;
  absl::Status status;
  if (!array_expr()->EvalSimple(params, context, &array_slot, &status))
    return status;
  std::unique_ptr<TupleIterator> iter =
      absl::make_unique<ArrayScanTupleIterator>(
          array_slot.value(), element(), position(), field_list(),
          CreateOutputSchema(), num_extra_slots, context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> ArrayScanOp::CreateOutputSchema() const {
  // Returns the variables to use for the scan of an
  // ArrayScanTupleIterator. These are the variables in field_list, followed by
  // 'element' (if it is valid), followed by 'position' (if it is valid). See
  // the class comment for ArrayScanTupleIterator for more details.
  std::vector<VariableId> vars;
  vars.reserve(field_list().size() + 2);
  for (const ArrayScanOp::FieldArg* field : field_list()) {
    vars.push_back(field->variable());
  }
  if (element().is_valid()) {
    vars.push_back(element());
  }
  if (position().is_valid()) {
    vars.push_back(position());
  }
  return absl::make_unique<TupleSchema>(vars);
}

std::string ArrayScanOp::IteratorDebugString() const {
  return GetIteratorDebugString("<array>");
}

std::string ArrayScanOp::DebugInternal(const std::string& indent,
                                       bool verbose) const {
  std::string indent_child = indent + kIndentSpace;
  std::string indent_input = indent + kIndentFork;
  const Type* element_type =
      array_expr()->output_type()->AsArray()->element_type();
  std::vector<std::string> fstr;
  for (auto ch : field_list()) {
    const std::string& field_name =
        element_type->AsStruct()->field(ch->field_index()).name;
    fstr.push_back(absl::StrCat(ch->DebugInternal(indent, verbose), ":",
                                field_name, ",", indent_input));
  }
  std::sort(fstr.begin(), fstr.end());
  return absl::StrCat(
      "ArrayScanOp(", indent_input,
      (!element().is_valid() ? ""
                             : absl::StrCat(GetArg(kElement)->DebugString(),
                                            " := element,", indent_input)),
      (!position().is_valid() ? ""
                              : absl::StrCat(GetArg(kPosition)->DebugString(),
                                             " := position,", indent_input)),
      absl::StrJoin(fstr, ""),
      "array: ", array_expr()->DebugInternal(indent_child, verbose), ")");
}

ArrayScanOp::ArrayScanOp(const VariableId& element, const VariableId& position,
                         absl::Span<const std::pair<VariableId, int>> fields,
                         std::unique_ptr<ValueExpr> array) {
  ZETASQL_CHECK(array->output_type()->IsArray());
  const Type* element_type = array->output_type()->AsArray()->element_type();
  SetArg(kElement, !element.is_valid()
                       ? nullptr
                       : absl::make_unique<ExprArg>(element, element_type));
  SetArg(kPosition, !position.is_valid() ? nullptr
                                         : absl::make_unique<ExprArg>(
                                               position, types::Int64Type()));
  SetArg(kArray, absl::make_unique<ExprArg>(std::move(array)));
  std::vector<std::unique_ptr<FieldArg>> field_args;
  field_args.reserve(fields.size());
  for (const auto& f : fields) {
    field_args.push_back(absl::make_unique<FieldArg>(
        f.first, f.second, element_type->AsStruct()->field(f.second).type));
  }
  SetArgs<FieldArg>(kField, std::move(field_args));
}

const ValueExpr* ArrayScanOp::array_expr() const {
  return GetArg(kArray)->value_expr();
}

ValueExpr* ArrayScanOp::mutable_array_expr() {
  return GetMutableArg(kArray)->mutable_value_expr();
}

absl::Span<const ArrayScanOp::FieldArg* const> ArrayScanOp::field_list() const {
  return GetArgs<FieldArg>(kField);
}

const VariableId& ArrayScanOp::element() const {
  static const VariableId* empty_str = new VariableId();
  return GetArg(kElement) != nullptr ? GetArg(kElement)->variable()
                                     : *empty_str;
}

const VariableId& ArrayScanOp::position() const {
  static const VariableId* empty_str = new VariableId();
  return GetArg(kPosition) != nullptr ? GetArg(kPosition)->variable()
                                      : *empty_str;
}

// -------------------------------------------------------
// DistinctOp
// -------------------------------------------------------
absl::StatusOr<std::unique_ptr<DistinctOp>> DistinctOp::Create(
    std::unique_ptr<RelationalOp> input,
    std::vector<std::unique_ptr<KeyArg>> keys, VariableId row_set_id) {
  return absl::WrapUnique(
      new DistinctOp(std::move(input), std::move(keys), row_set_id));
}

DistinctOp::DistinctOp(std::unique_ptr<RelationalOp> input,
                       std::vector<std::unique_ptr<KeyArg>> keys,
                       VariableId row_set_id) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
  SetArgs(kKeys, std::move(keys));
  SetArg(kRowSetId, MakeCppValueArgForRowSet(row_set_id));
}

const RelationalOp* DistinctOp::input() const {
  return GetArg(kInput)->relational_op();
}

RelationalOp* DistinctOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_relational_op();
}

absl::Span<const KeyArg* const> DistinctOp::keys() const {
  return GetArgs<KeyArg>(kKeys);
}

absl::Span<KeyArg* const> DistinctOp::mutable_keys() {
  return GetMutableArgs<KeyArg>(kKeys);
}

VariableId DistinctOp::row_set_id() const {
  return GetArg(kRowSetId)->variable();
}

absl::Status DistinctOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  ZETASQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));

  std::unique_ptr<TupleSchema> key_input_schema = input()->CreateOutputSchema();
  for (KeyArg* key : mutable_keys()) {
    ZETASQL_RETURN_IF_ERROR(key->mutable_value_expr()->SetSchemasForEvaluation(
        {key_input_schema.get()}));
  }

  return absl::OkStatus();
}

namespace {
// CppValueArg implementation representing a variable associated with a
// DistinctRowSet.
class DistinctRowSetValueArg : public CppValueArg {
 public:
  explicit DistinctRowSetValueArg(VariableId var_id)
      : CppValueArg(var_id, "DistinctRowSet") {}

  std::unique_ptr<CppValueBase> CreateValue(
      EvaluationContext* context) const override {
    return absl::make_unique<CppValue<DistinctRowSet>>(
        context->memory_accountant());
  }
};

// TupleIterator implementation for DistinctOp.
//
// For each row produced by 'input_iterator', evaluates a sequence of key
// expressions denoted by 'keys'. For each key-set produced, attempts to insert
// it into 'row_set'. If the key set is inserted successfully, emits that key
// set, followed by 'num_extra_slots' additional uninitialized slots. If the
// key-set duplicates an item already present in 'row_set', the key set is
// discarded. If an error occurs (for example, if inserting it into 'row_set'
// would exceed memory limits), the iteration fails and the error is propagated.
//
// 'output_schema' denotes the schema of the tuples emitted, and should contain
// one variable for each key.
class DistinctOpTupleIterator : public TupleIterator {
 public:
  DistinctOpTupleIterator(std::unique_ptr<TupleIterator> input_iterator,
                          DistinctRowSet* row_set,
                          std::unique_ptr<const TupleSchema> output_schema,
                          absl::Span<const KeyArg* const> keys,
                          EvaluationContext* context, int num_extra_slots)
      : input_iterator_(std::move(input_iterator)),
        row_set_(row_set),
        output_schema_(std::move(output_schema)),
        keys_(std::move(keys)),
        keys_data_(static_cast<int>(keys_.size()) + num_extra_slots),
        context_(context) {}

  const TupleSchema& Schema() const override { return *output_schema_; }

  TupleData* Next() override {
    while (true) {
      TupleData* input_data = input_iterator_->Next();
      if (input_data == nullptr) {
        status_ = input_iterator_->Status();
        return nullptr;
      }

      // Got a row; check if it's unique on the current DistinctRowSet.
      if (!EvaluateKeys(input_data)) {
        return nullptr;
      }

      // Generate a copy of the row data for the row set, ignoring any
      // "extra slots".
      auto keys_data_copy = absl::make_unique<TupleData>(keys_.size());
      for (int i = 0; i < keys_.size(); ++i) {
        keys_data_copy->mutable_slot(i)->CopyFromSlot(keys_data_.slot(i));
      }

      if (row_set_->InsertRowIfNotPresent(std::move(keys_data_copy),
                                          &status_)) {
        return &keys_data_;
      }
      if (!status_.ok()) {
        return nullptr;
      }
    }
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    return absl::StrCat("DistinctOp: ", input_iterator_->DebugString());
  }

 private:
  // Given a TupleData produced by <input_iterator_>, evaluates each of the
  // key expressions, storing the resuts in <keys_data_>. If unique
  // <keys_data_> will then be returned by Next(); if seen before, the current
  // row will be discarded.
  bool EvaluateKeys(TupleData* input_data) {
    for (int i = 0; i < keys_.size(); ++i) {
      const KeyArg* key = keys_.at(i);
      if (!key->value_expr()->EvalSimple(
              {input_data}, context_, keys_data_.mutable_slot(i), &status_)) {
        return false;
      }
    }
    return true;
  }

  const std::unique_ptr<TupleIterator> input_iterator_;
  DistinctRowSet* row_set_;
  const std::unique_ptr<const TupleSchema> output_schema_;
  absl::Span<const KeyArg* const> keys_;
  TupleData keys_data_;
  EvaluationContext* const context_;
  absl::Status status_;
};
}  // namespace

std::unique_ptr<CppValueArg> DistinctOp::MakeCppValueArgForRowSet(
    VariableId var) {
  return absl::make_unique<DistinctRowSetValueArg>(var);
}

absl::StatusOr<std::unique_ptr<TupleIterator>> DistinctOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<TupleIterator> input_iterator,
      input()->CreateIterator(params, /*num_extra_slots=*/0, context));

  DistinctRowSet* row_set =
      CppValue<DistinctRowSet>::Get(context->GetCppValue(row_set_id()));
  if (row_set == nullptr) {
    // This error means an outer tree node failed to set up the tuple data
    // input so that the row set id points to a valid DistinctRowSet.
    return zetasql_base::InternalErrorBuilder()
           << "DistinctOp unable to look up row set id " << row_set_id();
  }

  return absl::make_unique<DistinctOpTupleIterator>(
      std::move(input_iterator), row_set, CreateOutputSchema(), keys(), context,
      num_extra_slots);
}

// Returns the schema consisting of the variables for the keys, followed by
// the variables for the aggregators.
std::unique_ptr<TupleSchema> DistinctOp::CreateOutputSchema() const {
  std::vector<VariableId> variables;
  for (const KeyArg* key : keys()) {
    variables.push_back(key->variable());
  }
  return absl::make_unique<TupleSchema>(variables);
}

std::string DistinctOp::IteratorDebugString() const {
  return absl::StrCat("DistinctOp: ", input()->IteratorDebugString());
}

std::string DistinctOp::DebugInternal(const std::string& indent,
                                      bool verbose) const {
  return absl::StrCat("DistinctOp(",
                      ArgDebugString({"input", "keys", "row_set_id"},
                                     {k1, kN, k1}, indent, verbose),
                      ")");
}

// -------------------------------------------------------
// UnionAllOp
// -------------------------------------------------------

std::string UnionAllOp::GetIteratorDebugString(
    absl::Span<const std::string> input_iter_debug_strings) {
  return absl::StrCat("UnionAllTupleIterator(",
                      absl::StrJoin(input_iter_debug_strings, ","), ")");
}

static int rel_index(int i) { return i * 2; }
static int terms_index(int i) { return i * 2 + 1; }

absl::StatusOr<std::unique_ptr<UnionAllOp>> UnionAllOp::Create(
    std::vector<Input> inputs) {
  ZETASQL_RET_CHECK(!inputs.empty());
  for (int i = 0; i < inputs.size(); ++i) {
    // Check that all output variable names agree.
    ZETASQL_RET_CHECK_EQ(inputs[i].second.size(), inputs[0].second.size());
    for (int j = 0; j < inputs[i].second.size(); ++j) {
      ZETASQL_RET_CHECK_EQ(inputs[i].second[j]->variable(),
                   inputs[0].second[j]->variable());
    }
  }
  return absl::WrapUnique(new UnionAllOp(std::move(inputs)));
}

absl::Status UnionAllOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  for (int i = 0; i < num_rel(); ++i) {
    RelationalOp* rel = mutable_rel(i);
    ZETASQL_RETURN_IF_ERROR(rel->SetSchemasForEvaluation(params_schemas));
    const std::unique_ptr<const TupleSchema> schema = rel->CreateOutputSchema();
    for (ExprArg* value : mutable_values(i)) {
      ZETASQL_RETURN_IF_ERROR(value->mutable_value_expr()->SetSchemasForEvaluation(
          ConcatSpans(params_schemas, {schema.get()})));
    }
  }
  return absl::OkStatus();
}

namespace {
// Iterates over the tuples from 'iters'. For each one, produces an output tuple
// with values given by evaluating the ValueExprs in the corresponding element
// of 'values'.
class UnionAllTupleIterator : public TupleIterator {
 public:
  UnionAllTupleIterator(
      absl::Span<const TupleData* const> params,
      absl::Span<const absl::Span<const ExprArg* const>> values,
      std::unique_ptr<TupleSchema> output_schema, int num_extra_slots,
      std::vector<std::unique_ptr<TupleIterator>> iters,
      EvaluationContext* context)
      : params_(params.begin(), params.end()),
        values_(values.begin(), values.end()),
        output_schema_(std::move(output_schema)),
        iters_(std::move(iters)),
        data_(output_schema_->num_variables() + num_extra_slots),
        context_(context) {}

  UnionAllTupleIterator(const UnionAllTupleIterator&) = delete;
  UnionAllTupleIterator& operator=(const UnionAllTupleIterator&) = delete;

  const TupleSchema& Schema() const override { return *output_schema_; }

  TupleData* Next() override {
    const TupleData* next_input = GetNextInput();
    if (next_input == nullptr) {
      // 'status_' already updated.
      return nullptr;
    }

    absl::Span<const ExprArg* const> values = values_[iter_idx_];
    if (values.size() != output_schema_->num_variables()) {
      status_ = zetasql_base::InternalErrorBuilder()
                << "UnionAllTupleIterator::Next() expected "
                << output_schema_->num_variables() << " values, but found "
                << values.size();
      return nullptr;
    }

    for (int i = 0; i < values.size(); ++i) {
      TupleSlot* slot = data_.mutable_slot(i);
      absl::Status status;
      if (!values[i]->value_expr()->EvalSimple(
              ConcatSpans(absl::Span<const TupleData* const>(params_),
                          {next_input}),
              context_, slot, &status)) {
        status_ = status;
        return nullptr;
      }
    }

    return &data_;
  }

  absl::Status Status() const override { return status_; }

  std::string DebugString() const override {
    std::vector<std::string> iter_strings;
    iter_strings.reserve(iters_.size());
    for (const std::unique_ptr<TupleIterator>& iter : iters_) {
      iter_strings.push_back(iter->DebugString());
    }
    return UnionAllOp::GetIteratorDebugString(iter_strings);
  }

 private:
  // Iterates through the remaining iterators to return the first remaining
  // tuple. Returns NULL and updates 'status_' if there are no more tuples.
  const TupleData* GetNextInput() {
    while (true) {
      TupleIterator* iter = iters_[iter_idx_].get();
      TupleData* next = iter->Next();
      if (next != nullptr) return next;

      absl::Status iter_status = iter->Status();
      if (!iter_status.ok()) {
        status_ = iter_status;
        return nullptr;
      }
      ++iter_idx_;
      if (iter_idx_ == iters_.size()) {
        return nullptr;
      }
    }
  }

  const std::vector<const TupleData*> params_;
  const std::vector<absl::Span<const ExprArg* const>> values_;
  const std::unique_ptr<TupleSchema> output_schema_;

  std::vector<std::unique_ptr<TupleIterator>> iters_;
  int iter_idx_ = 0;  // Index of the current iterator in 'iters_'.
  TupleData data_;
  absl::Status status_;
  EvaluationContext* context_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> UnionAllOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  std::vector<absl::Span<const ExprArg* const>> tuple_values;
  tuple_values.reserve(num_rel());
  for (int i = 0; i < num_rel(); ++i) {
    tuple_values.push_back(values(i));
  }

  std::vector<std::unique_ptr<TupleIterator>> iters;
  iters.reserve(num_rel());
  for (int i = 0; i < num_rel(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<TupleIterator> iter,
        rel(i)->CreateIterator(params, /*num_extra_slots=*/0, context));
    iters.push_back(std::move(iter));
  }

  std::unique_ptr<TupleIterator> iter =
      absl::make_unique<UnionAllTupleIterator>(
          params, tuple_values, CreateOutputSchema(), num_extra_slots,
          std::move(iters), context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> UnionAllOp::CreateOutputSchema() const {
  std::vector<VariableId> variables;
  variables.reserve(num_variables());
  for (int i = 0; i < num_variables(); ++i) {
    variables.push_back(variable(i));
  }
  return absl::make_unique<TupleSchema>(variables);
}

std::string UnionAllOp::IteratorDebugString() const {
  std::vector<std::string> iter_strings;
  iter_strings.reserve(num_variables());
  for (int i = 0; i < num_variables(); ++i) {
    iter_strings.push_back(rel(i)->IteratorDebugString());
  }
  return GetIteratorDebugString(iter_strings);
}

std::string UnionAllOp::DebugInternal(const std::string& indent,
                                      bool verbose) const {
  std::vector<std::string> srels;
  for (int i = 0; i < num_rel(); i++) {
    std::vector<std::string> sterm;
    std::string indent_input = indent + kIndentFork;
    std::string indent_child = indent;
    if (i < num_rel() - 1) {
      absl::StrAppend(&indent_child, kIndentBar);
    } else {
      // No tree line is required beside the last child.
      absl::StrAppend(&indent_child, kIndentSpace);
    }
    for (auto ch : values(i)) {
      sterm.push_back(indent_child + kIndentFork +
                      ch->DebugInternal(indent_child, verbose));
    }
    std::string srel;
    absl::StrAppend(&srel, indent_input, "rel[", i, "]: {");
    absl::StrAppend(&srel, absl::StrJoin(sterm, ","), ",");
    absl::StrAppend(&srel, indent_child + kIndentFork, "input: ",
                    rel(i)->DebugInternal(indent_child + kIndentSpace, verbose),
                    "}");
    srels.push_back(srel);
  }
  return absl::StrCat("UnionAllOp(", absl::StrJoin(srels, ","), ")");
}

UnionAllOp::UnionAllOp(std::vector<Input> inputs) : num_rel_(inputs.size()) {
  for (int i = 0; i < inputs.size(); i++) {
    SetArg(rel_index(i),
           absl::make_unique<RelationalArg>(std::move(inputs[i].first)));
    SetArgs<ExprArg>(terms_index(i), std::move(inputs[i].second));
  }
}

absl::Span<const ExprArg* const> UnionAllOp::values(int i) const {
  return GetArgs<ExprArg>(terms_index(i));
}

absl::Span<ExprArg* const> UnionAllOp::mutable_values(int i) {
  return GetMutableArgs<ExprArg>(terms_index(i));
}

const VariableId& UnionAllOp::variable(int i) const {
  return values(0)[i]->variable();
}

int UnionAllOp::num_variables() const { return values(0).size(); }

const RelationalOp* UnionAllOp::rel(int i) const {
  return GetArg(rel_index(i))->node()->AsRelationalOp();
}

RelationalOp* UnionAllOp::mutable_rel(int i) {
  return GetMutableArg(rel_index(i))->mutable_node()->AsMutableRelationalOp();
}

// -------------------------------------------------------
// LoopOp
// -------------------------------------------------------
absl::StatusOr<std::unique_ptr<LoopOp>> LoopOp::Create(
    std::vector<std::unique_ptr<ExprArg>> initial_assign,
    std::unique_ptr<RelationalOp> body,
    std::vector<std::unique_ptr<ExprArg>> loop_assign) {
  // Make sure all variable targets of <loop_assign> are in <initial_assign>
  // and populate loop_assign_indexes_.
  absl::flat_hash_map<VariableId, int> varid_to_index;
  for (const std::unique_ptr<ExprArg>& arg : initial_assign) {
    ZETASQL_RET_CHECK(!varid_to_index.contains(arg->variable()))
        << "Duplicate variable " << arg->variable() << " in <initial_assign>";
    varid_to_index[arg->variable()] = static_cast<int>(varid_to_index.size());
  }

  std::vector<int> loop_assign_indexes;
  loop_assign_indexes.reserve(loop_assign.size());
  for (const auto& arg : loop_assign) {
    auto it = varid_to_index.find(arg->variable());
    ZETASQL_RET_CHECK(it != varid_to_index.end())
        << "Variable " << arg->variable()
        << " in <loop_assign>, but not <initial_assign>";
    loop_assign_indexes.push_back(it->second);
  }
  return absl::WrapUnique(new LoopOp(std::move(initial_assign), std::move(body),
                                     std::move(loop_assign),
                                     std::move(loop_assign_indexes)));
}

LoopOp::LoopOp(std::vector<std::unique_ptr<ExprArg>> initial_assign,
               std::unique_ptr<RelationalOp> body,
               std::vector<std::unique_ptr<ExprArg>> loop_assign,
               std::vector<int> loop_assign_indexes)
    : loop_assign_indexes_(std::move(loop_assign_indexes)) {
  SetArgs(kInitialAssign, std::move(initial_assign));
  SetArg(kBody, std::make_unique<RelationalArg>(std::move(body)));
  SetArgs(kLoopAssign, std::move(loop_assign));
}

absl::StatusOr<int> LoopOp::GetVariableIndexFromLoopAssignIndex(int i) const {
  ZETASQL_RET_CHECK_GE(i, 0);
  ZETASQL_RET_CHECK_LT(i, loop_assign_indexes_.size());
  return loop_assign_indexes_.at(i);
}

std::string LoopOp::DebugInternal(const std::string& indent,
                                  bool verbose) const {
  return absl::StrCat("LoopOp(",
                      ArgDebugString({"initial_assign", "body", "loop_assign"},
                                     {kN, k1, kN}, indent, verbose),
                      ")");
}

const RelationalOp* LoopOp::body() const {
  return GetArg(kBody)->relational_op();
}

RelationalOp* LoopOp::mutable_body() {
  return GetMutableArg(kBody)->mutable_relational_op();
}

int LoopOp::num_variables() const {
  return GetArgs<ExprArg>(kInitialAssign).size();
}

VariableId LoopOp::variable(int i) const {
  return GetArgs<ExprArg>(kInitialAssign).at(i)->variable();
}

const ValueExpr* LoopOp::initial_assign_expr(int i) const {
  return GetArgs<ExprArg>(kInitialAssign).at(i)->value_expr();
}

ValueExpr* LoopOp::mutable_initial_assign_expr(int i) {
  return GetMutableArgs<ExprArg>(kInitialAssign).at(i)->mutable_value_expr();
}

int LoopOp::num_loop_assign() const {
  return GetArgs<ExprArg>(kLoopAssign).size();
}

const ValueExpr* LoopOp::loop_assign_expr(int i) const {
  return GetArgs<ExprArg>(kLoopAssign).at(i)->value_expr();
}

ValueExpr* LoopOp::mutable_loop_assign_expr(int i) {
  return GetMutableArgs<ExprArg>(kLoopAssign).at(i)->mutable_value_expr();
}

absl::Status LoopOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  std::vector<const TupleSchema*> all_schemas(params_schemas.begin(),
                                              params_schemas.end());

  // During initial assignment expressions, allow each variable to be accessed
  // only after it has been assigned to.
  auto loop_variables_schema =
      absl::make_unique<TupleSchema>(std::vector<VariableId>{});
  all_schemas.push_back(loop_variables_schema.get());
  for (int i = 0; i < num_variables(); ++i) {
    ZETASQL_RETURN_IF_ERROR(
        mutable_initial_assign_expr(i)->SetSchemasForEvaluation(all_schemas));
    loop_variables_schema->AddVariable(variable(i));
  }

  // During the loop itself, all variables are accessible to the body and the
  // loop assignment expressions.
  ZETASQL_RETURN_IF_ERROR(mutable_body()->SetSchemasForEvaluation(all_schemas));
  for (int i = 0; i < num_loop_assign(); ++i) {
    ZETASQL_RETURN_IF_ERROR(
        mutable_loop_assign_expr(i)->SetSchemasForEvaluation(all_schemas));
  }

  return absl::OkStatus();
}

std::unique_ptr<TupleSchema> LoopOp::CreateOutputSchema() const {
  return body()->CreateOutputSchema();
}

std::string LoopOp::IteratorDebugString() const {
  return absl::StrCat("LoopTupleIterator: any_rows = false, inner iterator: ",
                      body()->IteratorDebugString());
}

namespace {
// Tuple iterator for LoopOp.
//
// Iteration begins by iterating through the rows of the body, passing each
// returned tuple on through to the caller. The body iterator is created with
// each loop variable set to its corresponding value from evaluating the
// expressions in op_->initial_assign().
//
// When a full pass through the body iterator is complete, we stop if no tuples
// have been produced. Otherwise, we advance the iteration by evaluating each
// expression in op_->loop_assign() and using the results to update loop
// variables. The new set of loop variables is used to create a new body
// iterator for the next iteration. The iteration continues until the body
// iterator eventually either fails or completes without producing any new
// tuples.
//
// While LoopTupleOperator does not contain any explicit checks to cut off
// runaway iteration, it is expected that inner evaluations will eventually
// start failing when memory limits are reached as a result of producing too
// many tuples in total.
class LoopTupleIterator : public TupleIterator {
 public:
  static absl::StatusOr<std::unique_ptr<LoopTupleIterator>> Create(
      const LoopOp* op, absl::Span<const TupleData* const> params,
      int num_extra_slots, EvaluationContext* context) {
    return absl::WrapUnique(
        new LoopTupleIterator(op, params, num_extra_slots, context));
  }

  const TupleSchema& Schema() const override { return *output_schema_; }

  absl::Status Status() const override { return status_; }

  TupleData* Next() override {
    absl::StatusOr<TupleData*> status_or_data = NextInternal();
    status_ = status_or_data.status();
    TupleData* data = status_.ok() ? *status_or_data : nullptr;
    if (data == nullptr) {
      // Free body iterator, including result from previous call to Next().
      iter_.reset();
    }
    return data;
  }

  std::string DebugString() const override {
    return absl::StrCat("LoopTupleIterator: inner iterator: ",
                        (iter_ != nullptr ? iter_->DebugString() : "nullptr"));
  }

 private:
  LoopTupleIterator(const LoopOp* op, absl::Span<const TupleData* const> params,
                    int num_extra_slots, EvaluationContext* context)
      : op_(op),
        loop_variables_(absl::make_unique<TupleData>(op->num_variables())),
        params_and_loop_variables_(
            ConcatSpans(absl::Span<const TupleData* const>(params),
                        {loop_variables_.get()})),
        num_extra_slots_(num_extra_slots),
        context_(context),
        output_schema_(op_->CreateOutputSchema()) {}

  // Returns the next tuple in the enumeration(), nullptr if enumeration is
  // complete, or a failed status if an error occurs.
  //
  // Invoked by Next(); caller is responsible for updating status_ based on the
  // result.
  absl::StatusOr<TupleData*> NextInternal() {
    TupleData* data = nullptr;
    if (iter_ == nullptr) {
      // We are beginning the first iteration.
      // Initialize loop variables by evaluating op_->initial_assign_expr().
      for (int i = 0; i < op_->num_variables(); ++i) {
        absl::Status status;
        if (!op_->initial_assign_expr(i)->EvalSimple(
                params_and_loop_variables_, context_,
                loop_variables_->mutable_slot(i), &status)) {
          return status;
        }
      }
      ZETASQL_ASSIGN_OR_RETURN(data, BeginNextIteration());

      if (!first_iteration_ || data != nullptr) {
        return data;
      }
      if (first_iteration_) {
        first_iteration_ = false;
      }
    }
    // An iteration is already in progress; fetch the next tuple.
    data = iter_->Next();
    if (data == nullptr) {
      // The current iteration is over; update variables and begin the next
      // one.
      ZETASQL_RETURN_IF_ERROR(UpdateLoopVariables());
      ZETASQL_ASSIGN_OR_RETURN(data, BeginNextIteration());
    }
    return data;
  }

  // Updates loop variables after each loop iteration in preparation for the
  // next one by evaluating each expression in op_->loop_assign_expr().
  absl::Status UpdateLoopVariables() {
    for (int i = 0; i < op_->num_loop_assign(); ++i) {
      ZETASQL_ASSIGN_OR_RETURN(int var_index,
                       op_->GetVariableIndexFromLoopAssignIndex(i));
      absl::Status status;
      if (!op_->loop_assign_expr(i)->EvalSimple(
              params_and_loop_variables_, context_,
              loop_variables_->mutable_slot(var_index), &status)) {
        return status;
      }
    }
    return absl::OkStatus();
  }

  // Returns the first row of the next iteration of the loop. The caller must
  // set up loop variables appropriately before calling this method.
  //
  // Returns:
  //  - A non-null TupleData if the next iteration contains at least one tuple
  //  - nullptr if the next iteration is empty (and terminates the loop)
  //  - An error status if an error occurred.
  absl::StatusOr<TupleData*> BeginNextIteration() {
    // Create a new iterator for the body
    ZETASQL_ASSIGN_OR_RETURN(iter_,
                     op_->body()->CreateIterator(params_and_loop_variables_,
                                                 num_extra_slots_, context_));

    // Fetch the first TupleData of the next iteration
    TupleData* data = iter_->Next();
    if (data == nullptr) {
      ZETASQL_RETURN_IF_ERROR(iter_->Status());
    }
    return data;
  }

  // The underlying LoopOp which produced this iterator.
  const LoopOp* op_;

  // Additional TupleData to store loop variables.
  const std::unique_ptr<TupleData> loop_variables_;

  // All TupleData parameters to be passed into child expressions/iterators.
  // Contains a copy of params passed to constructor with <variables_> appended.
  const std::vector<const TupleData*> params_and_loop_variables_;

  // Passed down into CreateIterator() on the loop body.
  const int num_extra_slots_;

  // EvaluationContext, passed down into child evaluations.
  EvaluationContext* const context_;

  // Output schema.
  const std::unique_ptr<const TupleSchema> output_schema_;

  // Inner iterator representing the current progress through the loop body.
  // Initialized in Init() and replaced with a new iterator inside Advance().
  // Once Init() completes, a null value indicates that the last iteration of
  // the loop is complete.
  std::unique_ptr<TupleIterator> iter_;

  // Current status of loop iteration.
  absl::Status status_;

  bool first_iteration_ = true;
};

}  // namespace

absl::StatusOr<std::unique_ptr<TupleIterator>> LoopOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  return LoopTupleIterator::Create(this, params, num_extra_slots, context);
}

// -------------------------------------------------------
// RootOp
// -------------------------------------------------------

absl::StatusOr<std::unique_ptr<RootOp>> RootOp::Create(
    std::unique_ptr<RelationalOp> input, std::unique_ptr<RootData> root_data) {
  return absl::WrapUnique(new RootOp(std::move(input), std::move(root_data)));
}

absl::Status RootOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  return mutable_input()->SetSchemasForEvaluation(params_schemas);
}

absl::StatusOr<std::unique_ptr<TupleIterator>> RootOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  return input()->CreateIterator(params, num_extra_slots, context);
}

std::unique_ptr<TupleSchema> RootOp::CreateOutputSchema() const {
  return input()->CreateOutputSchema();
}

std::string RootOp::IteratorDebugString() const {
  return input()->IteratorDebugString();
}

std::string RootOp::DebugInternal(const std::string& indent,
                                  bool verbose) const {
  return absl::StrCat("RootOp(",
                      ArgDebugString({"input"}, {k1}, indent, verbose), ")");
}

RootOp::RootOp(std::unique_ptr<RelationalOp> input,
               std::unique_ptr<RootData> root_data)
    : root_data_(std::move(root_data)) {
  SetArg(kInput, absl::make_unique<RelationalArg>(std::move(input)));
}

const RelationalOp* RootOp::input() const {
  return GetArg(kInput)->node()->AsRelationalOp();
}

RelationalOp* RootOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_node()->AsMutableRelationalOp();
}
}  // namespace zetasql
