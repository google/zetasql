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

#include "zetasql/reference_impl/functions/map.h"

#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/errors.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/map_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/tuple.h"
#include "absl/algorithm/container.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

class MapFromArrayFunction : public SimpleBuiltinScalarFunction {
 public:
  MapFromArrayFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 1);
    const Value& array_arg = args[0];

    ZETASQL_RET_CHECK(array_arg.type()->IsArray());
    const ArrayType* array_type = array_arg.type()->AsArray();

    ZETASQL_RET_CHECK(array_type->element_type()->IsStruct());
    const StructType* struct_type = array_type->element_type()->AsStruct();

    ZETASQL_RET_CHECK_EQ(struct_type->fields().size(), 2);
    TypeFactory type_factory;
    ZETASQL_ASSIGN_OR_RETURN(const Type* map_type,
                     type_factory.MakeMapType(struct_type->fields()[0].type,
                                              struct_type->fields()[1].type));

    if (array_arg.is_null()) {
      return Value::Null(map_type);
    }

    std::vector<std::pair<const Value, const Value>> map_entries;
    map_entries.reserve(array_arg.elements().size());
    for (const auto& struct_val : array_arg.elements()) {
      map_entries.push_back(
          std::make_pair(struct_val.fields()[0], struct_val.fields()[1]));
    }

    return Value::MakeMap(map_type, std::move(map_entries));
  }
};

// Defines implementation for kMapEntriesSorted and kMapEntriesUnsorted. The
// functions are nearly identical, since the internal map representation in the
// reference implementation is always sorted. The one differentiation is that
// the kMapEntriesUnsorted output array does not preserve order.
class MapEntriesFunction : public SimpleBuiltinScalarFunction {
 public:
  MapEntriesFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 1);
    const Value& map_arg = args[0];

    ZETASQL_RET_CHECK(map_arg.type()->IsMap());
    ZETASQL_RET_CHECK(output_type()->IsArray());
    const ArrayType* output_array_type = output_type()->AsArray();

    ZETASQL_RET_CHECK(output_array_type->element_type()->IsStruct());
    const StructType* output_array_struct_element_type =
        output_array_type->element_type()->AsStruct();
    ZETASQL_RET_CHECK(output_array_struct_element_type->fields().size() == 2);

    if (map_arg.is_null()) {
      return Value::Null(output_array_type);
    }

    std::vector<Value> struct_array_entries;
    struct_array_entries.reserve(map_arg.num_elements());

    // No additional sorting necessary, since reference impl MAP<> data is
    // sorted.
    for (const auto& map_val : map_arg.map_entries()) {
      ZETASQL_ASSIGN_OR_RETURN(const Value struct_val,
                       Value::MakeStruct(output_array_struct_element_type,
                                         {map_val.first, map_val.second}));
      struct_array_entries.push_back(struct_val);
    }

    // Output array has a known order if:
    //  - the function is sorted, or
    //  - the array's order is unambiguous because it has 0 or 1 elements.
    InternalValue::OrderPreservationKind array_order_kind;
    switch (kind()) {
      case FunctionKind::kMapEntriesUnsorted:
        array_order_kind = map_arg.num_elements() > 1
                               ? InternalValue::kIgnoresOrder
                               : InternalValue::kPreservesOrder;
        break;
      case FunctionKind::kMapEntriesSorted:
        array_order_kind = InternalValue::kPreservesOrder;
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unknown map function kind. Expected "
                            "kMapEntriesSorted or kMapEntriesUnsorted.";
    }

    return InternalValue::MakeArray(output_array_type, array_order_kind,
                                    std::move(struct_array_entries));
  }
};

// Checks that `type` is equivalent to the key type of `map_type`.
inline absl::Status CheckTypeEquivalentToMapKeyType(const Type* type,
                                                    const MapType* map_type) {
  ZETASQL_RET_CHECK(type->Equivalent(map_type->key_type()))
      << "Map key type mismatch. Expected: "
      << map_type->key_type()->DebugString()
      << " but got: " << type->DebugString();
  return absl::OkStatus();
}

// Returns the value associated with `key` in `map`. If `key` is not present,
// return `result_if_missing`, or error if `result_if_missing` is null.
absl::StatusOr<Value> ValueLookupImpl(
    const Value& map, const Value& key,
    const Value* result_if_missing = nullptr) {
  ZETASQL_RET_CHECK(map.type()->IsMap()) << map.type()->DebugString();
  const MapType* map_type = map.type()->AsMap();

  if (map.is_null()) {
    return Value::Null(map_type->value_type());
  }

  ZETASQL_RETURN_IF_ERROR(CheckTypeEquivalentToMapKeyType(key.type(), map_type));
  if (result_if_missing != nullptr) {
    ZETASQL_RET_CHECK(result_if_missing->type()->Equivalent(map_type->value_type()))
        << "Map value type mismatch. Expected: "
        << map_type->value_type()->DebugString()
        << " but got: " << result_if_missing->type()->DebugString();
  }

  auto it = map.map_entries().find(key);
  if (it != map.map_entries().end()) {
    return it->second;
  }

  if (result_if_missing == nullptr) {
    return MakeEvalError() << "Key not found in map: "
                           << key.Format(/*print_top_level_type=*/false);
  }
  return *result_if_missing;
}

class MapGetFunction : public SimpleBuiltinScalarFunction {
 public:
  MapGetFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3) << args.size();

    const Value& result_if_missing =
        args.size() == 3 ? args[2] : Value::Null(output_type());
    return ValueLookupImpl(args[0], args[1], &result_if_missing);
  }
};

class MapSubscriptFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit MapSubscriptFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
    const Value result_if_missing = Value::Null(output_type());
    return ValueLookupImpl(args[0], args[1], &result_if_missing);
  }
};

class MapSubscriptWithKeyFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit MapSubscriptWithKeyFunction(FunctionKind kind,
                                       const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
    return ValueLookupImpl(args[0], args[1]);
  }
};

class MapContainsKeyFunction : public SimpleBuiltinScalarFunction {
 public:
  MapContainsKeyFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 2) << args.size();
    const Value& map = args[0];
    const Value& key = args[1];

    if (map.is_null()) {
      return Value::Null(output_type());
    }

    ZETASQL_RET_CHECK(map.type()->IsMap()) << map.type()->DebugString();
    const MapType* map_type = map.type()->AsMap();
    ZETASQL_RETURN_IF_ERROR(CheckTypeEquivalentToMapKeyType(key.type(), map_type));

    return Value::Bool(map.map_entries().contains(key));
  }
};

// Defines which part of the map entry to return for functions returning a
// list of map keys or values.
enum class KeyOrValueSelector {
  kUseKey,
  kUseValue,
};

// Defines which part of the map entry to order by for functions returning a
// list of map keys or values.
enum class OrderBy {
  kNone,
  kByKey,
  kByValue,
};

absl::StatusOr<Value> MapKeysOrValuesListFunctionImpl(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context, const Type* output_type, OrderBy order_by,
    KeyOrValueSelector use_key_or_value) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  const Value& map = args[0];

  if (map.is_null()) {
    return Value::Null(output_type);
  }

  ZETASQL_RET_CHECK(map.type()->IsMap()) << map.type()->DebugString();
  ZETASQL_RET_CHECK(output_type->IsArray()) << output_type->DebugString();

  std::vector<Value> result;
  result.reserve(map.num_elements());
  if (use_key_or_value == KeyOrValueSelector::kUseKey) {
    for (const auto& [key, unused] : map.map_entries()) {
      result.push_back(key);
    }
  } else {
    for (const auto& [unused, value] : map.map_entries()) {
      result.push_back(value);
    }
  }

  if (order_by != OrderBy::kNone) {
    std::string no_ordering_type;
    ZETASQL_RET_CHECK(output_type->AsArray()->element_type()->SupportsOrdering(
        context->GetLanguageOptions(), &no_ordering_type))
        << no_ordering_type;
    // MAP is always ordered by key in the reference implementation, so an
    // additional sort operation is only necessary when ordering by value.
    if (order_by == OrderBy::kByValue) {
      absl::c_sort(result, [](auto& a, auto& b) { return a.LessThan(b); });
    }
  }

  // Output array has a known order if:
  //  - the function is sorted, or
  //  - the array's order is unambiguous because it has 0 or 1 elements.
  InternalValue::OrderPreservationKind array_order_kind =
      (map.num_elements() <= 1 || order_by != OrderBy::kNone)
          ? InternalValue::kPreservesOrder
          : InternalValue::kIgnoresOrder;
  return InternalValue::MakeArray(output_type->AsArray(), array_order_kind,
                                  std::move(result));
}

class MapKeysSortedFunction : public SimpleBuiltinScalarFunction {
 public:
  MapKeysSortedFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    return MapKeysOrValuesListFunctionImpl(
        params, args, context, output_type(),
        /*order_by=*/OrderBy::kByKey,
        /*use_key_or_value=*/KeyOrValueSelector::kUseKey);
  }
};

class MapKeysUnsortedFunction : public SimpleBuiltinScalarFunction {
 public:
  MapKeysUnsortedFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    return MapKeysOrValuesListFunctionImpl(
        params, args, context, output_type(),
        /*order_by=*/OrderBy::kNone,
        /*use_key_or_value=*/KeyOrValueSelector::kUseKey);
  }
};

class MapValuesSortedFunction : public SimpleBuiltinScalarFunction {
 public:
  MapValuesSortedFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    return MapKeysOrValuesListFunctionImpl(
        params, args, context, output_type(),
        /*order_by=*/OrderBy::kByValue,
        /*use_key_or_value=*/KeyOrValueSelector::kUseValue);
  }
};

class MapValuesUnsortedFunction : public SimpleBuiltinScalarFunction {
 public:
  MapValuesUnsortedFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    return MapKeysOrValuesListFunctionImpl(
        params, args, context, output_type(),
        /*order_by=*/OrderBy::kNone,
        /*use_key_or_value=*/KeyOrValueSelector::kUseValue);
  }
};

class MapValuesSortedByKeyFunction : public SimpleBuiltinScalarFunction {
 public:
  MapValuesSortedByKeyFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    return MapKeysOrValuesListFunctionImpl(
        params, args, context, output_type(),
        /*order_by=*/OrderBy::kByKey,
        /*use_key_or_value=*/KeyOrValueSelector::kUseValue);
  }
};

}  // namespace

void RegisterBuiltinMapFunctions() {
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapFromArray},
      [](FunctionKind kind, const Type* output_type) {
        return new MapFromArrayFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapEntriesSorted, FunctionKind::kMapEntriesUnsorted},
      [](FunctionKind kind, const Type* output_type) {
        return new MapEntriesFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapGet}, [](FunctionKind kind, const Type* output_type) {
        return new MapGetFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapSubscript},
      [](FunctionKind kind, const Type* output_type) {
        return new MapSubscriptFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapSubscriptWithKey},
      [](FunctionKind kind, const Type* output_type) {
        return new MapSubscriptWithKeyFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapContainsKey},
      [](FunctionKind kind, const Type* output_type) {
        return new MapContainsKeyFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapKeysSorted},
      [](FunctionKind kind, const Type* output_type) {
        return new MapKeysSortedFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapKeysUnsorted},
      [](FunctionKind kind, const Type* output_type) {
        return new MapKeysUnsortedFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapValuesSorted},
      [](FunctionKind kind, const Type* output_type) {
        return new MapValuesSortedFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapValuesUnsorted},
      [](FunctionKind kind, const Type* output_type) {
        return new MapValuesUnsortedFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapValuesSortedByKey},
      [](FunctionKind kind, const Type* output_type) {
        return new MapValuesSortedByKeyFunction(kind, output_type);
      });
}
}  // namespace zetasql
