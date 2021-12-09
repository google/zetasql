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

#ifndef ZETASQL_COMMON_INTERNAL_VALUE_H_
#define ZETASQL_COMMON_INTERNAL_VALUE_H_

#include <string>

#include "zetasql/common/float_margin.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/value.h"
#include "absl/types/span.h"

namespace zetasql {

// This class is a friend of Value. It is used to access internal methods that
// are intended only for use by the reference implementation.
class InternalValue {
 public:
  using OrderPreservationKind = Value::OrderPreservationKind;
  using ProtoRep = internal::ProtoRep;

  static constexpr OrderPreservationKind kPreservesOrder =
      Value::kPreservesOrder;
  static constexpr OrderPreservationKind kIgnoresOrder = Value::kIgnoresOrder;

  // Access the private accessor for order_kind.
  static OrderPreservationKind order_kind(const Value& v) {
    return v.order_kind();
  }

  // Creates an array of the given 'array_type' initialized by moving from
  // 'values'. The type of each value must be the same as
  // array_type->element_type(). If we are in debug mode, this is ZETASQL_CHECK'd.
  static Value ArrayNotChecked(const ArrayType* array_type,
                               OrderPreservationKind order_kind,
                               std::vector<Value>&& values) {
    absl::StatusOr<Value> value = Value::MakeArrayInternal(
        /*already_validated=*/true, array_type, order_kind, std::move(values));
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }

  // Same as ArrayNotChecked except we ZETASQL_CHECK the type of each element, even when
  // not in debug mode.
  static Value ArrayChecked(const ArrayType* array_type,
                            OrderPreservationKind order_kind,
                            std::vector<Value>&& values) {
    absl::StatusOr<Value> value = Value::MakeArrayInternal(
        /*already_validated=*/false, array_type, order_kind, std::move(values));
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }

  // DEPRECATED: use ArrayNotChecked/ArrayChecked() instead. (For some reason,
  // there are forks of the reference implementation outside zetasql code that
  // are allowed to call this class.)
  static Value Array(const ArrayType* array_type,
                     absl::Span<const Value> values,
                     OrderPreservationKind order_kind) {
    std::vector<Value> value_copies(values.begin(), values.end());
    absl::StatusOr<Value> value =
        Value::MakeArrayInternal(/*already_validated=*/false, array_type,
                                 order_kind, std::move(value_copies));
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }

  absl::StatusOr<Value> MakeArray(const ArrayType* array_type,
                                  std::vector<Value> values,
                                  OrderPreservationKind order_kind) {
    return Value::MakeArrayInternal(/*already_validated=*/false, array_type,
                                    order_kind, std::move(values));
  }

  // Checks equality of values. Arrays inside 'x' value with
  // order_kind()=kIgnoresOrder are compared as multisets to respective arrays
  // in 'y' value.
  static bool Equals(const Value& x, const Value& y,
                     const ValueEqualityCheckOptions& options = {}) {
    if (options.reason) {
      options.reason->clear();
    }
    return Value::EqualsInternal(x, y,
                                 true,     // allow_bags
                                 nullptr,  // deep order spec
                                 options);
  }

  static OrderPreservationKind GetOrderKind(const Value& x) {
    return x.order_kind();
  }

  static const ProtoRep* GetProtoRep(const Value& x) {
    if (x.type_kind() != TYPE_PROTO) return nullptr;
    return x.proto_ptr_;
  }
};

}  // namespace zetasql

#endif  // ZETASQL_COMMON_INTERNAL_VALUE_H_
