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

#include "zetasql/testing/test_value.h"

#include "zetasql/base/logging.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/public/type.h"

namespace zetasql {

namespace test_values {

TypeFactory* static_type_factory() {
  static TypeFactory* type_factory = new TypeFactory;
  return type_factory;
}

Value Struct(absl::Span<const std::string> names,
             absl::Span<const ValueConstructor> values,
             TypeFactory* type_factory) {
  ZETASQL_CHECK_EQ(names.size(), values.size());
  const StructType* struct_type;
  std::vector<StructType::StructField> fields;
  type_factory = type_factory ? type_factory : static_type_factory();
  fields.reserve(names.size());
  for (int i = 0; i < names.size(); i++) {
    fields.push_back({names[i], values[i].get().type()});
  }
  ZETASQL_CHECK_OK(type_factory->MakeStructType(fields, &struct_type));
  return Value::Struct(struct_type, ValueConstructor::ToValues(values));
}

Value Struct(absl::Span<const std::pair<std::string, Value>> pairs,
             TypeFactory* type_factory) {
  const StructType* struct_type;
  std::vector<StructType::StructField> fields;
  std::vector<Value> values;
  type_factory = type_factory ? type_factory : static_type_factory();
  for (const auto& p : pairs) {
    fields.push_back({p.first, p.second.type()});
    values.push_back(p.second);
  }
  ZETASQL_CHECK_OK(type_factory->MakeStructType(fields, &struct_type));
  return Value::Struct(struct_type, values);
}

Value Array(absl::Span<const ValueConstructor> values,
            OrderPreservationKind order_kind, TypeFactory* type_factory) {
  ZETASQL_CHECK(!values.empty());
  std::vector<Value> value_list = ValueConstructor::ToValues(values);
  return InternalValue::ArrayChecked(
      MakeArrayType(values[0].get().type(), type_factory), order_kind,
      std::move(value_list));
}

Value StructArray(absl::Span<const std::string> names,
                  absl::Span<const std::vector<ValueConstructor>> structs,
                  OrderPreservationKind order_kind, TypeFactory* type_factory) {
  std::vector<ValueConstructor> values;
  for (const auto& s : structs) {
    values.push_back(Struct(names, s, type_factory));
  }
  return Array(values, order_kind, type_factory);
}

const ArrayType* MakeArrayType(const Type* element_type,
                               TypeFactory* type_factory) {
  const ArrayType* array_type;
  type_factory = type_factory ? type_factory : static_type_factory();
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(element_type, &array_type));
  return array_type;
}

const StructType* MakeStructType(
    absl::Span<const StructType::StructField> fields,
    TypeFactory* type_factory) {
  const StructType* struct_type;
  type_factory = type_factory ? type_factory : static_type_factory();
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      std::vector<StructType::StructField>(fields.begin(), fields.end()),
      &struct_type));
  return struct_type;
}

const ProtoType* MakeProtoType(const google::protobuf::Descriptor* descriptor,
                               TypeFactory* type_factory) {
  const ProtoType* proto_type;
  type_factory = type_factory ? type_factory : static_type_factory();
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(descriptor, &proto_type));
  return proto_type;
}

const EnumType* MakeEnumType(const google::protobuf::EnumDescriptor* descriptor,
                             TypeFactory* type_factory) {
  const EnumType* enum_type;
  type_factory = type_factory ? type_factory : static_type_factory();
  ZETASQL_CHECK_OK(type_factory->MakeEnumType(descriptor, &enum_type));
  return enum_type;
}

bool AlmostEqualsValue(const Value& x, const Value& y, std::string* reason) {
  return InternalValue::Equals(
      x, y,
      {.interval_compare_mode = IntervalCompareMode::kAllPartsEqual,
       .float_margin = kDefaultFloatMargin,
       .reason = reason});
}

}  // namespace test_values

}  // namespace zetasql
