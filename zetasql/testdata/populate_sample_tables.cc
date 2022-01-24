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

#include "zetasql/testdata/populate_sample_tables.h"

#include "google/protobuf/descriptor.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/value.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

using zetasql_test__::KitchenSinkPB;

using zetasql::test_values::Struct;

using zetasql::values::Int32;
using zetasql::values::Int64;
using zetasql::values::Proto;
using zetasql::values::String;

absl::Status PopulateSampleTables(TypeFactory* type_factory,
                                  SampleCatalog* catalog) {
  // Table TwoIntegers
  catalog->GetTableOrDie("TwoIntegers")
      ->SetContents({{Int64(1), Int64(2)}, {Int64(3), Int64(4)}});

  // Table FourIntegers
  catalog->GetTableOrDie("FourIntegers")
      ->SetContents({{Int64(1), Int64(2), Int64(3), Int64(4)},
                     {Int64(5), Int64(6), Int64(7), Int64(8)}});

  // Table Int64ValueTable
  catalog->GetTableOrDie("Int64ValueTable")
      ->SetContents({{Int64(50)}, {Int64(51)}, {Int64(52)}});

  // Table KeyValue
  catalog->GetTableOrDie("KeyValue")
      ->SetContents({{Int64(1), String("Value1")},
                     {Int64(2), String("Value2")},
                     {Int64(3), String("Value3")},
                     {Int64(4), String("Value4")}});

  // Table KeyValueReadTimeIgnored
  catalog->GetTableOrDie("KeyValueReadTimeIgnored")
      ->SetContents({{Int64(5), String("Value5")},
                     {Int64(6), String("Value6")},
                     {Int64(7), String("Value7")},
                     {Int64(8), String("Value8")}});

  // Table KeyValue2 : Key isn't actually a key.
  catalog->GetTableOrDie("KeyValue2")
      ->SetContents({{Int64(1), String("Value1")},
                     {Int64(1), String("Value2")},
                     {Int64(2), String("Value3")},
                     {Int64(2), String("Value4")}});

  const Type* kitchen_sink_pb_type;
  ZETASQL_RETURN_IF_ERROR(catalog->catalog()->FindType(
      {KitchenSinkPB::descriptor()->full_name()}, &kitchen_sink_pb_type));

  KitchenSinkPB proto1;
  proto1.set_int64_key_1(1);
  proto1.set_int64_key_2(2);

  KitchenSinkPB proto2;
  proto2.set_int64_key_1(10);
  proto2.set_int64_key_2(20);

  const Value proto1_value = Proto(kitchen_sink_pb_type->AsProto(), proto1);
  const Value proto2_value = Proto(kitchen_sink_pb_type->AsProto(), proto2);

  // Table KitchenSinkValueTable
  catalog->GetTableOrDie("KitchenSinkValueTable")
      ->SetContents({{proto1_value}, {proto2_value}});

  // Table ComplexTypes
  const Type *enum_type, *test_extra_pb_type;
  ZETASQL_RETURN_IF_ERROR(catalog->catalog()->FindType(
      {zetasql_test__::TestEnum_descriptor()->full_name()}, &enum_type));
  ZETASQL_RETURN_IF_ERROR(catalog->catalog()->FindType(
      {zetasql_test__::TestExtraPB::descriptor()->full_name()},
      &test_extra_pb_type));
  KitchenSinkPB proto;
  proto.set_int64_key_1(1);
  proto.set_int64_key_2(2);
  catalog->GetTableOrDie("ComplexTypes")
      ->SetContents(
          {{Int32(1), Value::Enum(enum_type->AsEnum(), 2),
            Proto(kitchen_sink_pb_type->AsProto(), proto),
            values::Int32Array({3, 5, 7}),
            Struct({"c", "d"},
                   {Int32(10), Struct({"a", "b"}, {Int32(12), String("Twelve")},
                                      type_factory)},
                   type_factory),
            Value::Null(test_extra_pb_type->AsProto())}});

  return absl::OkStatus();
}
}  // namespace zetasql
