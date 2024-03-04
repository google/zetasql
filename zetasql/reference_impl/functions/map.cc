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

#include <utility>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/tuple.h"
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
                             EvaluationContext* context) const override;
};

absl::StatusOr<Value> MapFromArrayFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
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

}  // namespace

void RegisterBuiltinMapFunctions() {
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMapFromArray},
      [](FunctionKind kind, const Type* output_type) {
        return new MapFromArrayFunction(kind, output_type);
      });
}
}  // namespace zetasql
