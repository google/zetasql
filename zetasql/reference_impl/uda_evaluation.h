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

#ifndef ZETASQL_REFERENCE_IMPL_UDA_EVALUATION_H_
#define ZETASQL_REFERENCE_IMPL_UDA_EVALUATION_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/differential_privacy.pb.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/type_helpers.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/serialization.pb.h"

namespace zetasql {

std::unique_ptr<AggregateFunctionEvaluator>
MakeUserDefinedAggregateFunctionEvaluator(
    std::unique_ptr<RelationalOp> algebrized_tree,
    std::vector<std::string> argument_names,
    std::vector<bool> argument_is_aggregate);

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_UDA_EVALUATION_H_
