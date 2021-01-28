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

#include "zetasql/resolved_ast/resolved_ast_helper.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/public/function.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/make_node_vector.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"

namespace zetasql {

static std::unique_ptr<const ResolvedLiteral> MakeInt32Literal(int value) {
  return MakeResolvedLiteral(Value::Int32(value));
}

TEST(ResolvedAstHelperTest, FindProjectExpr) {
  const ResolvedColumn col1(10, zetasql::IdString::MakeGlobal("T"),
                            zetasql::IdString::MakeGlobal("C"),
                            types::Int32Type());
  const ResolvedColumn col2(11, zetasql::IdString::MakeGlobal("T"),
                            zetasql::IdString::MakeGlobal("C2"),
                            types::Int32Type());
  const ResolvedColumn col3(12, zetasql::IdString::MakeGlobal("T"),
                            zetasql::IdString::MakeGlobal("C3"),
                            types::Int32Type());
  auto project = MakeResolvedProjectScan(
      {col1, col2} /* column_list */,
      MakeNodeVector(MakeResolvedComputedColumn(col1, MakeInt32Literal(5)),
                     MakeResolvedComputedColumn(col2, MakeInt32Literal(6))),
      MakeResolvedSingleRowScan());

  EXPECT_EQ(nullptr, FindProjectComputedColumn(project.get(), col3));
  EXPECT_EQ(nullptr, FindProjectExpr(project.get(), col3));

  EXPECT_EQ(project->expr_list(1),
            FindProjectComputedColumn(project.get(), col2));
  EXPECT_EQ(project->expr_list(1)->expr(),
            FindProjectExpr(project.get(), col2));
}

}  // namespace zetasql
