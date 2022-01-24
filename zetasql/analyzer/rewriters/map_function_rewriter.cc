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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/analyzer/substitute.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class MapFunctionVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  MapFunctionVisitor(Catalog& catalog, TypeFactory& type_factory,
                     const AnalyzerOptions& analyzer_options)
      : catalog_(catalog),
        type_factory_(type_factory),
        analyzer_options_(analyzer_options) {}

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    if (!node->function()->IsZetaSQLBuiltin()) {
      return CopyVisitResolvedFunctionCall(node);
    }
    switch (node->signature().context_id()) {
      case FN_PROTO_MAP_AT_KEY:
      case FN_SAFE_PROTO_MAP_AT_KEY: {
        return GenerateMapAtSql(node);
      }
      case FN_CONTAINS_KEY:
        return GenerateContainsKeySql(node);
      case FN_MODIFY_MAP:
        return GenerateModifyMapSql(node);
      default:
        return CopyVisitResolvedFunctionCall(node);
    }
  }

  absl::Status GenerateMapAtSql(const ResolvedFunctionCall* node) {
    // We order by descending offset because proto2+3 defines the latest
    // instance of a key in the serialized form of a map as the controlling
    // instance.
    constexpr absl::string_view map_at_sql = R"sql(
    CASE
      WHEN m IS NULL THEN NULL
      WHEN k IS NULL THEN NULL
      -- 'value' fields are present by proto2+3 definition, so nulls are only
      -- possible when the key is absent.
      ELSE IFNULL( ( SELECT elem.value FROM UNNEST(m) elem WITH OFFSET offset
                     WHERE elem.key = k ORDER BY offset DESC LIMIT 1 ),
                   -- If the key isn't found, then it's an error.
                   ERROR(FORMAT("Key not found in map: %T", k)) )
    END
    )sql";
    constexpr absl::string_view safe_map_at_sql = R"sql(
    CASE
      WHEN m IS NULL THEN NULL
      WHEN k IS NULL THEN NULL
      ELSE ( SELECT elem.value FROM UNNEST(m) elem WITH OFFSET offset
             WHERE elem.key = k ORDER BY offset DESC LIMIT 1 )
    END
    )sql";

    ZETASQL_RET_CHECK_EQ(node->argument_list().size(), 2);
    ZETASQL_ASSIGN_OR_RETURN(auto map_arg, ProcessNode(node->argument_list(0)));
    ZETASQL_ASSIGN_OR_RETURN(auto key_arg, ProcessNode(node->argument_list(1)));

    const absl::string_view expression =
        node->signature().context_id() == FN_SAFE_PROTO_MAP_AT_KEY
            ? safe_map_at_sql
            : map_at_sql;
    ZETASQL_ASSIGN_OR_RETURN(auto rewritten_tree,
                     AnalyzeSubstitute(
                         analyzer_options_, catalog_, type_factory_, expression,
                         {{"m", map_arg.get()}, {"k", key_arg.get()}}));
    PushNodeToStack(std::move(rewritten_tree));
    return absl::OkStatus();
  }

  absl::Status GenerateContainsKeySql(const ResolvedFunctionCall* node) {
    constexpr absl::string_view kTemplate = R"sql(
    CASE
      WHEN m IS NULL THEN NULL
      ELSE EXISTS(SELECT 1 FROM UNNEST(m) elem WHERE elem.key = k)
    END
    )sql";

    ZETASQL_RET_CHECK_EQ(node->argument_list().size(), 2);
    ZETASQL_ASSIGN_OR_RETURN(auto map_arg, ProcessNode(node->argument_list(0)));
    ZETASQL_ASSIGN_OR_RETURN(auto key_arg, ProcessNode(node->argument_list(1)));

    ZETASQL_ASSIGN_OR_RETURN(
        auto rewritten_tree,
        AnalyzeSubstitute(analyzer_options_, catalog_, type_factory_, kTemplate,
                          {{"m", map_arg.get()}, {"k", key_arg.get()}}));
    PushNodeToStack(std::move(rewritten_tree));
    return absl::OkStatus();
  }

  absl::Status GenerateModifyMapSql(const ResolvedFunctionCall* node) {
    ZETASQL_RET_CHECK(IsProtoMap(node->type())) << node->type()->DebugString();

    constexpr absl::string_view kTemplate = R"sql(
    (SELECT CASE
        -- Error case: multiple keys are not allowed in the rewrite args.
        WHEN EXISTS(
            SELECT ERROR(
                FORMAT("MODIFY_MAP: Only one instance of each key is allowed. Found multiple instances of key: %T", key))
            FROM (SELECT mod.key, count(*) AS num_dups
                  FROM UNNEST(modifications) mod GROUP BY mod.key
                  HAVING num_dups > 1)) THEN NULL
        -- Error case: NULL keys are not allowed.
        WHEN EXISTS(
            SELECT ERROR(
                FORMAT("MODIFY_MAP: All key arguments must be non-NULL, but found NULL at argument %d",
                       -- Note that the MODIFY_MAP arg index is not the same
                       -- as the offset in the modifications array.
                       offset * 2 + 1))
            FROM (SELECT offset
                  FROM UNNEST(modifications) mod WITH OFFSET offset
                  WHERE mod.key IS NULL)) THEN NULL
        WHEN original_map IS NULL THEN NULL
        ELSE ARRAY(
          -- Select all the entries from orig that haven't been replaced.
          -- We retain the offset in the subquery to allow us to keep everything
          -- in the same order we originally saw it.
          SELECT AS STRUCT key, value FROM (
            (SELECT orig.key, orig.value value, offset
             FROM UNNEST(original_map) orig WITH OFFSET offset
             LEFT JOIN UNNEST(modifications) mod
             ON orig.key = mod.key
             WHERE mod.key IS NULL)
             UNION ALL
             -- Union those with each entry from the modifications where the
             -- value isn't NULL. We use an offset that starts past the end of
             -- the original map to ensure a deterministic output order.
            (SELECT mod.key, mod.value, ARRAY_LENGTH(original_map) + offset
             FROM UNNEST(modifications) mod WITH OFFSET offset
             WHERE mod.value IS NOT NULL))
          ORDER BY offset ASC)
        END
     FROM (SELECT AS VALUE $0) modifications)
    )sql";

    ZETASQL_RET_CHECK_LE(3, node->argument_list_size())
        << "MODIFY_MAP should have at least three arguments";
    ZETASQL_RET_CHECK(node->argument_list_size() % 2 == 1)
        << "MODIFY_MAP should have an odd number of arguments.";
    ZETASQL_ASSIGN_OR_RETURN(auto processed_arguments,
                     ProcessNodeList(node->argument_list()));
    const int num_modified_kvs = (node->argument_list_size() - 1) / 2;
    absl::flat_hash_map<std::string, const ResolvedExpr*> variables;
    variables["original_map"] = processed_arguments[0].get();

    // Build up an array expression like [STRUCT(k0 AS key, v0 AS value), ...],
    // one entry for each pair of modified keys in processed_arguments. Also
    // insert the variables with their corresponding resolved expressions into
    // the variables map.
    std::string kv_sql = "[";
    for (int i = 0; i < num_modified_kvs; ++i) {
      absl::SubstituteAndAppend(&kv_sql, "$0STRUCT(k$1 AS key, v$1 AS value)",
                                i > 0 ? ", " : "", i);

      variables[absl::StrCat("k", i)] = processed_arguments[i * 2 + 1].get();
      variables[absl::StrCat("v", i)] = processed_arguments[i * 2 + 2].get();
    }
    absl::StrAppend(&kv_sql, "]");

    ZETASQL_ASSIGN_OR_RETURN(
        auto rewritten_tree,
        AnalyzeSubstitute(analyzer_options_, catalog_, type_factory_,
                          absl::Substitute(kTemplate, kv_sql), variables));
    // The result will be coming out as an array of structs that are coercible
    // to the target map entry type, so we have to add a coercion to make it
    // into the required proto type.
    PushNodeToStack(
        MakeResolvedCast(node->type(), std::move(rewritten_tree), false));
    return absl::OkStatus();
  }

  Catalog& catalog_;
  TypeFactory& type_factory_;
  const AnalyzerOptions& analyzer_options_;
};

class MapFunctionRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    MapFunctionVisitor visitor(catalog, type_factory, options);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&visitor));
    return visitor.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "MapFunctionRewriter"; }
};

const Rewriter* GetMapFunctionRewriter() {
  static const auto* const kRewriter = new MapFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
