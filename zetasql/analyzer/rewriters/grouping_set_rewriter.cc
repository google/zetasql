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

#include "zetasql/analyzer/rewriters/grouping_set_rewriter.h"

#include <algorithm>
#include <bitset>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// Returns whether it should rewrite the resolved ast.
// This function also checks whether the number of grouping sets and number of
// distinct columns in grouping sets exceed the limit specified in the rewriter
// options. The rewriter won't expand the rollup/cube before checking the above
// two numbers are in the range, this is to make sure the expansion won't cause
// OOO issues.
absl::StatusOr<bool> ShouldRewrite(const ResolvedAggregateScanBase* node,
                                   const GroupingSetRewriteOptions& options) {
  if (node->grouping_set_list_size() > options.max_grouping_sets()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "At most %d grouping sets are allowed, but %d were provided",
        options.max_grouping_sets(), node->grouping_set_list_size()));
  }

  bool should_rewrite = false;
  int64_t grouping_set_count = 0;
  absl::flat_hash_set<ResolvedColumn> distinct_grouping_set_columns;
  for (const std::unique_ptr<const ResolvedGroupingSetBase>& grouping_set_base :
       node->grouping_set_list()) {
    ZETASQL_RET_CHECK(grouping_set_base->Is<ResolvedGroupingSet>() ||
              grouping_set_base->Is<ResolvedRollup>() ||
              grouping_set_base->Is<ResolvedCube>());
    if (grouping_set_base->Is<ResolvedGroupingSet>()) {
      const ResolvedGroupingSet* grouping_set =
          grouping_set_base->GetAs<ResolvedGroupingSet>();
      for (const auto& column_ref : grouping_set->group_by_column_list()) {
        zetasql_base::InsertIfNotPresent(&distinct_grouping_set_columns,
                                column_ref->column());
      }
      grouping_set_count++;
    } else if (grouping_set_base->Is<ResolvedRollup>()) {
      const ResolvedRollup* rollup = grouping_set_base->GetAs<ResolvedRollup>();
      for (const auto& multi_column : rollup->rollup_column_list()) {
        for (const auto& column_ref : multi_column->column_list()) {
          distinct_grouping_set_columns.insert(column_ref->column());
        }
      }
      grouping_set_count += rollup->rollup_column_list_size() + 1;
      should_rewrite = true;
    } else {
      const ResolvedCube* cube = grouping_set_base->GetAs<ResolvedCube>();
      for (const auto& multi_column : cube->cube_column_list()) {
        for (const auto& column_ref : multi_column->column_list()) {
          distinct_grouping_set_columns.insert(column_ref->column());
        }
      }
      grouping_set_count += 1ull << cube->cube_column_list_size();
      should_rewrite = true;
    }
    if (grouping_set_count > options.max_grouping_sets()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "At most %d grouping sets are allowed, but %d were provided",
          options.max_grouping_sets(), grouping_set_count));
    }
    if (distinct_grouping_set_columns.size() >
        options.max_columns_in_grouping_set()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("At most %d distinct columns are allowed in grouping "
                          "sets, but %d were provided",
                          options.max_columns_in_grouping_set(),
                          distinct_grouping_set_columns.size()));
    }
  }
  return should_rewrite;
}

// Expands ResolvedRollup to a list of ResolvedGroupingSets.
// For example, ROLLUP(a, b, c) will be expanded to grouping sets (a, b, c),
// (a, b), (a) and then (). ROLLUP((a, b), c) will be expanded to grouping sets
// (a, b, c), (a, b) and ().
absl::StatusOr<std::vector<std::unique_ptr<ResolvedGroupingSet>>> ExpandRollup(
    const ResolvedRollup* node) {
  ZETASQL_RET_CHECK(node != nullptr);
  std::vector<std::unique_ptr<ResolvedGroupingSet>> grouping_set_list;
  std::vector<std::unique_ptr<const ResolvedColumnRef>> current_grouping_set;
  absl::flat_hash_set<ResolvedColumn> distinct_grouping_set_columns;

  grouping_set_list.reserve(node->rollup_column_list().size() + 1);
  // Add the empty grouping set.
  grouping_set_list.push_back(MakeResolvedGroupingSet());
  for (const std::unique_ptr<const ResolvedGroupingSetMultiColumn>&
           multi_column : node->rollup_column_list()) {
    for (const std::unique_ptr<const ResolvedColumnRef>& column_ref :
         multi_column->column_list()) {
      // Duplicated columns in grouping sets are equivalent to deduplicated
      // columns, so for simplicity we keep the deduplicated columns in the
      // grouping set. E.g. (a, a, b) is equivalent to (a, b).
      if (zetasql_base::InsertIfNotPresent(&distinct_grouping_set_columns,
                                  column_ref->column())) {
        current_grouping_set.push_back(
            MakeResolvedColumnRef(column_ref->type(), column_ref->column(),
                                  /*is_correlated=*/false));
      }
    }
    std::vector<std::unique_ptr<const ResolvedColumnRef>> grouping_set_columns;
    grouping_set_columns.reserve(current_grouping_set.size());
    for (std::unique_ptr<const ResolvedColumnRef>& column_ref :
         current_grouping_set) {
      grouping_set_columns.push_back(
          MakeResolvedColumnRef(column_ref->type(), column_ref->column(),
                                /*is_correlated=*/false));
    }
    grouping_set_list.push_back(
        MakeResolvedGroupingSet(std::move(grouping_set_columns)));
  }
  // Order of the rows resulting from ROLLUP are not guaranteed, but engines
  // will generally want to compute aggregates from more to less granular
  // levels of subtotals, e.g. (a, b, c), (a, b), (a), and then ().
  std::reverse(grouping_set_list.begin(), grouping_set_list.end());
  return grouping_set_list;
}

// Expands ResolvedCube to ResolvedGroupingSet.
// For example, CUBE(a, b) will be expanded to grouping sets (a, b), (a), (b),
// and ().
// CUBE((a, b), c) will be expanded to grouping sets (a, b, c), (a, b), c, ()
absl::StatusOr<std::vector<std::unique_ptr<ResolvedGroupingSet>>> ExpandCube(
    const ResolvedCube* node) {
  ZETASQL_RET_CHECK(node != nullptr);

  int cube_size = node->cube_column_list_size();
  // This is the hard limit to avoid bitset overflow, it doesn't mean we will
  // allow this many columns in cube. There will be a numeric limit of the
  // maximum number of grouping sets each engine specifies.
  if (cube_size > 31) {
    return absl::InvalidArgumentError(
        "Cube can not have more than 31 elements");
  }
  uint32_t expanded_grouping_set_size = (1u << cube_size);

  std::vector<std::unique_ptr<ResolvedGroupingSet>> grouping_set_list;
  grouping_set_list.reserve(expanded_grouping_set_size);

  // Though expanded_grouping_set_size is an uint32_t, but the check above makes
  // sure uint32_t is smaller than or equal to 2^31, in which case,
  // expanded_grouping_set_size - 1 is still in the range of int.
  for (int i = expanded_grouping_set_size - 1; i >= 0; --i) {
    std::vector<std::unique_ptr<const ResolvedColumnRef>> current_grouping_set;
    absl::flat_hash_set<ResolvedColumn> distinct_grouping_set_columns;
    std::bitset<32> grouping_set_bit(i);
    for (int column_index = 0; column_index < cube_size; ++column_index) {
      if (!grouping_set_bit.test(column_index)) {
        continue;
      }
      for (const std::unique_ptr<const ResolvedColumnRef>& column_ref :
           node->cube_column_list(column_index)->column_list()) {
        if (zetasql_base::InsertIfNotPresent(&distinct_grouping_set_columns,
                                    column_ref->column())) {
          current_grouping_set.push_back(
              MakeResolvedColumnRef(column_ref->type(), column_ref->column(),
                                    /*is_correlated=*/false));
        }
      }
    }
    grouping_set_list.push_back(
        MakeResolvedGroupingSet(std::move(current_grouping_set)));
  }
  return grouping_set_list;
}

}  // namespace

class GroupingSetRewriterVisitor : public ResolvedASTRewriteVisitor {
 public:
  explicit GroupingSetRewriterVisitor(
      const GroupingSetRewriteOptions& rewriter_options)
      : rewriter_options_(rewriter_options) {}

 private:
  absl::StatusOr<std::vector<std::unique_ptr<const ResolvedGroupingSetBase>>>
  RewriteGroupingSetList(
      std::vector<std::unique_ptr<const ResolvedGroupingSetBase>>
          grouping_set_list) {
    std::vector<std::unique_ptr<const ResolvedGroupingSetBase>>
        new_grouping_set_list;
    for (int i = 0; i < grouping_set_list.size(); ++i) {
      ZETASQL_RET_CHECK(grouping_set_list[i]->Is<ResolvedGroupingSet>() ||
                grouping_set_list[i]->Is<ResolvedRollup>() ||
                grouping_set_list[i]->Is<ResolvedCube>());
      if (grouping_set_list[i]->Is<ResolvedGroupingSet>()) {
        // Simply put the ResolvedGroupingSet to the new grouping set list
        new_grouping_set_list.push_back(std::move(grouping_set_list[i]));
      } else if (grouping_set_list[i]->Is<ResolvedRollup>()) {
        const ResolvedRollup* rollup =
            grouping_set_list[i]->GetAs<ResolvedRollup>();
        ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<ResolvedGroupingSet>>
                             expanded_grouping_set_list,
                         ExpandRollup(rollup));
        // Insert expanded grouping sets to the end of the grouping_set_lists.
        absl::c_move(expanded_grouping_set_list,
                     std::back_inserter(new_grouping_set_list));
      } else {
        // This is a ResolvedCube node.
        const ResolvedCube* cube = grouping_set_list[i]->GetAs<ResolvedCube>();
        ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<ResolvedGroupingSet>>
                             expanded_grouping_set_list,
                         ExpandCube(cube));
        absl::c_move(expanded_grouping_set_list,
                     std::back_inserter(new_grouping_set_list));
      }
    }
    return new_grouping_set_list;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregateScan(
      std::unique_ptr<const ResolvedAggregateScan> node) override {
    // Only rewrite the resolved ast when rollup or cube exist.
    ZETASQL_ASSIGN_OR_RETURN(bool should_rewrite,
                     ShouldRewrite(node.get(), rewriter_options_));
    if (!should_rewrite) {
      return std::move(node);
    }

    ResolvedAggregateScanBuilder builder = ToBuilder(std::move(node));
    ZETASQL_ASSIGN_OR_RETURN(
        auto new_grouping_set_list,
        RewriteGroupingSetList(builder.release_grouping_set_list()));
    builder.set_grouping_set_list(std::move(new_grouping_set_list));
    return std::move(builder).Build();
  };

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregationThresholdAggregateScan(
      std::unique_ptr<const ResolvedAggregationThresholdAggregateScan> node)
      override {
    // Only rewrite the resolved ast when rollup or cube exist.
    ZETASQL_ASSIGN_OR_RETURN(bool should_rewrite,
                     ShouldRewrite(node.get(), rewriter_options_));
    if (!should_rewrite) {
      return std::move(node);
    }

    ResolvedAggregationThresholdAggregateScanBuilder builder =
        ToBuilder(std::move(node));
    ZETASQL_ASSIGN_OR_RETURN(
        auto new_grouping_set_list,
        RewriteGroupingSetList(builder.release_grouping_set_list()));
    builder.set_grouping_set_list(std::move(new_grouping_set_list));
    return std::move(builder).Build();
  };

  // The rewriter options.
  GroupingSetRewriteOptions rewriter_options_;
};

/**
 * The rewritter to expand ResolvedRollup and ResolvedCube to a list of
 * ResolvedGroupingSet.
 */
class GroupingSetRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    const GroupingSetRewriteOptions& rewrite_options =
        options.get_rewrite_options().grouping_set_rewrite_options();
    GroupingSetRewriterVisitor visitor(rewrite_options);
    return visitor.VisitAll(std::move(input));
  }

  std::string Name() const override { return "GroupingSetRewriter"; }
};

const Rewriter* GetGroupingSetRewriter() {
  static const auto* kRewriter = new GroupingSetRewriter();
  return kRewriter;
}

}  // namespace zetasql
