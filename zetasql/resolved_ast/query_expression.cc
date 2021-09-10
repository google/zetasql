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

#include "zetasql/resolved_ast/query_expression.h"

#include "zetasql/base/logging.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

// Joins entries present in <list> (with pairs as elements) separated by
// <delimiter>. While appending each pair we add the second element (if present)
// as an alias to the first element.
static std::string JoinListWithAliases(
    const std::vector<std::pair<std::string, std::string>>& list,
    const std::string& delimiter) {
  std::string list_str;
  bool first = true;
  for (const auto& entry : list) {
    if (!first) absl::StrAppend(&list_str, delimiter);

    if (entry.second.empty()) {
      absl::StrAppend(&list_str, entry.first);
    } else {
      absl::StrAppend(&list_str, entry.first, " AS ", entry.second);
    }
    first = false;
  }
  return list_str;
}

void QueryExpression::ClearAllClauses() {
  with_list_.clear();
  select_list_.clear();
  select_as_modifier_.clear();
  query_hints_.clear();
  from_.clear();
  where_.clear();
  set_op_type_.clear();
  set_op_modifier_.clear();
  set_op_scan_list_.clear();
  group_by_list_.clear();
  group_by_hints_.clear();
  order_by_list_.clear();
  order_by_hints_.clear();
  limit_.clear();
  offset_.clear();
  anonymization_options_.clear();
  with_recursive_ = false;
  pivot_.clear();
  unpivot_.clear();
}

std::string QueryExpression::GetSQLQuery() const {
  std::string sql;
  if (!with_list_.empty()) {
    absl::StrAppend(&sql, "WITH ");
    if (with_recursive_) {
      absl::StrAppend(&sql, "RECURSIVE ");
    }
    absl::StrAppend(&sql, JoinListWithAliases(with_list_, ", "), " ");
  }
  if (!select_list_.empty()) {
    ZETASQL_DCHECK(set_op_type_.empty() && set_op_modifier_.empty() &&
           set_op_scan_list_.empty());
    absl::StrAppend(&sql, "SELECT ",
                    anonymization_options_.empty()
                        ? ""
                        : absl::StrCat(anonymization_options_, " "),
                    query_hints_.empty() ? "" : absl::StrCat(query_hints_, " "),
                    select_as_modifier_.empty()
                        ? ""
                        : absl::StrCat(select_as_modifier_, " "),
                    JoinListWithAliases(select_list_, ", "));
  }

  if (!set_op_scan_list_.empty()) {
    ZETASQL_DCHECK(!set_op_type_.empty());
    ZETASQL_DCHECK(!set_op_modifier_.empty());
    ZETASQL_DCHECK(select_list_.empty());
    ZETASQL_DCHECK(from_.empty() && where_.empty() && group_by_list_.empty());
    for (int i = 0; i < set_op_scan_list_.size(); ++i) {
      const auto& qe = set_op_scan_list_[i];
      if (i > 0) {
        absl::StrAppend(&sql, " ", set_op_type_);
        if (i == 1) {
          absl::StrAppend(&sql, " ", query_hints_);
        }
        absl::StrAppend(&sql, " ", set_op_modifier_);
      }
      absl::StrAppend(&sql, "(", qe->GetSQLQuery(), ")");
    }
  }

  if (!from_.empty()) {
    absl::StrAppend(&sql, " FROM ", from_);
  }

  if (!pivot_.empty()) {
    absl::StrAppend(&sql, pivot_);
  }
  if (!unpivot_.empty()) {
    absl::StrAppend(&sql, unpivot_);
  }

  if (!where_.empty()) {
    absl::StrAppend(&sql, " WHERE ", where_);
  }

  if (!group_by_list_.empty()) {
    absl::StrAppend(
        &sql, " GROUP ",
        group_by_hints_.empty() ? "" : absl::StrCat(group_by_hints_, " "),
        "BY ");
    if (!rollup_column_id_list_.empty()) {
      absl::StrAppend(
          &sql, "ROLLUP(",
          absl::StrJoin(rollup_column_id_list_, ", ",
                        [this](std::string* out, int column_id) {
                          absl::StrAppend(
                              out, zetasql_base::FindOrDie(group_by_list_, column_id));
                        }),
          ")");
    } else {
      // We assume while iterating the group_by_list_, the entries will be
      // sorted by the column id.
      absl::StrAppend(
          &sql,
          absl::StrJoin(
              group_by_list_, ", ",
              [](std::string* out,
                 const std::pair<int, std::string>& column_id_and_string) {
                absl::StrAppend(out, column_id_and_string.second);
              }));
    }
  }

  if (!order_by_list_.empty()) {
    absl::StrAppend(
        &sql, " ORDER ",
        order_by_hints_.empty() ? "" : absl::StrCat(order_by_hints_, " "),
        "BY ", absl::StrJoin(order_by_list_, ", "));
  }

  if (!limit_.empty()) {
    absl::StrAppend(&sql, " LIMIT ", limit_);
  }

  if (!offset_.empty()) {
    absl::StrAppend(&sql, " OFFSET ", offset_);
  }

  return sql;
}

bool QueryExpression::CanFormSQLQuery() const {
  return !CanSetSelectClause();
}

void QueryExpression::Wrap(const std::string& alias) {
  ZETASQL_DCHECK(CanFormSQLQuery());
  ZETASQL_DCHECK(!alias.empty());
  const std::string sql = GetSQLQuery();
  ClearAllClauses();
  from_ = absl::StrCat("(", sql, ") AS ", alias);
}

bool QueryExpression::TrySetWithClause(
    const std::vector<std::pair<std::string, std::string>>& with_list,
    bool recursive) {
  if (!CanSetWithClause()) {
    return false;
  }
  with_list_ = with_list;
  with_recursive_ = recursive;
  return true;
}

bool QueryExpression::TrySetSelectClause(
    const std::vector<std::pair<std::string, std::string>>& select_list,
    const std::string& select_hints) {
  if (!CanSetSelectClause()) {
    return false;
  }
  select_list_ = select_list;
  ZETASQL_DCHECK(query_hints_.empty());
  query_hints_ = select_hints;
  return true;
}

void QueryExpression::ResetSelectClause() {
  select_list_.clear();
}

bool QueryExpression::TrySetFromClause(const std::string& from) {
  if (!CanSetFromClause()) {
    return false;
  }
  from_ = from;
  return true;
}

bool QueryExpression::TrySetWhereClause(const std::string& where) {
  if (!CanSetWhereClause()) {
    return false;
  }
  where_ = where;
  return true;
}

bool QueryExpression::TrySetSetOpScanList(
    std::vector<std::unique_ptr<QueryExpression>>* set_op_scan_list,
    const std::string& set_op_type, const std::string& set_op_modifier,
    const std::string& query_hints) {
  if (!CanSetSetOpScanList()) {
    return false;
  }
  ZETASQL_DCHECK(set_op_scan_list != nullptr);
  set_op_scan_list_ = std::move(*set_op_scan_list);
  set_op_scan_list->clear();
  ZETASQL_DCHECK(set_op_type_.empty());
  ZETASQL_DCHECK(set_op_modifier_.empty());
  set_op_type_ = set_op_type;
  set_op_modifier_ = set_op_modifier;
  query_hints_ = query_hints;
  return true;
}

bool QueryExpression::TrySetGroupByClause(
    const std::map<int, std::string>& group_by_list,
    const std::string& group_by_hints,
    const std::vector<int>& rollup_column_id_list) {
  if (!CanSetGroupByClause()) {
    return false;
  }
  group_by_list_ = group_by_list;
  ZETASQL_DCHECK(group_by_hints_.empty());
  group_by_hints_ = group_by_hints;
  rollup_column_id_list_ = rollup_column_id_list;
  return true;
}

bool QueryExpression::TrySetOrderByClause(
    const std::vector<std::string>& order_by_list,
    const std::string& order_by_hints) {
  if (!CanSetOrderByClause()) {
    return false;
  }
  order_by_list_ = order_by_list;
  ZETASQL_DCHECK(order_by_hints_.empty());
  order_by_hints_ = order_by_hints;
  return true;
}

bool QueryExpression::TrySetLimitClause(const std::string& limit) {
  if (!CanSetLimitClause()) {
    return false;
  }
  limit_ = limit;
  return true;
}

bool QueryExpression::TrySetOffsetClause(const std::string& offset) {
  if (!CanSetOffsetClause()) {
    return false;
  }
  offset_ = offset;
  return true;
}

bool QueryExpression::TrySetWithAnonymizationClause(
    const std::string& anonymization_options) {
  if (!CanSetWithAnonymizationClause()) {
    return false;
  }
  anonymization_options_ = anonymization_options;
  return true;
}

bool QueryExpression::TrySetPivotClause(const std::string& pivot) {
  if (!CanSetPivotClause()) {
    return false;
  }
  pivot_ = pivot;
  return true;
}

bool QueryExpression::TrySetUnpivotClause(const std::string& unpivot) {
  if (!CanSetUnpivotClause()) {
    return false;
  }
  unpivot_ = unpivot;
  return true;
}

bool QueryExpression::CanSetWithClause() const {
  return !HasWithClause();
}
bool QueryExpression::CanSetSelectClause() const {
  return !HasSelectClause() && !HasSetOpScanList();
}
bool QueryExpression::CanSetFromClause() const {
  return !HasFromClause() && CanSetSelectClause();
}
bool QueryExpression::CanSetWhereClause() const {
  return !HasWhereClause() && HasFromClause() && CanSetSelectClause();
}
bool QueryExpression::CanSetSetOpScanList() const {
  return !HasSetOpScanList() && !HasSelectClause() && !HasFromClause() &&
         !HasWhereClause() && !HasGroupByClause();
}
bool QueryExpression::CanSetGroupByClause() const {
  return !HasGroupByClause() && HasFromClause() && CanSetSelectClause();
}
bool QueryExpression::CanSetOrderByClause() const {
  return !HasOrderByClause() && !HasLimitClause() && !HasOffsetClause() &&
      HasFromClause();
}
bool QueryExpression::CanSetLimitClause() const {
  return !HasLimitClause() && !HasOffsetClause();
}
bool QueryExpression::CanSetOffsetClause() const { return !HasOffsetClause(); }

bool QueryExpression::CanSetPivotClause() const { return !HasPivotClause(); }

bool QueryExpression::CanSetUnpivotClause() const {
  return !HasUnpivotClause();
}

const std::vector<std::pair<std::string, std::string>>&
QueryExpression::SelectList() const {
  if (!set_op_scan_list_.empty()) {
    ZETASQL_DCHECK(select_list_.empty());
    return set_op_scan_list_[0]->SelectList();
  }

  return select_list_;
}
bool QueryExpression::CanSetWithAnonymizationClause() const {
  return !HasWithAnonymizationClause();
}

void QueryExpression::SetAliasForSelectColumn(int select_column_pos,
                                              const std::string& alias) {
  if (!set_op_scan_list_.empty()) {
    ZETASQL_DCHECK(select_list_.empty());
    set_op_scan_list_[0]->SetAliasForSelectColumn(select_column_pos, alias);
  } else {
    ZETASQL_DCHECK_LT(select_column_pos, select_list_.size());
    select_list_[select_column_pos].second = alias;
  }
}

void QueryExpression::SetSelectAsModifier(const std::string& modifier) {
  ZETASQL_DCHECK(select_as_modifier_.empty());
  select_as_modifier_ = modifier;
}

}  // namespace zetasql
