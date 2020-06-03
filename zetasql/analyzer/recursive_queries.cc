//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/analyzer/recursive_queries.h"

#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

namespace {
class AnalyzeWithDependenciesVisitor : public NonRecursiveParseTreeVisitor {
 public:
  AnalyzeWithDependenciesVisitor() = default;
  AnalyzeWithDependenciesVisitor(const AnalyzeWithDependenciesVisitor&) =
      delete;
  AnalyzeWithDependenciesVisitor& operator=(
      const AnalyzeWithDependenciesVisitor&) = delete;

  absl::Status Run(const ASTWithClause* with_clause) {
    for (const ASTWithClauseEntry* entry : with_clause->with()) {
      ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&name_entry_map_,
                                        entry->alias()->GetAsIdString(), entry))
          << "Duplicate WITH aliases should have been screened for earlier";
      dependencies_[entry] = absl::flat_hash_set<const ASTWithClauseEntry*>{};
      inner_aliases_[entry->alias()->GetAsIdString()] = 0;
    }

    for (const ASTWithClauseEntry* entry : with_clause->with()) {
      root_ = entry;
      ZETASQL_RETURN_IF_ERROR(entry->TraverseNonRecursive(this));
    }
    return absl::OkStatus();
  }

  const auto& dependency_graph() const { return dependencies_; }

 private:
  zetasql_base::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    return VisitResult::VisitChildren(node);
  }

  void AddInnerAlias(const ASTWithClauseEntry* entry) {
    IdString alias_name = entry->alias()->GetAsIdString();
    if (inner_aliases_.contains(alias_name)) {
      ++inner_aliases_[alias_name];
    }
  }

  void RemoveInnerAlias(const ASTWithClauseEntry* entry) {
    IdString alias_name = entry->alias()->GetAsIdString();
    if (inner_aliases_.contains(alias_name)) {
      --inner_aliases_[alias_name];
    }
  }

  zetasql_base::StatusOr<VisitResult> visitASTWithClauseEntry(
      const ASTWithClauseEntry* node) override {
    return VisitResult::VisitChildren(node, [this, node]() {
      if (node != root_ &&
          !node->parent()->GetAsOrDie<ASTWithClause>()->recursive()) {
        // After visiting the entry of an inner, non-recursive WITH, we need
        // to associate the alias as belonging to an inner WITH entry for the
        // duration of the WITH clause containing it.
        //
        // If the inner WITH is recursive, this was already done back in
        // visitASTWithClause(), so we don't want to do it again.
        AddInnerAlias(node);
      }
      return absl::OkStatus();
    });
  }

  zetasql_base::StatusOr<VisitResult> visitASTQuery(const ASTQuery* node) override {
    if (node->with_clause() != nullptr) {
      return VisitResult::VisitChildren(node, [this, node]() {
        // Inner WITH entries are now out-of-scope.
        for (const ASTWithClauseEntry* entry : node->with_clause()->with()) {
          RemoveInnerAlias(entry);
        }
        return absl::OkStatus();
      });
    }
    return VisitResult::VisitChildren(node);
  }

  zetasql_base::StatusOr<VisitResult> visitASTWithClause(
      const ASTWithClause* node) override {
    // For recursive WITH, add all inner aliases up front, for the entire WITH
    // clause; for non-recursive WITH, inner aliases are added only when we're
    // about to traverse the entry that defines it.
    if (node->recursive()) {
      for (const ASTWithClauseEntry* entry : node->with()) {
        AddInnerAlias(entry);
      }
    }
    return VisitResult::VisitChildren(node);
  }

  zetasql_base::StatusOr<VisitResult> visitASTTablePathExpression(
      const ASTTablePathExpression* node) override {
    if (node->path_expr() == nullptr) {
      // Table path expression does not have a direct path. Example: UNNEST(...)
      return VisitResult::VisitChildren(node);
    }
    if (node->path_expr()->num_names() != 1) {
      // Multi-part table name - cannot reference any WITH alias
      return VisitResult::Empty();
    }
    IdString alias_name = node->path_expr()->first_name()->GetAsIdString();
    if (!inner_aliases_.contains(alias_name)) {
      // Table name not an alias at all in current WITH clause
      return VisitResult::Empty();
    }
    if (inner_aliases_[alias_name] > 0) {
      // This is actually a reference to an alias from an inner WITH clause;
      // ignore
      return VisitResult::Empty();
    }

    // TODO: If we detect a query referencing itself, verify that
    // we have a union and that all self-references are confined to the
    // recursive term.

    // This is indeed a reference to a WITH alias we care about.
    dependencies_[root_].insert(name_entry_map_[alias_name]);
    return VisitResult::Empty();
  }

 private:
  // The root WITH clause entry being traversed.
  const ASTWithClauseEntry* root_;

  // Maps the name of each WITH clause entry belonging to the ASTWithClause
  // passed to Run() with the corresponding ASTWithClauseEntry.
  absl::flat_hash_map<IdString, const ASTWithClauseEntry*, IdStringCaseHash,
                      IdStringCaseEqualFunc>
      name_entry_map_;

  // Maps each WITH clause entry belonging to the ASTWithClause passed to Run()
  // with a list of other entries in the same WITH clause directly depended
  // upon.
  absl::flat_hash_map<const ASTWithClauseEntry*,
                      absl::flat_hash_set<const ASTWithClauseEntry*>>
      dependencies_;

  // Associates the name of each WITH clause entry in <root_> with the number of
  // nested WITH clause entries of the same name currently being processed.
  // Used to distinguish between references to an entry in <root_> vs. an entry
  // of some nested WITH clause inside of it.
  absl::flat_hash_map<IdString, int, IdStringCaseHash, IdStringCaseEqualFunc>
      inner_aliases_;
};

// Helper class which sorts the WITH entries based on the dependency graph.
//
// The implementation is based on the depth-first-search version of the
// topological sort algorithm described at
// https://en.wikipedia.org/wiki/Topological_sorting.
// It is modified to allow for direct self-references and to prevent long
// dependency chains from overflowing the runtime stack.
class WithEntrySorter {
 public:
  WithEntrySorter(const WithEntrySorter&) = delete;
  WithEntrySorter& operator=(const WithEntrySorter&) = delete;

  // Computes the sorted order and saves it for use by result(). Returns an
  // error status if the graph contains any cycles other than direct
  // self-references.
  static zetasql_base::StatusOr<WithEntrySortResult> Run(
      const ASTWithClause* with_clause);

 private:
  // Represents a task remaining in the work of sorting the WITH clause entries.
  struct Task {
    // The WITH clause entry to be processed.
    const ASTWithClauseEntry* entry;

    // Indicates which processing stage we are in for the given entry.
    enum {
      // Indicates that we are starting the processing of a WITH entry.
      kStart,

      // Indicates that we are finishing up the processing of the current WITH
      // entry, after all its dependencies are fully processed.
      kFinish
    } stage;
  };

  WithEntrySorter() {}

  zetasql_base::StatusOr<WithEntrySortResult> RunInternal(
      const ASTWithClause* with_clause);

  // Processes a WITH entry the first time we see it.
  absl::Status StartWithEntry(const ASTWithClauseEntry* entry);

  // Called after StartWithEntry() and FinishWithEntry() have completed for all
  // of the entry's direct and indirect dependencies, excluding itself.
  //
  // Appends <entry> to <sorted_entries_>, while also adding it to
  // <processed_entries_>.
  absl::Status FinishWithEntry(const ASTWithClauseEntry* entry);

  absl::Status PushCurrentChain(const ASTWithClauseEntry* entry);
  absl::Status PopCurrentChain();

  // The WITH clause whose entries are to be sorted in dependency order.
  const ASTWithClause* with_clause_;

  // List of sorted WITH clause entries. Each entry is added to the end from
  // inside FinishWithEntry(), guaranteeing that every entry appears after all
  // of its dependencies.
  std::vector<const ASTWithClauseEntry*> sorted_entries_;

  // WITH clause entries which have already been fully processed. Every element
  // in this set has had StartWithEntry() and FinishWithEntry() called on it,
  // plus all direct and indirect dependencies. These elements are the same as
  // those in <sorted_entries_>, but kept as a hash set for efficient lookup.
  absl::flat_hash_set<const ASTWithClauseEntry*> processed_entries_;

  // Set of WITH clause entries which directly reference themselves. Such
  // clauses must resolve to a ResolvedRecursiveScan, rather than a normal query
  // scan.
  absl::flat_hash_set<const ASTWithClauseEntry*> self_recursive_entries_;

  // Stack containing each of the pending operations necessary to complete the
  // sort.
  std::stack<Task> stack_;

  // Map associating each WITH clause with a list of WITH-clause entries
  // it directly references.
  absl::flat_hash_map<const ASTWithClauseEntry*,
                      std::vector<const ASTWithClauseEntry*>>
      references_;

  // Current chain of dependent elements being processed; each element in
  // current_chain_ has a direct dependency on the element following it. Used to
  // detect cycles.
  std::vector<const ASTWithClauseEntry*> current_chain_;

  // Set of elements in <current_chain_>, allowing for a quick determinination
  // whether an element is in the current chain (whether we have a cycle).
  absl::flat_hash_set<const ASTWithClauseEntry*> current_chain_elements_;
};

zetasql_base::StatusOr<WithEntrySortResult> WithEntrySorter::Run(
    const ASTWithClause* with_clause) {
  WithEntrySorter sorter;
  return sorter.RunInternal(with_clause);
}

zetasql_base::StatusOr<WithEntrySortResult> WithEntrySorter::RunInternal(
    const ASTWithClause* with_clause) {
  with_clause_ = with_clause;
  AnalyzeWithDependenciesVisitor visitor;
  ZETASQL_RETURN_IF_ERROR(visitor.Run(with_clause_));

  absl::flat_hash_map<const ASTWithClauseEntry*, int> entry_index_map;
  for (const ASTWithClauseEntry* entry : with_clause->with()) {
    entry_index_map[entry] = entry_index_map.size();
  }

  for (const auto& pair : visitor.dependency_graph()) {
    // Sort references in the order they appear in the with clause so that the
    // order they are traversed in is stable; while not strictly necessary for
    // correctness, this step is necessary to keep test output stable, which
    // can be affected by the ordering through tree dumps and error messages.
    std::vector<const ASTWithClauseEntry*> sorted_references(
        pair.second.begin(), pair.second.end());
    std::sort(sorted_references.begin(), sorted_references.end(),
              [entry_index_map](const ASTWithClauseEntry* e1,
                                const ASTWithClauseEntry* e2) -> bool {
                return entry_index_map.at(e1) < entry_index_map.at(e2);
              });

    references_[pair.first] = sorted_references;
  }

  for (const ASTWithClauseEntry* entry : with_clause_->with()) {
    stack_.push(Task{entry, Task::kStart});
  }

  while (!stack_.empty()) {
    Task task = stack_.top();
    stack_.pop();
    switch (task.stage) {
      case Task::kStart:
        ZETASQL_RETURN_IF_ERROR(StartWithEntry(task.entry));
        break;
      case Task::kFinish:
        ZETASQL_RETURN_IF_ERROR(FinishWithEntry(task.entry));
        break;
    }
  }
  return WithEntrySortResult{std::move(sorted_entries_),
                             std::move(self_recursive_entries_)};
}

absl::Status WithEntrySorter::StartWithEntry(const ASTWithClauseEntry* entry) {
  if (processed_entries_.contains(entry)) {
    return absl::OkStatus();  // Node already visited
  }

  if (current_chain_elements_.contains(entry)) {
    return MakeSqlErrorAt(with_clause_)
           << "Unsupported WITH entry dependency cycle: "
           << absl::StrJoin(
                  current_chain_, " => ",
                  [](std::string* out, const ASTWithClauseEntry* entry) {
                    absl::StrAppend(out, entry->alias()->GetAsString());
                  })
           << " => " << entry->alias()->GetAsString();
  }
  ZETASQL_RETURN_IF_ERROR(PushCurrentChain(entry));
  stack_.push(Task{entry, Task::kFinish});
  for (const ASTWithClauseEntry* ref : references_.at(entry)) {
    if (ref == entry) {
      self_recursive_entries_.insert(ref);
    } else {
      stack_.push(Task{ref, Task::kStart});
    }
  }

  return absl::OkStatus();
}

absl::Status WithEntrySorter::FinishWithEntry(const ASTWithClauseEntry* entry) {
  ZETASQL_RET_CHECK_EQ(current_chain_.back(), entry);
  ZETASQL_RETURN_IF_ERROR(PopCurrentChain());
  processed_entries_.insert(entry);
  sorted_entries_.push_back(entry);
  return absl::OkStatus();
}

absl::Status WithEntrySorter::PushCurrentChain(
    const ASTWithClauseEntry* entry) {
  ZETASQL_RET_CHECK(!current_chain_elements_.contains(entry));
  current_chain_.push_back(entry);
  current_chain_elements_.insert(entry);
  return absl::OkStatus();
}

absl::Status WithEntrySorter::PopCurrentChain() {
  ZETASQL_RET_CHECK(!current_chain_.empty());
  ZETASQL_RET_CHECK(current_chain_elements_.erase(current_chain_.back()));
  current_chain_.pop_back();
  return absl::OkStatus();
}
}  // namespace

zetasql_base::StatusOr<WithEntrySortResult> SortWithEntries(
    const ASTWithClause* with_clause) {
  return WithEntrySorter::Run(with_clause);
}

}  // namespace zetasql
