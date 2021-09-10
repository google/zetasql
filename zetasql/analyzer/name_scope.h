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

#ifndef ZETASQL_ANALYZER_NAME_SCOPE_H_
#define ZETASQL_ANALYZER_NAME_SCOPE_H_

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "gtest/gtest_prod.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {

class ASTAlias;
class ASTNode;
class ASTPathExpression;
class NameList;

typedef std::shared_ptr<const NameList> NameListPtr;

// The set of ResolvedColumns referenced in a particular subquery that resolved
// as correlated references to values from a parent NameScope.
// The bool value is true if the reference is to a column from more than one
// enclosing NameScope away; i.e. a column that was already a correlated
// reference in the enclosing query.
typedef std::map<ResolvedColumn, bool> CorrelatedColumnsSet;

// A list of the CorrelatedColumnsSets attached to all NameScopes traversed
// while looking up a name.  The sets are ordered from child scopes to
// parent scopes; i.e. the outermost query's NameScope is last.
typedef std::vector<CorrelatedColumnsSet*> CorrelatedColumnsSetList;


// Identifies valid name path (i.e., 'a.b.c') that resolves to the
// specified ResolvedColumn target.
// This class by itself doesn't have much meaning, but it gets attached
// to ValidFieldInfo and NameTarget objects to identify that a path
// starting at a source column or target is valid to access and resolves
// to 'target_column_'.
class ValidNamePath {
 public:
  ValidNamePath() {}
  ValidNamePath(const std::vector<IdString>& name_path,
                const ResolvedColumn& target_column)
      : name_path_(name_path),
        target_column_(target_column) {}
  ~ValidNamePath() {}

  const std::vector<IdString>& name_path() const {
    return name_path_;
  }
  std::vector<IdString>* mutable_name_path() {
    return &name_path_;
  }
  void set_name_path(const std::vector<IdString>& name_path) {
    name_path_ = name_path;
  }

  const ResolvedColumn& target_column() const {
    return target_column_;
  }
  void set_target_column(const ResolvedColumn& target_column) {
    target_column_ = target_column;
  }

  // Returns a string of the form:
  //   name_path:target_column
  std::string DebugString() const;

 private:
  std::vector<IdString> name_path_;
  ResolvedColumn target_column_;

  // Copyable.
};

typedef std::vector<ValidNamePath> ValidNamePathList;

// Identifies a 'target_column' that is valid to access given a 'name_path'
// from a 'source_column'.  This is used to help resolve a name path
// (for example, 'a.b.c') where the first name's column is not valid
// to access ('a') but the full path 'a.b.c' is valid to access.
//
// For example, this happens when grouping by 'a.b.c' - 'a' is not valid to
// access after GROUP BY, but 'a.b.c' is valid.  In this example
// 'source_column' identifies the pre-GROUP BY column associated with 'a'
// (that is not visible post-GROUP BY), the 'name_path' is 'b.c' and
// 'target_column' is the post-GROUP BY column that 'a.b.c' resolves to.
class ValidFieldInfo {
 public:
  // An uninitialized value, with uninitialized source_column and target_column.
  ValidFieldInfo() {}

  ValidFieldInfo(const ResolvedColumn& source_column,
                 const std::vector<IdString>& name_path,
                 const ResolvedColumn& target_column)
      : source_column_(source_column),
        valid_name_path_(name_path, target_column) {}
  ~ValidFieldInfo() {}

  const ResolvedColumn& source_column() const {
    return source_column_;
  }
  void set_source_column(const ResolvedColumn& source_column) {
    source_column_ = source_column;
  }

  const std::vector<IdString>& name_path() const {
    return valid_name_path_.name_path();
  }

  std::vector<IdString>* mutable_name_path() {
    return valid_name_path_.mutable_name_path();
  }

  const ResolvedColumn& target_column() const {
    return valid_name_path_.target_column();
  }
  void set_target_column(const ResolvedColumn& target_column) {
    valid_name_path_.set_target_column(target_column);
  }

  const ValidNamePath& valid_name_path() const {
    return valid_name_path_;
  }

  // Returns a string of the form:
  //   source_column:name_path:target_column
  // For example:
  //   col#1::col#2
  //   col#3:a:col#4
  //   col#5:a.b.c.d:col#6
  std::string DebugString() const;

 private:
  ResolvedColumn source_column_;
  ValidNamePath valid_name_path_;

  // Copyable.
};

// A map of columns to namepaths and associated target columns
// that are accessible from the original column.  Used in
// the 'ValidFieldInfoMap' class, which owns the 'ValidNamePathList'
// pointers in this map.
typedef absl::flat_hash_map<ResolvedColumn, std::unique_ptr<ValidNamePathList>,
                            ResolvedColumnHasher>
    ResolvedColumnToValidNamePathsMap;

// Map from ResolvedColumn column_id to a list of name paths that are
// valid to access from that ResolvedColumn.
class ValidFieldInfoMap {
 public:
  ValidFieldInfoMap() {}
  ValidFieldInfoMap(const ValidFieldInfoMap&) = delete;
  ValidFieldInfoMap& operator=(const ValidFieldInfoMap&) = delete;
  ~ValidFieldInfoMap();

  // Add a new 'valid_name_path' for the specified ResolvedColumn.
  // If there is already an entry in the map for the 'column' then adds
  // 'valid_name_path' to the related ValidNamePathList.
  // Otherwise inserts a new entry into the map for 'column' with a new
  // ValidNamePathList containing 'valid_name_path'.
  void InsertNamePath(const ResolvedColumn& column,
                      const ValidNamePath& valid_name_path);

  // Look up an entry in 'column_id_to_name_path_list_map_' based on the
  // input ResolvedColumn, and return true along with the related
  // 'valid_name_path_list' if it exists.  Otherwise returns false if
  // 'column' is not found.
  bool LookupNamePathList(const ResolvedColumn& column,
                          const ValidNamePathList** valid_name_path_list) const;

  void Clear();

  const ResolvedColumnToValidNamePathsMap& map() const {
    return column_to_valid_name_paths_map_;
  }
  std::string DebugString(const std::string& indent = "") const;

 private:
  // A map from a ResolvedColumn to a name path list that is valid
  // to access from that ResolvedColumn.
  ResolvedColumnToValidNamePathsMap column_to_valid_name_paths_map_;
};

// A NamedColumn is an element of a NameList, associating a name with a
// ResolvedColumn.
struct NamedColumn {
 public:
  NamedColumn() {}
  // Constructor for non-value table columns.
  NamedColumn(IdString name_in, const ResolvedColumn& column_in,
              bool is_explicit_in)
      : name(name_in), column(column_in), is_explicit(is_explicit_in),
        is_value_table_column(false) {}
  // Constructor for value table columns.
  NamedColumn(IdString name_in, const ResolvedColumn& column_in,
              bool is_explicit_in,
              const IdStringSetCase& excluded_field_names_in)
      : name(name_in), column(column_in), is_explicit(is_explicit_in),
        is_value_table_column(true),
        excluded_field_names(excluded_field_names_in) {}

  // Having a move constructor makes storing this in STL containers more
  // efficient.
  NamedColumn(NamedColumn&& old) = default;
  NamedColumn(const NamedColumn& other) = default;
  NamedColumn& operator=(const NamedColumn& other) = default;

  std::string DebugString(const absl::string_view prefix = "") const;

  IdString name;

  ResolvedColumn column;

  // True if the alias for this column is an explicit name. Generally, explicit
  // names come directly from the query text, and implicit names are those that
  // are generated automatically from something outside the query text, like
  // column names that come from a table schema. Explicitness does not change
  // any scoping behavior except for the final check in strict mode that may
  // raise an error. For more information, please see the beginning of
  // (broken link).
  bool is_explicit = false;

  // True if this column is the value produced in a value table scan.
  // The name acts more like a range variable in this case, but is
  // stored in columns_ so it shows up in the right order in SELECT *.
  bool is_value_table_column = false;

  // Only relevant for value table columns.  Indicates field names that
  // should be ignored for implicit lookups from the containing NameList.
  // For example, if a NameList includes value table column 'vt' and that
  // value table contains field 'f1', then doing a NameList lookup for 'f1'
  // will fail if 'f1' is in this set for 'vt'.  This also affects whether
  // a field is returned when expanding '*' (but not rangevar.*, for
  // instance 'vt.*').
  //
  // This uses a Set rather than HashSet because it is almost always empty,
  // or has just a few elements.
  // TODO I think using a CopyOnWrite of a HashSet here may be
  // worthwhile.  We copy identical sets around a lot, and using CopyOnWrite
  // would let us use a HashSet without making copy or destruct too expensive.
  IdStringSetCase excluded_field_names;

  // Copyable.
};

// A target that a name in a NameScope points at.
//
// A name can resolve to:
//   1. A range variable - i.e. a name that references one row of a scan as
//      we iterate through it.  e.g. A table alias introduced in a FROM clause.
//      This name points to a NameList giving the names visible in that scan.
//   2. An implicit value, for column names available on scans in the
//      from clause.
//   3. An explicit value, for column names that were given an explicit
//      alias (e.g. in the select list).
//   4. A field of a value table column.  In this case, we return the container
//      column value and the caller is expected to re-resolve the name as a
//      field of that container.
//   5. Ambiguous, meaning the name could point at multiple implicit values,
//      so a query that references it should give an error.
//   6. Access error, meaning that accessing this name is invalid and provides
//      an error.  Access error targets keep track of the Kind of the original
//      target, and may have a non-empty ValidNamePathList that
//      identifies (sub)fields that are valid to access even though this
//      target is not.
//
// Name collisions are resolved as follows:
//   - range variables always take precedence over column names
//   - duplicate column names become ambiguous
//   - duplicate range variable names are not possible (NameList gives an
//     error rather than allowing duplicates to be added)
//
// Note that for value tables, the alias introduced for the scan is somewhat
// of a hybrid between a range variable and an explicit column.  In NameScope,
// lookups will return the range variable, pointing at a NameList with
// is_value_table true.
//
// NameScopes cannot be built by inserting names directly.
// Callers build a NameList describing the set of columns available
// from part of a query and then build a NameScope from that NameList,
// attaching a parent NameScope if applicable (i.e. when inside a subquery).
//
// This is based on the ZetaSQL name scoping doc:
// (broken link)
class NameTarget {
 public:
  enum Kind {
    RANGE_VARIABLE,   // supports scan_columns()
    IMPLICIT_COLUMN,  // supports column()
    EXPLICIT_COLUMN,  // supports column()
    FIELD_OF,         // supports column_containing_field()
    AMBIGUOUS,
    ACCESS_ERROR,     // supports original_kind() which indicates the
                      // original Kind of this NameTarget, and a non-empty
                      // 'valid_field_info_list_'
  };

  // Default constructor makes an ambiguous NameTarget.
  NameTarget() : kind_(AMBIGUOUS) {}

  // Construct a NameTarget for a range variable pointing at a scan.
  explicit NameTarget(const NameListPtr& scan_columns)
      : kind_(RANGE_VARIABLE), scan_columns_(scan_columns) {}

  // Construct a NameTarget pointing at a column, implicitly or explicitly.
  NameTarget(const ResolvedColumn& column, bool is_explicit)
      : kind_(is_explicit ? EXPLICIT_COLUMN : IMPLICIT_COLUMN),
        column_(column) {}

  // Construct a FIELD_OF NameTarget pointing at a column, with a specific
  // FIELD_OF id.  For STRUCT fields, the field_id is the field index.
  // For PROTO fields, the field_id is the field tag number.
  // Only used for kind==FIELD_OF.
  NameTarget(const ResolvedColumn& column, int field_id)
      : kind_(FIELD_OF), column_(column), field_id_(field_id) {
  }

  // Having a move constructor makes storing this in STL containers more
  // efficient.
  NameTarget(NameTarget&& old) = default;
  NameTarget(const NameTarget& other) = default;
  NameTarget& operator=(const NameTarget& other) = default;

  // Determine if two NameTargets are equal.  The current implementation
  // relies on NameList and ValidNamePathList equality using DebugString()
  // which is not ideal, but this is only used for testing at this time.
  // If we want to use these for more than just testing, then we need to
  // implement full NameList and ValidNamePathList equality.
  bool Equals_TESTING(const NameTarget& other) const;

  void SetAmbiguous() {
    scan_columns_.reset();
    column_.Clear();
    kind_ = AMBIGUOUS;
  }

  // Mark this NameTarget as providing an error upon access.  This indicates
  // that the name actually exists, but the corresponding column or range
  // variable cannot validly be referenced by itself.  The original
  // 'name_target' Kind is stored for later use during error messaging.
  // If non-empty, 'access_error_message' indicates the error message
  // associated with this NameTarget.
  void SetAccessError(const Kind original_kind,
                      const std::string& access_error_message = "");

  Kind kind() const { return kind_; }

  Kind original_kind() const {
    ZETASQL_DCHECK(IsAccessError());
    return original_kind_;
  }

  const std::string& access_error_message() const {
    return access_error_message_;
  }

  static bool IsColumnKind(Kind kind) {
    return kind == IMPLICIT_COLUMN || kind == EXPLICIT_COLUMN;
  }
  bool IsColumn() const {
    return IsColumnKind(kind_);
  }
  static bool IsRangeVariableKind(Kind kind) {
    return kind == RANGE_VARIABLE;
  }
  bool IsRangeVariable() const {
    return IsRangeVariableKind(kind_);
  }
  static bool IsFieldOfKind(Kind kind) {
    return kind == FIELD_OF;
  }
  bool IsFieldOf() const {
    return IsFieldOfKind(kind_);
  }
  static bool IsAmbiguousKind(Kind kind) {
    return kind == AMBIGUOUS;
  }
  bool IsAmbiguous() const {
    return IsAmbiguousKind(kind_);
  }
  static bool IsAccessErrorKind(Kind kind) {
    return kind == ACCESS_ERROR;
  }
  bool IsAccessError() const {
    return IsAccessErrorKind(kind_);
  }
  static bool IsImplicitColumnKind(Kind kind) {
    return kind == IMPLICIT_COLUMN;
  }
  bool IsImplicitColumn() const {
    return IsImplicitColumnKind(kind_);
  }

  bool IsExplicit() const {
    return kind_ == RANGE_VARIABLE || kind_ == EXPLICIT_COLUMN;
  }
  bool IsImplicit() const {
    return kind_ == IMPLICIT_COLUMN || kind_ == FIELD_OF;
  }

  const std::shared_ptr<const NameList>& scan_columns() const {
    ZETASQL_DCHECK_EQ(kind_, RANGE_VARIABLE);
    return scan_columns_;
  }
  const ResolvedColumn& column() const {
    ZETASQL_DCHECK(IsColumn()) << DebugString();
    return column_;
  }
  const ResolvedColumn& column_containing_field() const {
    ZETASQL_DCHECK(IsFieldOf()) << DebugString();
    return column_;
  }
  int field_id() const {
    ZETASQL_DCHECK(IsFieldOf()) << DebugString();
    return field_id_;
  }

  const ValidNamePathList& valid_name_path_list() const {
    return valid_name_path_list_;
  }

  ValidNamePathList* mutable_valid_name_path_list() {
    return &valid_name_path_list_;
  }

  void set_valid_name_path_list(
      const ValidNamePathList& valid_name_path_list) {
    valid_name_path_list_ = valid_name_path_list;
  }

  // The NameTarget must be an ACCESS_ERROR, and the 'original_kind_'
  // must not be AMBIGUOUS since if the original NameTarget was
  // AMBIGUOUS then accessing fields from it cannot be valid.
  void AddNamePathToColumn(const ValidNamePath& info) {
    ZETASQL_DCHECK(IsAccessError()) << DebugString();
    ZETASQL_DCHECK(!IsAmbiguousKind(original_kind_)) << DebugString();
    valid_name_path_list_.push_back(info);
  }

  // The NameTarget must be an ACCESS_ERROR, and the 'original_kind_'
  // must not be AMBIGUOUS since if the original NameTarget was
  // AMBIGUOUS then accessing fields from it cannot be valid.
  void AppendValidNamePathList(const ValidNamePathList& info_list) {
    ZETASQL_DCHECK(IsAccessError()) << DebugString();
    ZETASQL_DCHECK(!IsAmbiguousKind(original_kind_)) << DebugString();
    valid_name_path_list_.insert(valid_name_path_list_.end(),
                                 info_list.begin(), info_list.end());
  }

  std::string DebugString() const;

 private:
  Kind kind_;

  // Populated if kind_ == RANGE_VARIABLE.
  std::shared_ptr<const NameList> scan_columns_;

  // Populated if kind_ is one of:
  //   {IMPLICIT_COLUMN, EXPLICIT_COLUMN, FIELD_OF}.
  ResolvedColumn column_;

  // Populated if kind_ == FIELD_OF.
  // Represents field index for STRUCT fields.
  // Represents field tag number for PROTO fields.
  int field_id_ = -1;

  // Populated if kind_ == ACCESS_ERROR.
  // Determines what the type of the original NameTarget was.  Used
  // for error messaging and validating function calls that populate
  // 'valid_field_info_list_'.
  Kind original_kind_;

  // Can only be populated if kind_ == ACCESS_ERROR, but can be empty.
  // If non-empty, provides a custom error message that can be used
  // when the NameTarget is referenced.
  std::string access_error_message_;

  // Can only be populated if kind_ == ACCESS_ERROR.
  // Requires original_kind_ to be RANGE_VARIABLE, EXPLICIT_COLUMN,
  // IMPLICIT_COLUMN, or FIELD_OF.
  // Identifies a list of valid name paths and their associated ResolvedColumns
  // that are accessible from this target.
  ValidNamePathList valid_name_path_list_;

  // Copyable.
};

// A name scope for aliases visible in a query, not including the global names
// visible in a Catalog.
//
// NameScopes can be chained together, and child scopes can hide names from
// parent scopes.
//
// NameScopes are normally stored in shared_ptrs to make lifetime management
// easier when multiple child scopes share a parent scope.
//
// All name lookups are case insensitive.
//
// How correlated lookups are tracked:
//   When resolving a correlated subquery, we'll build a local NameScope that
//   inherits the outer query's NameScope as its previous_scope.
//
//   In the child NameScope (the subquery's NameScope), we'll store a
//   CorrelatedColumnsSet, and that set will store all ResolvedColumns looked
//   up via this NameScope (i.e. inside the subquery) that resolve to a
//   column from a parent NameScope.  This set will provide the list of
//   parameters that must be passed to the ResolvedSubqueryExpr.
//
//   When LookupName finds a name from a parent scope, it returns the
//   CorrelatedColumnsSet pointer for the child NameScope, and the
//   caller must insert into it any columns resolved from a parent scope.
//   This happens in the caller (rather than here) because the NameScope lookup
//   may return a range variable, and won't know the specific columns
//   referenced using it (e.g. rangevar.col or rangevar.*).
//
//   With multiply nested NameScopes, a ResolvedColumn resolved from the
//   outermost ancestor scope must be added to CorrelatedColumnsSets for each
//   child scope that that lookup passes through, so the columns can be added
//   to the ResolvedSubqueryExpr parameters list at each subquery nesting
//   level.  To allow this, LookupName returns a list of CorrelatedColumnsSets.
//
// For full specification, see (broken link).
class NameScope {
 public:
  // Make a scope with no underlying fallback scope.
  NameScope() : previous_scope_(nullptr) {}

  // Make a scope with a fallback to look for names in <previous_scope>, if
  // non-NULL.
  // If <correlated_columns_set> is non-NULL, store that pointer with this scope
  // so it can be updated to compute the set of correlated column references.
  // The <correlated_columns_set> and <previous_scope> pointers must outlive
  // this NameScope.
  explicit NameScope(const NameScope* previous_scope,
                     CorrelatedColumnsSet* correlated_columns_set = nullptr)
      : previous_scope_(previous_scope),
        correlated_columns_set_(correlated_columns_set) {}

  // Make a NameScope that inherits names from <previous_scope>, if non-NULL,
  // and making names in <name_list> visible over top of it.
  // If <correlated_columns_set> is non-NULL, store that pointer with this scope
  // so it can be updated to compute the set of correlated column references.
  // The <correlated_columns_set> and <previous_scope> pointers must outlive
  // this NameScope.
  NameScope(const NameScope* previous_scope,
            const NameListPtr& name_list,
            CorrelatedColumnsSet* correlated_columns_set = nullptr);

  // Make a NameScope with names from <name_list>.
  explicit NameScope(const NameList& name_list);

  ~NameScope();

  // Creates a new NameScope copied from the current NameScope, where
  // locally defined names are updated with new NameTargets.  The entries
  // in 'valid_field_info_list_in' determine which local names (including
  // fields and subfields) remain valid to access, while other local names
  // become error targets.  The new NameScope has the same 'previous_scope_'
  // as this NameScope, so all previous scope names remain available for
  // lookup (if not overridden by local names).
  //
  // WARNING - The caller must ensure that the previous_scope_'s names
  // remain valid for access in the new NameScope.
  absl::Status CreateNameScopeGivenValidNamePaths(
      const ValidFieldInfoMap& valid_field_info_map_in,
      std::unique_ptr<NameScope>* new_name_scope) const;

  // Creates a new NameScope copied from this NameScope, with names in
  // 'name_list_with_overriding_names' overriding local names in this
  // NameScope.  The new NameScope has the same 'previous_scope_'
  // as this NameScope, so all previous scope names remain available for
  // lookup (if not overridden by local names).
  // A current limitation is that 'namelist_with_overriding_names'
  // cannot contain pseudocolumns, range variables, or value table
  // columns - it can only contain normal columns.  So this function
  // returns an error status if any non-normal columns are present.
  absl::Status CopyNameScopeWithOverridingNames(
      const std::shared_ptr<NameList>& namelist_with_overriding_names,
      std::unique_ptr<NameScope>* scope_with_new_names) const;

  // Creates a new NameScope copied from this NameScope, with NameTargets
  // in 'overriding_name_targets' overriding local names in this
  // NameScope.  The new NameScope has the same 'previous_scope_'
  // as this NameScope, so all previous scope names remain available for
  // lookup (if not overridden by local names).
  absl::Status CopyNameScopeWithOverridingNameTargets(
      const IdStringHashMapCase<NameTarget>& overriding_name_targets,
      std::unique_ptr<NameScope>* scope_with_new_names) const;

  // Look up a name in this scope, and underlying scopes if necessary.
  // Return true and copy result into <*found> if found.
  //
  // If <correlated_columns_sets> is non-NULL, on return, it will contain
  // any non-NULL correlated_columns_set_ from any NameScopes traversed before
  // reaching the NameScope containing <name>.  Any column used via this
  // resolved name should be added to all of these CorrelatedColumnsSets,
  // and then passed as a parameter to the corresponding ResolvedSubqueryExpr.
  //
  // The returned sets are ordered so the ones attached to child scopes come
  // before the ones attached to their parent scopes.
  bool LookupName(
      IdString name, NameTarget* found,
      CorrelatedColumnsSetList* correlated_columns_sets = nullptr) const;

  // Similar to the previous <LookupName> function, but allows multi-part
  // names to be looked up.  Looks into underlying scopes if necessary.
  //
  // Returns error status if the name path is ambiguous, is an access
  // error, or includes an invalid field reference.
  //
  // Returns OK (with 'num_names_consumed' = 0) if the first name is not
  // found in the scope.
  //
  // Also returns OK if a prefix of the name path resolves to a valid target
  // (range variable, column, or field).  In this case, 'target_out' is returned
  // as the target of the lookup, 'num_names_consumed' indicates how many names
  // were consumed (matching 'target_out'), and 'correlated_columns_sets'
  // identifies correlation information (if any).
  //
  // For example, if we look up 'a.b.c.d' and find that 'a' looks up
  // as a range variable and 'b' is a field of 'a', then a FIELD_OF
  // NameTarget is returned and 'num_names_consumed' is set to 2.  On
  // the other hand if we had found that 'a.b.c.d' as a whole looked
  // up as a column, then a column NameTarget would be returned and
  // 'num_names_consumed' would be set to 4.
  //
  // Note: The current implementation usually only looks up one name and
  // returns the associated NameTarget, but sometimes looks up two names.
  // The current implementation never looks up more than two names.
  // This limitation will be removed in a subsequent CL.
  //
  // 'in_strict_mode' identifies whether unqualified names are valid
  // to access.  'clause_name' and 'is_post_distinct' are only used for
  // error messaging.
  absl::Status LookupNamePath(const ASTPathExpression* path_expr,
                              const char* clause_name, bool is_post_distinct,
                              bool in_strict_mode,
                              CorrelatedColumnsSetList* correlated_columns_sets,
                              int* num_names_consumed,
                              NameTarget* target_out) const;

  // Look up a name in this scope, and underlying scopes if necessary.
  // Return true if the name exists, including ambiguous names and field
  // names of value tables.
  bool HasName(IdString name) const;

  // Returns the closest suggestion on a <mistyped_name> from the names present
  // in this scope, if one exists. Otherwise returns an empty string.
  std::string SuggestName(IdString mistyped_name) const;

  // Returns whether or not any of the local names (not in previous scopes)
  // are ValueTableColumns.
  bool HasLocalValueTableColumns() const {
    return !value_table_columns().empty();
  }

  // Returns whether or not any of the local names (not in previous scopes)
  // are range variables.
  bool HasLocalRangeVariables() const;

  std::string DebugString(const std::string& indent = "") const;

  const NameScope* previous_scope() const { return previous_scope_; }

 private:
  const NameScope* const previous_scope_ = nullptr;  // may be NULL

  // Set used to collect correlated columns referenced from previous scopes.
  // This is not used or updated by the NameScope class itself.
  // If this is non-NULL, this set pointer is returned from LookupName, and
  // any referenced columns should be added to it by the caller.
  CorrelatedColumnsSet* correlated_columns_set_ = nullptr;  // Not owned.

  // Value table column information, including a ResolvedColumn and a list of
  // field names that are excluded/ignored from a lookup in the containing
  // NameScope.  For instance, if the NameScope contains value table column vt1
  // with fields f1 and f2, and f2 is in <excluded_field_names>, then
  // a LookupName() for 'f2' into the NameScope returns not found.
  //
  // Additionally, a ValueTableColumn includes a flag indicating whether
  // or not it is valid to access, and a list of its fields that
  // are valid to access.  For example, in the example above if vt1 is
  // !is_valid_to_access then LookupName() for 'vt1' will return an error
  // NameTarget (which is different than returning not found).  This
  // returned error NameTarget identifies any field paths that are valid
  // to access, allowing the caller to access them if appropriate even
  // if 'vt1' itself is not valid to access.
  struct ValueTableColumn {
    ResolvedColumn column;
    IdStringSetCase excluded_field_names;
    bool is_valid_to_access = false;
    ValidNamePathList valid_name_path_list;

    std::string DebugString() const;

    ValueTableColumn() {}
    ValueTableColumn(
        const ResolvedColumn& column_in,
        const IdStringSetCase& excluded_field_names_in,
        bool is_valid_to_access_in,
        const ValidNamePathList& valid_name_path_list_in)
        : column(column_in),
          excluded_field_names(excluded_field_names_in),
          is_valid_to_access(is_valid_to_access_in),
          valid_name_path_list(valid_name_path_list_in) {}

    // Having a move constructor makes storing this in STL containers more
    // efficient.
    ValueTableColumn(ValueTableColumn&& old) = default;
    ValueTableColumn(const ValueTableColumn& other) = default;
    ValueTableColumn& operator=(const ValueTableColumn& other) = default;

    // Copyable.
  };

  // A private constructor for internal use only, taking already-constructed
  // NameTargets and ValueTableColumns as arguments.
  NameScope(const NameScope* previous_scope,
            const IdStringHashMapCase<NameTarget>& name_targets,
            const std::vector<ValueTableColumn>& value_table_columns,
            CorrelatedColumnsSet* correlated_columns_set);

  NameScope(const NameScope&) = delete;
  NameScope& operator=(const NameScope&) = delete;

  // Adds 'name' to the excluded columns list of the 'value_table_column', if
  // and only if 'value_table_column' contains a field of that 'name' and that
  // 'name' is not already excluded.
  static void ExcludeNameFromValueTableIfPresent(
      IdString name, ValueTableColumn* value_table_column);

  // Iterates over 'names', and for each entry inserts the name and
  // corresponding NameTarget into the current NameScope if
  // the name is not already present in the NameScope.
  void InsertNameTargetsIfNotPresent(
      const IdStringHashMapCase<NameTarget>& names);

  // Creates a new RangeVariable NameTarget derived from the
  // 'original_name_target', where the new NameTarget reflects whether
  // or not the name (and its fields) are valid to access based on
  // entries in 'valid_field_info_list_in'.
  static absl::Status CreateNewRangeVariableTargetGivenValidNamePaths(
      const NameTarget& original_name_target,
      const ValidFieldInfoMap& valid_field_info_map_in,
      NameTarget* new_name_target);

  // Creates a new map of (name, NameTarget) pairs derived from the local
  // names(), where the new NameTargets reflect whether or not the
  // name (and its fields) are valid to access based on entries in
  // 'valid_field_info_list_in'.
  //
  // Note that if an existing local name is invalid to access, it remains
  // invalid to access, but it may have valid name paths that are accessible
  // and need to be updated.  In that case, the resulting NameTarget valid
  // field path is generated by concatenating the existing valid name paths
  // with matching entries in 'valid_field_info_map_in'.  Currently, such
  // concatenation is only supported for 'valid_field_info_map_in' entries
  // that have empty name paths (that in effect is used to update the
  // target ResolvedColumn associated with the existing name path).
  absl::Status CreateNewLocalNameTargetsGivenValidNamePaths(
      const ValidFieldInfoMap& valid_field_info_map_in,
      IdStringHashMapCase<NameTarget>* new_name_targets) const;

  // Creates a new vector of ValueTableColumns from the local
  // ValueTableColumns, where the new ValueTableColumns and their fields
  // identify which are valid to access and which are invalid based
  // on the entries in 'valid_field_info_list_in'.
  void CreateNewValueTableColumnsGivenValidNamePaths(
      const ValidFieldInfoMap& valid_field_info_map_in,
      std::vector<ValueTableColumn>* new_value_table_columns) const;

  // Returns a new 'field_target' given a 'value_table_column' with
  // field 'field_name'.  Returns a valid target if 'value_table_column'
  // has a valid path name list that exactly matches this field name.
  // Otherwise returns an invalid target.  If returning an invalid
  // target, valid path names from the invalid
  // target are populated if corresponding valid path names exist in
  // the 'value_table_column'.  For instance, if the 'value_table_column'
  // has valid path 'a.b.c', and this is invoked for field 'a', then
  // an invalid target is returned with valid path 'b.c'.
  static absl::Status CreateGetFieldTargetFromInvalidValueTableColumn(
      const ValueTableColumn& value_table_column, IdString field_name,
      NameTarget* field_target);

  // The local state for this NameScope is stored in this struct which is
  // stored in a CopyOnWrite.  This allows cheap copies when constructing
  // NameScopes from NameLists and in NameList::MergeFrom.
  struct State {
    // This is the main map storing the names visible in this local scope
    // (not including names from parent scopes).
    // Using map rather than hash_map because the set is often small,
    // and these may be constructed and destructed frequently.
    IdStringHashMapCase<NameTarget> names;

    // Vector of ValueTableColumns for all value tables in this local scope.
    // When looking up a name, we also look for fields of any of these columns
    // (except for fields marked as excluded for each value table column).
    std::vector<ValueTableColumn> value_table_columns;

    void CopyFrom(const State& other) {
      names = other.names;
      value_table_columns = other.value_table_columns;
    }
  };
  State state_;

  // Accessors for fields inside the CopyOnWrite state_.
  const IdStringHashMapCase<NameTarget>& names() const {
    return state_.names;
  }
  IdStringHashMapCase<NameTarget>* mutable_names() {
    return &state_.names;
  }
  const std::vector<ValueTableColumn>& value_table_columns() const {
    return state_.value_table_columns;
  }
  std::vector<ValueTableColumn>* mutable_value_table_columns() {
    return &state_.value_table_columns;
  }

  // These are used internally to optimize copying.
  bool IsEmpty() const;
  void CopyStateFrom(const NameScope& other);

  // Add a name to this scope.  A NameTarget is constructed for this object.
  //
  // Collisions are resolved as described above, with range variables overriding
  // columns. Overriding names in the underlying scope is always allowed.
  void AddRangeVariable(IdString name, const NameListPtr& scan_columns);
  void AddColumn(IdString name, const ResolvedColumn& column,
                 bool is_explicit);

  void AddNameTarget(IdString name, const NameTarget& target);

  // Search for a field called <name> on any column in <value_table_columns_>.
  // Returns HAS_AMBIGUOUS_FIELD if <name> exists on multiple columns.
  Type::HasFieldResult LookupFieldTargetLocalOnly(
      IdString name, NameTarget* field_target) const;

  friend class NameList;
  FRIEND_TEST(NameScope, Test);
  FRIEND_TEST(NameScope, CorrelatedColumnsSet);
  FRIEND_TEST(NameScope, TestCreateNewRangeVariableTargetGivenValidNamePaths);
  FRIEND_TEST(NameScope, TestCreateNewLocalNameTargetsGivenValidNamePaths);
  FRIEND_TEST(NameScope, TestCreateNewValueTableColumnsGivenValidNamePaths);
  FRIEND_TEST(NameScope, TestCreateGetFieldTargetFromInvalidValueTableColumn);
  FRIEND_TEST(NameList, TestRangeVarCaseOverridesColumn);
};

// A NameList is an ordered list of visible column names produced by a scan.
// It corresponds to what will be visible in SELECT * or JOIN USING.
//
// The NameList also contains:
//   - The range variables visible for scans, which can be used in
//     SELECT alias.* or SELECT alias.column.
//   - The pseudo-columns visible coming out of this scan.
//     Order is not relevant for pseudo-columns since they never show up
//     in SELECT * or other ordered lists of outputs.
//
// Differences from a NameScope:
//   - NameLists are the ordered list of columns produced locally as part
//     of a FROM clause or SELECT list.
//   - NameScopes are unordered and just act like a map from names to
//     objects.
//   - NameScopes have hierarchy and can find names in parent scopes.
//     NameLists have no such hierarchy.
//
// As an implementation detail, for efficiency, the NameList is implemented
// using a NameScope inside it to store all names that can be resolved locally.
//
// Duplicate column names are not an error.  Lookups by name for duplicate
// columns will return ambiguous.  Duplicate range variables are not allowed
// and will give an error at insertion time.  (A NameList corresponds to one
// FROM clause and duplicate range variables are not allowed in the same FROM.)
//
// Value tables are added to the NameList as columns, but also act like range
// variables for the purpose of uniqueness checking.  For the purpose of
// resolution, and for loading into a NameScope, the value table alias is
// treated as a range variable (pointing at a value table NameList).
//
// Value tables may have a set of field names that are excluded from NameList
// lookups.  For instance, if a NameList includes value table column 't'
// with field 'f', and 'f' is in the field exclusion list of 't', then
// looking up 'f' in the NameList does not find it.  Additionally, if column
// 'f' also exists in the NameList as regular column then looking up 'f' in
// the NameList returns regular column 'f' - the look up is not ambiguous
// even though 'f' exists in 't' and would be ambiguous if 'f' was not in
// the field exclusion list.
//
// NameList also has an is_value_table() marker bit that is used on NameLists
// representing a query result to indicate that the query result is a
// value table.
//
// Several methods here take an <ast_location> argument and use that location
// for returned error messages (usually, name collisions).
class NameList {
 public:
  NameList();
  NameList(const NameList&) = delete;
  NameList& operator=(const NameList&) = delete;
  ~NameList();

  // Prepare this NameList for 'size' new columns. This is for efficiency
  // purposes only.
  void ReserveColumns(int size) { columns_.reserve(size); }

  // Add a named column.
  // <is_explicit> should be true if the alias for this column is an explicit
  // name. Generally, explicit names come directly from the query text, and
  // implicit names are those that are generated automatically from something
  // outside the query text, like column names that come from a table schema.
  // Explicitness does not change any scoping behavior except for the final
  // check in strict mode that may raise an error. For more information, please
  // see the beginning of (broken link).
  absl::Status AddColumn(IdString name, const ResolvedColumn& column,
                         bool is_explicit);

  // Add a column that stores the value produced by a value table scan,
  // and also a range variable that can be used to reference rows from the scan.
  //
  // If <pseudo_columns_name_list> is non-NULL, it provides a list of
  // pseudo-columns that should be usable via the new range variable.
  // <pseudo_columns_name_list> should have no other columns or range variables.
  // <pseudo_columns_name_list> can be the same NameList as <this>.
  //
  // <excluded_field_names> identifies a list of field names from the
  // value table that should be excluded from NameList lookups.  For example,
  // if 'f1' is on the excluded list for value table column 'vt', then a
  // lookup for name 'f1' will not find it.  But the excluded field can still
  // be found explicitly by resolving the path 'vt.f1'.  Also, star expansion
  // on the NameList does not include excluded fields, but 'rangvar.*'
  // expansion does include them.
  //
  // Lookups for <range_variable_name> will return the range variable, not the
  // column. The returned NameList will have is_value_table()=true.
  absl::Status AddValueTableColumn(
      IdString range_variable_name, const ResolvedColumn& column,
      const ASTNode* ast_location,
      const IdStringSetCase& excluded_field_names = {},
      const NameListPtr& pseudo_columns_name_list = nullptr);

  // Add a pseudo-column.  Pseudo-columns are always implicit.
  // They can be looked up by name but don't show up in columns().
  absl::Status AddPseudoColumn(IdString name,
                               const ResolvedColumn& column,
                               const ASTNode* ast_location);

  // Add a range variable.  Returns an error if the name conflicts with another
  // range variable.
  // Adding a NameList to itself is not allowed and will fail.
  // Adding a value table NameList is not allowed and will fail.
  // Adding cycles of NameLists is also not allowed but will not be caught.
  absl::Status AddRangeVariable(
      IdString name,
      const std::shared_ptr<const NameList>& scan_columns,
      const ASTNode* ast_location);

  // Adds a Column with <name> twice to the NameList, ensuring its ambiguity.
  // Note that a range variable with the same name still takes precedence
  // over the ambiguous column.
  // This is intended for testing only.
  absl::Status AddAmbiguousColumn_Test(IdString name);

  // Add all names from <other>.  Returns an error if there is collision
  // in range variable names.
  absl::Status MergeFrom(const NameList& other, const ASTNode* ast_location);

  // Add all names from <other>, except <excluded_field_names> (if non-NULL).
  // Range variables with matching names are also excluded.
  // Returns an error if there is collision in range variable names.
  absl::Status MergeFromExceptColumns(
      const NameList& other,
      const IdStringSetCase* excluded_field_names,  // May be NULL
      const ASTNode* ast_location);

  // Clone current NameList, invoking clone_column for each column to create new
  // columns.
  //
  // The <value_table_error> is used to produce a caller-context-specific error
  // message if the name list contains currently unsupported value table
  // columns.
  //
  // The <clone_column> function is invoked once for each column to be
  // cloned. Range variables and pseudo columns are not cloned.
  //
  // In practice, note that some <clone_column> function
  // implementations remember the mapping from the original
  // column to the cloned column.
  absl::StatusOr<std::shared_ptr<NameList>> CloneWithNewColumns(
      const ASTNode* ast_location, absl::string_view value_table_error,
      const ASTAlias* alias,
      std::function<ResolvedColumn(const ResolvedColumn&)> clone_column,
      IdStringPool* id_string_pool) const;

  // Get the regular columns in this NameList.  Does not include pseudo-columns.
  int num_columns() const { return columns_.size(); }
  const std::vector<NamedColumn>& columns() const { return columns_; }
  const NamedColumn& column(int i) const { return columns_[i]; }

  // Return vector of ResolvedColumns contained in columns().
  std::vector<ResolvedColumn> GetResolvedColumns() const;

  // Return vector of column names.  Excludes pseudo-columns.
  std::vector<IdString> GetColumnNames() const;

  // Look up a name in this NameList.
  bool LookupName(IdString name, NameTarget* found) const;

  // Check whether a column name will show up in SELECT * expansion for this
  // NameList.  The return value indicates if this column name was not present,
  // was present exactly once, or was present multiple times.
  //
  // This looks for the same set of columns that will show up in SELECT *,
  // so it excludes range variables, pseudo-columns, and for value table
  // columns with fields, looks for field names rather than the column name.
  //
  // Please note that since SELECT * queries will never return special
  // pseudo-columns like has_ fields or named extensions, this function will
  // never return Type::HAS_PSEUDO_FIELD.
  Type::HasFieldResult SelectStarHasColumn(IdString name) const;

  // is_value_table indicates that this NameList represents a FROM clause that
  // produced a value table.  It has no effect on any lookup or mutation
  // behavior on either NameList or NameScope.  It is used by the Resolver
  // as a marker.  The NameList gives the set of columns produced by a query,
  // and this marker bit indicates that the query produces a value table.
  //
  // When this is true, the NameList should have exactly one column.
  // This is enforced elsewhere.
  void set_is_value_table(bool value) { is_value_table_ = value; }
  bool is_value_table() const { return is_value_table_; }

  std::string DebugString(absl::string_view indent = absl::string_view()) const;

  bool HasRangeVariable(IdString name) const;

  bool HasValueTableColumns() const {
    return name_scope_.HasLocalValueTableColumns();
  }
  bool HasRangeVariables() const {
    return name_scope_.HasLocalRangeVariables();
  }

  // Add a range variable, using a wrapper NameList that gets the range
  // variable. Example:
  //   select ... from (select a,b,c) AS S
  // The subquery produces a NameList with [a,b,c].
  // To add the range variable S, we construct a NameList [a,b,c,S->[a,b,c]].
  // Adding S to the initial NameList would allow cyclic lookups like S.S.S.a.
  static absl::StatusOr<std::shared_ptr<NameList>>
  AddRangeVariableInWrappingNameList(
      IdString alias, const ASTNode* ast_location,
      std::shared_ptr<const NameList> original_name_list);

 private:
  bool is_value_table_ = false;

  // This is the vector of columns that will show up in SELECT *.
  // Some will be marked as value tables; those may be expanded further
  // during SELECT * to show their fields instead of the value itself.
  std::vector<NamedColumn> columns_;

  // This stores all resolvable names in the NameList, including range
  // variables and pseudo-columns, but excluding anonymous columns.
  // Duplicate names will be collapsed to one NameTarget, possibly indicating
  // the name is ambiguous.
  //
  // Range variables inside the NameScope always point at a NameList giving the
  // columns available inside the scan under that range variable.
  NameScope name_scope_;

  friend class NameScope;
  FRIEND_TEST(NameList, TestRangeVarCaseOverridesColumn);
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_NAME_SCOPE_H_
