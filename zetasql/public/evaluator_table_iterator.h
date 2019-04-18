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

// API for providing a table to the ZetaSQL Evaluator (see evaluator.h).
//
// TODO: DO NOT USE THIS API YET. The Evaluator support for it is still
// being implemented. b/110046614

#ifndef ZETASQL_PUBLIC_EVALUATOR_TABLE_ITERATOR_H_
#define ZETASQL_PUBLIC_EVALUATOR_TABLE_ITERATOR_H_

#include "zetasql/public/value.h"
#include "zetasql/base/status.h"

namespace zetasql {

// Iterator interface for a user-supplied table in a PreparedQuery.
//
// Example:
//   Table* table = ... Get table from catalog ...
//   ZETASQL_ASSIGN_OR_RETURN(
//     std::unique_ptr<EvaluatorTableIterator> iter,
//     table->CreateEvaluatorTableIterator(column_names));
//   while (true) {
//     if (!iter->NextRow()) {
//       ZETASQL_RETURN_IF_ERROR(iter->Status());
//     }
//     ... Do something with 'iter->GetValue(...)' ...
//   }
//
// TODO: Add a way for the user to specify the primary key of the table
// (if there is one) and a Seek interface to allow the reference implementation
// to exploit that to avoid unnecessarily scanning rows. E.g.:
//   SELECT * FROM SomeBigTable WHERE key >= "foo"
// should result in seeking to "foo" and scanning from there. Similarly,
//   SELECT * FROM SomeBigTable WHERE key <= "bar"
// should result in stopping the scan after "bar". b/110046614
class EvaluatorTableIterator {
 public:
  virtual ~EvaluatorTableIterator() {}

  // Returns the number of columns returned by the iterator.
  virtual int NumColumns() const = 0;

  // Returns the name of the i-th column. Anonymous columns have empty
  // names. There may be more than one column with the same name. 'i' must be in
  // [0, 'NumColumns()').
  virtual std::string GetColumnName(int i) const = 0;

  // Returns the type of the i-th column. 'i' must be in [0, 'NumColumns()').
  virtual const Type* GetColumnType(int i) const = 0;

  // Returns false if there is no next row. The caller must then check
  // 'Status()'. If NextRow() returns false, the only allowed operations on this
  // iterator are NumColumns(), GetColumnName(), GetColumnType(), and Status().
  virtual bool NextRow() = 0;

  // Returns the value of the i-th column of this iterator.
  // 'i' must be in [0, 'NumColumns()').
  // NextRow() must have been called at least once and the last call must have
  // returned true.
  virtual const Value& GetValue(int i) const = 0;

  // Returns OK unless the last call to NextRow() returned false because of an
  // error (including cancellation).
  virtual zetasql_base::Status Status() const = 0;

  // Best-effort cancellation that can be called from any thread. This must not
  // block for a long time.  The implementation could set a cancel bit and then
  // check for that inside processing loops or in NextRow().  The returned
  // Status does not necessarily indicate that the Cancel succeeded or failed,
  // but rather is just used for logging and can have
  // implementation-specific meaning.
  virtual zetasql_base::Status Cancel() = 0;

  // Best-effort deadline support. This may not be called after the first call
  // to NextRow(). This must not block for a long time. The implementation could
  // set a deadline member and check for its expiration inside processing loops
  // or in NextRow().
  virtual void SetDeadline(absl::Time deadline) {}
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_EVALUATOR_TABLE_ITERATOR_H_
