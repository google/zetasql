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

#ifndef ZETASQL_REFERENCE_IMPL_TUPLE_COMPARATOR_H_
#define ZETASQL_REFERENCE_IMPL_TUPLE_COMPARATOR_H_

#include <memory>
#include <vector>

#include "zetasql/common/internal_value.h"
#include "zetasql/public/collator.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"

namespace zetasql {

class EvaluationContext;
class KeyArg;
struct Tuple;
class TupleData;

class TupleComparator {
 public:
  // Validates the collators in <keys> if non-nullptr, returns both the tuple
  // comparator and the collators upon success. 'slots_for_keys[i]' is the index
  // of 'keys[i]' in a TupleData passed to operator().
  static absl::StatusOr<std::unique_ptr<TupleComparator>> Create(
      absl::Span<const KeyArg* const> keys,
      absl::Span<const int> slots_for_keys,
      absl::Span<const TupleData* const> params, EvaluationContext* context);

  // Returns true if t1 is less than t2.
  bool operator()(const TupleData& t1, const TupleData& t2) const;

  // t1 and t2  must not be NULL.
  bool operator()(const TupleData* t1, const TupleData* t2) const {
    return (*this)(*t1, *t2);
  }

  // t1 and t2 must not be NULL.
  bool operator()(const std::unique_ptr<TupleData>& t1,
                  const std::unique_ptr<TupleData>& t2) const {
    return (*this)(*t1, *t2);
  }

  // Returns true if for any two consecutive tuples a and b in 'tuples'
  // (which must be sorted), either a < b according to this object, or the
  // values corresponding to 'slots_for_values' in a and b are all equal. In
  // this case, not only is 'tuples' sorted according to this object, but that
  // sort order is unique.
  bool IsUniquelyOrdered(absl::Span<const TupleData* const> tuples,
                         absl::Span<const int> slot_idxs_for_values) const;

  const std::vector<const KeyArg*>& keys() const { return keys_; }

 private:
  using Collators = std::vector<std::unique_ptr<const ZetaSqlCollator>>;

  TupleComparator(absl::Span<const KeyArg* const> keys,
                  absl::Span<const int> slots_for_keys,
                  std::shared_ptr<const Collators> collators)
      : keys_(keys.begin(), keys.end()),
        slots_for_keys_(slots_for_keys.begin(), slots_for_keys.end()),
        collators_(collators) {}

  const std::vector<const KeyArg*> keys_;
  const std::vector<int> slots_for_keys_;
  // <collators_> indicates the COLLATE specific rules to compare strings for
  // each sort key in <keys_>. This corresponds 1-1 with keys_.
  // NOTE: If any element of <collators_> is nullptr, then the strings are
  // compared based on their UTF-8 encoding.
  // We use std::shared_ptr<const ...> to allow the comparator to be copied.
  const std::shared_ptr<const Collators> collators_;
};

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_TUPLE_COMPARATOR_H_
