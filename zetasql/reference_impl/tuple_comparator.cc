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

#include "zetasql/reference_impl/tuple_comparator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/message.h"
#include "zetasql/public/collator.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/common.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple.h"
#include <cstdint>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Populates 'collators' with the ZetaSqlCollators corresponding to the input
// arguments.
static absl::Status GetZetaSqlCollators(
    absl::Span<const KeyArg* const> keys,
    absl::Span<const TupleData* const> params, EvaluationContext* context,
    std::vector<std::unique_ptr<const ZetaSqlCollator>>* collators) {
  for (int i = 0; i < keys.size(); ++i) {
    if (keys.at(i)->collation() != nullptr) {
      TupleSlot collation_slot;
      absl::Status status;
      if (!keys.at(i)->collation()->EvalSimple(params, context, &collation_slot,
                                               &status)) {
        return status;
      }
      const Value& collation_value = collation_slot.value();
      if (collation_value.is_null()) {
        return ::zetasql_base::OutOfRangeErrorBuilder()
               << "COLLATE requires non-NULL collation name";
      }

      std::unique_ptr<const ZetaSqlCollator> collator;
      switch (collation_value.type_kind()) {
        case TYPE_STRING: {
          ZETASQL_ASSIGN_OR_RETURN(collator,
                           MakeSqlCollatorLite(collation_value.string_value()));
          break;
        }
        case TYPE_PROTO: {
          ZETASQL_ASSIGN_OR_RETURN(
              collator, GetCollatorFromResolvedCollationValue(collation_value));
          break;
        }
        default:
          ZETASQL_RET_CHECK_FAIL() << "Unexpected type kind of collation_value "
                           << Type::TypeKindToString(
                                  collation_value.type_kind(),
                                  PRODUCT_INTERNAL);
      }

      collators->emplace_back(std::move(collator));
    } else {
      collators->emplace_back(nullptr);
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<TupleComparator>> TupleComparator::Create(
    absl::Span<const KeyArg* const> keys, absl::Span<const int> slots_for_keys,
    absl::Span<const TupleData* const> params, EvaluationContext* context) {
  std::shared_ptr<Collators> collators =
      std::make_shared<Collators>(Collators());
  ZETASQL_RETURN_IF_ERROR(
      GetZetaSqlCollators(keys, params, context, collators.get()));
  return absl::WrapUnique(new TupleComparator(keys, slots_for_keys, collators));
}

bool TupleComparator::operator()(const TupleData& t1,
                                 const TupleData& t2) const {
  for (int i = 0; i < keys_.size(); ++i) {
    const KeyArg* key = keys_[i];
    const ZetaSqlCollator* collator = (*collators_)[i].get();

    const int slot_idx = slots_for_keys_[i];
    const Value& v1 = t1.slot(slot_idx).value();
    const Value& v2 = t2.slot(slot_idx).value();

    if (v1.is_null() || v2.is_null()) {
      if (v1.is_null() && v2.is_null()) {  // NULLs are considered equal.
        continue;
      }
      if (key->is_descending()) {
        // NULLS LAST is the default for DESC order.
        const bool nulls_last = key->null_order() != KeyArg::kNullsFirst;
        // Non-null sorts after null except with nulls-first ordering.
        return nulls_last ? !v1.is_null() : v1.is_null();
      } else {
        // NULLS FIRST is the default for ASC order.
        const bool nulls_first = key->null_order() != KeyArg::kNullsLast;
        // Null sorts before non-null except with nulls-last ordering.
        return nulls_first ? !v2.is_null() : v2.is_null();
      }
    }

    if (collator != nullptr) {
      ZETASQL_DCHECK(v1.type()->IsString());
      ZETASQL_DCHECK(v2.type()->IsString());
      absl::Status status;
      int64_t result =
          collator->CompareUtf8(v1.string_value(), v2.string_value(), &status);
      ZETASQL_DCHECK_OK(status);
      if (result != 0) {  // v1 != v2
        if (key->is_descending()) {
          return result > 0;  // v1 > v2
        } else {
          return result < 0;  // v1 < v2
        }
      }
    } else {
      if (!v1.Equals(v2)) {
        if (key->is_descending()) {
          return v2.LessThan(v1);
        } else {
          return v1.LessThan(v2);
        }
      }
    }
  }
  // The keys are equal.
  return false;
}

bool TupleComparator::IsUniquelyOrdered(
    absl::Span<const TupleData* const> tuples,
    absl::Span<const int> slot_idxs_for_values) const {
  for (int i = 1; i < tuples.size(); ++i) {
    const TupleData* a = tuples[i - 1];
    const TupleData* b = tuples[i];

    if ((*this)(*a, *b)) {
      continue;
    }

    bool equal = true;
    for (const int slot_idx : slot_idxs_for_values) {
      if (!a->slot(slot_idx).value().Equals(b->slot(slot_idx).value())) {
        equal = false;
        break;
      }
    }

    if (!equal) {
      // 'a' and 'b' are unequal when all their values are considered, but this
      // comparator does not yield 'a' < 'b'. Therefore, 'tuples' is either not
      // sorted or the sort order is not unique because 'a' and 'b' can be
      // reversed.
      return false;
    }
  }
  return true;
}

}  // namespace zetasql
