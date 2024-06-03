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

#include "zetasql/resolved_ast/column_factory.h"

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/strings/string_view.h"

namespace zetasql {

ResolvedColumn ColumnFactory::MakeCol(absl::string_view table_name,
                                      absl::string_view col_name,
                                      const Type* type) {
  UpdateMaxColId();
  if (id_string_pool_ != nullptr) {
    return ResolvedColumn(max_col_id_, id_string_pool_->Make(table_name),
                          id_string_pool_->Make(col_name), type);
  } else {
    return ResolvedColumn(max_col_id_,
                          zetasql::IdString::MakeGlobal(table_name),
                          zetasql::IdString::MakeGlobal(col_name), type);
  }
}

ResolvedColumn ColumnFactory::MakeCol(absl::string_view table_name,
                                      absl::string_view col_name,
                                      AnnotatedType annotated_type) {
  UpdateMaxColId();
  if (id_string_pool_ != nullptr) {
    return ResolvedColumn(max_col_id_, id_string_pool_->Make(table_name),
                          id_string_pool_->Make(col_name), annotated_type);
  } else {
    return ResolvedColumn(
        max_col_id_, zetasql::IdString::MakeGlobal(table_name),
        zetasql::IdString::MakeGlobal(col_name), annotated_type);
  }
}

void ColumnFactory::UpdateMaxColId() {
  if (sequence_ == nullptr) {
    ++max_col_id_;
  } else {
    while (true) {
      // Allocate from the sequence, but make sure it's higher than the max we
      // should start from.
      int next_col_id = static_cast<int>(sequence_->GetNext());
      if (next_col_id > max_col_id_) {
        max_col_id_ = next_col_id;
        break;
      }
    }
  }
}

}  // namespace zetasql
