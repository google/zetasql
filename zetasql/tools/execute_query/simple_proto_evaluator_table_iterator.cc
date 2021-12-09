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

#include "zetasql/tools/execute_query/simple_proto_evaluator_table_iterator.h"

#include <string>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"

namespace zetasql {

SimpleProtoEvaluatorTableIterator::SimpleProtoEvaluatorTableIterator(
    const ProtoType* proto_type)
    : proto_type_(proto_type) {}

int SimpleProtoEvaluatorTableIterator::NumColumns() const { return 1; }

std::string SimpleProtoEvaluatorTableIterator::GetColumnName(int i) const {
  ZETASQL_CHECK_EQ(i, 0);
  return kValueColumnName;
}

const Type* SimpleProtoEvaluatorTableIterator::GetColumnType(int i) const {
  ZETASQL_CHECK_EQ(i, 0);
  return proto_type_;
}

const Value& SimpleProtoEvaluatorTableIterator::GetValue(int i) const {
  ZETASQL_CHECK_EQ(i, 0);
  return current_value_;
}

absl::Status SimpleProtoEvaluatorTableIterator::Status() const {
  return status_;
}

// No cancellation or deadline support.
absl::Status SimpleProtoEvaluatorTableIterator::Cancel() {
  return absl::OkStatus();
}

}  // namespace zetasql
