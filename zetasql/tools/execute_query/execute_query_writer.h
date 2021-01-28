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

#ifndef ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WRITER_H_
#define ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WRITER_H_

#include <iosfwd>
#include <memory>

#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace zetasql {

class ExecuteQueryWriter {
 public:
  virtual ~ExecuteQueryWriter() = default;

  virtual absl::Status resolved(const ResolvedNode& ast) = 0;
  virtual absl::Status explained(const ResolvedNode& ast,
                                 absl::string_view explain) = 0;
  virtual absl::Status executed(
      const ResolvedNode& ast,
      std::unique_ptr<EvaluatorTableIterator> iter) = 0;
};

// Writes a human-readable representation of the query result to an output
// stream.
class ExecuteQueryStreamWriter : public ExecuteQueryWriter {
 public:
  explicit ExecuteQueryStreamWriter(std::ostream&);
  ExecuteQueryStreamWriter(const ExecuteQueryStreamWriter&) = delete;
  ExecuteQueryStreamWriter& operator=(const ExecuteQueryStreamWriter&) = delete;

  absl::Status resolved(const ResolvedNode& ast) override;
  absl::Status explained(const ResolvedNode& ast,
                         absl::string_view explain) override;
  absl::Status executed(const ResolvedNode& ast,
                        std::unique_ptr<EvaluatorTableIterator> iter) override;

 private:
  std::ostream& stream_;
};

}  // namespace zetasql

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WRITER_H_
