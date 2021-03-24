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

#ifndef ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROTO_WRITER_H_
#define ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROTO_WRITER_H_

#include <functional>
#include <iosfwd>
#include <memory>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/status/status.h"

namespace zetasql {

class ExecuteQueryStreamProtobufWriter : public ExecuteQueryWriter {
 public:
  ExecuteQueryStreamProtobufWriter(
      const google::protobuf::DescriptorPool* parent_descriptor_pool,
      std::function<absl::Status(const google::protobuf::Message& msg)>
          proto_writer_func);
  ExecuteQueryStreamProtobufWriter(const ExecuteQueryStreamProtobufWriter&) =
      delete;
  ExecuteQueryStreamProtobufWriter& operator=(
      const ExecuteQueryStreamProtobufWriter&) = delete;

  absl::Status executed(const ResolvedNode& ast,
                        std::unique_ptr<EvaluatorTableIterator> iter) override;

 private:
  const google::protobuf::DescriptorPool* parent_descriptor_pool_;
  std::function<absl::Status(const google::protobuf::Message& msg)> proto_writer_func_;
};

absl::Status ExecuteQueryWriteTextproto(const google::protobuf::Message& msg,
                                        std::ostream& stream);

absl::Status ExecuteQueryWriteJson(const google::protobuf::Message& msg,
                                   std::ostream& stream);

}  // namespace zetasql

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROTO_WRITER_H_
