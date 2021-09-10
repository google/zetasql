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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/value.h"
#include "zetasql/tools/execute_query/simple_proto_evaluator_table_iterator.h"
#include "zetasql/tools/execute_query/string_error_collector.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/file_util.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// Represents a text proto file as a value table with 1 row.
class TextProtoEvaluatorTableIterator
    : public SimpleProtoEvaluatorTableIterator {
 public:
  TextProtoEvaluatorTableIterator(absl::string_view path,
                                  const ProtoType* proto_type,
                                  absl::Span<const int> columns)
      : SimpleProtoEvaluatorTableIterator(proto_type), path_(path) {
    ZETASQL_CHECK_EQ(columns.size(), 1);
    ZETASQL_CHECK_EQ(columns[0], 0);
  }

  bool NextRow() override {
    if (done_) {
      return false;
    }
    done_ = true;

    google::protobuf::DynamicMessageFactory dynfac;
    std::unique_ptr<google::protobuf::Message> dynmsg(
        dynfac.GetPrototype(proto_type_->descriptor())->New());

    std::string buf;
    status_ = internal::GetContents(path_, &buf);
    if (!status_.ok()) return false;

    google::protobuf::TextFormat::Parser parser;
    std::string parse_errors;
    StringErrorCollector collector(&parse_errors);
    parser.RecordErrorsTo(&collector);
    if (!parser.ParseFromString(buf, dynmsg.get())) {
      status_ = absl::InvalidArgumentError(parse_errors);
      return false;
    }
    absl::Cord cord;
    std::string bytes_str;
    bool success = dynmsg->SerializeToString(&bytes_str);
    cord = absl::Cord(bytes_str);
    if (!success) {
      status_ = absl::InternalError("Can't serialize proto");
      return false;
    }
    current_value_ = Value::Proto(proto_type_, cord);
    return true;
  }

 private:
  const std::string path_;
  bool done_ = false;
};

}  // namespace

absl::StatusOr<std::unique_ptr<SimpleTable>> MakeTableFromTextProtoFile(
    absl::string_view table_name, absl::string_view path,
    const ProtoType* column_proto_type) {
  std::unique_ptr<SimpleTable> table;
  std::vector<SimpleTable::NameAndType> columns = {
      {SimpleProtoEvaluatorTableIterator::kValueColumnName, column_proto_type}};

  table = absl::make_unique<SimpleTable>(table_name, columns);
  table->set_is_value_table(true);
  // Make a copy, because we cannot trust the lifetime of `path`.
  std::string string_path = std::string(path);
  table->SetEvaluatorTableIteratorFactory(
      [string_path, column_proto_type](absl::Span<const int> columns)
          -> absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>> {
        return absl::make_unique<TextProtoEvaluatorTableIterator>(
            string_path, column_proto_type, columns);
      });
  return table;
}

}  // namespace zetasql
