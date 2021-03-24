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

#include "zetasql/common/proto_helper.h"

#include <cstdint>
#include <string>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "zetasql/common/errors.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Need to implement this to catch importing errors.
class StringAppendErrorCollector :
      public google::protobuf::DescriptorPool::ErrorCollector {
 public:
  StringAppendErrorCollector() {}
  StringAppendErrorCollector(const StringAppendErrorCollector&) = delete;
  StringAppendErrorCollector& operator=(const StringAppendErrorCollector&)
      = delete;

  void AddError(const std::string& filename, const std::string& element_name,
                const google::protobuf::Message* descriptor, ErrorLocation location,
                const std::string& message) override {
    absl::StrAppend(&error_, HasError() ? "\n" : "", filename, ": ",
                    element_name, ": ", message);
  }
  bool HasError() const { return !error_.empty(); }
  const std::string& GetError() const { return error_; }

 private:
  std::string error_;
};

}  // namespace

absl::Status PopulateFileDescriptorSet(
    const google::protobuf::FileDescriptor* file_descr,
    absl::optional<int64_t> file_descriptor_set_max_size_bytes,
    google::protobuf::FileDescriptorSet* file_descriptor_set,
    std::set<const google::protobuf::FileDescriptor*>* file_descriptors) {
  ZETASQL_RET_CHECK(file_descriptor_set != nullptr);
  ZETASQL_RET_CHECK(file_descriptors != nullptr);
  if (zetasql_base::InsertIfNotPresent(file_descriptors, file_descr)) {
    for (int idx = 0; idx < file_descr->dependency_count(); ++idx) {
      ZETASQL_RETURN_IF_ERROR(PopulateFileDescriptorSet(
          file_descr->dependency(idx), file_descriptor_set_max_size_bytes,
          file_descriptor_set, file_descriptors));
    }
    file_descr->CopyTo(file_descriptor_set->add_file());
  }
  if (file_descriptor_set_max_size_bytes.has_value() &&
      file_descriptor_set->ByteSizeLong() >
          file_descriptor_set_max_size_bytes.value()) {
    return MakeSqlError()
        << "Serializing proto descriptors failed due to maximum "
        << "FileDescriptorSet size exceeded, max = "
        << file_descriptor_set_max_size_bytes.value() << ", size = "
        << file_descriptor_set->ByteSizeLong();
  }
  return absl::OkStatus();
}

absl::Status AddFileDescriptorSetToPool(
    const google::protobuf::FileDescriptorSet* file_descriptor_set,
    google::protobuf::DescriptorPool* pool) {
  StringAppendErrorCollector error_collector;
  for (int idx = 0; idx < file_descriptor_set->file_size(); ++idx) {
    pool->BuildFileCollectingErrors(file_descriptor_set->file(idx),
                                    &error_collector);
    if (error_collector.HasError()) {
      return MakeSqlError()
             << "Error(s) encountered during protocol buffer analysis: "
             << error_collector.GetError();
    }
  }
  return absl::OkStatus();
}

}  // namespace zetasql
