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

#include "zetasql/testing/test_module_contents_fetcher.h"

#include <string>
#include <vector>

#include "zetasql/public/module_contents_fetcher.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {
namespace testing {

absl::Status TestModuleContentsFetcher::FetchModuleContents(
    const std::vector<std::string>& module_name_path,
    ModuleContentsInfo* module_info) {
  std::string* in_memory_module_contents =
      zetasql_base::FindOrNull(in_memory_modules_, module_name_path);
  if (in_memory_module_contents != nullptr) {
    module_info->module_name_path = module_name_path;
    module_info->filename = absl::StrJoin(module_name_path, ".");
    module_info->contents = *in_memory_module_contents;
    return absl::OkStatus();
  }
  return file_module_contents_fetcher_->FetchModuleContents(module_name_path,
                                                            module_info);
}

absl::Status TestModuleContentsFetcher::FetchProtoFileDescriptor(
    const std::string& proto_file_name,
    const google::protobuf::FileDescriptor** proto_file_descriptor) {
  ZETASQL_RET_CHECK_NE(descriptor_pool_, nullptr)
      << "Descriptor pool not provided during construction of "
         "TestModuleContentsFetcher";
  *proto_file_descriptor = descriptor_pool_->FindFileByName(proto_file_name);
  if (*proto_file_descriptor == nullptr) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Could not find imported proto '" << proto_file_name
           << "' in the test DescriptorPool";
  }
  return absl::OkStatus();
}

absl::Status TestModuleContentsFetcher::AddInMemoryModule(
    std::vector<std::string> module_name_path,
    absl::string_view module_contents) {
  ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&in_memory_modules_, module_name_path,
                                    std::string(module_contents)))
      << "Module name path " << absl::StrJoin(module_name_path, ".")
      << " already added to TestModuleContentsFetcher";
  return absl::OkStatus();
}

}  // namespace testing
}  // namespace zetasql
