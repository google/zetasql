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

#include "zetasql/compliance/test_util.h"

#include <cstdint>
#include <utility>

#include "zetasql/base/logging.h"
#include "absl/container/btree_map.h"
#include "absl/flags/commandlineflag.h"
#include "absl/flags/reflection.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

namespace {
constexpr size_t kLogBufferSize =
    15000;

constexpr absl::string_view kTestShardingStrategyFlag =
    "test_sharding_strategy";
constexpr absl::string_view kTestShardingStrategyValue = "disabled";
constexpr absl::string_view kTestShardIndex = "TEST_SHARD_INDEX";
constexpr absl::string_view kTestTotalShards = "TEST_TOTAL_SHARDS";
constexpr absl::string_view kTestTimeoutFlag = "test_timeout";
constexpr absl::string_view kTestTimeoutEnv = "TEST_TIMEOUT";
}  // namespace

// Recursively computes transitive closure of a set of protos P and a set of
// enums E.
// Input:  protos:        set of protos P
//         enums:         set of enums E
// Output: proto_closure: closure set of protos C(P)
//         enum_closure:  closure set of enums C(E)
// Algorithm:
//   1. C(P) <- C(P) union P
//      C(E) <- C(E) union E
//   2. dP <- ({contain_p : contain_p contains some p in P} union
//             {contain_e : contain_e contains some e in E} union
//             {child_p : child_p is a child of some p in P}) / C(P)
//      dE <- {child_e : child_e is a child of some p in P} / C(E)
//   3. if (dP not empty OR dE not empty) recursively expand transitive
//      closure using:
//          P' <- dP
//          E' <- dE
//   4. When both dP and dE are empty, C(P) and C(E) cannot be expanded further
//      and are closures of P and E, respectively.
//
absl::Status ComputeTransitiveClosure(const google::protobuf::DescriptorPool* pool,
                                      const std::set<std::string>& protos,
                                      const std::set<std::string>& enums,
                                      std::set<std::string>* proto_closure,
                                      std::set<std::string>* enum_closure) {
  // C(P) <- C(P) union P, C(E) <- C(E) union E.
  proto_closure->insert(protos.begin(), protos.end());
  enum_closure->insert(enums.begin(), enums.end());
  std::set<std::string> delta_proto;  // dP
  std::set<std::string> delta_enum;   // dE
  for (absl::string_view proto : protos) {
    const google::protobuf::Descriptor* descriptor =
        pool->FindMessageTypeByName(std::string(proto));
    if (!descriptor) {
      return absl::Status(absl::StatusCode::kNotFound,
                          absl::StrCat("Proto Message Type: ", proto));
    }
    if (descriptor->containing_type()) {
      // 'full_name' is in {contain_p}.
      const std::string& full_name = descriptor->containing_type()->full_name();
      if (!zetasql_base::ContainsKey(*proto_closure, full_name)) {
        delta_proto.insert(full_name);
      }
    }

    auto ProcessField = [&proto_closure, &enum_closure, &delta_proto,
                         &delta_enum](const google::protobuf::FieldDescriptor* field) {
      if (field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE ||
          field->type() == google::protobuf::FieldDescriptor::TYPE_GROUP) {
        // 'full_name' is in {child_p}.
        const std::string& full_name = field->message_type()->full_name();
        if (!zetasql_base::ContainsKey(*proto_closure, full_name)) {
          delta_proto.insert(full_name);
        }
      } else if (field->type() == google::protobuf::FieldDescriptor::TYPE_ENUM) {
        // 'full_name' is in {child_e}.
        const std::string& full_name = field->enum_type()->full_name();
        if (!zetasql_base::ContainsKey(*enum_closure, full_name)) {
          delta_enum.insert(full_name);
        }
      }
    };

    for (int i = 0; i < descriptor->field_count(); i++) {
      const google::protobuf::FieldDescriptor* field = descriptor->field(i);
      ProcessField(field);
    }
    for (int i = 0; i < descriptor->extension_count(); i++) {
      const google::protobuf::FieldDescriptor* field = descriptor->extension(i);
      ProcessField(field);
      // 'full_name' is in {contain_p}.
      const std::string& full_name = field->containing_type()->full_name();
      if (!zetasql_base::ContainsKey(*proto_closure, full_name)) {
        delta_proto.insert(full_name);
      }
    }
  }
  for (const std::string& enum_item : enums) {
    const google::protobuf::EnumDescriptor* enum_descriptor =
        pool->FindEnumTypeByName(enum_item);
    if (!enum_descriptor) {
      return absl::Status(absl::StatusCode::kNotFound,
                          absl::StrCat("Enum Type: ", enum_item));
    }
    if (enum_descriptor->containing_type()) {
      // 'full_name' is in {contain_e}.
      const std::string& full_name =
          enum_descriptor->containing_type()->full_name();
      if (!zetasql_base::ContainsKey(*proto_closure, full_name)) {
        delta_proto.insert(full_name);
      }
    }
  }
  if (!delta_proto.empty() || !delta_enum.empty()) {
    // Expand C(P) and C(E) using dP and dE.
    return ComputeTransitiveClosure(pool, delta_proto, delta_enum,
                                    proto_closure, enum_closure);
  }
  // Cannot expand C(P) and C(E) further. Algorithm terminates.
  return absl::OkStatus();
}

absl::string_view LogChunkDelimiter::Find(absl::string_view text,
                                          size_t pos) const {
  // Find returns the next delimiter in the string after pos.  In our case,
  // these delimiters are 0-length as we do not want them removed from the
  // output.
  absl::string_view substr = absl::ClippedSubstr(text, pos);
  // Use a max a little smaller than LogMessage to allow for prefix space.
  const size_t kMaxSize = kLogBufferSize - 256;
  if (substr.length() < kMaxSize) {
    return absl::string_view(text.end(), 0);
  }
  // Otherwise try to split on a newline.
  auto newline_pos = substr.rfind('\n', kMaxSize);
  if (newline_pos == absl::string_view::npos) {
    newline_pos = kMaxSize;
  }
  return absl::ClippedSubstr(text, pos + newline_pos + 1, 0);
}

static int64_t GetInt64FromEnv(absl::string_view env_var, int64_t def) {
  const char* env_val = getenv(std::string(env_var).c_str());
  if (env_val == nullptr) {return def;}
  int64_t val;
  ZETASQL_CHECK(absl::SimpleAtoi(env_val, &val));
  return val;
}

ReproCommand::ReproCommand() {
  const char* test_target_c_str = getenv("TEST_TARGET");
  std::string test_binary;
  if (test_target_c_str) {
    test_target_ = test_target_c_str;
  }
  int64_t test_shard_index = GetInt64FromEnv(kTestShardIndex, -1);
  if (test_shard_index != -1) {
    // If this is a sharded test, only run the shard that failed.
    build_flags_[std::string(kTestShardingStrategyFlag)] =
        kTestShardingStrategyValue;
    test_envs_[std::string(kTestTotalShards)] =
        absl::StrCat(GetInt64FromEnv(kTestTotalShards, 1));
    test_envs_[std::string(kTestShardIndex)] =
        absl::StrCat(GetInt64FromEnv(kTestShardIndex, 0));
  }
  build_flags_[std::string(kTestTimeoutFlag)] =
      absl::StrCat(GetInt64FromEnv(kTestTimeoutEnv, kDefaultTimeout));
}

bool ReproCommand::RemoveFromMapIfPresent(
    const std::string& key, absl::btree_map<std::string, std::string>* map) {
  auto it = map->find(key);
  if (it != map->end()) {
    map->erase(it);
    return true;
  }
  return false;
}

std::string ReproCommand::get() const {
  std::string command = absl::StrCat("bazel test ", test_target_, " ");

  for (const auto& build_flag : build_flags_) {
    absl::StrAppendFormat(&command, " --%s=%s", build_flag.first,
                          build_flag.second);
  }
  for (const auto& test_arg_flag : test_arg_flags_) {
    absl::StrAppendFormat(&command, " --test_arg=--%s=%s", test_arg_flag.first,
                          test_arg_flag.second);
  }
  for (const auto& test_env : test_envs_) {
    if (!test_env.second.empty()) {
      absl::StrAppendFormat(&command, " --test_env %s=%s", test_env.first,
                            test_env.second);
    }
  }
  return command;
}

}  // namespace zetasql
