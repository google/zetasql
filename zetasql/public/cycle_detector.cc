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

#include "zetasql/public/cycle_detector.h"

#include "zetasql/base/logging.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

absl::Status CycleDetector::ObjectInfo::DetectCycle(
    const std::string& object_type) const {
  if (!cycle_detected_) {
    return absl::OkStatus();
  }
  const std::vector<std::string>& cycle_names = cycle_detector_->ObjectNames();
  std::string message;
  if (cycle_names.size() == 1) {
    // This is a self-recursive object, so return a custom error message.
    absl::StrAppend(&message, "The ", object_type, " ", name_, " is recursive");
  } else {
    absl::StrAppend(&message, "Recursive dependencies detected when resolving ",
                    object_type, " ", name_, ", which include objects (",
                    absl::StrJoin(cycle_names, ", "), ", ", name_, ")");
  }
  // We return INVALID_ARGUMENT, which the ZetaSQL Resolver detects in
  // order to add an error location to the Status returned from FindFunction()
  // calls.
  return ::zetasql_base::InvalidArgumentErrorBuilder() << message;
}

bool CycleDetector::DetectCycleOrPushObject(const ObjectInfo* object_info) {
  const bool cycle_detected = !zetasql_base::InsertIfNotPresent(&objects_, object_info);
  if (!cycle_detected) {
    object_deque_.emplace_back(object_info);
  }
  // Expected invariant.
  ZETASQL_DCHECK_EQ(objects_.size(), object_deque_.size());
  return cycle_detected;
}

void CycleDetector::PopObject(const ObjectInfo* expected_object_info) {
  if (object_deque_.empty() ||
      expected_object_info != object_deque_.back()) {
    ZETASQL_LOG(DFATAL) << "Unexpected object being popped from CycleDetector: "
                << expected_object_info->name() << ":"
                << expected_object_info->object() << "\nCycle detector: "
                << DebugString();
    // In non-DEBUG code, return as a no-op.
    return;
  }
  objects_.erase(object_deque_.back());
  object_deque_.pop_back();
  // Expected invariant.
  ZETASQL_DCHECK_EQ(objects_.size(), object_deque_.size());
}

CycleDetector::ObjectInfo::ObjectInfo(const std::string& name,
                                      const void* object,
                                      CycleDetector* cycle_detector)
    : name_(name), object_(object) {
  cycle_detected_ = DetectCycleOrAddToCycleDetector(cycle_detector);
  cycle_detector_ = cycle_detector;
}

CycleDetector::ObjectInfo::~ObjectInfo() {
  if (!cycle_detected_) {
    cycle_detector_->PopObject(this);
  }
}

bool CycleDetector::ObjectInfo::DetectCycleOrAddToCycleDetector(
    CycleDetector* cycle_detector) {
  if (cycle_detector->DetectCycleOrPushObject(this)) {
    return true;
  }
  cycle_detector_ = cycle_detector;
  return false;
}

static inline void ObjectInfoNameFormatter(
    std::string* out, const CycleDetector::ObjectInfo* info) {
  absl::StrAppend(out, info->name());
}

std::string CycleDetector::DebugString() const {
  return absl::StrJoin(object_deque_, ", ", ObjectInfoNameFormatter);
}

std::vector<std::string> CycleDetector::ObjectNames() const {
  std::vector<std::string> names;
  for (const ObjectInfo* info : object_deque_) {
    names.push_back(info->name());
  }
  return names;
}

}  // namespace zetasql
