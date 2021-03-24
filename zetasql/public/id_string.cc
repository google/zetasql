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

#include "zetasql/public/id_string.h"

#include <cstdint>

#include "zetasql/base/logging.h"
#include "zetasql/base/case.h"
#include "absl/synchronization/mutex.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

#ifndef NDEBUG

absl::Mutex IdStringPool::global_mutex_;

// Initialize lazily in IdStringPool::AllocatePoolId when first pool is created.
std::unordered_set<int64_t>* IdStringPool::live_pool_ids_ = nullptr;

int64_t IdStringPool::max_pool_id_ = 0;

// static
void IdStringPool::CheckPoolIdAlive(int64_t pool_id) {
  absl::MutexLock l(&global_mutex_);
  ZETASQL_DCHECK(live_pool_ids_ != nullptr);
  if (!zetasql_base::ContainsKey(*live_pool_ids_, pool_id)) {
    ZETASQL_LOG(FATAL) << "IdString was accessed after its IdStringPool ("
               << pool_id << ") was destructed";
  }
}
#endif

IdStringPool::IdStringPool()
#ifndef NDEBUG
    : arena_(std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/1024)),
      pool_id_(AllocatePoolId()) {
  ZETASQL_VLOG(1) << "Allocated IdStringPool " << pool_id_;
#else
    : arena_(std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/1024)) {
#endif
}

IdStringPool::IdStringPool(const std::shared_ptr<zetasql_base::UnsafeArena>& arena)
#ifndef NDEBUG
    : arena_(arena), pool_id_(AllocatePoolId()) {
  ZETASQL_VLOG(1) << "Allocated IdStringPool " << pool_id_;
#else
    : arena_(arena) {
#endif
}

IdStringPool::~IdStringPool() {
#ifndef NDEBUG
  ZETASQL_VLOG(1) << "Deleting IdStringPool " << pool_id_;
  absl::MutexLock l(&global_mutex_);
  ZETASQL_CHECK_EQ(1, live_pool_ids_->erase(pool_id_));
#endif
}

#ifndef NDEBUG
int64_t IdStringPool::AllocatePoolId() {
  absl::MutexLock l(&global_mutex_);
  if (live_pool_ids_ == nullptr)
    live_pool_ids_ = new std::unordered_set<int64_t>;
  int64_t pool_id = ++max_pool_id_;
  zetasql_base::InsertOrDie(live_pool_ids_, pool_id);
  return pool_id;
}
#endif

// We want to keep one global empty string constant so that we can implement
// the default constructor and clear() as assignment, without allocating
// a new Shared object.
const IdString* const IdString::kEmptyString =
    new IdString(IdString::MakeGlobal(""));

IdString IdString::ToLower(IdStringPool* pool) const {
  return pool->Make(absl::AsciiStrToLower(ToStringView()));
}

}  // namespace zetasql
