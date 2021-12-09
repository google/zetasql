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

#include "zetasql/analyzer/rewriters/registration.h"

#include <functional>
#include <memory>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/public/options.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"

namespace zetasql {

RewriteRegistry& RewriteRegistry::global_instance() {
  static auto* const kRegistry = new RewriteRegistry;
  return *kRegistry;
}

const Rewriter* RewriteRegistry::Get(ResolvedASTRewrite key) const {
  absl::MutexLock l(&mu_);
  auto it = rewriters_.find(key);
  if (it == rewriters_.end()) {
    if (ZETASQL_DEBUG_MODE) {
      ZETASQL_LOG(FATAL) << "Rewriter was not registered: "
                 << ResolvedASTRewrite_Name(key);
    }
    return nullptr;
  }
  return it->second;
}

absl::Span<const ResolvedASTRewrite> RewriteRegistry::registration_order()
    const {
  return registration_order_;
}

void RewriteRegistry::Register(ResolvedASTRewrite key,
                               const Rewriter* rewriter) {
  absl::MutexLock l(&mu_);
  const bool did_insert = rewriters_.emplace(key, rewriter).second;
  if (ZETASQL_DEBUG_MODE) {
    ZETASQL_CHECK(did_insert) << "Key conflict for ZetaSQL Rewriter: "
                      << ResolvedASTRewrite_Name(key);
  }
  registration_order_.push_back(key);
}

}  // namespace zetasql
