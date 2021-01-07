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

namespace zetasql {

RewriteRegistry& RewriteRegistry::global_instance() {
  static auto* const kRegistry = new RewriteRegistry;
  return *kRegistry;
}

std::vector<const Rewriter*> RewriteRegistry::GetRewriters() const {
  absl::MutexLock l(&mu_);
  return rewriters_;
}

RewriteRegistry::RegistryToken RewriteRegistry::Register(Rewriter* r) {
  absl::MutexLock l(&mu_);
  rewriters_.push_back(r);
  return {};
}

}  // namespace zetasql
