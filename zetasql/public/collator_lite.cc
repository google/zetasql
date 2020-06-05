//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/public/collator.h"
#include "absl/base/thread_annotations.h"
#include "absl/strings/cord.h"
#include "absl/synchronization/mutex.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {
namespace {

class CaseSensitiveUnicodeCollator : public ZetaSqlCollator {
 public:
  CaseSensitiveUnicodeCollator() {}

  int64_t CompareUtf8(absl::string_view s1, absl::string_view s2,
                    absl::Status* error) const override {
    const int result = s1.compare(s2);
    return result < 0 ? -1 : (result > 0 ? 1 : 0);
  }

  bool IsBinaryComparison() const override { return true; }
};

class CollatorRegistration {
 public:
  using CreateFromCollationNameFn =
      std::function<zetasql_base::StatusOr<ZetaSqlCollator*>(absl::string_view)>;

  CollatorRegistration() {
    registered_fn_ = &CollatorRegistration::DefaultCreateFromCollationNameFn;
  }
  CollatorRegistration(const CollatorRegistration&) = delete;
  CollatorRegistration& operator=(const CollatorRegistration&) = delete;

  static CollatorRegistration* GetInstance() {
    static auto* instance = new CollatorRegistration();
    return instance;
  }

  void SetCreateFromCollationNameFn(const CreateFromCollationNameFn& fn) {
    absl::MutexLock lock(&mu_);
    registered_fn_ = fn;
  }

  zetasql_base::StatusOr<ZetaSqlCollator*> CreateFromCollationName(
      absl::string_view collation_name) {
    absl::MutexLock lock(&mu_);
    return registered_fn_(collation_name);
  }

  // This default function returns a basic case-sensitive Unicode collator
  // if that's what is requested, and fails otherwise. The ICU
  // implementation is needed for any more complex collations.
  static zetasql_base::StatusOr<ZetaSqlCollator*> DefaultCreateFromCollationNameFn(
      absl::string_view collation_name) {
    if (collation_name == "unicode" || collation_name == "unicode:cs") {
      return new CaseSensitiveUnicodeCollator();
    }
    return zetasql_base::UnimplementedErrorBuilder()
           << "The requested collation name \"" << collation_name
           << "\" requires a collator implementation that has not been "
              "registered in this binary.";
  }

 private:
  absl::Mutex mu_;
  CreateFromCollationNameFn registered_fn_ ABSL_GUARDED_BY(mu_);
};

}  // namespace

// Destructor for the pure virtual class is defined here, not in collator.cc, so
// that :collator_lite can be used without depending on :collator (which itself
// has a dependency on :collator_lite).
ZetaSqlCollator::~ZetaSqlCollator() {}

zetasql_base::StatusOr<ZetaSqlCollator*>
ZetaSqlCollator::CreateFromCollationNameLite(
    const std::string& collation_name) {
  return CollatorRegistration::GetInstance()->CreateFromCollationName(
      collation_name);
}

namespace internal {
void RegisterDefaultCollatorImpl() {
  CollatorRegistration::GetInstance()->SetCreateFromCollationNameFn(
      &CollatorRegistration::DefaultCreateFromCollationNameFn);
}

void RegisterIcuCollatorImpl(
    std::function<zetasql_base::StatusOr<ZetaSqlCollator*>(absl::string_view)>
        create_fn) {
  CollatorRegistration::GetInstance()->SetCreateFromCollationNameFn(create_fn);
}
}  // namespace internal

}  // namespace zetasql
