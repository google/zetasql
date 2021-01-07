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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace zetasql {
namespace {

using testing::Each;
using testing::IsNull;
using testing::NotNull;
using testing::UnorderedElementsAre;
using testing::WhenDynamicCastTo;

class Base : public Rewriter {
 public:
  ~Base() override {}

  bool ShouldRewrite(const AnalyzerOptions& analyzer_options,
                     const AnalyzerOutput& analyzer_output) const override {
    return false;
  }

  zetasql_base::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    return absl::UnimplementedError("");
  }
};

class R1 : public Base {
  ~R1() override {}
};
class R2 : public Base {
  ~R2() override {}
};
class R3 : public Base {
  ~R3() override {}
};

REGISTER_ZETASQL_REWRITER(R1);
REGISTER_ZETASQL_REWRITER(R2);

TEST(RegistrationTest, Test) {
  // The registry contains the classes we registered.
  EXPECT_THAT(RewriteRegistry::global_instance().GetRewriters(),
              UnorderedElementsAre(WhenDynamicCastTo<const R1*>(NotNull()),
                                   WhenDynamicCastTo<const R2*>(NotNull())));
  // On the other hand, none of the elements are of the class we didn't
  // register.
  EXPECT_THAT(RewriteRegistry::global_instance().GetRewriters(),
              Each(WhenDynamicCastTo<const R3*>(IsNull())));
}

}  // namespace
}  // namespace zetasql
