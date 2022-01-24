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

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"

namespace zetasql {
namespace {

using testing::NotNull;
using testing::WhenDynamicCastTo;

// A base rewriter that does nothing, but serves as the base class for our dummy
// rewriters below.
class Base : public Rewriter {
 public:
  ~Base() override {}

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    return absl::UnimplementedError("");
  }

  std::string Name() const override { return "Test rewriter"; }
};

// These fakes are only used to verify whether we are returning the correct
// object from the registry.
class FakeAnonRewriter : public Base {
 public:
  ~FakeAnonRewriter() override {}
};
class FakeArrayFilterTransformRewriter : public Base {
 public:
  ~FakeArrayFilterTransformRewriter() override {}
};

TEST(RegistrationTest, Test) {
  RewriteRegistry& r = RewriteRegistry::global_instance();

  FakeAnonRewriter anon_instance;
  r.Register(ResolvedASTRewrite::REWRITE_ANONYMIZATION, &anon_instance);
  FakeArrayFilterTransformRewriter array_instance;
  r.Register(ResolvedASTRewrite::REWRITE_ARRAY_FILTER_TRANSFORM,
             &array_instance);

  {
    const Rewriter* anon = r.Get(ResolvedASTRewrite::REWRITE_ANONYMIZATION);
    ASSERT_EQ(anon, &anon_instance);
  }

  {
    const Rewriter* array_filter =
        r.Get(ResolvedASTRewrite::REWRITE_ARRAY_FILTER_TRANSFORM);
    ASSERT_EQ(array_filter, &array_instance);
  }

  EXPECT_DEBUG_DEATH(
      {
        EXPECT_EQ(r.Get(ResolvedASTRewrite::REWRITE_ARRAY_INCLUDES), nullptr);
      },
      ".*Rewriter was not registered.*");

  EXPECT_DEBUG_DEATH(
      { r.Register(ResolvedASTRewrite::REWRITE_ANONYMIZATION, nullptr); },
      ".*Key conflict for ZetaSQL Rewriter.*");
}

}  // namespace
}  // namespace zetasql
