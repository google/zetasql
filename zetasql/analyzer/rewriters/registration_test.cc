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

#include <memory>
#include <string>

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
class FakeBuiltinFunctionInlinerRewriter : public Base {
 public:
  ~FakeBuiltinFunctionInlinerRewriter() override {}
};

TEST(RegistrationTest, Test) {
  RewriteRegistry& r = RewriteRegistry::global_instance();

  FakeAnonRewriter anon_instance;
  r.Register(ResolvedASTRewrite::REWRITE_ANONYMIZATION, &anon_instance);
  FakeBuiltinFunctionInlinerRewriter fn_inliner_instance;
  r.Register(ResolvedASTRewrite::REWRITE_BUILTIN_FUNCTION_INLINER,
             &fn_inliner_instance);

  {
    const Rewriter* anon = r.Get(ResolvedASTRewrite::REWRITE_ANONYMIZATION);
    ASSERT_EQ(anon, &anon_instance);
  }

  {
    const Rewriter* fn_inliner =
        r.Get(ResolvedASTRewrite::REWRITE_BUILTIN_FUNCTION_INLINER);
    ASSERT_EQ(fn_inliner, &fn_inliner_instance);
  }

  EXPECT_DEBUG_DEATH(
      {
        EXPECT_EQ(r.Get(ResolvedASTRewrite::REWRITE_TYPEOF_FUNCTION), nullptr);
      },
      ".*Rewriter was not registered.*");

  EXPECT_DEBUG_DEATH(
      { r.Register(ResolvedASTRewrite::REWRITE_ANONYMIZATION, nullptr); },
      ".*Key conflict for ZetaSQL Rewriter.*");
}

}  // namespace
}  // namespace zetasql
