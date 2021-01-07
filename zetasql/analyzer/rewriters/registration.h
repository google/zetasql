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

#ifndef ZETASQL_ANALYZER_REWRITERS_REGISTRATION_H_
#define ZETASQL_ANALYZER_REWRITERS_REGISTRATION_H_

#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/memory/memory.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"

namespace zetasql {

// A Rewriter rewrites known patterns in a ResolvedAST, typically to simpler or
// more universally supported forms. This is a mechanism that allows ZetaSQL
// to add new functionality without any additional backend effort, where that
// functionality can be expressed in plain SQL.
//
// Thread safety: all Rewriter subclasses must be thread-safe.
class Rewriter {
 public:
  virtual ~Rewriter() {}

  // Returns whether this rewriter should be activated. Typically this requires
  // both the presence of the rewriter's enum in 'analyzer_options', and also
  // the presence of relevant AST nodes detected in the original
  // 'analyzer_output'.
  virtual bool ShouldRewrite(const AnalyzerOptions& analyzer_options,
                             const AnalyzerOutput& analyzer_output) const = 0;

  // Rewrites 'input' according to the rules of this rewriter. Returns the
  // rewritten AST, and potentially other output properties, in the return
  // value.
  //
  // The rewriter must use pools and sequence numbers from 'options' to allocate
  // new columns and ids. Likewise, any new types must be allocated via
  // 'type_factory'.
  virtual zetasql_base::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const = 0;
};

// A registry for rewriter classes. A global instance of this class is available
// via global_instance().
class RewriteRegistry {
 public:
  struct RegistryToken {};

  // Registers the given rewriter. 'rewriter' is never deleted, but its pointer
  // will be returned in future calls to GetRewriters.
  RegistryToken Register(Rewriter* rewriter);

  std::vector<const Rewriter*> GetRewriters() const;

  static RewriteRegistry& global_instance();

 private:
  // Not to be constructed or destructed.
  RewriteRegistry() {}
  ~RewriteRegistry() {}

  friend RewriteRegistry& GetRewriteRegistry();

  mutable absl::Mutex mu_;
  std::vector<const Rewriter*> rewriters_ ABSL_GUARDED_BY(mu_);
};

// Registers a rewriter class. The macro should be in a cc file for the
// rewriter, and the library build rule for the rewriter should use
// alwayslink=1.
//
// Example Use:
//
// // my_rewriter.cc
// class MyRewriter : public Rewriter { ... };
// REGISTER_ZETASQL_REWRITER(MyRewriter);
//
// // BUILD
// cc_library(name = "my_rewriter",
//            srcs = ["my_rewriter.cc"],
//            alwayslink=1)
#define REGISTER_ZETASQL_REWRITER(clas)                               \
  static const auto kRewriteRegistryToken##clas ABSL_ATTRIBUTE_UNUSED = \
      RewriteRegistry::global_instance().Register(new clas)

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_REGISTRATION_H_
