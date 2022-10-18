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

#ifndef ZETASQL_ANALYZER_REWRITERS_REWRITER_INTERFACE_H_
#define ZETASQL_ANALYZER_REWRITERS_REWRITER_INTERFACE_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"

namespace zetasql {

class AnalyzerOptions;
class AnalyzerOutput;
class AnalyzerOutputProperties;
class Catalog;
class ResolvedNode;
class TypeFactory;

// A Rewriter rewrites known patterns in a ResolvedAST, typically to simpler or
// more universally supported forms. This is a mechanism that allows ZetaSQL
// to add new functionality without any additional backend effort, where that
// functionality can be expressed in plain SQL.
//
// Thread safety: all Rewriter subclasses must be logically stateless and
// thread-safe.
class Rewriter {
 public:
  virtual ~Rewriter() {}

  // Rewrites 'input' according to the rules of this rewriter. Returns the
  // rewritten AST, and potentially other output properties, in the return
  // value.
  //
  // The rewriter must use pools and sequence numbers from 'options' to allocate
  // new columns and ids. Likewise, any new types must be allocated via
  // 'type_factory'.
  //
  // The rewriter may return an error if the rewrite fails. In general, a
  // rewriter should never do semantic validation of the query being rewritten.
  // There are some other cases that can result in errors returned.
  // kInvalidArgument errors are possible especially if a catalog object is
  //     invalid. Because semantic validation is always handled before
  //     rewriting, a KInvalidArgument typically is the fault of the query
  //     engine and are generally not interesting to the user.
  // kInternal errors typically mean a broken invariant and likely signal a bug
  //     in the rewriter or an unexpected input shape. This typically is the
  //     fault of the compiler library and is generally not interesting to the
  //     user.
  // kUnimplemented errors are likely for legal shapes that are not supported by
  //     the rewriter implementation. Presumably, if the engine enables the
  //     rewrite and the rewrite doesn't support a shape, the engine doesn't
  //     support that shape either. The user needs to see these errors so they
  //     can work around the unsupported shape.
  virtual absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const = 0;

  virtual std::string Name() const = 0;
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_REWRITER_INTERFACE_H_
