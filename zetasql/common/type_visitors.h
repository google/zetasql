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

#ifndef ZETASQL_COMMON_TYPE_VISITORS_H_
#define ZETASQL_COMMON_TYPE_VISITORS_H_

#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Simple visitor, which does not rewrite the given type.
class TypeVisitor {
 public:
  virtual ~TypeVisitor() = default;

  virtual absl::Status PostVisit(AnnotatedType annotated_type) {
    return absl::OkStatus();
  }

  absl::Status Visit(AnnotatedType annotated_type);
};

// Rewriter which recursively visits an AnnotatedType and its component types.
// Note: this rewriter is unrelated to SignatureArgumentKinds.
class TypeRewriter {
 public:
  explicit TypeRewriter(TypeFactory& type_factory)
      : type_factory_(type_factory) {}

  virtual ~TypeRewriter() = default;

  virtual absl::StatusOr<AnnotatedType> PostVisit(
      AnnotatedType annotated_type) = 0;

  absl::StatusOr<AnnotatedType> Visit(AnnotatedType annotated_type);

  TypeFactory& type_factory() { return type_factory_; }

 private:
  TypeFactory& type_factory_;
};

}  // namespace zetasql

#endif  // ZETASQL_COMMON_TYPE_VISITORS_H_
