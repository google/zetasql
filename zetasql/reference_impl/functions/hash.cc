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

#include "zetasql/reference_impl/functions/hash.h"

#include "zetasql/public/functions/hash.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/function.h"
#include "absl/status/statusor.h"

namespace zetasql {
namespace {

class HashFunction : public SimpleBuiltinScalarFunction {
 public:
  explicit HashFunction(FunctionKind kind);
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;

 private:
  const std::unique_ptr<functions::Hasher> hasher_;
};

class FarmFingerprintFunction : public SimpleBuiltinScalarFunction {
 public:
  FarmFingerprintFunction()
      : SimpleBuiltinScalarFunction(FunctionKind::kFarmFingerprint,
                                    types::Int64Type()) {}
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override;
};

HashFunction::HashFunction(FunctionKind kind)
    : SimpleBuiltinScalarFunction(kind, types::BytesType()),
      hasher_(
          functions::Hasher::Create([kind]() -> functions::Hasher::Algorithm {
            switch (kind) {
              case FunctionKind::kMd5:
                return functions::Hasher::kMd5;
              case FunctionKind::kSha1:
                return functions::Hasher::kSha1;
              case FunctionKind::kSha256:
                return functions::Hasher::kSha256;
              case FunctionKind::kSha512:
                return functions::Hasher::kSha512;
              default:
                // Crash in debug mode, for non-debug mode fall back to MD5.
                ZETASQL_DLOG(FATAL)
                    << "Unexpected function kind: " << static_cast<int>(kind);
                return functions::Hasher::kMd5;
            }
          }())) {}

absl::StatusOr<Value> HashFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(1, args.size());
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  const absl::string_view input = args[0].type_kind() == TYPE_BYTES
                                      ? args[0].bytes_value()
                                      : args[0].string_value();

  return Value::Bytes(hasher_->Hash(input));
}

absl::StatusOr<Value> FarmFingerprintFunction::Eval(
    absl::Span<const TupleData* const> params, absl::Span<const Value> args,
    EvaluationContext* context) const {
  ZETASQL_RET_CHECK_EQ(1, args.size());
  if (args[0].is_null()) {
    return Value::Null(output_type());
  }

  const absl::string_view input = args[0].type_kind() == TYPE_BYTES
                                      ? args[0].bytes_value()
                                      : args[0].string_value();

  return Value::Int64(functions::FarmFingerprint(input));
}

}  // namespace

void RegisterBuiltinHashFunctions() {
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kMd5, FunctionKind::kSha1, FunctionKind::kSha256,
       FunctionKind::kSha512},
      [](FunctionKind kind, const Type* output_type) {
        return new HashFunction(kind);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kFarmFingerprint},
      [](FunctionKind kind, const Type* output_type) {
        return new FarmFingerprintFunction();
      });
}

}  // namespace zetasql
