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

#include "zetasql/reference_impl/functions/compression.h"

#include <cstdint>
#include <string>

#include "zetasql/public/functions/compression.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/base/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

class ZstdCompressFunction : public SimpleBuiltinScalarFunction {
 public:
  ZstdCompressFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    ABSL_DCHECK(output_type->IsBytes());
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
    ZETASQL_RET_CHECK(args[0].type()->IsBytes() || args[0].type()->IsString());
    if (args[0].is_null()) {
      return Value::NullBytes();
    }
    absl::string_view input;
    if (args[0].type()->IsString()) {
      input = args[0].string_value();
    } else {
      input = args[0].bytes_value();
    }
    ZETASQL_ASSIGN_OR_RETURN(std::string output,
                     functions::ZstdCompress(input, args[1].int64_value()));
    return Value::Bytes(output);
  }
};

class ZstdDecompressToBytesFunction : public SimpleBuiltinScalarFunction {
 public:
  ZstdDecompressToBytesFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    ABSL_DCHECK(output_type->IsBytes());
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
    ZETASQL_RET_CHECK(args[0].type()->IsBytes());
    if (args[0].is_null()) {
      return Value::NullBytes();
    }
    ZETASQL_ASSIGN_OR_RETURN(
        std::string output,
        functions::ZstdDecompress(args[0].bytes_value(), /*check_utf8=*/false,
                                  args[1].int64_value()));
    return Value::Bytes(output);
  }
};

class ZstdDecompressToStringFunction : public SimpleBuiltinScalarFunction {
 public:
  ZstdDecompressToStringFunction(FunctionKind kind, const Type* output_type)
      : SimpleBuiltinScalarFunction(kind, output_type) {
    ABSL_DCHECK(output_type->IsString());
  }
  absl::StatusOr<Value> Eval(absl::Span<const TupleData* const> params,
                             absl::Span<const Value> args,
                             EvaluationContext* context) const override {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
    ZETASQL_RET_CHECK(args[0].type()->IsBytes());
    if (args[0].is_null()) {
      return Value::NullString();
    }
    ZETASQL_ASSIGN_OR_RETURN(
        std::string output,
        functions::ZstdDecompress(args[0].bytes_value(), /*check_utf8=*/true,
                                  args[1].int64_value()));
    return Value::String(output);
  }
};

}  // namespace

void RegisterBuiltinCompressionFunctions() {
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kZstdCompress},
      [](FunctionKind kind, const Type* output_type) {
        return new ZstdCompressFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kZstdDecompressToBytes},
      [](FunctionKind kind, const Type* output_type) {
        return new ZstdDecompressToBytesFunction(kind, output_type);
      });
  BuiltinFunctionRegistry::RegisterScalarFunction(
      {FunctionKind::kZstdDecompressToString},
      [](FunctionKind kind, const Type* output_type) {
        return new ZstdDecompressToStringFunction(kind, output_type);
      });
}

}  // namespace zetasql
