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

// Tool for running a query against a Catalog constructed from various input
// sources. Also serves as a demo of the PreparedQuery API.
//
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/experimental/output_query_result.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/types/optional.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

ABSL_FLAG(std::string, mode, "execute",
          "The tool mode to use. Valid values are 'resolve', to print the "
          "resolved AST, 'explain', to show the query plan, and 'execute', "
          "to actually run the query and print the result.");

// TODO: Consider allowing <proto> to be equal to "string" to
// represent a std::string instead of a proto.

namespace zetasql {

namespace {
// Represents --mode.
enum class ToolMode { kResolve, kExplain, kExecute };

// Translates --mode to a ToolMode.
zetasql_base::StatusOr<ToolMode> GetToolMode() {
  const std::string mode = absl::GetFlag(FLAGS_mode);
  if (mode == "resolve") {
    return ToolMode::kResolve;
  } else if (mode == "explain") {
    return ToolMode::kExplain;
  } else if (mode == "execute") {
    return ToolMode::kExecute;
  } else if (mode.empty()) {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Must specify --mode";
  } else {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Invalid --mode: " << mode;
  }
}

// Constructs a Catalog corresponding to --table_spec.
zetasql_base::StatusOr<std::unique_ptr<Catalog>> ConstructCatalog(
    const google::protobuf::DescriptorPool* pool, TypeFactory* type_factory) {
  auto catalog = absl::make_unique<SimpleCatalog>("catalog", type_factory);
  catalog->AddZetaSQLFunctions();
  catalog->SetDescriptorPool(pool);

  return std::move(catalog);
}

// Prints the result of executing a query. Currently requires loading all the
// results into memory to format pretty output.
zetasql_base::Status PrintResults(std::unique_ptr<EvaluatorTableIterator> iter) {
  TypeFactory type_factory;

  std::vector<StructField> struct_fields;
  struct_fields.reserve(iter->NumColumns());
  for (int i = 0; i < iter->NumColumns(); ++i) {
    struct_fields.emplace_back(iter->GetColumnName(i), iter->GetColumnType(i));
  }

  const StructType* struct_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeStructType(struct_fields, &struct_type));

  std::vector<Value> rows;
  while (true) {
    if (!iter->NextRow()) {
      ZETASQL_RETURN_IF_ERROR(iter->Status());
      break;
    }

    std::vector<Value> fields;
    fields.reserve(iter->NumColumns());
    for (int i = 0; i < iter->NumColumns(); ++i) {
      fields.push_back(iter->GetValue(i));
    }

    rows.push_back(Value::Struct(struct_type, fields));
  }

  const ArrayType* array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeArrayType(struct_type, &array_type));

  const Value result = Value::Array(array_type, rows);

  std::vector<std::string> column_names;
  column_names.reserve(iter->NumColumns());
  for (int i = 0; i < iter->NumColumns(); ++i) {
    column_names.push_back(iter->GetColumnName(i));
  }

  std::cout << ToPrettyOutputStyle(result,
                                   /*is_value_table=*/false, column_names)
            << std::endl;

  return zetasql_base::OkStatus();
}

// Runs the tool.
zetasql_base::Status Run(const std::string& sql) {
  ZETASQL_ASSIGN_OR_RETURN(const ToolMode mode, GetToolMode());
  const google::protobuf::DescriptorPool &pool =
      *google::protobuf::DescriptorPool::generated_pool();
  TypeFactory type_factory;
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<Catalog> catalog,
                   ConstructCatalog(&pool, &type_factory));

  PreparedQuery query(sql, EvaluatorOptions());

  AnalyzerOptions options;
  options.mutable_language()->EnableMaximumLanguageFeaturesForDevelopment();
  ZETASQL_RETURN_IF_ERROR(query.Prepare(options, catalog.get()));

  switch (mode) {
    case ToolMode::kResolve:
      std::cout << query.resolved_query_stmt()->DebugString() << std::endl;
      return zetasql_base::OkStatus();
    case ToolMode::kExplain: {
      ZETASQL_ASSIGN_OR_RETURN(const std::string explain, query.ExplainAfterPrepare());
      std::cout << explain << std::endl;
      return zetasql_base::OkStatus();
    }
    case ToolMode::kExecute: {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());
      return PrintResults(std::move(iter));
    }
  }
}

}  // namespace
}  // namespace zetasql

int main(int argc, char* argv[]) {
  const char kUsage[] =
      "Usage: execute_query [--table_spec=<table_spec>] <sql>\n";
  std::vector<char*> remaining_args = absl::ParseCommandLine(argc, argv);
  if (argc <= 1) {
    LOG(QFATAL) << kUsage;
  }
  const std::string sql = absl::StrJoin(remaining_args.begin() + 1,
  remaining_args.end(), " ");

  const zetasql_base::Status status = zetasql::Run(sql);
  if (status.ok()) {
    return 0;
  } else {
    std::cout << "ERROR: " << status << std::endl;
    return 1;
  }
}
