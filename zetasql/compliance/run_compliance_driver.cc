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

#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/init_google.h"
#include "zetasql/base/logging.h"
#include "zetasql/common/options_utils.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/compliance/test_driver.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/value.h"

namespace zetasql {
struct QueryParameterFlagValue {
  std::map<std::string, zetasql::Value> parameters;
};

bool AbslParseFlag(absl::string_view text, QueryParameterFlagValue* flag,
                   std::string* err) {
  AnalyzerOptions analyzer_options;
  analyzer_options.set_language(
      zetasql::GetComplianceTestDriver()->GetSupportedLanguageOptions());

  auto catalog = std::make_unique<SimpleCatalog>("test");
  catalog->AddZetaSQLFunctions(
      ZetaSQLBuiltinFunctionOptions(analyzer_options.language()));

  return internal::ParseQueryParameterFlag(
      text, analyzer_options, catalog.get(), &flag->parameters, err);
}
std::string AbslUnparseFlag(const QueryParameterFlagValue& flag) {
  return internal::UnparseQueryParameterFlag(flag.parameters);
}
}  // namespace zetasql

ABSL_FLAG(std::string, sql_file, "",
          "Input file containing query; leave blank to read from stdin");

ABSL_FLAG(
    std::string, test_db, "",
    "File containing a TestDatabaseProto. Either binary or text format is "
    "accepted. If blank, the query will run against an empty TestDatabase.");

ABSL_FLAG(zetasql::QueryParameterFlagValue, parameters, {},
          zetasql::internal::kQueryParameterMapHelpstring);

namespace {
constexpr char kUsage[] = R"(Runs a query against a compliance test driver.

The query may be supplied in the following ways:
  1) As a direct commandline argument:
       run_compliance_test_driver 'SELECT 1'
  2) As a text file:
       run_compliance_test_driver /path/to/file
  3) Via stdin:
       echo 'SELECT 1' < $0
)";

std::string ReadSqlFromStdin() {
  if (isatty(fileno(stdin))) {
    std::cout << "Please type the sql you want to analyze." << std::endl;
    std::cout << "Press ctrl-D to terminate the query and run it." << std::endl;
  }
  std::string sql;
  for (std::string line; std::getline(std::cin, line);) {
    absl::StrAppend(&sql, line, "\n");
    break;
  }
  return sql;
}

std::string GetQuery(const std::vector<std::string>& args) {
  if (args.empty()) {
    if (!absl::GetFlag(FLAGS_sql_file).empty()) {
      std::string sql;
      ZETASQL_CHECK_OK(zetasql_base::GetContents(absl::GetFlag(FLAGS_sql_file), &sql,
                                  ::zetasql_base::Defaults()));
      return sql;
    } else {
      return ReadSqlFromStdin();
    }
  } else {
    return absl::StrJoin(args, " ");
  }
}
}  // namespace

int main(int argc, char* argv[]) {
  std::vector<std::string> args;
  {
    std::vector<char*> remaining_args = absl::ParseCommandLine(argc, argv);
    args.assign(remaining_args.cbegin() + 1, remaining_args.cend());
  }
  std::string sql = GetQuery(args);
  zetasql::TestDriver* test_driver = zetasql::GetComplianceTestDriver();
  zetasql::TestDatabase test_db;
  zetasql::TypeFactory type_factory;
  const std::vector<google::protobuf::DescriptorPool*> descriptor_pools;
  std::vector<std::unique_ptr<const zetasql::AnnotationMap>> annotation_maps;
  if (absl::GetFlag(FLAGS_test_db).empty()) {
    ZETASQL_CHECK_OK(test_driver->CreateDatabase(test_db));
  } else {
    zetasql::TestDatabaseProto proto;
    zetasql_base::ReadFileToProtoOrDie(absl::GetFlag(FLAGS_test_db), &proto);
    absl::StatusOr<zetasql::TestDatabase> db =
        zetasql::DeserializeTestDatabase(proto, &type_factory,
                                           descriptor_pools, annotation_maps);
    ZETASQL_CHECK_OK(db.status());
    test_db = *db;
  }
  ZETASQL_CHECK_OK(test_driver->CreateDatabase(test_db));

  absl::StatusOr<zetasql::Value> value = test_driver->ExecuteStatement(
      sql, absl::GetFlag(FLAGS_parameters).parameters, &type_factory);
  if (!value.ok()) {
    std::cout << value.status().ToString() << std::endl;
    return 1;
  }
  std::cout << value->DebugString() << std::endl;
  return 0;
}
