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
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/sql_formatter.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/status.h"


int main(int argc, char* argv[]) {
  const char kUsage[] =
      "Usage: format [--table_spec=<table_spec>] <sql>\n";
  std::vector<char*> remaining_args = absl::ParseCommandLine(argc, argv);
  if (argc <= 1) {
    LOG(QFATAL) << kUsage;
  }
  const std::string sql = absl::StrJoin(remaining_args.begin() + 1,
  remaining_args.end(), " ");

  std::string formatted;
  const absl::Status status = zetasql::FormatSql(sql, &formatted);
  if (status.ok()) {
    std::cout << formatted;
    return 0;
  } else {
    std::cout << "ERROR: " << status << std::endl;
    return 1;
  }
}
