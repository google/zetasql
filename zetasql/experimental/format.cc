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
#include <fstream>
#include <filesystem>

#include "zetasql/base/logging.h"
#include "zetasql/base/status.h"
#include "zetasql/public/sql_formatter.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/strip.h"
#include "absl/strings/str_join.h"

int format(const std::filesystem::path& file_path) {
  std::string formatted;
  if (file_path.extension() == ".bq" || file_path.extension() == ".sql") {
    std::cout << "formatting " << file_path << "..." << std::endl;
    std::ifstream file(file_path, std::ios::in);
    std::string sql(std::istreambuf_iterator<char>(file), {});
    const absl::Status status = zetasql::FormatSql(sql, &formatted);
    if (status.ok()) {
      std::ofstream out(file_path);
      out << formatted;
      if (formatted != sql) {
        std::cout << "successfully formatted " << file_path << "!" << std::endl;
        return 1;
      }
    } else {
      std::cout << "ERROR: " << status << std::endl;
      return 1;
    }
    std::cout << file_path << " is already formatted!" << std::endl;
  }
  return 0;
}

// format formats all sql files in specified directory and returns code 0
// if all files are formatted and 1 if error occurs or any file is formatted.
int main(int argc, char* argv[]) {
  const char kUsage[] =
      "Usage: format <directory paths...>\n";
  std::vector<char*> args = absl::ParseCommandLine(argc, argv);
  if (argc <= 1) {
    LOG(QFATAL) << kUsage;
  }
  std::vector<char*> remaining_args(args.begin() + 1, args.end());

  int rc = 0;
  for (const auto& path : remaining_args) {
    if (std::filesystem::is_regular_file(path)) {
      std::filesystem::path file_path(path);
      return format(file_path);
    }
    std::filesystem::recursive_directory_iterator file_path(path,
                                                  std::filesystem::directory_options::skip_permission_denied)
                                                  , end;
    std::error_code err;
    for (; file_path != end; file_path.increment(err)) {
      if (err) {
        std::cout << "WARNING: " << err << std::endl;
      }
      rc |= format(file_path->path());
    }
  }
  return rc;
}
