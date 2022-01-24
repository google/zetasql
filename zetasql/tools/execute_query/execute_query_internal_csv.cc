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

// Tool for running a query against a Catalog constructed from various input
// sources. Also serves as a demo of the PreparedQuery API.
//
// The tool uses the global proto db to lookup proto descriptors based on their
// names. For an example of how to use this tool with a custom proto db, see
// :execute_query_test. Note that not all protos are in the global proto db.

#include <fcntl.h>

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/csv/csv_reader.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::StatusOr<std::unique_ptr<SimpleTable>> MakeTableFromCsvFile(
    absl::string_view table_name, absl::string_view path) {
  riegeli::CsvReader csv_reader(riegeli::FdReader(path, O_RDONLY));

  std::vector<std::string> record;
  if (!csv_reader.ReadRecord(record)) {
    if (!csv_reader.healthy()) return csv_reader.status();
    return zetasql_base::UnknownErrorBuilder()
           << "CSV file " << path << " does not contain a header row";
  }
  std::vector<SimpleTable::NameAndType> columns;
  columns.reserve(record.size());
  for (const std::string& column_name : record) {
    columns.emplace_back(column_name, types::StringType());
  }

  std::vector<std::vector<Value>> contents;
  while (csv_reader.ReadRecord(record)) {
    if (record.size() != columns.size()) {
      return zetasql_base::UnknownErrorBuilder()
             << "CSV file " << path << " has a header row with "
             << columns.size() << " columns, but row "
             << csv_reader.last_record_index() << " has " << record.size()
             << " fields";
    }
    std::vector<Value>& row = contents.emplace_back();
    row.reserve(record.size());
    for (const std::string& field : record) {
      row.push_back(Value::String(field));
    }
  }
  if (!csv_reader.Close()) return csv_reader.status();

  auto table = absl::make_unique<SimpleTable>(table_name, columns);
  table->SetContents(contents);
  return table;
}

}  // namespace zetasql
