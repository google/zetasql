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

#include "zetasql/examples/tpch/catalog/tpch_catalog.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/common/simple_evaluator_table_iterator.h"
#include "zetasql/examples/tpch/catalog/customer.tbl.h"
#include "zetasql/examples/tpch/catalog/lineitem.tbl.h"
#include "zetasql/examples/tpch/catalog/nation.tbl.h"
#include "zetasql/examples/tpch/catalog/orders.tbl.h"
#include "zetasql/examples/tpch/catalog/part.tbl.h"
#include "zetasql/examples/tpch/catalog/partsupp.tbl.h"
#include "zetasql/examples/tpch/catalog/region.tbl.h"
#include "zetasql/examples/tpch/catalog/supplier.tbl.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/simple_catalog_util.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/base/const_init.h"
#include "absl/base/macros.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "riegeli/base/maker.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/csv/csv_reader.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/clock.h"

namespace zetasql {

// These are create statements for the TPCH tables.
// I found copies of these in various places with varying NOT NULL constraints.
// I'm not sure which version is official, but these constraints aren't
// supported or used here anyway so they are just commented out.
static const char* kCreateRegion =
    R"(CREATE TABLE Region (
  R_REGIONKEY UINT64, -- NOT NULL
  R_NAME STRING,
  R_COMMENT STRING,
))";

static const char* kCreateNation =
    R"(CREATE TABLE Nation (
  N_NATIONKEY UINT64, -- NOT NULL
  N_NAME STRING,
  N_REGIONKEY UINT64, -- NOT NULL
  N_COMMENT STRING,
))";

static const char* kCreateCustomer =
    R"(CREATE TABLE Customer (
  C_CUSTKEY UINT64, -- NOT NULL
  C_NAME STRING,
  C_ADDRESS STRING,
  C_NATIONKEY UINT64, -- NOT NULL
  C_PHONE STRING,
  C_ACCTBAL DOUBLE,
  C_MKTSEGMENT STRING,
  C_COMMENT STRING,
))";

static const char* kCreateSupplier =
    R"(CREATE TABLE Supplier (
  S_SUPPKEY UINT64, -- NOT NULL
  S_NAME STRING,
  S_ADDRESS STRING,
  S_NATIONKEY UINT64,
  S_PHONE STRING,
  S_ACCTBAL DOUBLE,
  S_COMMENT STRING,
))";

static const char* kCreateOrders =
    R"(CREATE TABLE Orders (
  O_ORDERKEY UINT64, -- NOT NULL
  O_CUSTKEY UINT64, -- NOT NULL
  O_ORDERSTATUS STRING,
  O_TOTALPRICE DOUBLE,
  O_ORDERDATE DATE,
  O_ORDERPRIORITY STRING,
  O_CLERK STRING,
  O_SHIPPRIORITY INT64,
  O_COMMENT STRING,
))";

static const char* kCreateLineItem =
    R"(CREATE TABLE LineItem (
  L_ORDERKEY UINT64, -- NOT NULL
  L_PARTKEY UINT64, -- NOT NULL
  L_SUPPKEY UINT64, -- NOT NULL
  L_LINENUMBER UINT64, -- NOT NULL
  L_QUANTITY DOUBLE,
  L_EXTENDEDPRICE DOUBLE,
  L_DISCOUNT DOUBLE,
  L_TAX DOUBLE,
  L_RETURNFLAG STRING,
  L_LINESTATUS STRING,
  L_SHIPDATE DATE,
  L_COMMITDATE DATE,
  L_RECEIPTDATE DATE,
  L_SHIPINSTRUCT STRING,
  L_SHIPMODE STRING,
  L_COMMENT STRING
))";

static const char* kCreatePart =
    R"(CREATE TABLE Part (
  P_PARTKEY UINT64, -- NOT NULL
  P_NAME STRING,
  P_MFGR STRING,
  P_BRAND STRING,
  P_TYPE STRING,
  P_SIZE INT64,
  P_CONTAINER STRING,
  P_RETAILPRICE DOUBLE,
  P_COMMENT STRING,
))";

static const char* kCreatePartSupp =
    R"(CREATE TABLE PartSupp (
  PS_PARTKEY UINT64, -- NOT NULL
  PS_SUPPKEY UINT64, -- NOT NULL
  PS_AVAILQTY INT64,
  PS_SUPPLYCOST DOUBLE,
  PS_COMMENT STRING,
))";

// This stores the data parsed from CSV and pre-processed for one TPCH table.
// It's computed lazily and then stored once so it can be reused each time
// we need to make an iterator to scan the table.
struct ParsedTpchTableData {
  std::vector<std::shared_ptr<const std::vector<Value>>> column_values;
  int num_rows = 0;
};
// This stores a ParsedTpchTableData or an error.
// The `optional` is filled in once loading this tables has been attempted.
typedef std::optional<
    absl::StatusOr<std::unique_ptr<const ParsedTpchTableData>>>
    ParsedTpchTableDataHolder;

// Load the data for one TPCH table and return it in a ParsedTpchTableData.
static ParsedTpchTableDataHolder::value_type MakeParsedTpchTableData(
    const SimpleTable& table, absl::string_view contents) {
  riegeli::CsvReader csv_reader(
      riegeli::Maker<riegeli::StringReader>(contents),
      riegeli::CsvReaderBase::Options().set_field_separator('|'));

  const int num_columns = table.NumColumns();

  // Values are in column_major order.
  std::vector<std::shared_ptr<std::vector<Value>>> column_values(num_columns);
  for (int i = 0; i < num_columns; ++i) {
    column_values[i] = std::make_shared<std::vector<Value>>();
  }

  int num_rows = 0;
  std::vector<std::string> record;
  while (csv_reader.ReadRecord(record)) {
    ++num_rows;

    // The TPCH files have a '|' separator at the end of the line, so they
    // look like they have one extra column.
    ZETASQL_RET_CHECK_EQ(record.size(), num_columns + 1);

    for (int i = 0; i < num_columns; ++i) {
      const Type* type = table.GetColumn(i)->GetType();
      const std::string& field = record[i];

      if (type->IsString()) {
        column_values[i]->push_back(Value::String(field));
      } else if (type->IsInt64()) {
        int64_t value;
        ZETASQL_RET_CHECK(absl::SimpleAtoi(field, &value)) << field;
        column_values[i]->push_back(Value::Int64(value));
      } else if (type->IsUint64()) {
        uint64_t value;
        ZETASQL_RET_CHECK(absl::SimpleAtoi(field, &value)) << field;
        column_values[i]->push_back(Value::Uint64(value));
      } else if (type->IsDouble()) {
        double value;
        ZETASQL_RET_CHECK(absl::SimpleAtod(field, &value)) << field;
        column_values[i]->push_back(Value::Double(value));
      } else if (type->IsDate()) {
        int32_t date;
        ZETASQL_RET_CHECK_OK(zetasql::functions::ConvertStringToDate(field, &date))
            << field;
        column_values[i]->push_back(Value::Date(date));
      } else {
        ZETASQL_RET_CHECK_FAIL() << "Unhandled type: " << type->DebugString();
      }
    }
  }
  if (!csv_reader.Close()) return csv_reader.status();

  auto data = std::make_unique<ParsedTpchTableData>();
  data->num_rows = num_rows;

  data->column_values.reserve(num_columns);
  for (auto& values : column_values) {
    ZETASQL_RET_CHECK_EQ(values->size(), num_rows);

    // Copying the outer vector was necessary to make the inner vector a
    // vector of const.
    data->column_values.push_back(values);
  }

  return data;
}

// Make the callback that makes an iterator for a particular TPCH table.
// All work happens lazily and at most once, storing the state in `data_holder`.
//
// This has enough functionality for the TPCH tables here.  It could be
// generalized to support more use cases.
static SimpleTable::EvaluatorTableIteratorFactory
MakeIteratorFactoryFromCsvFile(const absl::string_view contents,
                               const SimpleTable& table,
                               ParsedTpchTableDataHolder* data_holder) {
  auto factory = [contents, &table,
                  data_holder](absl::Span<const int> column_idxs)
      -> absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>> {
    // Initialize the ParsedTpchTableData if it hasn't been done for this table
    // yet, using a Mutex to avoid race conditions.
    {
      static absl::Mutex mutex(absl::kConstInit);
      absl::MutexLock lock(&mutex);
      if (!*data_holder) {
        *data_holder = MakeParsedTpchTableData(table, contents);
      }
    }

    const ParsedTpchTableData* data = (*data_holder)->value().get();
    std::vector<const Column*> columns;
    std::vector<std::shared_ptr<const std::vector<Value>>> column_values;
    column_values.reserve(column_idxs.size());
    for (const int column_idx : column_idxs) {
      columns.push_back(table.GetColumn(column_idx));
      column_values.push_back(data->column_values[column_idx]);
    }

    // Make the iterator to return the table contents.
    std::unique_ptr<EvaluatorTableIterator> iter(
        new SimpleEvaluatorTableIterator(
            columns, column_values, data->num_rows,
            /*end_status=*/absl::OkStatus(), /*filter_column_idxs=*/
            absl::flat_hash_set<int>(column_idxs.begin(), column_idxs.end()),
            /*cancel_cb=*/[]() {},
            /*set_deadline_cb=*/[](absl::Time t) {}, zetasql_base::Clock::RealClock()));
    return iter;
  };
  return factory;
}

absl::StatusOr<std::unique_ptr<SimpleCatalog>> MakeTpchCatalog() {
  auto catalog = std::make_unique<SimpleCatalog>("");

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_TABLE_STMT);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  // These are pairs with {create_statement, file_contents}.
  static const char* kTableDefinitions[][2] = {
      {kCreateRegion, embedded_resources::kTpchData_region},
      {kCreateLineItem, embedded_resources::kTpchData_lineitem},
      {kCreateNation, embedded_resources::kTpchData_nation},
      {kCreateCustomer, embedded_resources::kTpchData_customer},
      {kCreateSupplier, embedded_resources::kTpchData_supplier},
      {kCreateOrders, embedded_resources::kTpchData_orders},
      {kCreatePart, embedded_resources::kTpchData_part},
      {kCreatePartSupp, embedded_resources::kTpchData_partsupp},
  };
  static const int kNumTables = ABSL_ARRAYSIZE(kTableDefinitions);
  static ParsedTpchTableDataHolder* const parsed_datas =
      new ParsedTpchTableDataHolder[kNumTables];

  for (int i = 0; i < kNumTables; ++i) {
    SimpleTable* table;
    ZETASQL_RETURN_IF_ERROR(AddTableFromCreateTable(
        kTableDefinitions[i][0], analyzer_options,
        /*allow_non_temp=*/true, analyzer_output, table, *catalog));

    table->SetEvaluatorTableIteratorFactory(MakeIteratorFactoryFromCsvFile(
        kTableDefinitions[i][1], *table, &parsed_datas[i]));
  }

  return catalog;
}

}  // namespace zetasql
