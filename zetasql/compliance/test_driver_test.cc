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

#include "zetasql/compliance/test_driver.h"

#include <algorithm>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

TEST(TestDriverTest, ClassAndProtoSize) {
  // We're forced to replicate the exact structure for this test because the
  // presence of bools makes computing an 'expected'
  // (sizeof(X) == sizeof(X.field1) + sizeof(X.field2) +...) weird due
  // to alignment (bool will take 64 or 32 bytes in this case).
  struct MockTestDatabase {
    std::set<std::string> proto_files;
    bool runs_as_test;
    std::set<std::string> proto_names;
    std::set<std::string> enum_names;
    std::map<std::string, TestTable> tables;
  };
  struct MockTestTestTableOptions {
    int expected_table_size_min;
    int expected_table_size_max;
    bool is_value_table;
    double nullable_probability;
    std::set<LanguageFeature> required_features;
    std::string userid_column;
  };
  struct MockTestTable {
    Value table_as_value;
    MockTestTestTableOptions options;
  };
  static_assert(sizeof(TestDatabase) == sizeof(MockTestDatabase),
                "Please change SerializeTestDatabase (test_driver.cc) and "
                "TestDatabaseProto (test_driver.proto) tests if TestDatabase "
                "is modified.");
  EXPECT_EQ(5, TestDatabaseProto::descriptor()->field_count());
  EXPECT_EQ(7, TestTableOptionsProto::descriptor()->field_count());
  EXPECT_EQ(3, TestTableProto::descriptor()->field_count());
}

}  // namespace zetasql
