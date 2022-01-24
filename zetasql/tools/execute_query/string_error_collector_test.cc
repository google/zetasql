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

#include "zetasql/tools/execute_query/string_error_collector.h"

#include <string>

#include "gtest/gtest.h"

namespace zetasql {
namespace {

class StringErrorCollectorTest : public ::testing::Test {
 protected:
  StringErrorCollectorTest() : test_collector_(&error_string_) {
    error_string_.clear();
  }

  std::string error_string_;
  StringErrorCollector test_collector_;
};

TEST_F(StringErrorCollectorTest, AppendsError) {
  test_collector_.AddError(1, 2, "foo");
  EXPECT_EQ("1(2): foo\n", error_string_);
}

TEST_F(StringErrorCollectorTest, AppendsWarning) {
  test_collector_.AddWarning(1, 2, "foo");
  EXPECT_EQ("1(2): foo\n", error_string_);
}

TEST_F(StringErrorCollectorTest, AppendsMultipleError) {
  test_collector_.AddError(1, 2, "foo");
  test_collector_.AddError(3, 4, "bar");
  EXPECT_EQ("1(2): foo\n3(4): bar\n", error_string_);
}

TEST_F(StringErrorCollectorTest, AppendsMultipleWarning) {
  test_collector_.AddWarning(1, 2, "foo");
  test_collector_.AddWarning(3, 4, "bar");
  EXPECT_EQ("1(2): foo\n3(4): bar\n", error_string_);
}

TEST_F(StringErrorCollectorTest, OffsetWorks) {
  StringErrorCollector test_collector(&error_string_, true);
  test_collector.AddError(1, 2, "foo");
  test_collector.AddWarning(3, 4, "bar");
  EXPECT_EQ("2(3): foo\n4(5): bar\n", error_string_);
}

}  // namespace
}  // namespace zetasql
