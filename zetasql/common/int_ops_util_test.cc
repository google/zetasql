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

#include "zetasql/common/int_ops_util.h"

#include <limits>

#include "gtest/gtest.h"

namespace zetasql {

TEST(IntOpsUtilTest, LossLessConvertDoubleToInt64) {
  int64_t output;

  EXPECT_TRUE(LossLessConvertDoubleToInt64(1.0, &output));
  EXPECT_EQ(output, 1);

  EXPECT_TRUE(LossLessConvertDoubleToInt64(-123456, &output));
  EXPECT_EQ(output, -123456);

  EXPECT_FALSE(LossLessConvertDoubleToInt64(1.1, &output));

  EXPECT_TRUE(LossLessConvertDoubleToInt64(
      static_cast<double>(std::numeric_limits<int64_t>::min()), &output));
  EXPECT_EQ(output, std::numeric_limits<int64_t>::min());

  EXPECT_FALSE(LossLessConvertDoubleToInt64(
      static_cast<double>(std::numeric_limits<int64_t>::max()), &output));

  EXPECT_FALSE(LossLessConvertDoubleToInt64(1e100, &output));
  EXPECT_FALSE(LossLessConvertDoubleToInt64(
      std::numeric_limits<double>::infinity(), &output));
  EXPECT_FALSE(LossLessConvertDoubleToInt64(
      -std::numeric_limits<double>::infinity(), &output));
  EXPECT_FALSE(LossLessConvertDoubleToInt64(
      std::numeric_limits<double>::quiet_NaN(), &output));
}

}  // namespace zetasql
