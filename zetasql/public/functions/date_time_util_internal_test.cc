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

#include "zetasql/public/functions/date_time_util_internal.h"

#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "absl/time/civil_time.h"

namespace zetasql {
namespace functions {
namespace date_time_util_internal {
namespace {
struct IsoWeekTestCase {
  struct Day {
    int y;
    int m;
    int d;
  };
  Day input;
  Day expected_iso_1st_day;
  int expected_iso_year;
  int expected_iso_week;
};

std::ostream& operator<<(std::ostream& out, const IsoWeekTestCase& test) {
  return out << "{"
             << "{" << test.input.y << ", " << test.input.m << ", "
             << test.input.d << "}, {" << test.expected_iso_1st_day.y << ", "
             << test.expected_iso_1st_day.m << ", "
             << test.expected_iso_1st_day.d << "}, " << test.expected_iso_year
             << ", " << test.expected_iso_week << "}";
}

const IsoWeekTestCase kIsoWeekTestCases[] = {
    {{0000, 1, 1}, {-1, 1, 4}, -1, 52},
    {{9999, 12, 31}, {9999, 1, 4}, 9999, 52},

    // 2004 is a really interesting "year"!
    //   Start: 2003-12-29
    //   End  : 2005-01-02
    {{2003, 12, 29}, {2003, 12, 29}, 2004, 1},
    {{2004, 1, 1}, {2003, 12, 29}, 2004, 1},
    {{2005, 1, 1}, {2003, 12, 29}, 2004, 53},
    {{2005, 1, 2}, {2003, 12, 29}, 2004, 53},

    {{2005, 12, 31}, {2005, 1, 3}, 2005, 52},

    {{2007, 1, 1}, {2007, 1, 1}, 2007, 1},
    {{2007, 12, 30}, {2007, 1, 1}, 2007, 52},
    {{2007, 12, 31}, {2007, 12, 31}, 2008, 1},

    {{2008, 1, 1}, {2007, 12, 31}, 2008, 1},
    {{2008, 12, 28}, {2007, 12, 31}, 2008, 52},
    {{2008, 12, 29}, {2008, 12, 29}, 2009, 1},
    {{2008, 12, 30}, {2008, 12, 29}, 2009, 1},
    {{2008, 12, 31}, {2008, 12, 29}, 2009, 1},

    {{2009, 1, 1}, {2008, 12, 29}, 2009, 1},
    {{2009, 12, 31}, {2008, 12, 29}, 2009, 53},

    {{2010, 1, 1}, {2008, 12, 29}, 2009, 53},
    {{2010, 1, 2}, {2008, 12, 29}, 2009, 53},
    {{2010, 1, 3}, {2008, 12, 29}, 2009, 53},
    {{2010, 1, 4}, {2010, 1, 4}, 2010, 1},

    {{2018, 10, 7}, {2018, 1, 1}, 2018, 40},   // sunday
    {{2018, 10, 8}, {2018, 1, 1}, 2018, 41},   // monday
    {{2018, 10, 9}, {2018, 1, 1}, 2018, 41},   // tuesday
    {{2018, 10, 10}, {2018, 1, 1}, 2018, 41},  // wednesday
    {{2018, 10, 11}, {2018, 1, 1}, 2018, 41},  // thursday
    {{2018, 10, 12}, {2018, 1, 1}, 2018, 41},  // friday
    {{2018, 10, 13}, {2018, 1, 1}, 2018, 41},  // saturday
    {{2018, 10, 14}, {2018, 1, 1}, 2018, 41},  // sunday
    {{2018, 10, 15}, {2018, 1, 1}, 2018, 42},  // monday
};

}  // namespace

class IsoWeekTest : public ::testing::TestWithParam<IsoWeekTestCase> {};

TEST_P(IsoWeekTest, ISOWeekNumberTest) {
  IsoWeekTestCase test = GetParam();
  absl::CivilDay day(test.input.y, test.input.m, test.input.d);
  int iso_week = GetIsoWeek(day);
  int iso_year = static_cast<int>(GetIsoYear(day));
  absl::CivilDay iso_1st_day = GetFirstDayOfIsoYear(day);
  EXPECT_EQ(iso_week, test.expected_iso_week) << test;
  EXPECT_EQ(iso_year, test.expected_iso_year) << test;
  EXPECT_EQ(iso_1st_day, absl::CivilDay(test.expected_iso_1st_day.y,
                                        test.expected_iso_1st_day.m,
                                        test.expected_iso_1st_day.d))
      << test;
}

INSTANTIATE_TEST_SUITE_P(IsoWeekTests, IsoWeekTest,
                         ::testing::ValuesIn(kIsoWeekTestCases));

}  // namespace date_time_util_internal
}  // namespace functions
}  // namespace zetasql
