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

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/collator.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::Eq;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::StatusIs;

namespace zetasql {
namespace {

TEST(CreateFromCollationNameLite, DefaultImplSupportsUnicodeCsDeprecated) {
  internal::RegisterDefaultCollatorImpl();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ZetaSqlCollator* collator,
      ZetaSqlCollator::CreateFromCollationNameLite("unicode:cs"));

  absl::Status error;
  EXPECT_THAT(collator->CompareUtf8("a", "b", &error), Eq(-1));
  EXPECT_THAT(error, IsOk());

  EXPECT_THAT(collator->IsBinaryComparison(), Eq(true));

  delete collator;
}

TEST(CreateFromCollationNameLite, DefaultImplSupportsUnicodeCs) {
  internal::RegisterDefaultCollatorImpl();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollatorLite("unicode:cs"));

  absl::Status error;
  EXPECT_THAT(collator->CompareUtf8("a", "b", &error), Eq(-1));
  EXPECT_THAT(error, IsOk());

  EXPECT_THAT(collator->IsBinaryComparison(), Eq(true));
}

TEST(CreateFromCollationNameLite, DefaultImplDoesNotSupportEnUS) {
  internal::RegisterDefaultCollatorImpl();

  EXPECT_THAT(MakeSqlCollatorLite("en_US:ci"),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(CreateFromCollationNameLite, UsesRegisteredImpl) {
  internal::RegisterIcuCollatorImpl(
      [](absl::string_view collation_name)
          -> zetasql_base::StatusOr<std::unique_ptr<ZetaSqlCollator>> {
        return zetasql_base::InternalErrorBuilder() << "expected error";
      });

  EXPECT_THAT(MakeSqlCollatorLite("foo"),
              StatusIs(absl::StatusCode::kInternal, "expected error"));
}

}  // namespace
}  // namespace zetasql
