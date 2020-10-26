//
// Copyright 2020 Google LLC
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

#include "zetasql/base/file_util_oss.h"

#include <sys/stat.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/testing/status_matchers.h"

namespace zetasql::internal {

using ::zetasql_base::testing::StatusIs;

TEST(FileUtilTest, MatchNoWildcard) {
  const std::string filespec = absl::StrCat(
      TestSrcRootDir(), "/zetasql/base/file_util_oss_test.input_file");
  std::vector<std::string> files;
  ZETASQL_EXPECT_OK(Match(filespec, &files));
  EXPECT_THAT(files, testing::UnorderedElementsAre(filespec));
}

TEST(FileUtilTest, MatchEndingWithWildcard) {
  const std::string filespec =
      absl::StrCat(TestSrcRootDir(), "/zetasql/base/file_util_oss_test*");
  std::vector<std::string> files;
  ZETASQL_EXPECT_OK(Match(filespec, &files));
  EXPECT_THAT(
      files,
      testing::UnorderedElementsAre(
          absl::StrCat(TestSrcRootDir(), "/zetasql/base/file_util_oss_test"),
          absl::StrCat(TestSrcRootDir(),
                       "/zetasql/base/file_util_oss_test.input_file")));
}

TEST(FileUtilTest, MatchMiddleWildcard) {
  const std::string filespec =
      absl::StrCat(TestSrcRootDir(), "/zetasql/base/file_util*.input_file");
  std::vector<std::string> files;
  ZETASQL_EXPECT_OK(Match(filespec, &files));
  EXPECT_THAT(files, testing::UnorderedElementsAre(absl::StrCat(
                         TestSrcRootDir(),
                         "/zetasql/base/file_util_oss_test.input_file")));
}

TEST(FileUtilTest, MatchStartWildcard) {
  const std::string filespec =
      absl::StrCat(TestSrcRootDir(), "/zetasql/base/*.input_file");
  std::vector<std::string> files;
  ZETASQL_EXPECT_OK(Match(filespec, &files));
  EXPECT_THAT(files, testing::UnorderedElementsAre(absl::StrCat(
                         TestSrcRootDir(),
                         "/zetasql/base/file_util_oss_test.input_file")));
}

TEST(FileUtilTest, MatchFullWildcard) {
  const std::string filespec =
      absl::StrCat(TestSrcRootDir(), "/zetasql/base/*");
  std::vector<std::string> files;
  ZETASQL_EXPECT_OK(Match(filespec, &files));
  EXPECT_THAT(
      files,
      testing::UnorderedElementsAre(
          absl::StrCat(TestSrcRootDir(), "/zetasql/base/file_util_oss_test"),
          absl::StrCat(TestSrcRootDir(),
                       "/zetasql/base/file_util_oss_test.input_file")));
}

TEST(FileUtilTest, MatchNoMatch) {
  const std::string filespec =
      absl::StrCat(TestSrcRootDir(), "/zetasql/base/*.no_match");
  std::vector<std::string> files;
  ZETASQL_EXPECT_OK(Match(filespec, &files));
  EXPECT_THAT(files, testing::IsEmpty());
}

TEST(FileUtilTest, MatchBadDirectory) {
  const std::string filespec =
      absl::StrCat(TestSrcRootDir(), "/path/does/not/exist/*.input_file");
  std::vector<std::string> files;
  EXPECT_THAT(Match(filespec, &files), StatusIs(absl::StatusCode::kNotFound));
}

TEST(FileUtilTest, NullFreeString) {
  std::string str;
  ZETASQL_EXPECT_OK(NullFreeString("abcd0", &str));
  EXPECT_EQ(str, "abcd0");
  constexpr absl::string_view v("\0123\0", 5);
  EXPECT_EQ(v.size(), 5);
  str.clear();
  EXPECT_THAT(NullFreeString(v, &str),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(str, "");
}

TEST(FileUtilTest, GetContentsFailsOnMissingFile) {
  std::string contents;
  EXPECT_THAT(GetContents("/file/does/not/exist", &contents),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_TRUE(contents.empty());
}

TEST(FileUtilTest, GetContentsFailsOnWrongTypeOfFile) {
  // Directory exists, but is not a regular file.
  std::string contents;
  EXPECT_THAT(GetContents(getenv("TEST_SRCDIR"), &contents),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_TRUE(contents.empty());
}

TEST(FileUtilTest, GetContentsSucceed) {
  const std::string filespec = absl::StrCat(
      TestSrcRootDir(), "/zetasql/base/file_util_oss_test.input_file");

  std::string contents;
  ZETASQL_EXPECT_OK(GetContents(filespec, &contents));
  EXPECT_FALSE(contents.empty());
  EXPECT_EQ(contents, R"(This file
loads super

great!
)");
}

}  // namespace zetasql::internal
