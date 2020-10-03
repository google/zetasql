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

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_FILE_UTIL_OSS_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_FILE_UTIL_OSS_H_

#include <sys/stat.h>

#include <cstdio>
#include <fstream>
#include <iostream>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"

namespace zetasql::internal {

inline absl::Status NullFreeString(absl::string_view str,
                                   std::string* out_str) {
  if (str.find('\0') != absl::string_view::npos) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        absl::StrCat("filename contains null characters: ", str));
  }
  *out_str = std::string(str);
  return absl::OkStatus();
}

inline absl::Status GetContents(absl::string_view filename,
                                std::string* file_contents) {
  // Because we are using a c api, check for in-string nulls.
  std::string filename_str;
  if (absl::Status status = NullFreeString(filename, &filename_str);
      !status.ok()) {
    return status;
  }

  struct stat status;
  if (stat(filename.data(), &status) != 0) {
    return absl::Status(absl::StatusCode::kNotFound,
                        absl::StrCat("Could not find", filename));
  } else if (S_ISREG(status.st_mode) == 0) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        absl::StrCat("File is not regular", filename));
  }

  std::ifstream stream(std::string(filename), std::ifstream::in);
  if (!stream) {
    // Could be a wider range of reasons.
    return absl::Status(absl::StatusCode::kNotFound,
                        absl::StrCat("Unable to open: ", filename));
  }
  stream.seekg(0, stream.end);
  int length = stream.tellg();
  stream.seekg(0, stream.beg);
  file_contents->clear();
  file_contents->resize(length);
  stream.read(&(*file_contents)[0], length);
  stream.close();
  return absl::OkStatus();
}

// The path in blaze where we expect to find inputs like `.test` files.
inline std::string TestSrcRootDir() {
  return zetasql_base::JoinPath(getenv("TEST_SRCDIR"), "com_google_zetasql");
}

// The path in bazel where we expect to find inputs like `.test` files.
inline std::string TestTmpDir() { return getenv("TEST_TMPDIR"); }

}  // namespace zetasql::internal

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_FILE_UTIL_OSS_H_
