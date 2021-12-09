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

// Classes that represent parse input location points and ranges, and that
// translate points into line/columns in the original input buffer.
#ifndef ZETASQL_PUBLIC_PARSE_LOCATION_H_
#define ZETASQL_PUBLIC_PARSE_LOCATION_H_

#include <ostream>
#include <string>
#include <vector>

#include "zetasql/public/parse_location_range.pb.h"
#include "absl/base/attributes.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"

namespace zetasql {

class InternalErrorLocation;

// Point location in the input string.
// Use ParseLocationTranslator to translate a ParseLocationPoint into something
// that can be used in relation to the input query.
// The <filename> is informational, and only used for error messaging.
class ParseLocationPoint {
 public:
  ParseLocationPoint() : byte_offset_(-1) {}

  // Creates a ParseLocationPoint from a filename and byte offset.
  // <filename> must remain valid for the lifetime of this ParseLocationPoint.
  static ParseLocationPoint FromByteOffset(absl::string_view filename,
                                           int byte_offset) {
    ParseLocationPoint point;
    point.filename_ = filename;
    point.byte_offset_ = byte_offset;
    return point;
  }

  // Creates a ParseLocationPoint from a byte offset (with empty filename).
  static ParseLocationPoint FromByteOffset(int byte_offset) {
    return FromByteOffset(absl::string_view(), byte_offset);
  }

  absl::string_view filename() const { return filename_; }

  // Returns the byte offset corresponding to this parse location point. Returns
  // a negative value for invalid ParseLocationPoints.
  int GetByteOffset() const { return byte_offset_; }

  // Creates a ParseLocationPoint from the contents of <info>. Not intended for
  // public use.
  static ParseLocationPoint FromInternalErrorLocation(
      const InternalErrorLocation& info);

  // Returns the contents of this point as an internal error location. Not
  // intended for public use.
  InternalErrorLocation ToInternalErrorLocation() const;

  // Returns the string representation of this ParseLocationPoint, in the
  // form of [filename:]byte_offset.
  std::string GetString() const {
    if (byte_offset_ >= 0) {
      return absl::StrCat(
          (!filename_.empty() ? absl::StrCat(filename_, ":") : ""),
          byte_offset_);
    } else {
      return "INVALID";
    }
  }

  friend bool operator==(const ParseLocationPoint& lhs,
                         const ParseLocationPoint& rhs) {
    return lhs.filename_ == rhs.filename_ &&
           lhs.byte_offset_ == rhs.byte_offset_;
  }

  friend bool operator!=(const ParseLocationPoint& lhs,
                         const ParseLocationPoint& rhs) {
    return !(lhs == rhs);
  }

  friend bool operator<(const ParseLocationPoint& lhs,
                        const ParseLocationPoint& rhs) {
    if (lhs.filename_ == rhs.filename_) {
      return lhs.byte_offset_ < rhs.byte_offset_;
    }
    return lhs.filename_ < rhs.filename_;
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const ParseLocationPoint& point) {
    return os << "ParseLocationPoint at offset " << point.GetByteOffset();
  }

 private:
  absl::string_view filename_;
  int byte_offset_;

  // Intentionally copyable.
};

// A half-open range of ParseLocationPoints [start(), end()).
class ParseLocationRange {
 public:
  ParseLocationRange() {}

  void set_start(ParseLocationPoint start) { start_ = start; }
  void set_end(ParseLocationPoint end) { end_ = end; }

  ParseLocationPoint start() const { return start_; }
  ParseLocationPoint end() const { return end_; }

  absl::StatusOr<ParseLocationRangeProto> ToProto() const {
    // The ParseLocationProto only has a single field for the filename, so it
    // cannot represent a ParseLocationRange where the start and end locations
    // have different filenames. We ZETASQL_CHECK that condition here.
    ZETASQL_RET_CHECK_EQ(start().filename(), end().filename());
    ParseLocationRangeProto proto;
    proto.set_filename(std::string(start().filename()));
    proto.set_start(start().GetByteOffset());
    proto.set_end(end().GetByteOffset());
    return proto;
  }

  // The 'filename' in start and end fields of ParseLocationRange is a
  // string_view. This filename will point to the filename string in the
  // ParseLocationRangeProto. Therefore 'proto' must outlive the returned
  // ParseLocationRange.
  // TODO Add support for storing filename as string in
  // ParseLocationPoint.
  static absl::StatusOr<ParseLocationRange> Create(
      const ParseLocationRangeProto& proto) {
    ZETASQL_RET_CHECK(proto.has_start() && proto.has_end())
        << "Provided ParseLocationRangeProto does not have start and/or end "
           "byte offsets";

    ParseLocationRange parse_location_range;
    // ParseLocationRangeProto has a single filename that is used for both the
    // start and end location in the output ParseLocationRange.
    parse_location_range.set_start(
        ParseLocationPoint::FromByteOffset(proto.filename(), proto.start()));
    parse_location_range.set_end(
        ParseLocationPoint::FromByteOffset(proto.filename(), proto.end()));
    return parse_location_range;
  }

  // Returns the string representation of this parse location.
  std::string GetString() const {
    if (!start_.filename().empty() && start_.filename() == end_.filename()) {
      return absl::StrCat(start_.filename(), ":", start_.GetByteOffset(), "-",
                          end_.GetByteOffset());
    }
    return absl::StrCat(start_.GetString(), "-", end_.GetString());
  }

  friend bool operator==(const ParseLocationRange& lhs,
                         const ParseLocationRange& rhs) {
    return lhs.start() == rhs.start() && lhs.end() == rhs.end();
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const ParseLocationRange& range) {
    return os << "ParseLocationRange from " << range.start().GetByteOffset()
              << " to " << range.end().GetByteOffset();
  }

 private:
  ParseLocationPoint start_;
  ParseLocationPoint end_;

  // Intentionally copyable.
};

// Translates ParseLocationPoints to offsets and line/column numbers. For
// translation of offsets to line/column numbers, does a pass over the input
// string to record the byte offsets of every line. Accepted end of line
// characters are \n, \r\n, or \r.
//
// The input absl::string_view must outlive this class.
//
// NOT thread compatible, because the line offset table is calculated on demand
// without any locking.
class ParseLocationTranslator {
 public:
  explicit ParseLocationTranslator(absl::string_view input);
  ParseLocationTranslator(const ParseLocationTranslator&) = delete;
  ParseLocationTranslator& operator=(const ParseLocationTranslator&) = delete;

  // Calculates the line and column number corresponding to <point>. The
  // returned column number is a 1-based UTF-8 character index in
  // ExpandTabs(GetLineText(*line)). The character index can currently be
  // incorrect for strings containing multi-byte characters because the column
  // number calculation algorithm and ExpandTabs both assume single-byte
  // characters.
  //
  // TODO: Fix that. We no longer need to conform to what JavaCC
  // returns, and this is the slow path, so we can do more expensive things.
  //
  // Returns a generic::INTERNAL status for invalid positions (byte offset < 0
  // or > length of input).
  absl::StatusOr<std::pair<int, int>> GetLineAndColumnAfterTabExpansion(
      ParseLocationPoint point) const;

  // Gets the text for line number <line>.  If the line is invalid, returns
  // a failed absl::Status.
  absl::StatusOr<absl::string_view> GetLineText(int line) const;

  // Return <input> with tabs expanded to spaces, assuming 8-char tabs.
  // <input> must not contain any new line characters.
  // TODO: Fix this once we switch over to offsets.
  static std::string ExpandTabs(absl::string_view input);

  // Calculates the byte offset from the start of the input that corresponds to
  // 'line' and 'column' and returns it in 'byte_offset'. Returns a failed
  // status if the line and/or column are invalid.  Line and column have the
  // same semantics as described in GetLineAndColumnAfterTabExpansion, i.e.,
  // they are post-tab-expansion.
  absl::StatusOr<int> GetByteOffsetFromLineAndColumn(int line,
                                                     int column) const;

 private:
  // Calculates and returns the line and column number for byte offset
  // 'byte_offset', using the same line and column semantics as described in
  // GetLineAndColumnAfterTabExpansion().
  absl::StatusOr<std::pair<int, int>> GetLineAndColumnFromByteOffset(
      int byte_offset) const;

  // Calculates line_offsets_ if it has not been calculated yet.
  void CalculateLineOffsets() const;

  absl::string_view input_;

  // line_offset_[i] is start offset of line (i-1) in input_. Calculated on
  // demand using CalculateLineOffsets().
  mutable std::vector<int> line_offsets_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_PARSE_LOCATION_H_
