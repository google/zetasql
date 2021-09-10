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

#ifndef ZETASQL_PARSER_FLEX_ISTREAM_H_
#define ZETASQL_PARSER_FLEX_ISTREAM_H_

#include <cstring>
#include <istream>
#include <streambuf>
#include <string>

#include "zetasql/base/logging.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// An implementation of std::streambuf used by ZetaSQL parser that
// automatically appends a sentinel character ("\n") to the end of the source
// string view. This class is NOT a standard stream implementation and should
// only be used by ZetaSQL analyzer.
//
// The sentinel is added because some tokenizer rules try to match trailing
// context of the form [^...] where "..." is a set of characters that should
// *not* be present after the token. Unfortunately these rules actually also
// need to be triggered if, instead of "any character that is not in [...]",
// there is EOF. For instance, the unterminated comment rule cannot include the
// last star in "/* abcdef *" because it looks for a * followed by "something
// that is not a star". To solve this, we add a sentinel character at the end of
// the stream that is not in any [...] used by affected rules. The sentinel
// character is never returned as a token; when it is found, we return EOF
// instead. All "open ended tokens" (unclosed string literal / comment) that
// would include this bogus character in their location range are not affected
// because they are all error tokens, and they immediately produce errors that
// mention only their start location.
class StringStreamBufWithSentinel final : public std::basic_streambuf<char> {
 public:
  static constexpr absl::string_view kEofSentinelInput = "\n";

  // Does not copy the string view data. Both 'src' and 'kEofSentinelInput'
  // must outlive this class.
  explicit StringStreamBufWithSentinel(absl::string_view src) noexcept
      : src_(src.data() == nullptr ? absl::string_view("") : src) {
    auto *buff = const_cast<char_type *>(src_.data());
    setg(buff, buff, buff + src_.length());
  }

  // Override of xsgetn: Get sequence of characters.
  // Retrieves characters from the controlled input sequence and stores them in
  // the array pointed by s, until either n characters have been extracted or
  // the end of the sequence is reached.
  std::streamsize xsgetn(char_type *s, std::streamsize n) override {
    if (n == 0) return 0;
    // Whether the output should include sentinel.
    bool append_sentinel = false;
    if (gptr() + n > egptr()) {
      // The number of characters requested has passed the end of source.
      if (passed_sentinel_) {
        // Return 0 when reaching EOF since sentinel has already been returned
        // previously and no character was read.
        return 0;
      }
      // Retrieves all the left-over characters including the sentinel.
      append_sentinel = true;
      n = egptr() + kEofSentinelInput.size() - gptr();
    }

    if (append_sentinel) {
      std::memcpy(static_cast<void *>(s), gptr(), n - 1);
      // Append the sentinel.
      std::memcpy(static_cast<void *>(s + n - 1), kEofSentinelInput.data(),
                  kEofSentinelInput.size());
      passed_sentinel_ = true;
      auto *buff = const_cast<char_type *>(kEofSentinelInput.data());
      setg(buff, buff + kEofSentinelInput.size(),
           buff + kEofSentinelInput.size());
    } else {
      std::memcpy(static_cast<void *>(s), gptr(), n);
      gbump(static_cast<int>(n));
    }
    return n;
  }

  // Override of showmanyc: Get number of characters available
  // Virtual function (to be read s-how-many-c) called by other member functions
  // to get an estimate on the number of characters available in the associated
  // input sequence.
  std::streamsize showmanyc() override {
    if (passed_sentinel_) {
      return static_cast<std::streamsize>(egptr() - gptr());
    }
    // Include the size of sentinel since the cursor hasn't passed it yet.
    return static_cast<std::streamsize>(egptr() + kEofSentinelInput.size() -
                                        gptr());
  }

  // Override of underflow: Get character on underflow.
  // Virtual function called by other member functions to get the current
  // character in the controlled input sequence without changing the current
  // position.
  int_type underflow() override {
    if (gptr() >= egptr()) {
      if (passed_sentinel_) {
        // EOF after passing sentinel.
        return traits_type::eof();
      }
      // Cursor has passed the end of source view. Now move it to sentinel.
      passed_sentinel_ = true;
      auto *buff = const_cast<char_type *>(kEofSentinelInput.data());
      setg(buff, buff, buff + kEofSentinelInput.size());
    }
    return gptr()[0];
  }

  // Override of uflow: Get character on underflow and advance position
  // Virtual function called by other member functions to get the current
  // character in the controlled input sequence and then advance the position
  // indicator to the next character.
  int_type uflow() override {
    if (gptr() >= egptr()) {
      if (passed_sentinel_) {
        // EOF after passing sentinel.
        return traits_type::eof();
      }
      // Cursor has passed the end of source view. Now move it to sentinel.
      passed_sentinel_ = true;
      auto *buff = const_cast<char_type *>(kEofSentinelInput.data());
      setg(buff, buff, buff + kEofSentinelInput.size());
    }
    gbump(1);
    return gptr()[-1];
  }

  // Override of pbackfail: Put character back in the case of backup underflow
  // Virtual function called by other member functions to put a character back
  // into the controlled input sequence and decrease the position indicator.
  int_type pbackfail(int_type c) override {
    if (gptr() > egptr()) {
      return traits_type::eof();
    }
    if (passed_sentinel_ && gptr() == eback()) {
      // If cursor is at the sentinel, point it back to the last character of
      // 'src_'.
      passed_sentinel_ = false;
      auto *buff = const_cast<char_type *>(src_.data());
      setg(buff, buff + src_.length() - 1, buff + src_.length());
      return traits_type::to_int_type(kEofSentinelInput.data()[0]);
    }
    gbump(-1);
    return c;
  }

  // Override of seekoff: Set internal position pointer to relative position.
  // Virtual function called by the public member function pubseekoff to alter
  // the stream positions of one or more of the controlled sequences in a
  // specific way for each derived class.
  pos_type seekoff(off_type off, std::ios_base::seekdir way,
                   std::ios_base::openmode which) override {
    if ((which & std::ios_base::in) == 0) return pos_type(-1);
    switch (way) {
      case std::ios::beg:
        break;
      case std::ios::cur:
        off += gptr() - eback();
        break;
      case std::ios::end:
        off += egptr() - eback();
        break;
      default:
        return pos_type(-1);
    }
    return seekpos(off, which);
  }

  // Override of seekpos: Set internal position pointer to absolute position.
  // Virtual function called by the public member function pubseekpos to alter
  // the stream positions of one or more of the controlled sequences in a
  // specific way for each derived class.
  pos_type seekpos(pos_type pos, std::ios_base::openmode which) override {
    if ((which & std::ios_base::in) == 0) return pos_type(-1);
    if (pos > egptr() - eback()) return pos_type(-1);
    setg(eback(), eback() + pos, egptr());
    return pos;
  }

  ~StringStreamBufWithSentinel() override {}

 private:
  // Indicates whether the cursor has passed the sentinel.
  bool passed_sentinel_ = false;

  absl::string_view src_;
};

// An implementation of std::basic_istream used by ZetaSQL parser.
// This is NOT a standard implementation of std::basic_istream. Please see the
// class comments of StringStreamWithSentinelBuf for more details.
class StringStreamWithSentinel final : public std::basic_istream<char> {
 public:
  explicit StringStreamWithSentinel(absl::string_view src)
      : std::basic_istream<char>(&sb_), sb_(src) {}

  // Not movable or copyable.
  StringStreamWithSentinel(const StringStreamWithSentinel &) = delete;
  StringStreamWithSentinel &operator=(const StringStreamWithSentinel &) =
      delete;
  StringStreamWithSentinel(StringStreamWithSentinel &&other) = delete;
  StringStreamWithSentinel &operator=(StringStreamWithSentinel &&rhs) = delete;

  ~StringStreamWithSentinel() override = default;

 private:
  StringStreamBufWithSentinel sb_;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_FLEX_ISTREAM_H_
