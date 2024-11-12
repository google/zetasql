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
#include <ios>
#include <istream>
#include <streambuf>

#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// An implementation of std::streambuf used by ZetaSQL. This class is NOT a
// standard stream implementation and should only be used by ZetaSQL analyzer.
class StringViewStreamBuf final : public std::basic_streambuf<char> {
 public:
  // Does not copy the string view data. `src` must outlive this class.
  explicit StringViewStreamBuf(absl::string_view src) noexcept
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
    if (gptr() + n > egptr()) {
      n = egptr() - gptr();
    }
    std::memcpy(static_cast<void *>(s), gptr(), n);
    gbump(static_cast<int>(n));
    return n;
  }

  // Override of showmanyc: Get number of characters available
  // Virtual function (to be read s-how-many-c) called by other member functions
  // to get an estimate on the number of characters available in the associated
  // input sequence.
  std::streamsize showmanyc() override {
    return static_cast<std::streamsize>(egptr() - gptr());
  }

  // Override of underflow: Get character on underflow.
  // Virtual function called by other member functions to get the current
  // character in the controlled input sequence without changing the current
  // position.
  int_type underflow() override {
    if (gptr() >= egptr()) {
      return traits_type::eof();
    }
    return gptr()[0];
  }

  // Override of uflow: Get character on underflow and advance position
  // Virtual function called by other member functions to get the current
  // character in the controlled input sequence and then advance the position
  // indicator to the next character.
  int_type uflow() override {
    if (gptr() >= egptr()) {
      return traits_type::eof();
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

  ~StringViewStreamBuf() override = default;

 private:
  absl::string_view src_;
};

// An implementation of std::basic_istream used by ZetaSQL parser.
// This is NOT a standard implementation of std::basic_istream. Please see the
// class comments of StringViewStreamBuf for more details.
class StringViewStream final : public std::basic_istream<char> {
 public:
  explicit StringViewStream(absl::string_view src)
      : std::basic_istream<char>(&sb_), sb_(src) {}

  // Not movable or copyable.
  StringViewStream(const StringViewStream &) = delete;
  StringViewStream &operator=(const StringViewStream &) = delete;
  StringViewStream(StringViewStream &&other) = delete;
  StringViewStream &operator=(StringViewStream &&rhs) = delete;

  ~StringViewStream() override = default;

 private:
  StringViewStreamBuf sb_;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_FLEX_ISTREAM_H_
