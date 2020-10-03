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

#include "zetasql/public/functions/convert_proto.h"

#include <string>

#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/text_format.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {
namespace functions {

static bool ProtoToStringInternal(const google::protobuf::Message* value,
                                  std::string* out, bool multiline,
                                  absl::Status* error) {
  google::protobuf::TextFormat::Printer printer;
  printer.SetUseUtf8StringEscaping(true);
  printer.SetSingleLineMode(!multiline);
  google::protobuf::io::StringOutputStream stream(out);
  if (!printer.Print(*value, &stream)) {
    // This can happen (e.g.) when the algorithm thinks it is out of buffer
    // space. Generally it is unexpected here.
    *error = ::zetasql_base::InternalErrorBuilder()
             << "Failed to generate proto2 text format for printing a proto2 "
             << "message to a string.";
    return false;
  }
  // The "SingleLineMode" sometimes puts an extra space at the end of the
  // printed proto.
  if (absl::EndsWith(*out, " ")) {
    out->resize(out->size() - 1);
  }
  return true;
}

static bool ProtoToStringInternal(const google::protobuf::Message* value, absl::Cord* out,
                                  bool multiline, absl::Status* error) {
  std::string str_out;
  if (!ProtoToStringInternal(value, &str_out, multiline, error)) {
    return false;
  }
  *out = absl::Cord(str_out);
  return true;
}

bool ProtoToString(const google::protobuf::Message* value, absl::Cord* out,
                   absl::Status* error) {
  return ProtoToStringInternal(value, out, /*multiline=*/false, error);
}

bool ProtoToMultilineString(const google::protobuf::Message* value, absl::Cord* out,
                            absl::Status* error) {
  return ProtoToStringInternal(value, out, /*multiline=*/true, error);
}

bool StringToProto(const absl::string_view value, google::protobuf::Message* out,
                   absl::Status* error) {
  class Proto2ErrorCollector : public google::protobuf::io::ErrorCollector {
   public:
    explicit Proto2ErrorCollector(absl::Status* error) : error_(error) {}
    Proto2ErrorCollector(const Proto2ErrorCollector&) = delete;
    Proto2ErrorCollector& operator=(const Proto2ErrorCollector&) = delete;
    ~Proto2ErrorCollector() final {}

    void AddError(int line, int column, const std::string& message) final {
      *error_ = ::zetasql_base::OutOfRangeErrorBuilder()
                << "Error parsing proto: " << message << " [" << line + 1 << ":"
                << column + 1 << "]";
    }

   private:
    absl::Status* error_;  // Not owned
  };

  google::protobuf::TextFormat::Parser parser;
  Proto2ErrorCollector error_collector(error);
  parser.RecordErrorsTo(&error_collector);
  return parser.ParseFromString(std::string(value), out);
}

}  // namespace functions
}  // namespace zetasql
