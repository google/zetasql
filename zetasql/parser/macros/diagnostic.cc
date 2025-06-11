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

#include "zetasql/parser/macros/diagnostic.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "zetasql/common/errors.h"
#include "zetasql/parser/token_with_location.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {
namespace parser {
namespace macros {

// Converts the error source struct to proto.
static ErrorSource ConvertErrorSourceStructToProto(
    const StackFrame::ErrorSource& error_source) {
  ErrorSource error_source_proto;
  error_source_proto.set_error_message(error_source.error_message);
  error_source_proto.set_error_message_caret_string(
      error_source.error_message_caret_string);
  ErrorLocation* error_location = error_source_proto.mutable_error_location();
  if (!error_source.filename.empty()) {
    error_location->set_filename(error_source.filename);
  }
  error_location->set_line(error_source.line);
  error_location->set_column(error_source.column);
  error_location->set_input_start_line_offset(
      error_source.input_start_line_offset);
  error_location->set_input_start_column_offset(
      error_source.input_start_column_offset);
  return error_source_proto;
}

absl::Status MakeSqlErrorWithStackFrame(
    const ParseLocationPoint& location, absl::string_view message,
    absl::string_view input_text, const StackFrame* /*absl_nullable*/ stack_frame,
    int offset_in_original_input,
    const parser::macros::DiagnosticOptions& diagnostic_options) {
  zetasql_base::StatusBuilder status_builder = MakeSqlError() << message;
  InternalErrorLocation internal_location = location.ToInternalErrorLocation();

  const StackFrame* next_ancestor = stack_frame;
  std::vector<ErrorSource> error_sources;
  while (next_ancestor != nullptr) {
    error_sources.push_back(
        ConvertErrorSourceStructToProto(next_ancestor->error_source));
    next_ancestor = next_ancestor->parent;
  }
  // ErrorSources are supposed to be supplied in the reverse order of display.
  // See the ErrorSource proto definition.
  std::reverse(error_sources.begin(), error_sources.end());
  for (auto& error_source : error_sources) {
    *internal_location.add_error_source() = std::move(error_source);
  }

  status_builder.AttachPayload(std::move(internal_location));
  const ErrorMessageOptions& error_options =
      diagnostic_options.error_message_options;
  absl::Status status = ConvertInternalErrorLocationToExternal(
      std::move(status_builder), input_text,
      error_options.input_original_start_line - 1,
      error_options.input_original_start_column - 1, offset_in_original_input);
  return MaybeUpdateErrorFromPayload(error_options, input_text, status);
}

}  // namespace macros
}  // namespace parser
}  // namespace zetasql
