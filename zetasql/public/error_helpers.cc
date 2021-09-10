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

#include "zetasql/public/error_helpers.h"

#include <ctype.h>

#include <algorithm>

#include "zetasql/base/logging.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/parse_location.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"  
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// Format an ErrorLocation using <format>, which is a string as in
// strings::Substitute, with $0 being file, $1 being line, and $2 being column.
// ErrorSource information is ignored (if present).
static std::string FormatErrorLocation(const ErrorLocation& location,
                                       const absl::string_view format) {
  return absl::Substitute(format, location.filename(), location.line(),
                          location.column());
}

std::string FormatErrorLocation(const ErrorLocation& location) {
  return (location.has_filename() ?
          FormatErrorLocation(location, "$0:$1:$2") :
          FormatErrorLocation(location, "$1:$2"));
}

// Internal helper function to format the ErrorLocation string with format
// [at file:line:column]
static std::string FormatErrorLocationAtFileLineColumn(
    const ErrorLocation& location) {
  return absl::StrCat("[at ", FormatErrorLocation(location), "]");
}

std::string FormatErrorLocation(const ErrorLocation& location,
                                absl::string_view input_text,
                                ErrorMessageMode mode) {
  std::string error_location_string =
      FormatErrorLocationAtFileLineColumn(location);
  if (mode == ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET) {
    absl::StrAppend(&error_location_string, "\n",
                    GetErrorStringWithCaret(input_text, location));
  }

  if (!location.error_source().empty()) {
    const std::string error_source_separator =
        (mode == ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET ? "\n"
                                                                       : "; ");
    std::string error_source_string;
    for (const ErrorSource& error_source : location.error_source()) {
      const std::string source_message = FormatErrorSource(error_source, mode);
      error_source_string = absl::StrCat(
          source_message,
          (!error_source_string.empty() ? error_source_separator : ""),
          error_source_string);
    }
    absl::StrAppend(
        &error_location_string,
        (!error_source_string.empty() ? error_source_separator : ""),
        error_source_string);
  }
  return error_location_string;
}

std::string FormatErrorSource(const ErrorSource& error_source,
                              ErrorMessageMode mode) {
  if (mode == ErrorMessageMode::ERROR_MESSAGE_WITH_PAYLOAD) {
    return "";
  }
  std::string message = error_source.error_message();
  if (!message.empty() && error_source.has_error_location()) {
    // Note - if error_source.error_location has an ErrorSource, it is ignored.
    absl::StrAppend(
        &message, " ",
        FormatErrorLocationAtFileLineColumn(error_source.error_location()));
  }
  if (mode == ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET &&
      error_source.has_error_message_caret_string()) {
    absl::StrAppend(&message, (!message.empty() ? "\n" : ""),
                    error_source.error_message_caret_string());
  }
  return message;
}

std::string FormatError(const absl::Status& status) {
  if (status.code() != absl::StatusCode::kInvalidArgument) {
    return internal::StatusToString(status);
  }

  std::string message = std::string(status.message());
  if (internal::HasPayload(status)) {
    std::string payload_string;
    std::string location_string;
    if (internal::HasPayloadWithType<ErrorLocation>(status)) {
      // Perform special formatting for location data.
      ErrorLocation location = internal::GetPayload<ErrorLocation>(status);

      location_string = absl::StrCat(
          " ", FormatErrorLocation(location, /*input_text=*/"",
                                   ErrorMessageMode::ERROR_MESSAGE_ONE_LINE));

      absl::Status stripped_status = status;

      internal::ErasePayloadTyped<ErrorLocation>(&stripped_status);
      payload_string = internal::PayloadToString(stripped_status);
    } else {
      payload_string = internal::PayloadToString(status);
    }

    // Error messages with a caret look strange if the payload immediately
    // follows the caret, so put it on separate line in that case, being careful
    // to avoid turning a single-line error message into a multi-line one.
    absl::string_view payload_separator;
    if (!payload_string.empty()) {
      const bool multiline = absl::StrContains(message, '\n');
      payload_separator = multiline ? "\n" : " ";
    }
    absl::StrAppend(&message, location_string, payload_separator,
                    payload_string);
  }
  return message;
}

bool HasErrorLocation(const absl::Status& status) {
  return internal::HasPayloadWithType<ErrorLocation>(status);
}

bool GetErrorLocation(const absl::Status& status, ErrorLocation* location) {
  if (HasErrorLocation(status)) {
    *location = internal::GetPayload<ErrorLocation>(status);
    return true;
  }
  return false;
}

void ClearErrorLocation(absl::Status* status) {
  return internal::ErasePayloadTyped<ErrorLocation>(status);
}

static bool IsWordChar(char c) {
  return isalnum(c) || c == '_';
}

// Return true if <column> (0-based) in <str> starts a word.
static bool IsWordStart(const std::string& str, int column) {
  ZETASQL_DCHECK_LT(column, str.size());
  if (column == 0 || column >= str.size()) return true;
  return !IsWordChar(str[column - 1]) && IsWordChar(str[column]);
}

// Constructs and returns a truncated input string based on <input>, <location>,
// and <max_width_in>.  Also returns the error column.
static void GetTruncatedInputStringInfo(absl::string_view input,
                                        const ErrorLocation& location,
                                        int max_width_in,
                                        std::string* truncated_input,
                                        int* error_column) {
  // We don't allow a max_width below a certain size.
  constexpr int kMinimumMaxWidth = 30;
  // If the error line is longer than max_width, give a substring of up
  // to max_width characters, with the caret near the middle of it.
  // We need some minimum width.
  const int max_width = std::max(max_width_in, kMinimumMaxWidth);

  ZETASQL_DCHECK_GT(location.line(), 0);
  ZETASQL_DCHECK_GT(location.column(), 0);

  ParseLocationTranslator translator(input);
  absl::StatusOr<absl::string_view> line_text =
      translator.GetLineText(location.line());
  ZETASQL_DCHECK_OK(line_text.status());

  *truncated_input = translator.ExpandTabs(line_text.value_or(""));

  // location.column() may be one off the end of the line for EOF errors.
  ZETASQL_DCHECK_LE(location.column(), truncated_input->size() + 1);
  // error_column is 0-based.
  *error_column =
      std::max(1, std::min(static_cast<int>(truncated_input->size() + 1),
                           location.column())) -
      1;

  if (truncated_input->size() > max_width) {
    const int one_half = max_width / 2;
    const int one_third = max_width / 3;
    // If the error is near the start, just use a prefix of the string.
    if (*error_column > max_width - one_third) {
      // Otherwise, try to find a word boundary to start the string on
      // that puts the caret in the middle third of the output line.
      int found_start = -1;
      for (int start_column = std::max(0, *error_column - 2 * one_third);
           start_column < std::max(0, *error_column - one_third);
           ++start_column) {
        if (IsWordStart(*truncated_input, start_column)) {
          found_start = start_column;
          break;
        }
      }
      if (found_start == -1) {
        // Didn't find a good separator.  Just split in the middle.
        found_start = std::max(*error_column - one_half, 0);
      }

      // Add ... prefix if necessary.
      if (found_start < 3) {
        found_start = 0;
      } else {
        *truncated_input =
            absl::StrCat("...", truncated_input->substr(found_start));
        *error_column -= found_start - 3;
      }
    }
    *truncated_input = PrettyTruncateUTF8(*truncated_input, max_width);
    ZETASQL_DCHECK_LE(*error_column, truncated_input->size());
  }
}

// Helper function to return an error string from an error line and column.
static std::string GetErrorStringFromErrorLineAndColumn(
    const std::string& error_line, const int error_column) {
  return absl::StrFormat("%s\n%*s^", error_line, error_column, "");
}

std::string GetErrorStringWithCaret(absl::string_view input,
                                    const ErrorLocation& location,
                                    int max_width_in) {
  std::string error_line;
  int error_column;
  GetTruncatedInputStringInfo(input, location, max_width_in, &error_line,
                              &error_column);
  return GetErrorStringFromErrorLineAndColumn(error_line, error_column);
}

// Updates the <status> error string based on <input_text> and <mode>.
// See header comment for MaybeUpdateErrorFromPayload for details.
static absl::Status UpdateErrorFromPayload(const absl::Status& status,
                                           absl::string_view input_text,
                                           ErrorMessageMode mode) {
  if (mode == ErrorMessageMode::ERROR_MESSAGE_WITH_PAYLOAD) {
    // In this case, we do not update the error message and the payload
    // remains on the Status.
    return status;
  }
  ZETASQL_RET_CHECK(!internal::HasPayloadWithType<InternalErrorLocation>(status))
      << "Status must not have InternalErrorLocation: "
      << internal::StatusToString(status);
  if (!status.ok()) {
    ErrorLocation location;
    if (GetErrorLocation(status, &location)) {
      std::string new_message =
          absl::StrCat(status.message(), " ",
                       FormatErrorLocation(location, input_text, mode));
      // Update the message.  Leave everything else as is.
      absl::Status new_status =
          absl::Status(status.code(), new_message);
      // Copy payloads
      status.ForEachPayload([&new_status](
          absl::string_view type_url, const absl::Cord& payload) {
        new_status.SetPayload(type_url, payload);});
      ClearErrorLocation(&new_status);
      return new_status;
    }
  }
  return status;
}

absl::Status MaybeUpdateErrorFromPayload(ErrorMessageMode mode,
                                         absl::string_view input_text,
                                         const absl::Status& status) {
  ZETASQL_RET_CHECK(!internal::HasPayloadWithType<InternalErrorLocation>(status))
      << "Status must not have InternalErrorLocation: "
      << internal::StatusToString(status);
  if (status.ok() || mode == ErrorMessageMode::ERROR_MESSAGE_WITH_PAYLOAD) {
    // We do not update the error string with error payload, which
    // could include location and/or nested errors.  We leave any payload
    // attached to the Status.
    return status;
  }

  return UpdateErrorFromPayload(status, input_text, mode);
}

absl::Status UpdateErrorLocationPayloadWithFilenameIfNotPresent(
    const absl::Status& status, const std::string& filename) {
  ErrorLocation error_location;
  if (filename.empty() || !GetErrorLocation(status, &error_location)) {
    return status;
  }

  if (error_location.has_filename()) {
    return status;
  }
  // The error location does not have a filename, so use the specified
  // 'module_filename'.
  error_location.set_filename(filename);

  absl::Status copy = status;
  ClearErrorLocation(&copy);
  internal::AttachPayload(&copy, error_location);
  return copy;
}

}  // namespace zetasql
