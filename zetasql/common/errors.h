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

#ifndef ZETASQL_COMMON_ERRORS_H_
#define ZETASQL_COMMON_ERRORS_H_

// This header creates common error status factory functions for ZetaSQL. The
// factory functions return zetasql_base::StatusBuilder so that the error message can
// be constructed using << operators.
//
// Functions that depend on parser objects (ASTNode, etc.) are located in
// parser/parse_tree_errors.
//
// We use util::INVALID_ARGUMENT for all SQL parsing and analysis errors.
// We use util::OUT_OF_RANGE for all runtime evaluation errors.
//
// ZetaSQL uses ErrorLocation and InternalErrorLocation as payloads on
// Status.  ErrorLocations are exposed through the public ZetaSQL interfaces,
// while InternalErrorLocations are used internally.  InternalErrorLocations
// are converted to ErrorLocations before returning them through public apis.
//
// ErrorLocations/InternalErrorLocations can contain a list of ErrorSources.
// An ErrorSource identifies a source error which caused the current error.
// The ErrorSource list is ordered, identifying a chain of dependencies
// between errors (i.e., error1 caused error2, which caused error3, etc.).
//
// These helpers are defined as follows:
//
//   // Return an error with code util::INVALID_ARGUMENT.
//   return MakeSqlError() << "Message";
//
//   // Return an error with code util::INVALID_ARGUMENT, and add an
//   // ErrorLocation payload with position <parse_location_point>, which must
//   // be a ParseLocationPoint.
//   return MakeSqlErrorAtPoint(parse_location_point) << "Message";
//
//   // Return an error with code util::OUT_OF_RANGE.
//   return MakeEvalError() << "Message";
//
// For lack of a better place, this header also defines helpers for translating
// between DeprecationWarningForFunctionBody protos and the corresponding
// absl::Status objects (that are used, for example, inside the analyzer).

#include <string>
#include <vector>

#include "google/protobuf/repeated_field.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/optimization.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// Make an ErrorSource from <status> and <text>.  If <text> is non-empty and
// <mode> is ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, then <text>
// must be related to the ErrorLocation in <status> and a caret string is
// constructed from <text> and <status> ErrorLocation. Otherwise no caret string
// is constructed.
// The returned ErrorSource gets its ErrorLocation from <status>.
// Requires non-OK status.
// TODO: This function currently only supports Status with
// ErrorLocation, not Status with InternalErrorLocation.  Extend this to
// support a Status with InternalErrorLocation if/when we need it.
ErrorSource MakeErrorSource(const absl::Status& status, const std::string& text,
                            ErrorMessageMode mode);

// Creates a StatusBuilder for ZetaSQL errors using the INVALID_ARGUMENT
// error code. Note: if you enable logging on the StatusBuilder, the logged
// file/line location will not be useful.
inline ::zetasql_base::StatusBuilder MakeSqlError() {
  return ::zetasql_base::InvalidArgumentErrorBuilder();
}

// Returns MakeSqlError() annotated with <point> as the error location.
inline ::zetasql_base::StatusBuilder MakeSqlErrorAtPoint(ParseLocationPoint point) {
  return MakeSqlError().Attach(point.ToInternalErrorLocation());
}

// Returns an UnimplementedError with a payload corresponding to the given error
// location. This is similar to MakeSqlErrorAtPoint(), except for the error
// code.
inline ::zetasql_base::StatusBuilder MakeUnimplementedErrorAtPoint(
    ParseLocationPoint point) {
  return zetasql_base::UnimplementedErrorBuilder().Attach(
      point.ToInternalErrorLocation());
}

// Returns a StatusBuilder for SQL evaluation errors, using the OUT_OF_RANGE
// error code. Note: if you enable logging on the StatusBuilder, the logged
// file/line location will not be useful.
inline ::zetasql_base::StatusBuilder MakeEvalError() {
  return ::zetasql_base::OutOfRangeErrorBuilder();
}

// Same, but uses <error_location> as the error location.
absl::Status StatusWithInternalErrorLocation(
    const absl::Status& status, const ParseLocationPoint& error_location);

// If <status> is OK or if it does not have a InternalErrorLocation payload,
// returns <status>. Otherwise, replaces the InternalErrorLocation payload by an
// ErrorLocation payload. 'query' should be the query that was parsed; it is
// used to convert the error location from line/column to byte offset or vice
// versa. This function must be called on all errors before returning them to
// the client. An InternalErrorLocation contained in 'status' must be valid for
// 'query'. If it is not, then this function returns an internal error.
absl::Status ConvertInternalErrorLocationToExternal(absl::Status status,
                                                    absl::string_view query);

inline std::string ExtractingNotSupportedDatePart(
    absl::string_view from_type, absl::string_view date_part_name) {
  return absl::StrCat("EXTRACT from ", from_type, " does not support the ",
                      date_part_name, " date part");
}

// Returns ErrorSources from <status>, if present.
const absl::optional<::google::protobuf::RepeatedPtrField<ErrorSource>> GetErrorSources(
    const absl::Status& status);

// Sets ErrorSources on <error_location_in> from <status>, including
// a new ErrorSource built from <status> (using <input_text_for_status> to
// build a caret string for it), along with other ErrorSources from inside
// <status> (if any).  If <input_text_for_status> is empty, then the new
// ErrorSource based on <status> will not include a caret string.
// The 'ErrorLocationType' should be either an ErrorLocation or an
// InternalErrorLocation.
template <typename ErrorLocationType>
ErrorLocationType SetErrorSourcesFromStatus(
    const ErrorLocationType& error_location_in, const absl::Status& status,
    ErrorMessageMode mode, const std::string& input_text_for_status = "") {
  if (status.ok()) {
    // An OK status should not have any payload, so just return the
    // InternalErrorLocation.
    return error_location_in;
  }
  ErrorLocationType error_location =
      SetErrorSourcesFromStatusWithoutOutermostError(error_location_in, status);
  const ErrorSource additional_error_source =
      MakeErrorSource(status, input_text_for_status, mode);
  *error_location.add_error_source() = additional_error_source;
  return error_location;
}

// If <status> has ErrorSources, copies them into <error_location_in>.
// Otherwise returns <error_location_in>.
// Note: This does not copy the outermost error from <status>.  This is
// intended for cases where the caller will replace the outermost error
// message and location, but wants to preserve its original recursive
// sources. The 'ErrorLocationType' should be either an ErrorLocation or an
// InternalErrorLocation.
template <typename ErrorLocationType>
ErrorLocationType SetErrorSourcesFromStatusWithoutOutermostError(
    const ErrorLocationType& error_location_in, const absl::Status& status) {
  if (status.ok()) {
    // An OK status should not have any payload, so just return the
    // error location.
    return error_location_in;
  }
  ErrorLocationType error_location = error_location_in;
  absl::optional<const ::google::protobuf::RepeatedPtrField<ErrorSource>> error_sources =
      GetErrorSources(status);
  if (error_sources.has_value()) {
    *error_location.mutable_error_source() = *error_sources;
  }
  return error_location;
}

// Returns <warnings> as a string suitable for debug output.
std::string DeprecationWarningsToDebugString(
    const std::vector<FreestandingDeprecationWarning>& warnings);

// Converts <warning> to a absl::Status.
inline absl::Status DeprecationWarningToStatus(
    const FreestandingDeprecationWarning& warning) {
  return MakeSqlError()
             .Attach(warning.error_location())
             .Attach(warning.deprecation_warning())
         << warning.message();
}

// Converts <from_status> to a FreestandingDeprecationWarning. Returns an error
// if <from_status> does not represent a valid deprecation warning. In
// particular, 'from_status' must have a DeprecationWarning extension and an
// ErrorLocation (and cannot have an InternalErrorLocation or any other
// payload).
absl::StatusOr<FreestandingDeprecationWarning> StatusToDeprecationWarning(
    const absl::Status& from_status, absl::string_view sql);

// Same as above, but for a vector of absl::Statuses.
absl::StatusOr<std::vector<FreestandingDeprecationWarning>>
StatusesToDeprecationWarnings(const std::vector<absl::Status>& from_statuses,
                              absl::string_view sql);

// This function potentially performs two actions:
// 1) Converts a ZetaSQL 'internal' Status (with InternalErrorLocation)
//    into a Status that can be returned from the ZetaSQL library (that
//    has ErrorLocation in place of InternalErrorLocation).  If <status>
//    does not have an InternalErrorLocation (including OK status) then
//    this step is a no-op.
// 2) Updates the Status error message and/or removes the location payload,
//    as determined by <mode>.
//
// If <status> has an InternalErrorLocation (as byte-offset) payload,
// then converts it to an ErrorLocation (line/column) payload instead
// (the line/column is determined from the byte-offset relative to
// <input_string>).
//
// If <mode> is ERROR_MESSAGE_WITH_PAYLOAD, then returns the (possibly
// updated) Status (with ErrorLocation if applicable).  For other modes,
// updates the Status error string to include the external ErrorLocation
// info (line/offset), then clears the ErrorLocation payload from the
// Status and returns that Status.
inline absl::Status ConvertInternalErrorLocationAndAdjustErrorString(
    ErrorMessageMode mode, absl::string_view input_string,
    const absl::Status& status) {
  if (status.ok()) return status;

  const absl::Status new_status =
      ConvertInternalErrorLocationToExternal(status, input_string);
  if (mode == ERROR_MESSAGE_WITH_PAYLOAD) {
    return new_status;
  }

  return MaybeUpdateErrorFromPayload(mode, input_string, new_status);
}

// Same as above, but for a vector of absl::Statuses.
inline std::vector<absl::Status>
ConvertInternalErrorLocationsAndAdjustErrorStrings(
    ErrorMessageMode mode, absl::string_view input_string,
    const std::vector<absl::Status>& statuses) {
  if (statuses.empty()) return statuses;

  std::vector<absl::Status> new_statuses;
  new_statuses.reserve(statuses.size());
  for (const absl::Status& status : statuses) {
    new_statuses.push_back(ConvertInternalErrorLocationAndAdjustErrorString(
        mode, input_string, status));
  }
  return new_statuses;
}

}  // namespace zetasql

#endif  // ZETASQL_COMMON_ERRORS_H_
