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

#include "zetasql/common/errors.h"

#include <ctype.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "zetasql/common/status_payload_utils.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/attributes.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

std::string AbslUnparseFlag(ErrorMessageStability value) {  // NOLINT
  const auto* enum_d = google::protobuf::GetEnumDescriptor<ErrorMessageStability>();
  const auto* value_d = enum_d->FindValueByNumber(value);
  if (value_d == nullptr) {
    ABSL_LOG(ERROR) << "Expended to find descriptor for strongly typed enum value";
    return absl::StrCat(value);  // Best effort
  }
  return std::string(value_d->name());
}

bool AbslParseFlag(absl::string_view text,  // NOLINT
                   ErrorMessageStability* value, std::string* error) {
  const auto* enum_d = google::protobuf::GetEnumDescriptor<ErrorMessageStability>();
  const auto* value_d = enum_d->FindValueByName(text);
  if (value_d == nullptr) {
    // The command line supplied a string that is not an enum value name.
    *value = ERROR_MESSAGE_STABILITY_UNSPECIFIED;
    return false;
  }
  *value = static_cast<ErrorMessageStability>(value_d->number());
  return true;
}

}  // namespace zetasql

ABSL_FLAG(
    zetasql::ErrorMessageStability, zetasql_default_error_message_stability,
    zetasql::ERROR_MESSAGE_STABILITY_UNSPECIFIED,
    "Intented for tests. Specifies a default level of ZetaSQL error message "
    "stability for the binary. See ErrorMessageStability for details on "
    "different levels. The flag value `ERROR_MESSAGE_STABILITY_UNSPECIFIED` is "
    "ignored as if the flag is not set.");

ABSL_DEPRECATED("Use zetasql_default_error_message_stability instead.")
ABSL_FLAG(bool, zetasql_redact_error_messages_for_tests, false,
          "Replace error message details with 'SQL ERROR'. This is often the "
          "correct thing to do in unit tests because SQL error message text is"
          "*not* part of the API contract and testing exact error message text "
          "is likely to cause flaky tests. When "
          "--zetasql_default_error_message_stability is set to any other "
          "than `ERROR_MESSAGE_STABILITY_UNSPECIFIED` this flag is ignored.");

namespace zetasql {

// Returns true if <status> has a internalErrorLocation payload.
static bool HasInternalErrorLocation(const absl::Status& status) {
  return internal::HasPayloadWithType<InternalErrorLocation>(status);
}

absl::Status StatusWithInternalErrorLocation(
    const absl::Status& status, const ParseLocationPoint& error_location) {
  if (status.ok()) return status;

  absl::Status result = status;
  internal::AttachPayload(&result, error_location.ToInternalErrorLocation());
  return result;
}

ErrorSource MakeErrorSource(const absl::Status& status, absl::string_view text,
                            ErrorMessageMode mode) {
  ABSL_DCHECK(!status.ok());
  // Sanity check that status does not have an InternalErrorLocation.
  ABSL_DCHECK(!HasInternalErrorLocation(status)) << status;

  ErrorSource error_source;
  error_source.set_error_message(status.message());
  ErrorLocation status_error_location;
  if (GetErrorLocation(status, &status_error_location)) {
    *error_source.mutable_error_location() = status_error_location;
    if (mode == ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET &&
        !text.empty()) {
      error_source.set_error_message_caret_string(
          GetErrorStringWithCaret(text, status_error_location));
    }
  }
  return error_source;
}

// Returns ErrorSources from <status>, if present.
std::optional<::google::protobuf::RepeatedPtrField<ErrorSource>> GetErrorSources(
    const absl::Status& status) {
  if (internal::HasPayloadWithType<ErrorLocation>(status)) {
    // Sanity check that an OK status does not have a payload.
    ABSL_DCHECK(!status.ok());

    return internal::GetPayload<ErrorLocation>(status).error_source();
  }
  return std::nullopt;
}

std::string DeprecationWarningsToDebugString(
    absl::Span<const FreestandingDeprecationWarning> warnings) {
  if (warnings.empty()) return "";
  return absl::StrCat("(", warnings.size(), " deprecation warning",
                      (warnings.size() > 1 ? "s" : ""), ")");
}

absl::StatusOr<FreestandingDeprecationWarning> StatusToDeprecationWarning(
    const absl::Status& from_status, absl::string_view sql) {
  ZETASQL_RET_CHECK(absl::IsInvalidArgument(from_status))
      << "Deprecation statuses must have code INVALID_ARGUMENT";

  FreestandingDeprecationWarning warning;
  warning.set_message(from_status.message());

  ZETASQL_RET_CHECK(internal::HasPayload(from_status))
      << "Deprecation statuses must have payloads";

  ZETASQL_RET_CHECK(!internal::HasPayloadWithType<InternalErrorLocation>(from_status))
      << "Deprecation statuses cannot have InternalErrorLocation payloads";

  ZETASQL_RET_CHECK(internal::HasPayloadWithType<ErrorLocation>(from_status))
      << "Deprecation statuses must have ErrorLocation payloads";
  *warning.mutable_error_location() =
      internal::GetPayload<ErrorLocation>(from_status);

  ZETASQL_RET_CHECK(internal::HasPayloadWithType<DeprecationWarning>(from_status))
      << "Deprecation statuses must have DeprecationWarning payloads";
  *warning.mutable_deprecation_warning() =
      internal::GetPayload<DeprecationWarning>(from_status);

  ZETASQL_RET_CHECK_EQ(internal::GetPayloadCount(from_status), 2)
      << "Found invalid extra payload in deprecation status";

  warning.set_caret_string(
      GetErrorStringWithCaret(sql, warning.error_location()));

  return warning;
}

absl::StatusOr<std::vector<FreestandingDeprecationWarning>>
StatusesToDeprecationWarnings(absl::Span<const absl::Status> from_statuses,
                              absl::string_view sql) {
  std::vector<FreestandingDeprecationWarning> warnings;
  for (const absl::Status& from_status : from_statuses) {
    ZETASQL_ASSIGN_OR_RETURN(const FreestandingDeprecationWarning warning,
                     StatusToDeprecationWarning(from_status, sql));
    warnings.emplace_back(warning);
  }

  return warnings;
}

ErrorMessageStability GetDefaultErrorMessageStability() {
  ErrorMessageStability flag_value =
      absl::GetFlag(FLAGS_zetasql_default_error_message_stability);
  if (flag_value != ERROR_MESSAGE_STABILITY_UNSPECIFIED) {
    // --zetasql_default_error_message_stability wins if its set to anything
    // other than "unspecified".
    return flag_value;
  }
  if (absl::GetFlag(FLAGS_zetasql_redact_error_messages_for_tests)) {
    return ErrorMessageStability::ERROR_MESSAGE_STABILITY_TEST_REDACTED;
  }
  return ErrorMessageStability::ERROR_MESSAGE_STABILITY_PRODUCTION;
}

std::string FirstCharLower(absl::string_view str) {
  if (str.empty() || (str.size() >= 2 && isupper(str[1]))) {
    return std::string(str);
  }
  return absl::StrCat(absl::AsciiStrToLower(str.substr(0, 1)), str.substr(1));
}

}  // namespace zetasql
