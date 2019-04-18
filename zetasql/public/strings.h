//
// Copyright 2019 ZetaSQL Authors
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

#ifndef ZETASQL_PUBLIC_STRINGS_H_
#define ZETASQL_PUBLIC_STRINGS_H_

#include <string>
#include <vector>

#include "zetasql/public/id_string.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"

namespace zetasql {

// These escaping and unescaping interfaces were originally copied from
// strings/escaping.h, matching Utf8SafeCHexEscape and CUnescape, but
// customized for SQL-style quote characters to allow 'abc"def', "abc'def"
// and `abc"'def`, provide different hex and octal escaping semantics, and
// also give errors for strings like 'abc'def'.

// Expand escaped characters according to ZetaSQL escaping rules.
// This is for raw strings with no quoting.  If an error occurs and
// <error_string> is not NULL, then it is populated with the relevant error
// message. If <error_offset> is not NULL, it is populated with the offset in
// <str> at which the invalid input occurred.
zetasql_base::Status UnescapeString(absl::string_view str, std::string* out,
                            std::string* error_string = nullptr,
                            int* error_offset = nullptr);

// Expand escaped characters according to ZetaSQL escaping rules.
// Rules for bytes values are slightly different than those for strings.  This
// is for raw literals with no quoting.  If an error occurs and <error_string>
// is not NULL then it is populated with the relevant error message.  If
// <error_offset> is not NULL, it is populated with the offset in  <str> at
// which the invalid input occurred.
zetasql_base::Status UnescapeBytes(absl::string_view str, std::string* out,
                           std::string* error_string = nullptr,
                           int* error_offset = nullptr);

// Escape a std::string without quoting it. All quote characters are escaped.
std::string EscapeString(absl::string_view str);

// Escape a bytes value without quoting it.  Escaped bytes use hex escapes.
// If <escape_all_bytes> is true then all bytes are escaped.  Otherwise only
// unprintable bytes and escape/quote characters are escaped.
// If <escape_quote_char> is not 0, then quotes that do not match are not
// escaped.
std::string EscapeBytes(absl::string_view str, bool escape_all_bytes = false,
                   char escape_quote_char = 0);

// Unquote and unescape a quoted ZetaSQL std::string literal (of the form '...',
// "...", r'...' or r"...").
// If an error occurs and <error_string> is not NULL, then it is populated with
// the relevant error message. If <error_offset> is not NULL, it is populated
// with the offset in <str> at which the invalid input occurred.
zetasql_base::Status ParseStringLiteral(absl::string_view str, std::string* out,
                                std::string* error_string = nullptr,
                                int* error_offset = nullptr);

// Unquote and unescape a ZetaSQL bytes literal (of the form b'...',
// b"...", rb'...', rb"...", br'...' or br"...").
// If an error occurs and <error_string> is not NULL, then it is populated with
// the relevant error message. If <error_offset> is not NULL, it is populated
// with the offset in <str> at which the invalid input occurred.
zetasql_base::Status ParseBytesLiteral(absl::string_view str, std::string* out,
                               std::string* error_string = nullptr,
                               int* error_offset = nullptr);

// Return a quoted and escaped ZetaSQL std::string literal for <str>.
// May choose to quote with ' or " to produce nicer output.
std::string ToStringLiteral(absl::string_view str);

// Return a quoted and escaped ZetaSQL std::string literal for <str>.
// Always uses single quotes.
std::string ToSingleQuotedStringLiteral(absl::string_view str);

// Return a quoted and escaped ZetaSQL std::string literal for <str>.
// Always uses double quotes.
std::string ToDoubleQuotedStringLiteral(absl::string_view str);

// Return a quoted and escaped ZetaSQL bytes literal for <str>.
// Prefixes with b and may choose to quote with ' or " to produce nicer output.
std::string ToBytesLiteral(absl::string_view str);

// Return a quoted and escaped ZetaSQL bytes literal for <str>.
// Prefixes with b and always uses single quotes.
std::string ToSingleQuotedBytesLiteral(absl::string_view str);

// Return a quoted and escaped ZetaSQL bytes literal for <str>.
// Prefixes with b and always uses double quotes.
std::string ToDoubleQuotedBytesLiteral(absl::string_view str);

// Parse a ZetaSQL identifier.
// The std::string may be quoted with backticks.  If so, unquote and unescape it.
// Otherwise, the std::string must be a valid ZetaSQL identifier.
// If an error occurs and <error_string> is not NULL, then it is populated with
// the relevant error message. If <error_offset> is not NULL, it is populated
// with the offset in <str> at which the invalid input occurred.
zetasql_base::Status ParseIdentifier(absl::string_view str, std::string* out,
                             std::string* error_string = nullptr,
                             int* error_offset = nullptr);

// Same as above, but allow reserved keywords to be identifiers. For example,
// "SELECT" is valid as a generalized identifier but not as an identifier.
// "`SELECT`" is valid for either.
zetasql_base::Status ParseGeneralizedIdentifier(absl::string_view str, std::string* out,
                                        std::string* error_string = nullptr,
                                        int* error_offset = nullptr);

// Convert a std::string to a ZetaSQL identifier literal.
// The output will be quoted (with backticks) and escaped if necessary.
// If <quote_reserved_keywords> is true then the reserved keyword are quoted and
// escaped.
std::string ToIdentifierLiteral(absl::string_view str,
                           const bool quote_reserved_keywords = true);
std::string ToIdentifierLiteral(IdString str,
                           const bool quote_reserved_keywords = true);

// Convert an identifier path to a std::string, quoting identifiers if necessary.
// If <quote_reserved_keywords> is true then all reserved keywords are quoted
// and escaped. Otherwise only quote and escape the keyword when it is first in
// the path.
std::string IdentifierPathToString(absl::Span<const std::string> path,
                              const bool quote_reserved_keywords = true);
std::string IdentifierPathToString(absl::Span<const IdString> path,
                              const bool quote_reserved_keywords = true);

// Convert an identifier std::string to a vector of path components, unquoting
// identifiers if necessary. This inverts IdentifierPathToString and also
// handles paths that contain valid unquoted identifiers (e.g. abc.123 in
// addition to abc.`123`, and abc.select in addition to abc.`select`).
// (More formally, this function supports paths that rely on the DOT_IDENTIFIER
// mode in the lexer to handle generalized identifiers after the first
// component.)
//
//  Examples:
//   "abc.123" => {"abc", "123"}
//   "abc.`123`" => {"abc", "123"}
//   "abc.select.from.table" => {"abc", "select", "from", "table"}
//   "abc.`def.ghi`" => {"abc", "def.ghi"}
//   "123" => error
zetasql_base::Status ParseIdentifierPath(absl::string_view str,
                                 std::vector<std::string>* out);

// Return true if the std::string is a ZetaSQL keyword.
bool IsKeyword(absl::string_view str);

// Return true if the std::string is a ZetaSQL reserved keyword.
// These strings need to be backtick-quoted to be used as identifiers.
bool IsReservedKeyword(absl::string_view str);

// Get a set of all reserved keywords in ZetaSQL, in upper case.
// These strings need to be backtick-quoted to be used as identifiers.
const absl::flat_hash_set<std::string>& GetReservedKeywords();

// Return true for an alias generated internally for an anonymous column.
// These names are not meant to be user-visible.
// Currently, internal names start with '$'.
// TODO Consider removing this.
bool IsInternalAlias(const std::string& alias);
bool IsInternalAlias(IdString alias);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_STRINGS_H_
