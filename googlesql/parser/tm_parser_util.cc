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

#include "googlesql/parser/tm_parser_util.h"

#include <optional>

#include "googlesql/common/errors.h"
#include "googlesql/common/status_payload_utils.h"
#include "googlesql/common/warning_sink.h"
#include "googlesql/parser/parse_tree.h"
#include "googlesql/public/deprecation_warning.pb.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/parse_location.h"
#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {
namespace parser {
namespace internal {

absl::Status MakeSyntaxError(const ParseLocationRange& location,
                             absl::string_view msg) {
  absl::Status status = absl::InvalidArgumentError(msg);
  googlesql::internal::AttachPayload(
      &status, location.start().ToInternalErrorLocation());
  return status;
}

absl::Status MakeSyntaxError(const std::optional<ParseLocationRange>& location,
                             absl::string_view msg) {
  GOOGLESQL_RET_CHECK(location.has_value())
      << "MakeSyntaxError called with nullopt location";
  return MakeSyntaxError(*location, msg);
}

absl::Status ValidateNoWhitespace(absl::string_view left,
                                  const ParseLocationRange& left_loc,
                                  absl::string_view right,
                                  const ParseLocationRange& right_loc) {
  if (!left_loc.IsAdjacentlyFollowedBy(right_loc)) {
    return MakeSyntaxError(
        left_loc, absl::StrCat("Syntax error: Unexpected whitespace between \"",
                               left, "\" and \"", right, "\""));
  }
  return absl::OkStatus();
}

absl::Status ErrorIfUnparenthesizedNotExpression(ASTNode* rhs_expr) {
  const ASTUnaryExpression* expr = rhs_expr->GetAsOrNull<ASTUnaryExpression>();
  if (expr != nullptr && !expr->parenthesized() &&
      expr->op() == ASTUnaryExpression::NOT) {
    // TODO: nbales - Make this error message actionable by suggesting parens.
    return MakeSqlErrorAtStart(rhs_expr->location())
           << "Syntax error: Unexpected keyword NOT";
  }
  return absl::OkStatus();
}

absl::StatusOr<TVFDeclarationOptions> ProcessTVFDeclarationOptions(
    absl::Span<ASTNode* const> nodes) {
  TVFDeclarationOptions result;
  for (ASTNode* node : nodes) {
    if (node->Is<ASTIdentifier>()) {
      if (result.language != nullptr) {
        return MakeSyntaxError(node->location(),
                               "Syntax error: Duplicate LANGUAGE clause");
      }
      result.language = node->GetAsOrDie<ASTIdentifier>();
    } else if (node->Is<ASTWithConnectionClause>()) {
      if (result.connection != nullptr) {
        return MakeSyntaxError(
            node->location(), "Syntax error: Duplicate WITH CONNECTION clause");
      }
      result.connection = node->GetAsOrDie<ASTWithConnectionClause>();
    } else if (node->Is<ASTOptionsList>()) {
      if (result.options != nullptr) {
        return MakeSyntaxError(node->location(),
                               "Syntax error: Duplicate OPTIONS clause");
      }
      result.options = node->GetAsOrDie<ASTOptionsList>();
    } else {
      GOOGLESQL_RET_CHECK_FAIL() << "Unexpected node type in TVF declaration options: "
                       << node->GetNodeKindString();
    }
  }
  return result;
}

const auto& GetWarningKeywords() {
  static const absl::NoDestructor<LanguageOptions::KeywordSet> kWarningKeywords(
      {kQualify, kGraphTable, kAlign});
  return *kWarningKeywords;
}

absl::Status AddWarningIfReserved(absl::string_view keyword,
                                  const ParseLocationRange& location,
                                  const LanguageOptions& language_options,
                                  WarningSink& warning_sink) {
  // TODO: Add warnings for all reservable keywords and then remove
  // GetWarningKeywords().
  if (!GetWarningKeywords().contains(keyword)) {
    return absl::OkStatus();
  }
  // TODO: this warning should point to documentation once
  // we have the engine-specific root URI to use.
  GOOGLESQL_RET_CHECK(!language_options.IsReservedKeyword(keyword));
  constexpr absl::string_view kReservedWordWarning =
      "$0 is used as an identifier. $0 may become a reserved word in the "
      "future. To make this statement robust, add backticks around $0 to make "
      "the identifier unambiguous";
  return warning_sink.AddWarning(
      DeprecationWarning::RESERVED_KEYWORD,
      MakeSqlErrorAtStart(location) << absl::Substitute(
          kReservedWordWarning, absl::AsciiStrToUpper(keyword)));
}

}  // namespace internal
}  // namespace parser
}  // namespace googlesql
