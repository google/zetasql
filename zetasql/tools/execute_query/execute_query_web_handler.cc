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

#include "zetasql/tools/execute_query/execute_query_web_handler.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/execute_query_tool.h"
#include "zetasql/tools/execute_query/execute_query_web_writer.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "zetasql/tools/execute_query/web/embedded_resources.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "external/mstch/mstch/include/mstch/mstch.hpp"

namespace zetasql {

namespace {

ModeSet ModeSetFromStrings(absl::Span<const std::string> mode_strings) {
  ModeSet modes;
  for (const auto &mode : mode_strings) {
    std::optional<ExecuteQueryConfig::ToolMode> tool_mode =
        ExecuteQueryConfig::parse_tool_mode(mode);
    if (tool_mode.has_value()) {
      modes.insert(*tool_mode);
    }
  }

  if (modes.empty()) {
    modes.insert(ExecuteQueryConfig::ToolMode::kExecute);
  }

  return modes;
}

}  // namespace

ExecuteQueryWebRequest::ExecuteQueryWebRequest(
    const std::vector<std::string> &str_modes, std::string query,
    std::string catalog)
    : modes_(ModeSetFromStrings(str_modes)),
      query_(std::move(query)),
      catalog_(std::move(catalog)) {
  // The query input sometimes contains non-breaking spaces (or the HTML entity
  // &nbsp;). This is 0xA0 (encoded in UTF-8 as \xc2\xa0). A bare 0xa0 is
  // sometimes seen when a query is copied from an HTML editor.
  // We explicitly replace these sequences with a normal space character.
  absl::StrReplaceAll({{"\xc2\xa0", " "}, {"\xa0", " "}}, &query_);

  if (catalog_.empty()) {
    // Get the value from the flag on the initial page load.
    catalog_ = absl::GetFlag(FLAGS_catalog);
  }
}

bool ExecuteQueryWebHandler::HandleRequest(
    const ExecuteQueryWebRequest &request, const Writer &writer) {
  mstch::map template_params = {{"query", request.query()},
                                {"css", templates_.GetWebPageCSS()}};

  mstch::array catalogs;
  for (const auto *selectable_catalog : GetSelectableCatalogs()) {
    mstch::map entry = {
        {"name", selectable_catalog->name()},
        {"label", absl::StrCat(selectable_catalog->name(), " - ",
                               selectable_catalog->description())}};
    if (selectable_catalog->name() == request.catalog()) {
      entry["selected"] = std::string("selected");
    }
    catalogs.push_back(entry);
  }

  template_params.insert(std::pair("catalogs", catalogs));

  if (!request.query().empty()) {
    std::string error_msg;
    ExecuteQueryWebWriter params_writer(template_params);
    ExecuteQuery(request, error_msg, params_writer);
    params_writer.FlushStatement(/*at_end=*/true, error_msg);
  }

  // Add the selected modes back into the template so that the same checkboxes
  // are checked when the page is rendered again.
  for (const auto &mode : request.modes()) {
    template_params[absl::StrCat(
        "mode_", ExecuteQueryConfig::tool_mode_name(mode))] = "1";
  }

  // Render the page.
  std::string rendered =
      mstch::render(templates_.GetWebPageContents(), template_params,
                    {{"body", templates_.GetWebPageBody()},
                     {"table", templates_.GetTable()}});

  if (writer(rendered) <= 0) {
    ABSL_LOG(WARNING) << "Error writing rendered HTML.";
    return false;
  }

  return true;
}

absl::Status ExecuteQueryWebHandler::ExecuteQueryImpl(
    const ExecuteQueryWebRequest &request,
    ExecuteQueryWriter &exec_query_writer) {
  // TODO: Try to avoid creating a new config each time
  ExecuteQueryConfig config;
  ZETASQL_RETURN_IF_ERROR(zetasql::InitializeExecuteQueryConfig(config));

  config.set_tool_modes(request.modes());
  config.mutable_analyzer_options().set_error_message_mode(
      ERROR_MESSAGE_MULTI_LINE_WITH_CARET);

  ZETASQL_RETURN_IF_ERROR(config.SetCatalogFromString(request.catalog()));

  ZETASQL_RETURN_IF_ERROR(
      zetasql::ExecuteQuery(request.query(), config, exec_query_writer));
  return absl::OkStatus();
}

bool ExecuteQueryWebHandler::ExecuteQuery(
    const ExecuteQueryWebRequest &request, std::string &error_msg,
    ExecuteQueryWriter &exec_query_writer) {
  absl::Status st = ExecuteQueryImpl(request, exec_query_writer);
  if (!st.ok()) {
    error_msg = st.message();
  }
  return st.ok();
}

}  // namespace zetasql
