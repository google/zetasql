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

#include "zetasql/common/options_utils.h"

#include <string>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/language_options.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql::internal {

template <typename EnumT>
std::string UnparseEnumOptionsSet(
    const absl::flat_hash_map<absl::string_view, absl::btree_set<EnumT>>&
        base_factory,
    absl::string_view strip_prefix, absl::btree_set<EnumT> options) {
  for (const auto& [base_name, base_options] : base_factory) {
    if (base_options == options) {
      return std::string(base_name);
    }
  }

  // No exact match, just pick none, and add
  ZETASQL_CHECK(base_factory.contains("NONE"));
  const google::protobuf::EnumDescriptor* enum_descriptor =
      google::protobuf::GetEnumDescriptor<EnumT>();

  std::string output = "NONE";
  for (EnumT option : options) {
    const google::protobuf::EnumValueDescriptor* enum_value_descriptor =
        enum_descriptor->FindValueByNumber(option);
    ZETASQL_CHECK(enum_value_descriptor);
    absl::string_view name = enum_value_descriptor->name();
    absl::ConsumePrefix(&name, strip_prefix);
    absl::StrAppend(&output, ",+", name);
  }
  return output;
}

//////////////////////////////////////////////////////////////////////////
// ResolvedASTRewrite
//////////////////////////////////////////////////////////////////////////
AnalyzerOptions::ASTRewriteSet GetAllRewrites() {
  AnalyzerOptions::ASTRewriteSet enabled_set;
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<ResolvedASTRewrite>();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor = descriptor->value(i);
    if (value_descriptor->number() == 0) {
      // This is the "INVALID" entry. Skip this case.
      continue;
    }
    enabled_set.insert(
        static_cast<ResolvedASTRewrite>(value_descriptor->number()));
  }
  return enabled_set;
}

absl::StatusOr<EnumOptionsEntry<ResolvedASTRewrite>> ParseEnabledAstRewrites(
    absl::string_view options_str) {
  return internal::ParseEnumOptionsSet<ResolvedASTRewrite>(
      {{"NONE", {}},
       {"ALL", GetAllRewrites()},
       {"DEFAULTS", AnalyzerOptions::DefaultRewrites()}},
      "REWRITE_", "Rewrite", options_str);
}

bool AbslParseFlag(absl::string_view text, EnabledAstRewrites* p,
                   std::string* error) {
  absl::StatusOr<EnumOptionsEntry<ResolvedASTRewrite>> entry =
      ParseEnabledAstRewrites(text);
  if (!entry.ok()) {
    *error = entry.status().message();
    return false;
  }
  p->enabled_ast_rewrites = entry->options;
  return true;
}

std::string AbslUnparseFlag(EnabledAstRewrites p) {
  return UnparseEnumOptionsSet<ResolvedASTRewrite>(
      {{"NONE", {}},
       {"ALL", GetAllRewrites()},
       {"DEFAULTS", AnalyzerOptions::DefaultRewrites()}},
      "REWRITE_", p.enabled_ast_rewrites);
}

//////////////////////////////////////////////////////////////////////////
// LanguageFeature
//////////////////////////////////////////////////////////////////////////
static absl::btree_set<LanguageFeature> GetAllLanguageFeatures() {
  absl::btree_set<LanguageFeature> enabled_set;
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<LanguageFeature>();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor = descriptor->value(i);
    enabled_set.insert(
        static_cast<LanguageFeature>(value_descriptor->number()));
  }
  for (LanguageFeature fake_feature :
       {__LanguageFeature__switch_must_have_a_default__}) {
    enabled_set.erase(fake_feature);
  }
  return enabled_set;
}

static absl::btree_set<LanguageFeature> GetMaximumLanguageFeatures() {
  auto tmp = LanguageOptions::MaximumFeatures().GetEnabledLanguageFeatures();
  return absl::btree_set<LanguageFeature>{tmp.begin(), tmp.end()};
}

static absl::btree_set<LanguageFeature> GetDevLanguageFeatures() {
  LanguageOptions options;
  options.EnableMaximumLanguageFeaturesForDevelopment();

  return absl::btree_set<LanguageFeature>{
      options.GetEnabledLanguageFeatures().begin(),
      options.GetEnabledLanguageFeatures().end()};
}

absl::StatusOr<EnumOptionsEntry<LanguageFeature>> ParseEnabledLanguageFeatures(
    absl::string_view options_str) {
  return internal::ParseEnumOptionsSet<LanguageFeature>(
      {{"NONE", {}},
       {"ALL", GetAllLanguageFeatures()},
       {"MAXIMUM", GetMaximumLanguageFeatures()},
       {"DEV", GetDevLanguageFeatures()}},
      "FEATURE_", "Rewrite", options_str);
}

bool AbslParseFlag(absl::string_view text, EnabledLanguageFeatures* p,
                   std::string* error) {
  absl::StatusOr<EnumOptionsEntry<LanguageFeature>> entry =
      ParseEnabledLanguageFeatures(text);
  if (!entry.ok()) {
    *error = entry.status().message();
    return false;
  }
  p->enabled_language_features = entry->options;
  return true;
}

std::string AbslUnparseFlag(EnabledLanguageFeatures p) {
  return UnparseEnumOptionsSet<LanguageFeature>(
      {{"NONE", {}},
       {"ALL", GetAllLanguageFeatures()},
       {"MAXIMUM", GetMaximumLanguageFeatures()},
       {"DEV", GetDevLanguageFeatures()}},
      "FEATURE_", p.enabled_language_features);
}

}  // namespace zetasql::internal
