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

#include "zetasql/parser/deidentify.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/parser/unparser.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace parser {
namespace {

// Deidentify SQL but identifiers and literals.
//
// Unfortunately, we cannot replace all literals just with ? parameters as that
// does not reparse properly; for example, literals in hints cannot be
// parameterised.
//
// The compromise
// - numbers are replaced with 0
// - strings with <redacted>
// - JSON with '{"<redacted>": null}'
// - and names of standard builtin, well known functions etc are not replaced
class DeidentifyingUnparser : public Unparser {
 public:
  DeidentifyingUnparser(Catalog& catalog,
                        const LanguageOptions& language_options)
      : zetasql::parser::Unparser(&unparsed_output_),
        catalog_(catalog),
        language_options_(language_options) {}

  absl::string_view GetUnparsedOutput() const { return unparsed_output_; }
  void ResetOutput() { unparsed_output_.clear(); }

 private:
  void visitASTIntLiteral(const ASTIntLiteral* node, void* data) override {
    formatter_.Format("0");
  }
  void visitASTNumericLiteral(const ASTNumericLiteral* node,
                              void* data) override {
    formatter_.Format("0");
  }
  void visitASTBigNumericLiteral(const ASTBigNumericLiteral* node,
                                 void* data) override {
    formatter_.Format("0");
  }
  void visitASTJSONLiteral(const ASTJSONLiteral* node, void* data) override {
    formatter_.Format("JSON '{\"<redacted>\": null}'");
  }
  void visitASTFloatLiteral(const ASTFloatLiteral* node, void* data) override {
    formatter_.Format("0.0");
  }
  void visitASTStringLiteral(const ASTStringLiteral* node,
                             void* data) override {
    formatter_.Format("\"<redacted>\"");
  }
  void visitASTBytesLiteral(const ASTBytesLiteral* node, void* data) override {
    formatter_.Format("?");
  }
  void visitASTDateOrTimeLiteral(const ASTDateOrTimeLiteral* node,
                                 void* data) override {
    formatter_.Format("?");
  }
  void visitASTRangeLiteral(const ASTRangeLiteral* node, void* data) override {
    formatter_.Format("?");
  }

  void visitASTIdentifier(const ASTIdentifier* node, void* data) override {
    if (ShouldRemapIdentifier(node->GetAsStringView())) {
      formatter_.Format(RemapIdentifier(node->GetAsStringView()));
    } else {
      Unparser::visitASTIdentifier(node, data);
    }
  }

  bool ShouldRemapIdentifier(absl::string_view identifier) {
    if (zetasql::IsKeyword(identifier)) {
      return false;
    }
    if (language_options_.GenericEntityTypeSupported(identifier)) {
      return false;
    }
    if (language_options_.GenericSubEntityTypeSupported(identifier)) {
      return false;
    }

    std::vector<std::string> path;
    path.push_back(std::string(identifier));

    if (const Constant* constant = nullptr;
        catalog_.FindConstant(path, &constant).ok()) {
      return false;
    }
    if (const Type* type = nullptr; catalog_.FindType(path, &type).ok()) {
      return false;
    }
    if (const Function* function = nullptr;
        catalog_.FindFunction(path, &function).ok()) {
      return false;
    }
    if (const TableValuedFunction* function = nullptr;
        catalog_.FindTableValuedFunction(path, &function).ok()) {
      return false;
    }
    if (const Table* table = nullptr; catalog_.FindTable(path, &table).ok()) {
      return false;
    }
    return true;
  }

  std::string RemapIdentifier(absl::string_view identifier) {
    if (auto i = remapped_identifiers_.find(std::string(identifier));
        i != remapped_identifiers_.end()) {
      return i->second;
    }

    std::string remapped_name = "";
    size_t index = remapped_identifiers_.size();

    while (index > 25) {
      remapped_name = 'A' + (index % 26);
      index /= 26;
      index -= 1;
    }
    remapped_name += 'A' + index;
    std::reverse(remapped_name.begin(), remapped_name.end());

    while (zetasql::IsKeyword(remapped_name)) {
      remapped_name += "_";
    }

    remapped_identifiers_[std::string(identifier)] = remapped_name;
    return remapped_name;
  }

  Catalog& catalog_;
  const zetasql::LanguageOptions& language_options_;
  std::string unparsed_output_;
  std::unordered_map<std::string, std::string> remapped_identifiers_;
};
}  // namespace

absl::StatusOr<std::string> DeidentifySQLIdentifiersAndLiterals(
    absl::string_view input,
    const zetasql::LanguageOptions& language_options) {
  ParserOptions parser_options(language_options);
  std::unique_ptr<zetasql::ParserOutput> parser_output;
  zetasql::ParseResumeLocation parse_resume =
      zetasql::ParseResumeLocation::FromStringView(input);

  bool at_end_of_input = false;
  std::string all_deidentified;
  SimpleCatalog catalog("allowed identifiers for deidentification");
  ZETASQL_RETURN_IF_ERROR(catalog.AddBuiltinFunctionsAndTypes(
      zetasql::BuiltinFunctionOptions::AllReleasedFunctions()));
  DeidentifyingUnparser unparser(catalog, language_options);

  do {
    ZETASQL_RETURN_IF_ERROR(zetasql::ParseNextStatement(
        &parse_resume, parser_options, &parser_output, &at_end_of_input));
    parser_output->node()->Accept(&unparser, nullptr);
    unparser.FlushLine();

    all_deidentified += unparser.GetUnparsedOutput();
    unparser.ResetOutput();
  } while (!at_end_of_input);
  return all_deidentified;
}

}  // namespace parser
}  // namespace zetasql
