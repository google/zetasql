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

#include "zetasql/compliance/compliance_label_extractor.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "zetasql/common/function_utils.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

using NameToFunctionMap = std::map<std::string, std::unique_ptr<Function>>;
using PrefixSet =
    absl::btree_set<std::string, zetasql_base::CaseLess>;
using CaseInsensitiveMap =
    absl::btree_map<std::string, int, zetasql_base::CaseLess>;
using UnderscoreAndDotPrefixSets = std::pair<PrefixSet, PrefixSet>;

// Fetch all the functions from GetZetaSQLFunctions with maximum language
// options in internal mode and populate the prefix group set.
//
// Any underscore prefix that shows up in 2 or more functions is considered a
// prefix group.
// Any underscore prefix that happens on exactly 1 function is not a prefix.
// All dot prefix group detected are considered valid for now.
//
// UnderscoreAndDotPrefixSets pair stores underscore prefix set as the first
// and dot prefix set as the element.
// TODO: There might be false positive detected. Add a skiplist
// if the outliers have side effect.
static UnderscoreAndDotPrefixSets*
GetFunctionPrefixSetsFromZetaSQLFunctions() {
  TypeFactory type_factory;
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  options.set_product_mode(PRODUCT_INTERNAL);
  NameToFunctionMap functions;
  GetZetaSQLFunctions(&type_factory, options, &functions);

  CaseInsensitiveMap underscore_prefix_cnt;
  UnderscoreAndDotPrefixSets* prefix_sets = new UnderscoreAndDotPrefixSets;

  for (const auto& fn : functions) {
    // Ignore operators when building up valid prefix sets
    if (FunctionIsOperator(*fn.second)) {
      continue;
    }
    // Try finding dot in function name first
    std::string function_sql_name = fn.second->SQLName();
    absl::string_view dot_prefix =
        *(absl::StrSplit(function_sql_name, '.', absl::SkipEmpty()).begin());
    if (dot_prefix.length() != function_sql_name.length()) {
      prefix_sets->second.insert(std::string(dot_prefix));
    } else {
      // If function name does not contains dot, then try finding underscore.
      absl::string_view underscore_prefix =
          *(absl::StrSplit(function_sql_name, '_', absl::SkipEmpty()).begin());
      if (underscore_prefix.length() != function_sql_name.length()) {
        ++underscore_prefix_cnt[std::string(underscore_prefix)];
      }
    }
  }

  for (const auto& counter : underscore_prefix_cnt) {
    if (counter.second > 1) {
      prefix_sets->first.insert(counter.first);
    }
  }
  return prefix_sets;
}

static const PrefixSet& GetPrefixSet(PrefixGroup prefix_group) {
  static const UnderscoreAndDotPrefixSets* kPrefixSets =
      GetFunctionPrefixSetsFromZetaSQLFunctions();
  return prefix_group == PrefixGroup::kUnderscore ? kPrefixSets->first
                                                  : kPrefixSets->second;
}

void ExtractPrefixGroupAndFunctionPrefix(absl::string_view function_sql_name,
                                         absl::string_view& function_prefix_out,
                                         PrefixGroup& prefix_group_out) {
  auto extract = [&](char separator, PrefixGroup group) {
    const PrefixSet& prefixes = GetPrefixSet(group);
    absl::string_view prefix =
        *(absl::StrSplit(function_sql_name, separator, absl::SkipEmpty())
              .begin());
    if (prefix.length() != function_sql_name.length() &&
        prefixes.contains(std::string(prefix))) {
      function_prefix_out = prefix;
      prefix_group_out = group;
      return true;
    }
    return false;
  };
  if (!extract('.', PrefixGroup::kDot)) {
    extract('_', PrefixGroup::kUnderscore);
  }
}

struct FunctionSignatureLabel {
  FunctionSignatureLabel(FunctionSignatureId signature_id,
                         std::string& sql_name, std::string prefix = "",
                         PrefixGroup prefix_group = PrefixGroup::kNone,
                         bool is_operator = false)
      : signature_id(signature_id),
        sql_name(sql_name),
        prefix(prefix),
        prefix_group(prefix_group),
        is_operator(is_operator) {}

  FunctionSignatureId signature_id;
  std::string sql_name;
  std::string prefix = "";
  PrefixGroup prefix_group = PrefixGroup::kNone;
  bool is_operator;
};

struct TypeCastLabel {
  TypeCastLabel(TypeKind from, TypeKind to) : from_type(from), to_type(to) {}

  template <typename H>
  friend H AbslHashValue(H h, const TypeCastLabel& s) {
    return H::combine(std::move(h), s.from_type, s.to_type);
  }

  bool operator==(const TypeCastLabel& rhs) const {
    return from_type == rhs.from_type && to_type == rhs.to_type;
  }

  bool operator!=(const TypeCastLabel& rhs) const { return !(*this == rhs); }

  std::string DebugString() {
    return absl::StrCat(TypeKind_Name(from_type), ":", TypeKind_Name(to_type));
  }

  TypeKind from_type;
  TypeKind to_type;
};

class ComplianceLabelSets {
 public:
  bool AddTypeCast(TypeKind from_type, TypeKind to_type) {
    TypeCastLabel type_cast = TypeCastLabel(from_type, to_type);
    return type_casts_set_.insert(type_cast).second;
  }

  bool AddStatementNodeKind(ResolvedNodeKind node_kind) {
    return stmt_node_kinds_set_.insert(node_kind).second;
  }

  bool AddExpressionNodeKind(ResolvedNodeKind node_kind) {
    return expr_node_kinds_set_.insert(node_kind).second;
  }

  bool AddScanNodeKind(ResolvedNodeKind node_kind) {
    return scan_node_kinds_set_.insert(node_kind).second;
  }

  bool AddTypeKind(TypeKind type_kind) {
    return type_kinds_set_.insert(type_kind).second;
  }

  bool AddFunctionSignatureLabel(FunctionSignatureId signature_id,
                                 const Function* function) {
    std::string sql_name = function->SQLName();
    PrefixGroup prefix_group = PrefixGroup::kNone;
    absl::string_view prefix = "";
    bool is_operator = FunctionIsOperator(*function);
    if (!is_operator) {
      ExtractPrefixGroupAndFunctionPrefix(sql_name, prefix, prefix_group);
    }
    FunctionSignatureLabel signature_label = FunctionSignatureLabel(
        signature_id, sql_name, std::string(prefix), prefix_group, is_operator);
    return function_signatures_.try_emplace(signature_id, signature_label)
        .second;
  }

  void GenerateLabelStrings(absl::btree_set<std::string>& output) {
    // Generate node kind label in a format of
    // "ResolvedNodeKind:<nonleaf_node_group>:<leaf_node_kind_name>"
    for (ResolvedNodeKind node_kind : stmt_node_kinds_set_) {
      output.insert(absl::StrCat("ResolvedNodeKind:Statement:",
                                 ResolvedNodeKind_Name(node_kind)));
    }
    for (ResolvedNodeKind node_kind : expr_node_kinds_set_) {
      output.insert(absl::StrCat("ResolvedNodeKind:Expression:",
                                 ResolvedNodeKind_Name(node_kind)));
    }
    for (ResolvedNodeKind node_kind : scan_node_kinds_set_) {
      output.insert(absl::StrCat("ResolvedNodeKind:Scan:",
                                 ResolvedNodeKind_Name(node_kind)));
    }

    // Generate type kind label in a format of "TypeKind:<type_kind_name>"
    for (TypeKind type_kind : type_kinds_set_) {
      output.insert(absl::StrCat("TypeKind:", TypeKind_Name(type_kind)));
    }

    // Generate type cast label for CAST function
    for (TypeCastLabel type_cast : type_casts_set_) {
      output.insert(absl::StrCat("TypeCast:", type_cast.DebugString()));
    }

    // Generate function signature label for each signature.
    // Function label is in the following formats:
    //   "FunctionSignature:<prefix>:<sql_name>:<signature_id_name>"
    //   "FunctionSignature:<prefix>:<sql_name>"
    //   "FunctionSignature:<prefix>"
    // Operator label is in the following formats:
    //   "OperatorSignature:<sql_name>:<signature_id_name>"
    for (const auto& fn_signature : function_signatures_) {
      const FunctionSignatureLabel& signature_label = fn_signature.second;
      if (signature_label.is_operator) {
        output.insert(absl::StrCat("OperatorName:", signature_label.sql_name));
        output.insert(absl::StrCat(
            "OperatorSignature:", signature_label.sql_name, ":",
            FunctionSignatureId_Name(signature_label.signature_id)));
      } else {
        output.insert(absl::StrCat(
            "FunctionSignature:", signature_label.prefix, ":",
            signature_label.sql_name, ":",
            FunctionSignatureId_Name(signature_label.signature_id)));
        output.insert(absl::StrCat("FunctionName:", signature_label.prefix, ":",
                                   signature_label.sql_name));
        // Exclude prefix label for functions with empty prefix group.
        if (signature_label.prefix_group != PrefixGroup::kNone) {
          output.insert(
              absl::StrCat("FunctionFamily:", signature_label.prefix));
        }
      }
    }
  }

 private:
  absl::flat_hash_set<ResolvedNodeKind> stmt_node_kinds_set_;
  absl::flat_hash_set<ResolvedNodeKind> expr_node_kinds_set_;
  absl::flat_hash_set<ResolvedNodeKind> scan_node_kinds_set_;
  absl::flat_hash_set<TypeCastLabel> type_casts_set_;
  absl::flat_hash_set<TypeKind> type_kinds_set_;
  absl::flat_hash_map<FunctionSignatureId, FunctionSignatureLabel>
      function_signatures_;
};

class ComplianceLabelExtractor : public ResolvedASTVisitor {
 public:
  explicit ComplianceLabelExtractor(ComplianceLabelSets* compliance_labels)
      : compliance_labels_(*compliance_labels) {}

  absl::Status DefaultVisit(const ResolvedNode* node) override {
    if (node->IsScan()) {
      compliance_labels_.AddScanNodeKind(node->node_kind());
      ZETASQL_VLOG(5) << "Inserted scan node kind: "
              << ResolvedNodeKind_Name(node->node_kind()) << ".\n";
    } else if (node->IsExpression()) {
      compliance_labels_.AddExpressionNodeKind(node->node_kind());
      ZETASQL_VLOG(5) << "Inserted expression node kind: "
              << ResolvedNodeKind_Name(node->node_kind()) << ".\n";

      auto* expr_node = node->GetAs<ResolvedExpr>();
      ZETASQL_RET_CHECK_NE(expr_node, nullptr);
      compliance_labels_.AddTypeKind(expr_node->type()->kind());
      ZETASQL_VLOG(5) << "Inserted type kind (ResolvedExpr): "
              << TypeKind_Name(expr_node->type()->kind()) << ".\n";
    } else if (node->IsStatement()) {
      compliance_labels_.AddStatementNodeKind(node->node_kind());
      ZETASQL_VLOG(5) << "Inserted statement node kind: "
              << ResolvedNodeKind_Name(node->node_kind()) << ".\n";
    }
    return ResolvedASTVisitor::DefaultVisit(node);
  }

  template <typename T>
  absl::Status ExtractSignatureIdAndDefaultVisit(const T* node) {
    auto* function = node->function();
    ZETASQL_RET_CHECK_NE(function, nullptr);
    if (function->IsZetaSQLBuiltin()) {
      compliance_labels_.AddFunctionSignatureLabel(
          static_cast<FunctionSignatureId>(node->signature().context_id()),
          function);
      ZETASQL_VLOG(5) << "Inserted function id: "
              << FunctionSignatureId_Name(static_cast<FunctionSignatureId>(
                     node->signature().context_id()))
              << ".\n";
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    return ExtractSignatureIdAndDefaultVisit<ResolvedFunctionCall>(node);
  }

  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    return ExtractSignatureIdAndDefaultVisit<ResolvedAggregateFunctionCall>(
        node);
  }

  absl::Status VisitResolvedAnalyticFunctionCall(
      const ResolvedAnalyticFunctionCall* node) override {
    return ExtractSignatureIdAndDefaultVisit<ResolvedAnalyticFunctionCall>(
        node);
  }

  absl::Status VisitResolvedCast(const ResolvedCast* node) override {
    auto* expr = node->expr();
    ZETASQL_RET_CHECK_NE(expr, nullptr);
    compliance_labels_.AddTypeCast(expr->type()->kind(), node->type()->kind());
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedExtendedCastElement(
      const ResolvedExtendedCastElement* node) override {
    compliance_labels_.AddTypeKind(node->from_type()->kind());
    compliance_labels_.AddTypeKind(node->to_type()->kind());
    ZETASQL_VLOG(5) << "Inserted type kind (ResolvedExtendedCastElement): "
            << TypeKind_Name(node->from_type()->kind()) << ", "
            << TypeKind_Name(node->to_type()->kind()) << ".\n";
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedColumnDefinition(
      const ResolvedColumnDefinition* node) override {
    compliance_labels_.AddTypeKind(node->type()->kind());
    ZETASQL_VLOG(5) << "Inserted type kind (ResolvedColumnDefinition): "
            << TypeKind_Name(node->type()->kind()) << ".\n";
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedArgumentDef(
      const ResolvedArgumentDef* node) override {
    compliance_labels_.AddTypeKind(node->type()->kind());
    ZETASQL_VLOG(5) << "Inserted type kind (ResolvedArgumentDef): "
            << TypeKind_Name(node->type()->kind()) << ".\n";
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedAlterColumnSetDataTypeAction(
      const ResolvedAlterColumnSetDataTypeAction* node) override {
    compliance_labels_.AddTypeKind(node->updated_type()->kind());
    ZETASQL_VLOG(5) << "Inserted type kind (ResolvedAlterColumnSetDataTypeAction): "
            << TypeKind_Name(node->updated_type()->kind()) << ".\n";
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedCreateFunctionStmt(
      const ResolvedCreateFunctionStmt* node) override {
    compliance_labels_.AddTypeKind(node->return_type()->kind());
    ZETASQL_VLOG(5) << "Inserted type kind (ResolvedCreateFunctionStmt): "
            << TypeKind_Name(node->return_type()->kind()) << ".\n";
    return DefaultVisit(node);
  }

 private:
  ComplianceLabelSets& compliance_labels_;
};

absl::Status ExtractComplianceLabels(const ResolvedNode* node,
                                     absl::btree_set<std::string>& labels_out) {
  // Create resolved AST visitor and traverse the tree to collect labels against
  // each node.
  ComplianceLabelSets compliance_labels = ComplianceLabelSets();
  ComplianceLabelExtractor visitor =
      ComplianceLabelExtractor(&compliance_labels);
  ZETASQL_RETURN_IF_ERROR(node->Accept(&visitor));
  compliance_labels.GenerateLabelStrings(labels_out);
  return absl::OkStatus();
}

}  // namespace zetasql
