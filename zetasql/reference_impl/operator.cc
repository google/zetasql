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

#include "zetasql/reference_impl/operator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/type.h"
#include "absl/base/attributes.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/stl_util.h"

namespace zetasql {

constexpr char AlgebraNode::kIndentFork[];
constexpr char AlgebraNode::kIndentBar[];
constexpr char AlgebraNode::kIndentSpace[];

// -------------------------------------------------------
// AlgebraArg
// -------------------------------------------------------

AlgebraArg::AlgebraArg(const VariableId& variable,
                       std::unique_ptr<AlgebraNode> node)
    : variable_(variable), node_(std::move(node)) {}

AlgebraArg::~AlgebraArg() {}

const ValueExpr* AlgebraArg::value_expr() const {
  return node() ? node()->AsValueExpr() : nullptr;
}

ValueExpr* AlgebraArg::mutable_value_expr() {
  return node() ? mutable_node()->AsMutableValueExpr() : nullptr;
}

const RelationalOp* AlgebraArg::relational_op() const {
  return node() ? node()->AsRelationalOp() : nullptr;
}

RelationalOp* AlgebraArg::mutable_relational_op() {
  return node() ? mutable_node()->AsMutableRelationalOp() : nullptr;
}

const InlineLambdaExpr* AlgebraArg::inline_lambda_expr() const {
  return node() ? node()->AsInlineLambdaExpr() : nullptr;
}

InlineLambdaExpr* AlgebraArg::mutable_inline_lambda_expr() {
  return node() ? mutable_node()->AsMutableInlineLambdaExpr() : nullptr;
}

std::string AlgebraArg::DebugString(bool verbose) const {
  return this->DebugInternal("\n", verbose);
}

std::string AlgebraArg::DebugInternal(const std::string& indent,
                                      bool verbose) const {
  std::string result;
  if (has_variable()) {
    absl::StrAppend(&result, "$", variable().ToString());
    if (has_node()) {
      if (verbose) {
        absl::StrAppend(&result, "[",
                        node()->AsValueExpr()->output_type()->DebugString(),
                        "]");
      }
      absl::StrAppend(&result, " := ");
    }
  }
  if (has_node()) {
    absl::StrAppend(&result, node()->DebugInternal(indent, verbose));
  }
  return result;
}

// -------------------------------------------------------
// CppValueArg
// -------------------------------------------------------
CppValueArg::CppValueArg(const VariableId variable,
                         absl::string_view value_debug_string)
    : AlgebraArg(variable, nullptr), value_debug_string_(value_debug_string) {}

std::string CppValueArg::DebugInternal(const std::string& indent,
                                       bool verbose) const {
  std::string result;
  if (has_variable()) {
    absl::StrAppend(&result, "$", variable().ToString());
    if (!value_debug_string_.empty()) {
      absl::StrAppend(&result, " := ");
    }
  }
  if (value_debug_string_.empty()) {
    absl::StrAppend(&result, "CppValue {", value_debug_string_, "}");
  }
  return result;
}

// -------------------------------------------------------
// ExprArg
// -------------------------------------------------------

ExprArg::ExprArg(const VariableId& variable, std::unique_ptr<ValueExpr> expr)
    : AlgebraArg(variable, std::move(expr)) {
  type_ = node()->AsValueExpr()->output_type();
}

ExprArg::ExprArg(const VariableId& variable, const Type* type)
    : AlgebraArg(variable, nullptr) {
  type_ = type;
}

ExprArg::ExprArg(std::unique_ptr<ValueExpr> expr)
    : AlgebraArg(VariableId(), std::move(expr)) {
  type_ = node()->AsValueExpr()->output_type();
}

// -------------------------------------------------------
// InlineLambdaArg
// -------------------------------------------------------
InlineLambdaArg::InlineLambdaArg(std::unique_ptr<InlineLambdaExpr> lambda)
    : AlgebraArg(VariableId(), std::move(lambda)) {}

// -------------------------------------------------------
// AlgebraNode
// -------------------------------------------------------

AlgebraNode::~AlgebraNode() { zetasql_base::STLDeleteElements(&args_); }

const ValueExpr* AlgebraNode::AsValueExpr() const {
  return nullptr;
}

ValueExpr* AlgebraNode::AsMutableValueExpr() {
  return nullptr;
}

const RelationalOp* AlgebraNode::AsRelationalOp() const {
  return nullptr;
}

RelationalOp* AlgebraNode::AsMutableRelationalOp() {
  ZETASQL_LOG(FATAL);
  return nullptr;
}

const InlineLambdaExpr* AlgebraNode::AsInlineLambdaExpr() const {
  ZETASQL_LOG(FATAL);
  return nullptr;
}

InlineLambdaExpr* AlgebraNode::AsMutableInlineLambdaExpr() {
  ZETASQL_LOG(FATAL);
  return nullptr;
}

void AlgebraNode::SetArg(int kind, std::unique_ptr<AlgebraArg> argument) {
  const AlgebraArg* argument_ptr = argument.get();
  if (argument) {
    std::vector<std::unique_ptr<AlgebraArg>> args;
    args.push_back(std::move(argument));
    SetArgs<AlgebraArg>(kind, std::move(args));
  } else {
    SetArgs<AlgebraArg>(kind, {});
  }
  ZETASQL_DCHECK_EQ(GetArg(kind), argument_ptr);
}

const AlgebraArg* AlgebraNode::GetArg(int kind) const {
  int start = arg_slices_[kind].start;
  int size = arg_slices_[kind].size;
  if (size > 0) {
    ZETASQL_DCHECK_EQ(1, size);
    return args_[start];
  } else {
    return nullptr;
  }
}

AlgebraArg* AlgebraNode::GetMutableArg(int kind) {
  int start = arg_slices_[kind].start;
  int size = arg_slices_[kind].size;
  if (size > 0) {
    ZETASQL_DCHECK_EQ(1, size);
    return args_[start];
  } else {
    return nullptr;
  }
}

std::string AlgebraNode::DebugString(bool verbose) const {
  return this->DebugInternal("\n", verbose);
}

std::string AlgebraNode::ArgDebugString(absl::Span<const std::string> arg_names,
                                        absl::Span<const ArgPrintMode> arg_mode,
                                        const std::string& indent, bool verbose,
                                        bool more_children) const {
  ZETASQL_CHECK_EQ(arg_names.size(), arg_mode.size());
  std::string result;
  std::string separator;
  for (int kind = 0; kind < arg_names.size(); kind++) {
    std::string indent_child = indent;
    if (more_children || kind < arg_names.size() - 1) {
      absl::StrAppend(&indent_child, kIndentBar);
    } else {
      // No tree line is required beside the last child.
      absl::StrAppend(&indent_child, kIndentSpace);
    }
    absl::Span<const AlgebraArg* const> args = GetArgs<AlgebraArg>(kind);
    switch (arg_mode[kind]) {
      case kNOpt:
        if (args.empty()) {
          break;
        }
        ABSL_FALLTHROUGH_INTENDED;
      case kN: {
        std::vector<std::string> str;
        for (auto ch : args) {
          str.push_back(indent_child + kIndentFork +
                        ch->DebugInternal(indent_child, verbose));
        }
        absl::StrAppend(&result, separator, indent + kIndentFork,
                        arg_names[kind], ": {", absl::StrJoin(str, ","), "}");
        separator = ",";
        break;
      }
      case kOpt:
        if (args.empty()) {
          break;
        }
        ABSL_FALLTHROUGH_INTENDED;
      case k1:
        ZETASQL_CHECK_EQ(1, args.size());
        if (args[0] != nullptr) {
          absl::StrAppend(&result, separator, indent, kIndentFork,
                          arg_names[kind], ": ",
                          args[0]->DebugInternal(indent_child, verbose));
          separator = ",";
        }
        break;
      case k0:
        ZETASQL_CHECK(args.empty());
        break;
    }
  }
  return result;
}

// -------------------------------------------------------
// KeyArg
// -------------------------------------------------------

std::string KeyArg::DebugInternal(const std::string& indent,
                                  bool verbose) const {
  std::string sort;
  switch (order()) {
    case kAscending:
      sort = " ASC";
      break;
    case kDescending:
      sort = " DESC";
      break;
    case kNotApplicable:
      break;
  }
  switch (null_order()) {
    case kNullsFirst:
      absl::StrAppend(&sort, " NULLS FIRST");
      break;
    case kNullsLast:
      absl::StrAppend(&sort, " NULLS LAST");
      break;
    default:
      break;
  }

  if (collation() != nullptr) {
    absl::StrAppend(
        &sort, " collation=", collation()->DebugInternal(indent, verbose));
  }

  return absl::StrCat(ExprArg::DebugInternal(indent, verbose), sort);
}

}  // namespace zetasql
