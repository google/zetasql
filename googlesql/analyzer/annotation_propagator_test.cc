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

#include "googlesql/analyzer/annotation_propagator.h"

#include <memory>
#include <utility>
#include <vector>

#include "googlesql/base/testing/status_matchers.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/annotation/is_versioned.h"
#include "googlesql/public/builtin_function_options.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/id_string.h"
#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/array_type.h"
#include "googlesql/public/types/simple_value.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/rewrite_utils.h"
#include "googlesql/resolved_ast/test_utils.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/check.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

using testing::BuildResolvedLiteralsWithCollationForTest;
using testing::GetBuiltinFunctionFromCatalogForTest;

static AnalyzerOptions MakeAnalyzerOptions() {
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->SetSupportsAllStatementKinds();
  analyzer_options.mutable_language()->EnableLanguageFeature(
      LanguageFeature::FEATURE_COLLATION_SUPPORT);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      LanguageFeature::FEATURE_ANNOTATION_FRAMEWORK);
  return analyzer_options;
}

static std::unique_ptr<SimpleCatalog> MakeSimpleCatalog(
    const AnalyzerOptions& analyzer_options) {
  auto catalog = std::make_unique<SimpleCatalog>("function_builder_catalog");
  catalog->AddBuiltinFunctions(
      BuiltinFunctionOptions(analyzer_options.language()));
  return catalog;
}

class CollationAnnotationPropagatorTest : public ::testing::Test {
 public:
  CollationAnnotationPropagatorTest()
      : analyzer_options_(MakeAnalyzerOptions()),
        catalog_(MakeSimpleCatalog(analyzer_options_)),
        function_call_builder_(analyzer_options_, *catalog_, type_factory_) {}

 protected:
  AnalyzerOptions analyzer_options_;
  std::unique_ptr<SimpleCatalog> catalog_;
  TypeFactory type_factory_;
  FunctionCallBuilder function_call_builder_;
};

// Creates resolved array function node without annotations correctly set.
static absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
MakeArrayFaulty(std::vector<std::unique_ptr<const ResolvedExpr>> elements,
                AnalyzerOptions& analyzer_options, Catalog& catalog,
                TypeFactory& type_factory,
                AnnotationPropagator* annotation_propagator) {
  GOOGLESQL_RET_CHECK(!elements.empty());
  GOOGLESQL_ASSIGN_OR_RETURN(const Function* make_array_fn,
                   GetBuiltinFunctionFromCatalogForTest(
                       "$make_array", analyzer_options, catalog));
  GOOGLESQL_RET_CHECK(make_array_fn != nullptr);
  // make_array has only one signature in catalog.
  GOOGLESQL_RET_CHECK_EQ(make_array_fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature = make_array_fn->GetSignature(0);
  GOOGLESQL_RET_CHECK(catalog_signature != nullptr);

  // Construct arguments type and result type to pass to FunctionSignature.
  const ArrayType* array_type;
  GOOGLESQL_RETURN_IF_ERROR(type_factory.MakeArrayType(elements[0]->type(), &array_type));
  FunctionArgumentType result_type(array_type,
                                   catalog_signature->result_type().options(),
                                   /*num_occurrences=*/1);
  result_type.set_original_kind(
      SignatureArgumentKind::ARG_KIND_EXPR_ARRAY_ANY_1);
  FunctionArgumentType arguments_type(array_type->element_type(),
                                      catalog_signature->argument(0).options(),
                                      static_cast<int>(elements.size()));
  arguments_type.set_original_kind(SignatureArgumentKind::ARG_KIND_EXPR_ANY_1);
  FunctionSignature make_array_signature(result_type, {arguments_type},
                                         catalog_signature->context_id(),
                                         catalog_signature->options());

  return MakeResolvedFunctionCall(array_type, make_array_fn,
                                  make_array_signature, std::move(elements),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

TEST_F(CollationAnnotationPropagatorTest,
       TestCollationAnnotationPropagationForMakeArrayWithCollation) {
  AnnotationPropagator annotation_propagator(analyzer_options_, type_factory_);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<const ResolvedExpr>> args,
                       BuildResolvedLiteralsWithCollationForTest(
                           {{"foo", "und:ci"}, {"bar", "und:ci"}, {"baz", ""}},
                           analyzer_options_, *catalog_, type_factory_));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const ResolvedExpr> make_arr_fn,
      MakeArrayFaulty(std::move(args), analyzer_options_, *catalog_,
                      type_factory_, &annotation_propagator));
  EXPECT_EQ(make_arr_fn->DebugString(), absl::StripLeadingAsciiWhitespace(R"(
FunctionCall(GoogleSQL:$make_array(repeated(3) STRING) -> ARRAY<STRING>)
+-FunctionCall(GoogleSQL:collate(STRING, STRING) -> STRING)
| +-type_annotation_map={Collation:"und:ci"}
| +-Literal(type=STRING, value="foo", has_explicit_type=TRUE)
| +-Literal(type=STRING, value="und:ci", preserve_in_literal_remover=TRUE)
+-FunctionCall(GoogleSQL:collate(STRING, STRING) -> STRING)
| +-type_annotation_map={Collation:"und:ci"}
| +-Literal(type=STRING, value="bar", has_explicit_type=TRUE)
| +-Literal(type=STRING, value="und:ci", preserve_in_literal_remover=TRUE)
+-FunctionCall(GoogleSQL:collate(STRING, STRING) -> STRING)
  +-Literal(type=STRING, value="baz", has_explicit_type=TRUE)
  +-Literal(type=STRING, value="", preserve_in_literal_remover=TRUE)
)"));

  GOOGLESQL_CHECK_OK(annotation_propagator.CheckAndPropagateAnnotations(
      /*error_node=*/nullptr, const_cast<ResolvedExpr*>(make_arr_fn.get())));

  EXPECT_EQ(make_arr_fn->DebugString(), absl::StripLeadingAsciiWhitespace(R"(
FunctionCall(GoogleSQL:$make_array(repeated(3) STRING) -> ARRAY<STRING>)
+-type_annotation_map=<{Collation:"und:ci"}>
+-FunctionCall(GoogleSQL:collate(STRING, STRING) -> STRING)
| +-type_annotation_map={Collation:"und:ci"}
| +-Literal(type=STRING, value="foo", has_explicit_type=TRUE)
| +-Literal(type=STRING, value="und:ci", preserve_in_literal_remover=TRUE)
+-FunctionCall(GoogleSQL:collate(STRING, STRING) -> STRING)
| +-type_annotation_map={Collation:"und:ci"}
| +-Literal(type=STRING, value="bar", has_explicit_type=TRUE)
| +-Literal(type=STRING, value="und:ci", preserve_in_literal_remover=TRUE)
+-FunctionCall(GoogleSQL:collate(STRING, STRING) -> STRING)
  +-Literal(type=STRING, value="baz", has_explicit_type=TRUE)
  +-Literal(type=STRING, value="", preserve_in_literal_remover=TRUE)
)"));
}

// TODO: Add collation annotation propagation test cases for all
// remaining functions that uses collation operation -
// array_min, array_max, array_offset, array_offsets, array_find,
// array_find_all, min, max, percentile_disc, approx_count_distinct,
// approx_quantlies, approx_top_count, approx_top_sum, equal, not_equal,
// is_distinct_from, is_not_distinct_from, less, less_or_equal, greater,
// greater_or_equal, between, like, in, min, max, strpos, starts_with,
// ends_with, replace, split, instr, case_with_value, range_bucket, greatest,
// least.

class IsVersionedAnnotationPropagatorTest : public ::testing::Test {
 protected:
  IsVersionedAnnotationPropagatorTest() : type_factory_() {}

  TypeFactory type_factory_;
};

TEST_F(IsVersionedAnnotationPropagatorTest,
       TestIsVersionedAnnotationDoesNotPropagate) {
  AnalyzerOptions options;
  options.mutable_language()->EnableLanguageFeature(
      LanguageFeature::FEATURE_ANNOTATION_FRAMEWORK);
  options.mutable_language()->EnableLanguageFeature(
      LanguageFeature::FEATURE_VERSION_AWARE_DML);

  AnnotationPropagator propagator(options, type_factory_);

  std::unique_ptr<AnnotationMap> annotation_map =
      AnnotationMap::Create(type_factory_.get_string());
  annotation_map->SetAnnotation<IsVersionedAnnotation>(SimpleValue::Bool(true));
  ASSERT_TRUE(annotation_map->Has<IsVersionedAnnotation>());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const AnnotationMap* owned_map,
                       type_factory_.TakeOwnership(std::move(annotation_map)));

  ResolvedColumn column(1, IdString::MakeGlobal("table"),
                        IdString::MakeGlobal("col"),
                        AnnotatedType(type_factory_.get_string(), owned_map));
  ASSERT_TRUE(column.type_annotation_map() != nullptr &&
              column.type_annotation_map()->Has<IsVersionedAnnotation>());

  auto column_ref = MakeResolvedColumnRef(column, /*is_correlated=*/false);
  EXPECT_EQ(column_ref->type_annotation_map(), owned_map);

  auto cast =
      MakeResolvedCast(type_factory_.get_string(), std::move(column_ref),
                       /*return_null_on_error=*/false);
  GOOGLESQL_ASSERT_OK(propagator.CheckAndPropagateAnnotations(/*error_node=*/nullptr,
                                                    cast.get()));
  EXPECT_EQ(cast->type_annotation_map(), nullptr);
}

}  // namespace googlesql
