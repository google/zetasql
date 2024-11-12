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

#include "zetasql/common/match_recognize/nfa_builder.h"

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "zetasql/common/match_recognize/nfa.h"
#include "zetasql/common/match_recognize/nfa_matchers.h"
#include "zetasql/common/match_recognize/test_pattern_resolver.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql::functions::match_recognize {
namespace {

using ::zetasql_base::testing::StatusIs;

class NFABuilderTest : public testing::Test {
 protected:
  TestPatternResolver resolver_;
};

TEST_F(NFABuilderTest, BasicSymbol) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, ConcatTwoSymbols) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A B"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(1)}),
                        State("N2", {Edge("N3")}),
                        State("N3", {}),
                    })));
}

TEST_F(NFABuilderTest, ConcatSymbolWithItselfInDifferentCase) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A a"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N3")}),
                        State("N3", {}),
                    })));
}

TEST_F(NFABuilderTest, ConcatThreeSymbols) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A B A"));
  std::vector<std::string> vars = {"A", "B"};

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(1)}),
                        State("N2", {Edge("N3").On(0)}),
                        State("N3", {Edge("N4")}),
                        State("N4", {}),
                    })));
}

TEST_F(NFABuilderTest, NestedConcat) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A (B C)"));
  std::vector<std::string> vars = {"A", "B", "C"};

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(1)}),
                        State("N2", {Edge("N3").On(2)}),
                        State("N3", {Edge("N4")}),
                        State("N4", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternativeOfTwoSymbols) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A|B"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0), Edge("N2").On(1)}),
                        State("N1", {Edge("N3")}),
                        State("N2", {Edge("N3")}),
                        State("N3", {}),
                    })));
}
TEST_F(NFABuilderTest, AlternativeOfThreeSymbols) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A|B|C"));
  std::vector<std::string> vars = {"A", "B", "C"};

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(
      *nfa,
      EquivToGraph(Graph({{
          State("N0", {Edge("N1").On(0), Edge("N2").On(1), Edge("N3").On(2)}),
          State("N1", {Edge("N4")}),
          State("N2", {Edge("N4")}),
          State("N3", {Edge("N4")}),
          State("N4", {}),
      }})));
}
TEST_F(NFABuilderTest, AlternativeSymbolAndConcatCombo) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("(A|(B C)) (A|C)"));
  std::vector<std::string> vars = {"A", "B", "C"};

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0), Edge("N4").On(1)}),
                        State("N1", {Edge("N2").On(0), Edge("N3").On(2)}),
                        State("N2", {Edge("N6")}),
                        State("N3", {Edge("N6")}),
                        State("N4", {Edge("N5").On(2)}),
                        State("N5", {Edge("N2").On(0), Edge("N3").On(2)}),
                        State("N6", {}),
                    })));
}

TEST_F(NFABuilderTest, EmptyConcatSymbol) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("() A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, EmptyConcatEmptyConcatSymbol) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("() () A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, SymbolConcatEmpty) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A ()"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, SymbolOrEmpty) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A | "));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0), Edge("N2")}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, EmptyOrSymbol) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("| A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N2"), Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, ComplexWithEmpty) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A | () | (B C ())|"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa,
              EquivToGraph(Graph({
                  State("N0", {Edge("N1").On(0), Edge("N4"), Edge("N2").On(1)}),
                  State("N1", {Edge("N4")}),
                  State("N2", {Edge("N3").On(2)}),
                  State("N3", {Edge("N4")}),
                  State("N4", {}),
              })));
}

TEST_F(NFABuilderTest, GreedyQuestionMark) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A?"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0), Edge("N2")}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}
TEST_F(NFABuilderTest, ReluctantQuestionMark) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A??"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N2"), Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, RepZeroToFourGreedy) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{0,4}"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0), Edge("N5")}),
                        State("N1", {Edge("N2").On(0), Edge("N5")}),
                        State("N2", {Edge("N3").On(0), Edge("N5")}),
                        State("N3", {Edge("N4").On(0), Edge("N5")}),
                        State("N4", {Edge("N5")}),
                        State("N5", {}),
                    })));
}

TEST_F(NFABuilderTest, RepZeroToFourReluctant) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{0,4}?"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N5"), Edge("N1").On(0)}),
                        State("N1", {Edge("N5"), Edge("N2").On(0)}),
                        State("N2", {Edge("N5"), Edge("N3").On(0)}),
                        State("N3", {Edge("N5"), Edge("N4").On(0)}),
                        State("N4", {Edge("N5")}),
                        State("N5", {}),
                    })));
}

TEST_F(NFABuilderTest, RepZeroGreedy) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{0,0}"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1")}),
                        State("N1", {}),
                    })));
}

TEST_F(NFABuilderTest, RepZeroReluctant) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{0,0}?"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1")}),
                        State("N1", {}),
                    })));
}

TEST_F(NFABuilderTest, QuantifierBoundExceedsInt32Max) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{4294967296}"));
  EXPECT_THAT(NFABuilder::BuildNFAForPattern(*scan),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(NFABuilderTest, PatternTooComplex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{1000000}"));
  EXPECT_THAT(NFABuilder::BuildNFAForPattern(*scan),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(NFABuilderTest, PatternTooComplex2) {
  // Equivalent to A{,1000000}, but written in a way so that no quantifier bound
  // individually exceeds 10.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("(((((A{,10}){,10}){,10}){,10}){,10}){,10}"));
  EXPECT_THAT(NFABuilder::BuildNFAForPattern(*scan),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(NFABuilderTest, RepTwoToFiveGreedy) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{2,5}"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N3").On(0), Edge("N6")}),
                        State("N3", {Edge("N4").On(0), Edge("N6")}),
                        State("N4", {Edge("N5").On(0), Edge("N6")}),
                        State("N5", {Edge("N6")}),
                        State("N6", {}),
                    })));
}

TEST_F(NFABuilderTest, RepTwoToFiveReluctant) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{2,5}?"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N6"), Edge("N3").On(0)}),
                        State("N3", {Edge("N6"), Edge("N4").On(0)}),
                        State("N4", {Edge("N6"), Edge("N5").On(0)}),
                        State("N5", {Edge("N6")}),
                        State("N6", {}),
                    })));
}

TEST_F(NFABuilderTest, RepThreeExact) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{3}"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N3").On(0)}),
                        State("N3", {Edge("N4")}),
                        State("N4", {}),
                    })));
}

TEST_F(NFABuilderTest, GreedyStar) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A*"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0), Edge("N2")}),
                        State("N1", {Edge("N1").On(0), Edge("N2")}),
                        State("N2", {}),
                    })));
}
TEST_F(NFABuilderTest, ReluctantStar) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A*?"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N2"), Edge("N1").On(0)}),
                        State("N1", {Edge("N2"), Edge("N1").On(0)}),
                        State("N2", {}),
                    })));
}
TEST_F(NFABuilderTest, GreedyPlus) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A+"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N1").On(0), Edge("N2")}),
                        State("N2", {}),
                    })));
}
TEST_F(NFABuilderTest, ReluctantPlus) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A+?"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2"), Edge("N1").On(0)}),
                        State("N2", {}),
                    })));
}
TEST_F(NFABuilderTest, GreedyTwoOrMore) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{2,}"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N2").On(0), Edge("N3")}),
                        State("N3", {}),
                    })));
}
TEST_F(NFABuilderTest, ReluctantTwoOrMore) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A{2,}?"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N3"), Edge("N2").On(0)}),
                        State("N3", {}),
                    })));
}

TEST_F(NFABuilderTest, NamedQueryParameterInQuantifierBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("A{@lower_bound, @upper_bound}",
                               {.parameters = QueryParametersMap{
                                    {"lower_bound", types::Int64Type()},
                                    {"upper_bound", types::Int64Type()}}}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const NFA> nfa,
      NFABuilder::BuildNFAForPattern(
          *scan,
          [](std::variant<int, absl::string_view> param)
              -> absl::StatusOr<Value> {
            absl::string_view param_name = std::get<absl::string_view>(param);
            if (param_name == "lower_bound") {
              return values::Int64(2);
            } else if (param_name == "upper_bound") {
              return values::Int64(4);
            } else {
              return absl::InvalidArgumentError("Unexpected param name");
            }
          }));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N3").On(0), Edge("N5")}),
                        State("N3", {Edge("N4").On(0), Edge("N5")}),
                        State("N4", {Edge("N5")}),
                        State("N5", {}),
                    })));
}

TEST_F(NFABuilderTest, PositionalQueryParameterInQuantifierBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("A{?, ?}",
                               {.parameters = std::vector<const Type*>{
                                    types::Int64Type(), types::Int64Type()}}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const NFA> nfa,
      NFABuilder::BuildNFAForPattern(
          *scan,
          [](std::variant<int, absl::string_view> param)
              -> absl::StatusOr<Value> {
            int param_position = std::get<int>(param);
            if (param_position == 1) {
              return values::Int64(2);
            } else if (param_position == 2) {
              return values::Int64(4);
            } else {
              return absl::InvalidArgumentError("Unexpected param position");
            }
          }));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N3").On(0), Edge("N5")}),
                        State("N3", {Edge("N4").On(0), Edge("N5")}),
                        State("N4", {Edge("N5")}),
                        State("N5", {}),
                    })));
}

TEST_F(NFABuilderTest, QueryParametersUnavailable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("A{@foo}", {.parameters = QueryParametersMap{
                                               {"foo", types::Int64Type()}}}));

  EXPECT_THAT(NFABuilder::BuildNFAForPattern(*scan),
              StatusIs(absl::StatusCode::kInternal));
}
TEST_F(NFABuilderTest, NullAsQuantifierBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("A{?}", {.parameters = std::vector<const Type*>{
                                            types::Int64Type()}}));

  EXPECT_THAT(
      NFABuilder::BuildNFAForPattern(
          *scan,
          [](std::variant<int, absl::string_view> param)
              -> absl::StatusOr<Value> {
            if (std::get<int>(param) == 1) {
              return values::NullInt64();
            }
            return absl::InvalidArgumentError("Unexpected param position");
          }),
      StatusIs(absl::StatusCode::kOutOfRange));
}
TEST_F(NFABuilderTest, NegativeQuantifierBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("A{?}", {.parameters = std::vector<const Type*>{
                                            types::Int64Type()}}));

  EXPECT_THAT(
      NFABuilder::BuildNFAForPattern(
          *scan,
          [](std::variant<int, absl::string_view> param)
              -> absl::StatusOr<Value> {
            if (std::get<int>(param) == 1) {
              return values::Int64(-1);
            }
            return absl::InvalidArgumentError("Unexpected param position");
          }),
      StatusIs(absl::StatusCode::kOutOfRange));
}
TEST_F(NFABuilderTest, QuantifierLowerBoundBoundGreaterThanUpperBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("A{?, ?}",
                               {.parameters = std::vector<const Type*>{
                                    types::Int64Type(), types::Int64Type()}}));

  EXPECT_THAT(
      NFABuilder::BuildNFAForPattern(
          *scan,
          [](std::variant<int, absl::string_view> param)
              -> absl::StatusOr<Value> {
            if (std::get<int>(param) == 1) {
              return values::Int64(2);
            } else if (std::get<int>(param) == 2) {
              return values::Int64(1);
            }
            return absl::InvalidArgumentError("Unexpected param position");
          }),
      StatusIs(absl::StatusCode::kOutOfRange));
}
TEST_F(NFABuilderTest, Int32QueryParameterAsQuantifierBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("A{?}", {.parameters = std::vector<const Type*>{
                                            types::Int32Type()}}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const NFA> nfa,
      NFABuilder::BuildNFAForPattern(
          *scan,
          [](std::variant<int, absl::string_view> param)
              -> absl::StatusOr<Value> {
            if (std::get<int>(param) == 1) {
              return values::Int64(2);
            } else {
              return absl::InvalidArgumentError("Unexpected param position");
            }
          }));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").On(0)}),
                        State("N2", {Edge("N3")}),
                        State("N3", {}),
                    })));
}

TEST_F(NFABuilderTest, HeadAnchor) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("^A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0).WithHeadAnchor()}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, TailAnchor) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("A$"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").WithTailAnchor()}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, HeadAndTailAnchorSameSpot) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("(^)($)A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {}),
                        State("N1", {}),
                    })));
}

TEST_F(NFABuilderTest, OptionalHeadAnchor) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("(^)?A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, ComplexWithAnchors) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("(^|$)A (^|$|) B ($|^)"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0).WithHeadAnchor()}),
                        State("N1", {Edge("N2").On(1)}),
                        State("N2", {Edge("N3").WithTailAnchor()}),
                        State("N3", {}),
                    })));
}

TEST_F(NFABuilderTest, AnchorInQuantifier) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("(^A B)*"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));

  // Note: Even though the quantifier is "*", the "^" makes it impossible to
  // repeat more than once, so "*" behaves like "?".
  EXPECT_THAT(*nfa,
              EquivToGraph(Graph({
                  State("N0", {Edge("N1").On(0).WithHeadAnchor(), Edge("N3")}),
                  State("N1", {Edge("N2").On(1)}),
                  State("N2", {Edge("N3")}),
                  State("N3", {}),
              })));
}

TEST_F(NFABuilderTest, AlternationOfAnchorsInQuantifier) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("(^|^|$|$)+ A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0).WithHeadAnchor()}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternationOfAnchorsInQuantifier2) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("( () (^|^|$|$))+ A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0).WithHeadAnchor()}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternationOfAnchorsInQuantifier3) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("( () A (^|^|$|$))+"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").WithTailAnchor()}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternationOfAnchorsInNestedPlusQuantifier) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("((^|^|$|$)+)+ A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0).WithHeadAnchor()}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternationOfAnchorsInNestedStarQuantifier) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("((^|^|$|$)+)* A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, SimpleAnchorInNestedStarQuantifier) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("((^)+)* A"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AnchorInNestedStarQuantifier2) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("((^ (A?))+)* B"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(
      *nfa,
      EquivToGraph(Graph({
          State("N0", {Edge("N1").On(0).WithHeadAnchor(), Edge("N2").On(1)}),
          State("N1", {Edge("N2").On(1)}),
          State("N2", {Edge("N3")}),
          State("N3", {}),
      })));
}

TEST_F(NFABuilderTest, AnchorInNestedStarQuantifier3) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("((^ (A??))+?)*? B"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(
      *nfa,
      EquivToGraph(Graph({
          State("N0", {Edge("N1").On(1), Edge("N2").On(0).WithHeadAnchor()}),
          State("N1", {Edge("N3")}),
          State("N2", {Edge("N1").On(1)}),
          State("N3", {}),
      })));
}

TEST_F(NFABuilderTest, AnchorInBoundedQuantifier_SmallBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("B (($){,3})"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AnchorInBoundedQuantifier_LargeBound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("B (($){,2000})"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AnchorInBoundedQuantifier_LargeBound2) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("B (($){2,2000})"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").WithTailAnchor()}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AnchorInBoundedQuantifier_LargeBound3) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("B (($){1500,2000})"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N1").On(0)}),
                        State("N1", {Edge("N2").WithTailAnchor()}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternationHeadAnchorAndQuestion) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("(^)|A?"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N2").WithHeadAnchor(),
                                     Edge("N1").On(0), Edge("N2")}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternationTailAnchorAndQuestion) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("($)|A?"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa, EquivToGraph(Graph({
                        State("N0", {Edge("N2").WithTailAnchor(),
                                     Edge("N1").On(0), Edge("N2")}),
                        State("N1", {Edge("N2")}),
                        State("N2", {}),
                    })));
}

TEST_F(NFABuilderTest, AlternationHeadAndTailAnchorAndQuestion) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const ResolvedMatchRecognizeScan* scan,
                       resolver_.ResolvePattern("^|B$|(A?)"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const NFA> nfa,
                       NFABuilder::BuildNFAForPattern(*scan));
  EXPECT_THAT(*nfa,
              EquivToGraph(Graph({
                  State("N0", {Edge("N3").WithHeadAnchor(), Edge("N1").On(1),
                               Edge("N2").On(0), Edge("N3")}),
                  State("N1", {Edge("N3").WithTailAnchor()}),
                  State("N2", {Edge("N3")}),
                  State("N3", {}),
              })));
}

TEST_F(NFABuilderTest, PatternTooComplexInEpsilonRemover) {
  // This pattern has a reasonable-sized graph pre-epsilon-removal, but the
  // number of edges gets too large during epsilon-removal. Make sure it gets
  // rejected.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMatchRecognizeScan* scan,
      resolver_.ResolvePattern("(((A|B|C|D|E|F|){,50}){,10})"));
  EXPECT_THAT(NFABuilder::BuildNFAForPattern(*scan),
              StatusIs(absl::StatusCode::kOutOfRange));
}

}  // namespace
}  // namespace zetasql::functions::match_recognize
