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

#include "zetasql/common/match_recognize/compiled_nfa.h"

#include <memory>

#include "zetasql/common/match_recognize/edge_matchers.h"
#include "zetasql/common/match_recognize/nfa.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/functions/match_recognize/compiled_pattern.pb.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace zetasql::functions::match_recognize {
namespace {
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Test;
using ::zetasql_base::testing::StatusIs;

class CompiledNFATest : public Test {
 public:
  void SetUp() override {
    start_ = nfa_.NewState();
    s0_ = nfa_.NewState();
    s1_ = nfa_.NewState();
    s2_ = nfa_.NewState();
    s3_ = nfa_.NewState();
    end_ = nfa_.NewState();
    a_var_ = PatternVariableId(0);
    b_var_ = PatternVariableId(1);

    ZETASQL_ASSERT_OK(nfa_.AddEdge(start_, a_var_, s0_));
    ZETASQL_ASSERT_OK(nfa_.AddEdge(s0_, NFAEdge(a_var_, s1_).SetHeadAnchored()));
    ZETASQL_ASSERT_OK(nfa_.AddEdge(s0_, b_var_, s2_));
    ZETASQL_ASSERT_OK(nfa_.AddEdge(s1_, a_var_, s0_));
    ZETASQL_ASSERT_OK(nfa_.AddEdge(s2_, a_var_, s0_));
    ZETASQL_ASSERT_OK(nfa_.AddEdge(s2_, a_var_, s3_));
    ZETASQL_ASSERT_OK(nfa_.AddEdge(s3_, NFAEdge(end_).SetTailAnchored()));
    nfa_.SetAsStartState(start_);
    nfa_.SetAsFinalState(end_);
    ZETASQL_ASSERT_OK(nfa_.Validate(NFA::ValidationMode::kForMatching));
  }

 protected:
  NFA nfa_;
  NFAState start_;
  NFAState s0_;
  NFAState s1_;
  NFAState s2_;
  NFAState s3_;
  NFAState end_;
  PatternVariableId a_var_;
  PatternVariableId b_var_;
};

// Returns a sample NFA that satisfies all required properties of an NFA
// post epsilon-removal.

TEST_F(CompiledNFATest, GetEdge) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<CompiledNFA> compiled_nfa,
                       CompiledNFA::Create(nfa_));

  EXPECT_EQ(compiled_nfa->num_edges(), 7);
  EXPECT_THAT(compiled_nfa->GetEdge(0), IsEdge(0, start_, s0_, a_var_));
  EXPECT_THAT(compiled_nfa->GetEdge(1),
              IsHeadAnchoredEdge(1, s0_, s1_, a_var_));
  EXPECT_THAT(compiled_nfa->GetEdge(2), IsEdge(2, s0_, s2_, b_var_));
  EXPECT_THAT(compiled_nfa->GetEdge(3), IsEdge(3, s1_, s0_, a_var_));
  EXPECT_THAT(compiled_nfa->GetEdge(4), IsEdge(4, s2_, s0_, a_var_));
  EXPECT_THAT(compiled_nfa->GetEdge(5), IsEdge(5, s2_, s3_, a_var_));
  EXPECT_THAT(compiled_nfa->GetEdge(6), IsTailAnchoredEdge(6, s3_, end_));
}

TEST_F(CompiledNFATest, GetAllEdges) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<CompiledNFA> compiled_nfa,
                       CompiledNFA::Create(nfa_));
  EXPECT_THAT(
      compiled_nfa->GetAllEdges(),
      ElementsAre(IsEdge(0, start_, s0_, a_var_),
                  IsHeadAnchoredEdge(1, s0_, s1_, a_var_),
                  IsEdge(2, s0_, s2_, b_var_), IsEdge(3, s1_, s0_, a_var_),
                  IsEdge(4, s2_, s0_, a_var_), IsEdge(5, s2_, s3_, a_var_),
                  IsTailAnchoredEdge(6, s3_, end_)));
}

TEST_F(CompiledNFATest, StartState) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<CompiledNFA> compiled_nfa,
                       CompiledNFA::Create(nfa_));
  EXPECT_EQ(compiled_nfa->start_state(), start_);
}

TEST_F(CompiledNFATest, FinalState) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<CompiledNFA> compiled_nfa,
                       CompiledNFA::Create(nfa_));
  EXPECT_EQ(compiled_nfa->final_state(), end_);
}

TEST_F(CompiledNFATest, NumStates) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<CompiledNFA> compiled_nfa,
                       CompiledNFA::Create(nfa_));
  EXPECT_EQ(compiled_nfa->num_states(), 6);  // start, s0, s1, s2, s3, end.
}

TEST_F(CompiledNFATest, GetEdgesFrom) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<CompiledNFA> compiled_nfa,
                       CompiledNFA::Create(nfa_));

  EXPECT_THAT(compiled_nfa->GetEdgesFrom(start_),
              ElementsAre(IsEdge(0, start_, s0_, a_var_)));
  EXPECT_THAT(compiled_nfa->GetEdgesFrom(s0_),
              ElementsAre(IsHeadAnchoredEdge(1, s0_, s1_, a_var_),
                          IsEdge(2, s0_, s2_, b_var_)));
  EXPECT_THAT(compiled_nfa->GetEdgesFrom(s1_),
              ElementsAre(IsEdge(3, s1_, s0_, a_var_)));
  EXPECT_THAT(
      compiled_nfa->GetEdgesFrom(s2_),
      ElementsAre(IsEdge(4, s2_, s0_, a_var_), IsEdge(5, s2_, s3_, a_var_)));
  EXPECT_THAT(compiled_nfa->GetEdgesFrom(s3_),
              ElementsAre(IsTailAnchoredEdge(6, s3_, end_)));
  EXPECT_THAT(compiled_nfa->GetEdgesFrom(end_), IsEmpty());
}

TEST_F(CompiledNFATest, SerializeRoundTrip) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<CompiledNFA> compiled_nfa,
                       CompiledNFA::Create(nfa_));
  StateMachineProto::CompiledNFAProto proto = compiled_nfa->Serialize();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const CompiledNFA> deserialized,
                       CompiledNFA::Deserialize(proto));

  EXPECT_THAT(
      deserialized->GetAllEdges(),
      ElementsAre(IsEdge(0, start_, s0_, a_var_),
                  IsHeadAnchoredEdge(1, s0_, s1_, a_var_),
                  IsEdge(2, s0_, s2_, b_var_), IsEdge(3, s1_, s0_, a_var_),
                  IsEdge(4, s2_, s0_, a_var_), IsEdge(5, s2_, s3_, a_var_),
                  IsTailAnchoredEdge(6, s3_, end_)));
  EXPECT_EQ(deserialized->start_state(), start_);
  EXPECT_EQ(deserialized->final_state(), end_);
  EXPECT_EQ(deserialized->num_states(), 6);  // start, s0, s1, s2, s3, end.
}

TEST_F(CompiledNFATest, DeserializeInvalidProtoStartState) {
  StateMachineProto::CompiledNFAProto proto;
  proto.add_states();
  proto.set_start_state(1);  // start state out of bounds
  EXPECT_THAT(CompiledNFA::Deserialize(proto),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(CompiledNFATest, DeserializeInvalidProtoTargetStateOfEdge) {
  StateMachineProto::CompiledNFAProto proto;
  proto.add_states()->add_edges()->set_to_state(2);
  proto.add_states();
  proto.set_start_state(0);
  proto.set_final_state(1);
  EXPECT_THAT(CompiledNFA::Deserialize(proto),
              StatusIs(absl::StatusCode::kInternal));
}

}  // namespace
}  // namespace zetasql::functions::match_recognize
