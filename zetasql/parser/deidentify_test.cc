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

#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {
namespace parser {
namespace {

TEST(Deidentify, SimpleExample) {
  EXPECT_THAT(DeidentifySQLIdentifiersAndLiterals(
                  "SELECT X + 1232, USER_FUNCTION(X) FROM business.financial"),
              zetasql_base::testing::IsOkAndHolds(
                  R"(SELECT
  A + 0,
  B(A)
FROM
  C.D
)"));
}

TEST(Deidentify, BigExample) {
  EXPECT_THAT(
      DeidentifySQLIdentifiersAndLiterals(
          "SELECT "
          "a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,"
          "a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,"
          "a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,"
          "a54,a55,a56,a57,a58,a59,a60,a61,a62,a63,a64,a65,a66,a67,a68,a69,a70,"
          "a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86,a87,"
          "a88,a89,a90,a91,a92,a93,a94,a95,a96,a97,a98,a99,a100"),
      zetasql_base::testing::IsOkAndHolds(
          R"(SELECT
  A,
  B,
  C,
  D,
  E,
  F,
  G,
  H,
  I,
  J,
  K,
  L,
  M,
  N,
  O,
  P,
  Q,
  R,
  S,
  T,
  U,
  V,
  W,
  X,
  Y,
  Z,
  AA,
  AB,
  AC,
  AD,
  AE,
  AF,
  AG,
  AH,
  AI,
  AJ,
  AK,
  AL,
  AM,
  AN,
  AO,
  AP,
  AQ,
  AR,
  AS_,
  AT_,
  AU,
  AV,
  AW,
  AX,
  AY,
  AZ,
  BA,
  BB,
  BC,
  BD,
  BE,
  BF,
  BG,
  BH,
  BI,
  BJ,
  BK,
  BL,
  BM,
  BN,
  BO,
  BP,
  BQ,
  BR,
  BS,
  BT,
  BU,
  BV,
  BW,
  BX,
  BY_,
  BZ,
  CA,
  CB,
  CC,
  CD,
  CE,
  CF,
  CG,
  CH,
  CI,
  CJ,
  CK,
  CL,
  CM,
  CN,
  CO,
  CP,
  CQ,
  CR,
  CS,
  CT,
  CU,
  CV
)"));
}

}  // namespace
}  // namespace parser
}  // namespace zetasql
