//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/base/general_trie.h"

#include <iostream>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "gtest/gtest.h"

namespace zetasql_base {
namespace {

class TestIntTraverser : public GeneralTrie<int, 0>::Traverser {
 public:
  explicit TestIntTraverser(std::list<int> *expected)
    : expected_(expected) {
  }

  virtual void Process(const std::string& s, const int& data) {
    CHECK_GT(expected_->size(), 0);
    std::list<int>::iterator it = expected_->begin();
    CHECK_EQ(*it, data);
    expected_->erase(it);
  }

 private:
  std::list<int> *expected_;
};

TEST(GeneralTrie, Basic) {
  GeneralTrie<int, 0> trie;
  const GeneralTrie<int, 0>& const_trie = trie;

  trie.Insert("one", 1);
  trie.Insert("two", 2);
  trie.Insert("three", 3);
  trie.Insert("oneflew", 4);
  trie.Insert("oneflewover", 5);
  trie.Insert("oneflewoverthe", 6);
  trie.Insert("twoiscompany", 7);

  // Make sure trie traversal works as expected.
  std::list<int> expected;
  expected.push_back(1);
  expected.push_back(4);
  expected.push_back(5);
  expected.push_back(6);
  expected.push_back(3);
  expected.push_back(2);
  expected.push_back(7);
  TestIntTraverser *traverser = new TestIntTraverser(&expected);
  trie.PreorderTraverse(traverser);
  CHECK_EQ(expected.size(), 0);
  delete traverser;

  std::list<int> postorder_expected = {6, 5, 4, 1, 3, 7, 2};
  traverser = new TestIntTraverser(&postorder_expected);
  trie.PostorderTraverse(traverser);
  CHECK_EQ(postorder_expected.size(), 0);
  delete traverser;

  expected.push_back(4);
  expected.push_back(5);
  expected.push_back(6);
  traverser = new TestIntTraverser(&expected);
  trie.PreorderTraverseAllMatchingStrings("onefl", strlen("onefl"), traverser);
  CHECK_EQ(expected.size(), 0);
  delete traverser;

  postorder_expected.push_back(6);
  postorder_expected.push_back(5);
  postorder_expected.push_back(4);
  traverser = new TestIntTraverser(&postorder_expected);
  trie.PostorderTraverseAllMatchingStrings("onefl", strlen("onefl"), traverser);
  CHECK_EQ(postorder_expected.size(), 0);
  delete traverser;

  CHECK_EQ(trie.GetData("one"), 1);
  CHECK_EQ(trie.GetData("two"), 2);
  CHECK_EQ(trie.GetData("three"), 3);
  CHECK_EQ(trie.GetData("oneflew"), 4);
  CHECK_EQ(trie.GetData("oneflewover"), 5);
  CHECK_EQ(trie.GetData("oneflewoverthe"), 6);
  CHECK_EQ(trie.GetData("twoiscompany"), 7);
  // test non existent entry
  CHECK_EQ(trie.GetData("foo"), 0);

  // Test GetData on const trie.
  CHECK_EQ(const_trie.GetData("one"), 1);
  CHECK_EQ(const_trie.GetData("foo"), 0);

  int& one_data = trie.GetData("one");
  one_data--;
  CHECK_EQ(trie.GetData("one"), 0);
  one_data++;

  int chars_matched = -1;
  CHECK_EQ(trie.GetDataForMaximalPrefix("one", &chars_matched, nullptr), 1);
  CHECK_EQ(chars_matched, 3);
  bool is_terminators[256];
  is_terminators['b'] = true;
  CHECK_EQ(
      trie.GetDataForMaximalPrefix("oneby", &chars_matched, is_terminators),
      1);
  CHECK_EQ(chars_matched, 3);
  std::vector<std::pair<std::string, int> > outdata;
  trie.GetAllMatchingStrings("onefl", strlen("onefl"), &outdata);
  CHECK_EQ(outdata.size(), 3);
  CHECK_EQ(outdata[0].first, "oneflew");
  CHECK_EQ(outdata[0].second, 4);
  CHECK_EQ(outdata[1].first, "oneflewover");
  CHECK_EQ(outdata[1].second, 5);
  CHECK_EQ(outdata[2].first, "oneflewoverthe");
  CHECK_EQ(outdata[2].second, 6);

  trie.GetAllMatchingStrings("two", strlen("two"), &outdata);
  CHECK_EQ(outdata.size(), 2);
  CHECK_EQ(outdata[0].first, "two");
  CHECK_EQ(outdata[0].second, 2);
  CHECK_EQ(outdata[1].first, "twoiscompany");
  CHECK_EQ(outdata[1].second, 7);

  trie.GetAllMatchingStrings("three", strlen("three"), &outdata);
  CHECK_EQ(outdata.size(), 1);
  CHECK_EQ(outdata[0].first, "three");
  CHECK_EQ(outdata[0].second, 3);

  trie.GetAllMatchingStrings("foo", strlen("foo"), &outdata);
  CHECK_EQ(outdata.size(), 0);

  trie.SetData("two", -2);
  trie.SetData("twoiscompany", -7);
  // test non-existent entry
  CHECK(!trie.SetData("foo", 1));

  CHECK_EQ(trie.GetData("two"), -2);
  CHECK_EQ(trie.GetData("twoiscompany"), -7);
  trie.GetAllMatchingStrings("two", strlen("two"), &outdata);
  CHECK_EQ(outdata.size(), 2);
  CHECK_EQ(outdata[0].first, "two");
  CHECK_EQ(outdata[0].second, -2);
  CHECK_EQ(outdata[1].first, "twoiscompany");
  CHECK_EQ(outdata[1].second, -7);
}

TEST(GeneralTrie, TraverseIterator) {
  GeneralTrie<int, 0> trie;
  GeneralTrie<int, 0>::TraverseIterator iter = trie.Traverse();
  CHECK(iter.Done());

  trie.Insert("one", 1);
  trie.Insert("two", 2);
  trie.Insert("three", 3);
  trie.Insert("oneflew", 4);
  trie.Insert("oneflewover", 5);
  trie.Insert("oneflewoverthe", 6);
  trie.Insert("twoiscompany", 7);

  std::vector<std::pair<std::string, int> > expected;
  expected.push_back(std::make_pair("one", 1));
  expected.push_back(std::make_pair("oneflew", 4));
  expected.push_back(std::make_pair("oneflewover", 5));
  expected.push_back(std::make_pair("oneflewoverthe", 6));
  expected.push_back(std::make_pair("three", 3));
  expected.push_back(std::make_pair("two", 2));
  expected.push_back(std::make_pair("twoiscompany", 7));

  int i = 0;
  for (GeneralTrie<int, 0>::TraverseIterator iter = trie.Traverse();
       !iter.Done(); iter.Next()) {
    CHECK_EQ(expected[i].first, iter.Key());
    CHECK_EQ(expected[i].second, iter.Value());
    i++;
  }
  CHECK_EQ(7, i);

  // Test a trie that has data at the root.
  GeneralTrie<int, 0> root_data_trie;
  root_data_trie.Insert("", 1234);

  iter = root_data_trie.Traverse();
  CHECK(!iter.Done());
  CHECK_EQ("", iter.Key());
  CHECK_EQ(1234, iter.Value());
  iter.Next();
  CHECK(iter.Done());

  // Insert another item, and iterate through again.
  root_data_trie.Insert("a", 5678);
  iter = root_data_trie.Traverse();
  CHECK(!iter.Done());
  CHECK_EQ("", iter.Key());
  CHECK_EQ(1234, iter.Value());
  iter.Next();
  CHECK(!iter.Done());
  CHECK_EQ("a", iter.Key());
  CHECK_EQ(5678, iter.Value());
  iter.Next();
  CHECK(iter.Done());
}

//
// Test the legacy idiom used to have class type values in GeneralTrie. This
// idiom goes against our coding guidelines because it demands a global
// variable of class type, and new code should use ClassGeneralTrie instead.
//
// Notice that initialization is as complex as it looks like. Any of the
// following changes will result in code that does not compile:
// * declare kNullValueClass outside an enclosing class
// * make kNullValueClass const
// * pass a null pointer as the second argument for the typedef
// * pass static_cast<DummyValueClass*>(nullptr) as the second argument for the
// typedef (http://gcc.gnu.org/bugzilla/show_bug.cgi?id=10541)
class DummyValueClass {
 public:
};

class DummyAppClass {
 public:
  static DummyValueClass kNullValueClass;
  typedef GeneralTrie<DummyValueClass*, &kNullValueClass> ValueClassTrie;
};
DummyValueClass DummyAppClass::kNullValueClass = DummyValueClass();

TEST(GeneralTrie, LegacyGeneralTrieWithClassValue) {
  DummyAppClass::ValueClassTrie trie_instance;
}

}  // namespace
}  // namespace zetasql_base
