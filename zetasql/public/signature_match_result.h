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

#ifndef ZETASQL_PUBLIC_SIGNATURE_MATCH_RESULT_H_
#define ZETASQL_PUBLIC_SIGNATURE_MATCH_RESULT_H_

#include <map>
#include <string>
#include <utility>

namespace zetasql {

class Type;

// Contains statistics and properties related to matching a function signature
// against a set of arguments.  Includes the number of arguments that
// matched, that did not match, and the total coercion distance for the
// matched arguments (for both literals and non-literals).  Coercion
// distance indicates how close types are together, where two same types
// have distance 0 and smaller distances indicate closer types (i.e.,
// int32_t is closer to int64_t than to double, and the distance between int32_t and
// int64_t is less than the distance between int32_t and double).  Distance is
// computed as the difference between the Type::KindSpecificity() values
// of the types.
class SignatureMatchResult {
 public:
  SignatureMatchResult()
      : non_matched_arguments_(0), non_literals_coerced_(0),
        non_literals_distance_(0), literals_coerced_(0),
        literals_distance_(0) {}
  ~SignatureMatchResult() {}

  int non_matched_arguments() const { return non_matched_arguments_; }
  void set_non_matched_arguments(int non_matched_arguments) {
    non_matched_arguments_ = non_matched_arguments;
  }
  void incr_non_matched_arguments() { non_matched_arguments_++; }

  int non_literals_coerced() const { return non_literals_coerced_; }
  void set_non_literals_coerced(int non_literals_coerced) {
    non_literals_coerced_ = non_literals_coerced;
  }
  void incr_non_literals_coerced() { non_literals_coerced_++; }

  int non_literals_distance() const { return non_literals_distance_; }
  void set_non_literals_distance(int non_literals_distance) {
    non_literals_distance_ = non_literals_distance;
  }
  void incr_non_literals_distance(int distance = 1) {
    non_literals_distance_ += distance;
  }

  int literals_coerced() const { return literals_coerced_; }
  void set_literals_coerced(int literals_coerced) {
    literals_coerced_ = literals_coerced;
  }
  void incr_literals_coerced() { literals_coerced_++; }

  int literals_distance() const { return literals_distance_; }
  void set_literals_distance(int literals_distance) {
    literals_distance_ = literals_distance;
  }
  void incr_literals_distance(int distance = 1) {
    literals_distance_ += distance;
  }

  std::string tvf_bad_call_error_message() const {
    return tvf_bad_call_error_message_;
  }
  void set_tvf_bad_call_error_message(const std::string& message) {
    tvf_bad_call_error_message_ = message;
  }

  int tvf_bad_argument_index() const { return tvf_bad_argument_index_; }
  void set_tvf_bad_argument_index(int index) {
    tvf_bad_argument_index_ = index;
  }

  // This represents a map from each TVF (argument index, column index) pair to
  // the result type to coerce the argument to. For more information, please see
  // the comments for tvf_arg_col_nums_to_coerce_type_.
  typedef std::map<std::pair<int /* argument index */,
                   int /* column index */>,
              const Type* /* coerce-to type */ >
      TVFArgumentsToCoerceTypeMap;

  const TVFArgumentsToCoerceTypeMap& tvf_arg_col_nums_to_coerce_type() const {
    return tvf_arg_col_nums_to_coerce_type_;
  }
  const void tvf_map_arg_col_nums_to_coerce_type(
      int arg_num, int col_num, const Type* coerce_type) {
    tvf_arg_col_nums_to_coerce_type_.emplace(std::make_pair(arg_num, col_num),
                                             coerce_type);
  }

  // Returns whether this result is a better signature match than
  // <other_result>.  Considers in order of preference:
  // 1) The number of non-literal arguments that were coerced.
  // 2) The total coercion distance of non-literal arguments.
  // 3) The total coercion distance of literal arguments.
  bool IsCloserMatchThan(const SignatureMatchResult& other_result) const;

  // Adds the individual results from <other_result> to this.
  void UpdateFromResult(const SignatureMatchResult& other_result);

  std::string DebugString() const;

 private:
  int non_matched_arguments_;  // Number of non-matched arguments for function.
  int non_literals_coerced_;   // Number of non-literal coercions.
  int non_literals_distance_;  // How far non-literals were coerced.
  int literals_coerced_;       // Number of literal coercions.
  int literals_distance_;      // How far non-literals were coerced.

  // The following fields are only relevant for table-valued functions where the
  // function signature contains relation argument(s) with required schema.
  //
  // If the TVF call was invalid, this field stores an error message to return
  // to the user.
  std::string tvf_bad_call_error_message_;

  // If the TVF call was invalid because of a particular argument, this
  // zero-based index is updated to indicate which argument was invalid.
  int tvf_bad_argument_index_ = -1;

  // If the TVF call was valid, this stores type coercions necessary for
  // relation arguments. The key is (argument index, column index) where the
  // argument index indicates which TVF argument contains the relation, and the
  // column index indicates which column within the relation (defined as the
  // offset in the column_list in that input scan). Both are zero-based. The map
  // value is the result type to coerce to.
  TVFArgumentsToCoerceTypeMap tvf_arg_col_nums_to_coerce_type_;

  // Copyable;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_SIGNATURE_MATCH_RESULT_H_
