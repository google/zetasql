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

#ifndef ZETASQL_PARSER_TOKEN_WITH_LOCATION_H_
#define ZETASQL_PARSER_TOKEN_WITH_LOCATION_H_

#include "zetasql/parser/tm_token.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// Represents a stack trace for the macro expansions.
// The stack is used to provide more context for errors.
// `name` is the name of the invocation. It can be a macro/argument invocation
// or an argument expansion. `location` is the location of the macro invocation.
//
// NOTE : All the string_view defined in this struct are allocated in the arena
// and will be valid for the lifetime of the arena.
// The arena should outlive the stack frame.
//
// Keep this cheap to copy.
struct StackFrame {
  enum class FrameType {
    kMacroInvocation,  // The macro invocation. For example, "$m" in $m(abc).
    kMacroArg,         // The macro argument. For example, "abc" in $m("abc").
    kArgRef,  // The reference of the macro argument. For example, $1 in
              // DEFINE MACRO m $1, pqr;
  };
  absl::string_view name;
  FrameType frame_type;
  ParseLocationRange location;

  // The input text and offsets in the original input from which this frame is
  // created.
  absl::string_view input_text;
  int offset_in_original_input;   // Byte offset in the original input.
  int input_start_line_offset;    // 1-based line number for the original input.
  int input_start_column_offset;  // 1-based column number for the original
                                  // input.

  StackFrame* /*absl_nullable*/ parent;

  // The invocation frame where this frame is produced from.
  // This is only set for kMacroArg. For example, $m(abc) arg:$1 is kMacroArg,
  // and $m is the invocation frame. Parent frame can be different from the
  // invocation frame.
  // For example,
  //
  // DEFINE MACRO m1 $1,
  // DEFINE MACRO m2 $m1($1);
  // $m2(abc)
  //
  // For above example, expansion for ABC will look like this:
  // ABC         :   Representing token definition
  // |
  // |
  // Arg:$1(m2)  :   Argument is created for first time in m2.
  // |
  // |
  // $1(ArgRef)  :   Above chain is getting used in DEFINE MACRO m2 $m1($1)
  // |
  // |
  // Arg:$1(m2)  :   Argument is created for time in m1.
  // |
  // |
  // $1(ArgRef)  :   Above chain is getting used in DEFINE MACRO m1 $1
  // |
  // |
  // $m1()
  // |
  // |
  // $m2()
  //
  // Here, In above example, For Arg:$1(m2), its parent will be $1(ArgRef)
  // and invocation frame will be $m2().

  StackFrame* /*absl_nullable*/ invocation_frame;

  // Returns the location range without the offset.
  ParseLocationRange LocationRangeWithoutStartOffset() const {
    return ParseLocationRange(
        ParseLocationPoint::FromByteOffset(
            location.start().filename(),
            location.start().GetByteOffset() - offset_in_original_input),
        ParseLocationPoint::FromByteOffset(
            location.end().filename(),
            location.end().GetByteOffset() - offset_in_original_input));
  }

  // Returns the invocation string for this stack from the original input.
  absl::string_view GetInvocationString() const {
    return this->LocationRangeWithoutStartOffset().GetTextFrom(input_text);
  }
};

// Represents one token in the unexpanded input stream. 'Kind' is the lexical
// token kind, e.g. STRING_LITERAL, IDENTIFIER, KW_SELECT.
// Offsets and 'text' refer to the token *without* any whitespaces.
// Preceding whitespaces are stored in their own string_view; they
// are used when serializing the end result to preserve the original user
// spacing.
struct TokenWithLocation {
  Token kind;
  ParseLocationRange location;
  absl::string_view text;
  absl::string_view preceding_whitespaces;

  // TEMPORARY FIELD: Please do not use as it will be removed shortly.
  //
  // Location offsets must be valid for the source they refer to.
  // Currently, the parser & analyzer only have the unexpanded source, so we
  // use the unexpanded offset.
  // In the future, the resolver should show the expanded location and where
  // it was expanded from. The expander would have the full location map and
  // the sources of macro definitions as well, so we would not need this
  // adjustment nor the `topmost_invocation_location` at all, since the
  // expander will be able to provide the stack. All layers, however, will need
  // to ask for that mapping.
  ParseLocationRange topmost_invocation_location;

  int start_offset() const { return location.start().GetByteOffset(); }
  int end_offset() const { return location.end().GetByteOffset(); }

  // Returns whether `other` and `this` are adjacent tokens (no spaces in
  // between) and `this` precedes `other`.
  //
  // If the location of either token is invalid, i.e. with one end smaller than
  // 0, the function call returns false.
  bool AdjacentlyPrecedes(const TokenWithLocation& other) const;

  // The stack frame from which this token was expanded.
  // This field will be null for tokens that were not expanded from a macro or
  // macro args.
  // StackFrame has its own `location`, which is the location of the macro
  // invocation. which is different from the `location` of the token.
  StackFrame* stack_frame = nullptr;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_TOKEN_WITH_LOCATION_H_
