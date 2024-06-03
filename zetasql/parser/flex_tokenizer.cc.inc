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

// This file contains C++ code that is to be included into the cc file generated
// by flex. It is inserted near the top of the file outside of any classes or
// functions. Mostly we use this to define macros to customize flex behavior and
// to call from the various rules.

#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/flex_tokenizer.h"  
#include "zetasql/public/parse_location.h"  
#include "absl/strings/escaping.h"  
#include "absl/strings/str_format.h"  
#include "absl/strings/string_view.h"  

using Tokens = zetasql::parser::TokenKinds;

#undef YY_DECL
#define YY_DECL                                                        \
  int zetasql::parser::ZetaSqlFlexTokenizer::GetNextTokenFlexImpl( \
      ParseLocationRange* location)

// This action is executed for every token that is matched, before the defined
// actions are executed. We use this to keep location up to date. We abuse the
// column value and treat it as a byte offset.
#define YY_USER_ACTION                                    \
  location->set_start(ParseLocationPoint::FromByteOffset( \
      filename_, location->end().GetByteOffset()));       \
  location->set_end(ParseLocationPoint::FromByteOffset(   \
      filename_, location->end().GetByteOffset() + yyleng));

// This action is executed once before the first token is matched. We use this
// to initialize location.
#define YY_USER_INIT                                      \
  location->mutable_start().SetByteOffset(start_offset_); \
  location->mutable_end().SetByteOffset(start_offset_);

// Called to produce an error when a string-like literal is not closed.
#define UNCLOSED(unclosed_kind)                                           \
  do {                                                                    \
    SetOverrideError(*location, "Syntax error: Unclosed " unclosed_kind); \
    yyterminate();                                                        \
  } while (0)

// As above, but a short-hand for triple-quoted literals.
#define UNCLOSED_3(unclosed_kind) UNCLOSED("triple-quoted " unclosed_kind)

// Called from the catchall rule to produce an error when an unexpected char
// is seen.
#define ILLEGAL_INPUT_CHAR                                               \
  do {                                                                   \
    SetOverrideError(                                                    \
        *location,                                                       \
        absl::StrFormat(R"(Syntax error: Illegal input character "%s")", \
                        absl::CEscape(absl::string_view(YYText(), 1)))); \
    yyterminate();                                                       \
  } while (0)
