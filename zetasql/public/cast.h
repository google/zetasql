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

#ifndef ZETASQL_PUBLIC_CAST_H_
#define ZETASQL_PUBLIC_CAST_H_

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "zetasql/base/statusor.h"

// The full specification for ZetaSQL casting and coercion is at:
//   (broken link)

namespace zetasql {

class LanguageOptions;

// Identifies the conditions where casting/coercion from type <A> to type <B>
// is valid.  EXPLICIT means coercion can only be performed if explicitly
// present in the SQL query, i.e. CAST(column AS INT32).
// EXPLICIT_OR_LITERAL_OR_PARAMETER allows explicit casts, implicit coercion
// of parameters, and implicit literal coercion if it can be done successfully
// (for example, coercing an INT64 to INT32 without overflow).
// EXPLICIT_OR_LITERAL allows explicit casts and implicit literal coercion.
// IMPLICIT means type <A> can be implicitly coerced to type <B> when
// required, and also implies explicit casting is allowed.
enum class CastFunctionType {
  IMPLICIT = 1,  // Implies EXPLICIT.
  EXPLICIT_OR_LITERAL_OR_PARAMETER = 2,
  EXPLICIT_OR_LITERAL = 3,
  EXPLICIT = 4,
};

// Helper functions identifying if implicit, explicit, literal, or
// parameter coercion is supported.
bool SupportsImplicitCoercion(CastFunctionType type);
bool SupportsLiteralCoercion(CastFunctionType type);
bool SupportsParameterCoercion(CastFunctionType type);
bool SupportsExplicitCast(CastFunctionType type);

// Properties associated with casting from one type to another.
// A CastFunctionProperty is associated with a TypeKindPair in the CastHashMap
// defined below.
struct CastFunctionProperty {
  CastFunctionProperty(CastFunctionType cast_type, int coercion_cost)
      : type(cast_type), cost(coercion_cost) {}
  CastFunctionType type;
  int cost;  // Cost to cast from one type to another.
};

using CastHashMap = absl::flat_hash_map<TypeKindPair, CastFunctionProperty,
                                        TypeKindPairHasher>;

// Returns a hash map with TypeKindPair as key, and CastFunctionProperty as
// value.  This identifies whether the (from, to) cast pairs in the key are
// allowed explicitly, implicitly, etc., and what the cost is for the cast.
// If a (from, to) pair is not in the map, then that cast is not allowed.
const CastHashMap& GetZetaSQLCasts();

// Returns <from_value> casted to <to_type>. Returns an error if the types are
// incompatible or the value cannot be cast successfully to <to_type>.
//
// Successful casting depends on the input value, which fails if it violates
// the range bounds of <to_type> (e.g. casting a large INT64 value to
// INT32), the result value is invalid for the target domain (e.g. casting
// a non-UTF8 BYTES value to STRING), etc.
//
// A Value always successfully casts to the same exact type (even for
// non-simple types).
//
// Casting float/double to integer rounds the result.
//
// Casting finer-granularity timestamps truncate to coarser granularity types
// (i.e. milliseconds to seconds).
//
// When casting between PROTO and BYTES Types, no validation is performed when
// producing the coerced Value.  In the ZetaSQL Value system, a Proto is an
// uninterpreted descriptor/bytes pair.  This cast behavior is not dictated
// by ZetaSQL, and conforming implementations are allowed to perform some
// validation.
//
// Casting behavior between std::string/date and timestamp uses a time zone for
// the conversion, defined as follows:
//   1) Timestamp to std::string/date
//      The timestamp is interpreted as of the <default_timezone>
//      when producing the std::string/date.
//   2) String to Timestamp
//      If the timezone is present in the std::string then it is used, otherwise
//      the <default_timezone> is used.
//   3) Date to Timestamp
//      The date is interpreted as of midnight in the <default_timezone>.
zetasql_base::StatusOr<Value> CastValue(const Value& from_value,
                                absl::TimeZone default_timezone,
                                const LanguageOptions& language_options,
                                const Type* to_type);
// DEPRECATED name for CastValue()
inline zetasql_base::StatusOr<Value> CastStatusOrValue(
    const Value& from_value, absl::TimeZone default_timezone,
    const LanguageOptions& language_options, const Type* to_type) {
  return CastValue(from_value, default_timezone, language_options, to_type);
}

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_CAST_H_
