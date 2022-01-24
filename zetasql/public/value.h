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

#ifndef ZETASQL_PUBLIC_VALUE_H_
#define ZETASQL_PUBLIC_VALUE_H_

#include <stddef.h>

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/public/value_content.h"
#include "absl/base/attributes.h"
#include <cstdint>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"

namespace zetasql {

// Represents a value in the ZetaSQL type system. Each valid value has a
// type. Values are copied by value. Struct and array values have refcounted
// contents so they can be copied cheaply. A value has type-specific methods
// that are applicable only to struct or array values.
//
// Array values carry a boolean indicating whether they preserve order or ignore
// order. The latter is used whenever the order of array elements is partially
// specified (e.g., after sorting with duplicate keys). A given query might
// produce arrays whose order is fully or partially specified depending on the
// input data. In the reference implementation, we track order preservation of
// arrays and compare arrays as multisets whenever their order is not fully
// specified. Tracking order preservation is not required otherwise; in the
// ZetaSQL data model, all arrays are totally ordered.
//
// Value's content is managed by corresponding Type through CopyValueContent and
// ClearValueContent functions. Value calls these functions during value copying
// and destruction. ValueContent class is used as a container to transfer
// content from Type to Value. A Value has a content if and only if this Value
// is non-null. Value's content can be up to 12 bytes in size. First 8 bytes are
// accessible to any type, while the remaining 4 can be used only by simple
// types.
//
// Caveat: since a Value contains a reference to a Type, referenced Type should
// outlive the Value. While simple built-in types are static, lifetime of
// parameterized types (e.g. proto, struct or enum) depends on a lifetime of the
// TypeFactory that created them and it's user's responsibility to make sure
// that a Value is released strictly before a Type that this Value references.
// Please check comments for zetasql::TypeFactory and
// zetasql::TypeFactoryOptions classes for the details on type's lifetime. The
// same way, user is responsible for making sure that DescriptorPool that
// provided proto descriptors for proto and enum types outlived Value belonging
// to these types.
//
// Thread safety
// -------------
// Value has the same thread-safety properties as many other types like
// string, vector<>, int, etc -- it is thread-compatible as defined by
// (broken link). In particular, if no thread may call a non-const
// method, then it is safe to concurrently call const methods. Copying a Value
// produces a new instance that can be used concurrently with the original in
// arbitrary ways.
//
class Value {
 public:
  // Constructs an invalid value. Needed for using values with STL. All methods
  // other than is_valid() will crash if called on invalid values.
  #ifndef SWIG
  // SWIG has trouble with constexpr.
  constexpr
  #endif
  Value();
  Value(const Value& that);
  const Value& operator=(const Value& that);
#ifndef SWIG
  Value(Value&& that) noexcept;  // NOLINT(build/c++11)
  Value& operator=(Value&& that) noexcept;  // NOLINT(build/c++11)
#endif
  ~Value();

  // Returns the type of the value.
  const Type* type() const;

  // Returns the type kind of the value. Same as type()->type_kind() but in some
  // cases can be a bit more efficient.
  TypeKind type_kind() const;

  // Returns the estimated size of the in-memory C++ representation of this
  // value.
  uint64_t physical_byte_size() const;

  // Returns true if the value is null.
  bool is_null() const;

  // Returns true if the value is an empty array.
  bool is_empty_array() const;

  // Returns true if the value is valid (invalid values are created by the
  // default constructor).
  bool is_valid() const;

  // Returns true if the Value is valid and non-null.
  bool has_content() const;

  // Accessors for accessing the data within atomic typed Values.
  // REQUIRES: !is_null().
  int32_t int32_value() const;         // REQUIRES: int32_t type
  int64_t int64_value() const;         // REQUIRES: int64_t type
  uint32_t uint32_value() const;       // REQUIRES: uint32_t type
  uint64_t uint64_value() const;       // REQUIRES: uint64_t type
  bool bool_value() const;             // REQUIRES: bool type
  float float_value() const;           // REQUIRES: float type
  double double_value() const;         // REQUIRES: double type
  const std::string& string_value() const;  // REQUIRES: string type
  const std::string& bytes_value() const;   // REQUIRES: bytes type
  int32_t date_value() const;               // REQUIRES: date type
  int32_t enum_value() const;               // REQUIRES: enum type
  const std::string& enum_name() const;  // REQUIRES: enum type

  // Returns timestamp value as absl::Time at nanoseconds precision.
  absl::Time ToTime() const;  // REQUIRES: timestamp type

  // Returns timestamp value as Unix epoch microseconds.
  int64_t ToUnixMicros() const;  // REQUIRES: timestamp type.
  // Returns timestamp value as Unix epoch nanoseconds or an error if the value
  // does not fit into an int64_t.
  absl::Status ToUnixNanos(int64_t* nanos) const;  // REQUIRES: timestamp type.

  // Returns time and datetime values at micros precision as bitwise encoded
  // int64_t, see public/civil_time.h for the encoding.
  int64_t ToPacked64TimeMicros() const;      // REQUIRES: time type
  int64_t ToPacked64DatetimeMicros() const;  // REQUIRES: datetime type

  TimeValue time_value() const;                 // REQUIRES: time type
  DatetimeValue datetime_value() const;         // REQUIRES: datetime type
  const IntervalValue& interval_value() const;  // REQUIRES: interval type

  // REQUIRES: numeric type
  const NumericValue& numeric_value() const;

  // REQUIRES: bignumeric type
  const BigNumericValue& bignumeric_value() const;

  // Checks whether the value belongs to the JSON type, non-NULL and is in
  // validated representation. JSON values can be in one of the two
  // representations:
  //  1) Validated: JSON is parsed, validated and transformed into an efficient
  //    (for field read/updates) in-memory representation (field tree) that can
  //    be accessed through json_value() method. ZetaSQL Analyzer uses this
  //    representation by default to represent JSON values (e.g. literals).
  //  2) Unparsed: string that was not validated and thus potentially can be
  //    an invalid JSON. ZetaSQL Analyzer uses this representation when
  //    LanguageFeature FEATURE_JSON_NO_VALIDATION is enabled.
  bool is_validated_json() const;
  // Returns true if the value belongs to the JSON type, non-null and is in
  // unparsed representation. See comments to is_validated_json() for more
  // details.
  bool is_unparsed_json() const;
  // REQUIRES: json type
  // REQUIRES: is_unparsed_json()
  // Returns the JSON representation in the unparsed mode.
  const std::string& json_value_unparsed() const;
  // REQUIRES: json type
  // REQUIRES: is_validated_json()
  // Returns the JSON value in the validated mode.
  JSONValueConstRef json_value() const;
  // REQUIRES: json type
  // Returns the string representing stored JSON value.
  std::string json_string() const;

  // Returns the value content of extended type.
  // REQUIRES: type_kind() == TYPE_EXTENDED
  ValueContent extended_value() const;

  // Generic accessor for numeric PODs.
  // REQUIRES: T is one of int32_t, int64_t, uint32_t, uint64_t, bool, float, double,
  // NumericValue, BigNumericValue, IntervalValue
  // T must match exactly the type_kind() of this value.
  template <typename T> inline T Get() const;

  // Accessors that coerce the data to the requested C++ type.
  // REQUIRES: !is_null().
  // Use of this method for timestamp_ values is DEPRECATED.
  int64_t ToInt64() const;    // For bool, int_, uint32_t, date, enum
  uint64_t ToUint64() const;  // For bool, uint32_t, uint64_t
  // Use of this method for timestamp_ values is DEPRECATED.
  double ToDouble() const;  // For bool, int_, date, timestamp_, enum types.
  absl::Cord ToCord() const;  // For string, bytes, and protos

  // Convert this value to a dynamically allocated proto Message.
  //
  // If 'return_null_on_error' is false, this does a best-effort conversion of
  // the bytes to the Message type, and ignores errors during parsing, and does
  // not check that the constructed Message is valid. If 'return_null_on_error'
  // is true, returns null if there is a parse error (missing required fields
  // is still not considered an error).
  //
  // REQUIRES: !is_null() && type()->IsProto()
  // Caller owns the returned object, which cannot outlive <message_factory>.
  //
  // Note: If all you want to do is to convert a proto Value into a c++ proto
  // with a known type, consider using ToCord() and ParseFromCord() as follows:
  //   MyMessage pb;
  //   ...
  //   if (bool parse_ok = pb.ParseFromCord(value.ToCord()); !parse_ok) {
  //     /* Handle error */
  //   }
  // This simpler pattern avoids passing in a message factory and avoids the
  // ownership issues associated with the returned Message.
  google::protobuf::Message* ToMessage(google::protobuf::DynamicMessageFactory* message_factory,
                             bool return_null_on_error = false) const;

  // Struct-specific methods. REQUIRES: !is_null().
  int num_fields() const;
  const Value& field(int i) const;
  const std::vector<Value>& fields() const;
  // Returns the value of the first field with the given 'name'. If one doesn't
  // exist, returns an invalid value.
  // Does not find anonymous fields (those with empty names).
  const Value& FindFieldByName(absl::string_view name) const;

  // Array-specific methods. REQUIRES: !is_null().
  bool empty() const;
  int num_elements() const;
  const Value& element(int i) const;
  const std::vector<Value>& elements() const;

  // Returns true if 'this' equals 'that' or both are null. This is *not* SQL
  // equality which returns null when either value is null. Returns false if
  // 'this' and 'that' have different types. For floating point values, returns
  // 'true' if both values are NaN of the same type.
  // For protos, returns true if the protos have the equivalent descriptors
  // (using Type::Equivalent) and the values are equivalent according to
  // MessageDifferencer::Equals, using the descriptor from 'this'.
  bool Equals(const Value& that) const;

  // Returns the BOOL (possibly NULL) Value of the SQL expression (*this =
  // that). Handles corner cases involving NULLs and NaNs. Returns an invalid
  // Value if the value types are not directly comparable (without an implicit
  // or explicit cast). (For example, INT64 and UINT64 are directly comparable,
  // but INT64 and INT32 are not comparable without a widening cast.) To be
  // safe, it is best to only use this method with Types that appear as
  // arguments to a call to the $equals function in a resolved AST generated by
  // the resolver.
  Value SqlEquals(const Value& that) const;

  // When the types of 'this' and 'that' are compatible, this function returns
  // true when 'this' is smaller than 'that'. When the types of 'this' and
  // 'that' are not compatible, the behavior is undefined.
  //
  // For simple types, this uses '<' operator on native c++ types to compare the
  // values. A null value is less than any non-null value. This is
  // *not* SQL inequality. For floating point values, NaN sorts smaller than
  // any other value including negative infinity.
  // For struct types, this compares the fields of the STRUCT pairwise in
  // ordinal order, returns true as soon as LessThan returns true for a field.
  // For array types, this compares the arrays in lexicographical order.
  bool LessThan(const Value& that) const;

  // Returns the BOOL (possibly NULL) Value of the SQL expression (*this <
  // that). Handles corner cases involving NULLs, NaNs. Returns an invalid Value
  // if the value types are not directly comparable (without an implicit or
  // explicit cast). (For example, INT64 and UINT64 are directly comparable, but
  // INT64 and INT32 are not comparable without a widening cast.) To be safe, it
  // is best to only use this method with Types that appear as arguments to a
  // call to a comparison function ($equals, $less, $less_or_equals, $greater,
  // or $greater_or_equals) in a resolved AST generated by the resolver.
  Value SqlLessThan(const Value& that) const;

  // Returns the hash code of a value.
  //
  // Result is not guaranteed stable across different runs of a program. It is
  // not cryptographically secure. It should not be used for distributed
  // coordination, security or storage which requires a stable computation.
  //
  // For more background see https://abseil.io/docs/cpp/guides/hash.
  //
  size_t HashCode() const;

  template <typename H>
  friend H AbslHashValue(H h, const Value& v);

  // Returns printable string for this Value.
  // Verbose DebugStrings include the type name.
  std::string ShortDebugString() const { return DebugString(false); }
  std::string FullDebugString() const { return DebugString(true); }
  std::string DebugString(bool verbose = false) const;

  // Returns a pretty-printed (e.g. wrapped) string for the value.
  // Suitable for printing in golden-tests and documentation.
  std::string Format() const;

  // Returns a SQL expression that produces this value.
  // This is not necessarily a literal since we don't have literal syntax
  // for all values.
  // This assumes that any types used in Value can be resolved using the name
  // returned from type->TypeName().  Returned type names are sensitive to
  // the SQL ProductMode (INTERNAL or EXTERNAL).
  //
  // Note: Arguably, GetSQL() and GetSQLLiteral() don't work quite right for
  // STRUCTs.  In particular, they do not preserve field names in the result
  // string.  For example, if you have a STRUCT value like
  // STRUCT<a INT64, b STRING>(1, 'a'), and call GetSQL(), the result will
  // be "(1, 'a')".  If we're only interested in the value itself and not the
  // original type (with named fields) then maybe that's ok.  Note that
  // GetSQLLiteral() is used in ZetaSQL's FORMAT() function implementation
  // (Format() in zetasql/public_functions/format.cc) so we cannot change
  // the output without breaking existing ZetaSQL function semantics.
  std::string GetSQL(ProductMode mode = PRODUCT_EXTERNAL) const;

  // Returns a SQL expression that is compatible as a literal for this value.
  // This won't include CASTs except for non-finite floating point values, and
  // won't necessarily produce the exact same type when parsed on its own, but
  // it should be the closest SQL literal form for this value.  Returned type
  // names are sensitive to the SQL ProductMode (INTERNAL or EXTERNAL).
  std::string GetSQLLiteral(ProductMode mode = PRODUCT_EXTERNAL) const;

  // We do not define < operator to prevent accidental use of values of mixed
  // types in STL set and map.
  bool operator==(const Value& that) const { return Equals(that); }
  bool operator!=(const Value& that) const { return !Equals(that); }

  // Factory methods to create atomic non-null values.
  static Value Int32(int32_t v);
  static Value Int64(int64_t v);
  static Value Uint32(uint32_t v);
  static Value Uint64(uint64_t v);
  static Value Bool(bool v);
  static Value Float(float v);
  static Value Double(double v);
  // Unfortunately using the function name String causes issues for
  // cs/zetasql::ValueConstructor constructor which takes string-like objects.
  // Therefore using the name StringValue is a pragmatic way around this issue.
  static Value StringValue(std::string v);
  static Value String(absl::string_view v);
  static Value String(const absl::Cord& v);
  // str may contain '\0' in the middle, without getting truncated.
  template <size_t N> static Value String(const char (&str)[N]);
  static Value Bytes(std::string v);
  static Value Bytes(absl::string_view v);
  static Value Bytes(const absl::Cord& v);
  // str may contain '\0' in the middle, without getting truncated.
  template <size_t N> static Value Bytes(const char (&str)[N]);
  // Create a date value. 'v' is the number of days since unix epoch 1970-1-1
  static Value Date(int32_t v);
  // Creates a timestamp value from absl::Time at nanoseconds precision.
  static Value Timestamp(absl::Time t);
  // Creates a timestamp value from Unix micros.
  static Value TimestampFromUnixMicros(int64_t v);

  static Value Time(TimeValue time);
  static Value Datetime(DatetimeValue datetime);

  // Creates a Value from an bitwise encoded int64_t at micros precision.
  // see public/civil_time.h for the encoding.
  static Value TimeFromPacked64Micros(int64_t v);
  static Value DatetimeFromPacked64Micros(int64_t v);

  static Value Interval(IntervalValue interval);

  static Value Numeric(NumericValue v);

  static Value BigNumeric(BigNumericValue v);

  // Creates a Value storing JSON document in as a plain string without doing
  // any validation.
  static Value UnvalidatedJsonString(std::string v);

  // Creates a Value that stores parsed JSON document using representation
  // optimized for member access operations.
  static Value Json(JSONValue value);

  // Creates a value of extended type with the given content.
  static Value Extended(const ExtendedType* type, const ValueContent& value);

  // Generic factory for numeric PODs.
  // REQUIRES: T is one of int32_t, int64_t, uint32_t, uint64_t, bool, float, double,
  // NumericValue, BigNumericValue, IntervalValue
  template <typename T>
  inline static Value Make(T value) {
    if constexpr (std::is_same_v<T, NumericValue>) {
      return Value::Numeric(value);
    } else if constexpr (std::is_same_v<T, BigNumericValue>) {
      return Value::BigNumeric(value);
    } else if constexpr (std::is_same_v<T, IntervalValue>) {
      return Value::Interval(value);
    } else if constexpr (std::is_same_v<T, bool>) {
      return Value::Bool(value);
    } else if constexpr (std::is_same_v<T, float>) {
      return Value::Float(value);
    } else if constexpr (std::is_same_v<T, double>) {
      return Value::Double(value);
    } else {
      constexpr int kNumBits = sizeof(T) * CHAR_BIT;
      if constexpr (std::is_signed_v<T>) {
        if constexpr (kNumBits == 32) {
          return Value::Int32(value);
        } else if constexpr (kNumBits == 64) {
          return Value::Int64(value);
        }
      } else if constexpr (std::is_unsigned_v<T>) {
        if constexpr (kNumBits == 32) {
          return Value::Uint32(value);
        } else if constexpr (kNumBits == 64) {
          return Value::Uint64(value);
        }
      } else {
        static_assert(std::is_void_v<T>, "Unsupported type.");
      }
    }
  }

  // Generic factory for null values.
  // REQUIRES: T is one of int32_t, int64_t, uint32_t, uint64_t, bool, float, double.
  template <typename T>
  inline static Value MakeNull() {
    if constexpr (std::is_same_v<T, NumericValue>) {
      return Value::NullNumeric();
    } else if constexpr (std::is_same_v<T, BigNumericValue>) {
      return Value::NullBigNumeric();
    } else if constexpr (std::is_same_v<T, bool>) {
      return Value::NullBool();
    } else if constexpr (std::is_same_v<T, float>) {
      return Value::NullFloat();
    } else if constexpr (std::is_same_v<T, double>) {
      return Value::NullDouble();
    } else {
      constexpr int kNumBits = sizeof(T) * CHAR_BIT;
      if constexpr (std::is_signed_v<T>) {
        if constexpr (kNumBits == 32) {
          return Value::NullInt32();
        } else if constexpr (kNumBits == 64) {
          return Value::NullInt64();
        }
      } else if constexpr (std::is_unsigned_v<T>) {
        if constexpr (kNumBits == 32) {
          return Value::NullUint32();
        } else if constexpr (kNumBits == 64) {
          return Value::NullUint64();
        }
      } else {
        static_assert(std::is_void_v<T>, "Unsupported type.");
      }
    }
  }

  // Factory methods to create atomic null values.
  static Value NullInt32();
  static Value NullInt64();
  static Value NullUint32();
  static Value NullUint64();
  static Value NullBool();
  static Value NullFloat();
  static Value NullDouble();
  static Value NullBytes();
  static Value NullString();
  static Value NullDate();
  static Value NullTimestamp();
  static Value NullTime();
  static Value NullDatetime();
  static Value NullInterval();
  static Value NullGeography();
  static Value NullNumeric();
  static Value NullBigNumeric();
  static Value NullJson();

  // Returns an empty but non-null Geography value.
  static Value EmptyGeography();

  // Creates an enum value of the specified 'enum_type'. 'value' must be a valid
  // numeric value declared in 'enum_type', otherwise created Value is invalid.
  // NOTE: Enum types could only be 4 bytes, so this will always return an
  // invalid value if <value> is out-of-range for int32_t.
  static Value Enum(const EnumType* type, int64_t value);
  // Creates an enum value of the specified 'type'. 'name' must be a valid name
  // declared in 'type', otherwise created Value is invalid. 'name' is case
  // sensitive.
  static Value Enum(const EnumType* type, absl::string_view name);
  // Creates a protocol buffer value.
  static Value Proto(const ProtoType* type, absl::Cord value);

  // Creates a struct of the specified 'type' and given 'values'. The size of
  // 'values' must agree with the number of fields in 'type', and the
  // types of those values must match the corresponding struct fields, otherwise
  // returns an error. 'type' must outlive the returned object.
  static absl::StatusOr<Value> MakeStruct(const StructType* type,
                                          absl::Span<const Value> values);

#ifndef SWIG
  static absl::StatusOr<Value> MakeStruct(const StructType* type,
                                          std::vector<Value>&& values);

  // This overload is required to disambiguate vector&& and absl::Span.
  static absl::StatusOr<Value> MakeStruct(const StructType* type,
                                          std::initializer_list<Value> values);

  // Creates a struct of the specified 'type' and given 'values'. The size of
  // the 'values' vector must agree with the number of fields in 'type', and the
  // types of those values must match the corresponding struct fields.
  // This precondition is tested only during debug mode, and will result in
  // undefined behavior at a later time if it is violated.
  // This is an optimization, but it is dangerous and should only be used
  // when a demonstrated performance concern requires it. 'type' must outlive
  // the returned object.
  static absl::StatusOr<Value> MakeStructFromValidatedInputs(
      const StructType* type, std::vector<Value>&& values);

  // Creates a struct of the specified 'type' and given 'values'. The size of
  // the 'values' vector must agree with the number of fields in 'type', and the
  // types of those values must match the corresponding struct fields, otherwise
  // this will crash with a ZETASQL_CHECK failure.
  ABSL_DEPRECATED("Inline me!")
  static Value Struct(const StructType* type, absl::Span<const Value> values) {
    absl::StatusOr<Value> value = MakeStruct(type, values);
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }

  // Creates a struct of the specified 'type' by moving 'values'. The size of
  // the 'values' vector must agree with the number of fields in 'type', and the
  // types of those values must match the corresponding struct fields. However,
  // this is only ZETASQL_CHECK'd in debug mode.
  ABSL_DEPRECATED("Inline me!")
  static Value UnsafeStruct(const StructType* type,
                            std::vector<Value>&& values) {
    absl::StatusOr<Value> value =
        MakeStructFromValidatedInputs(type, std::move(values));
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }
#endif
  // Creates an empty array of the given 'array_type'.
  static Value EmptyArray(const ArrayType* array_type);

  // Creates an array of the given 'array_type' initialized with 'values'.
  // The type of each value must be the same as array_type->element_type(),
  // otherwise returns an error. 'array_type' must outlive the returned object.
  static absl::StatusOr<Value> MakeArray(const ArrayType* array_type,
                                         absl::Span<const Value> values);

#ifndef SWIG
  static absl::StatusOr<Value> MakeArray(const ArrayType* array_type,
                                         std::vector<Value>&& values);

  // This overload is required to disambiguate vector&& and absl::Span.
  static absl::StatusOr<Value> MakeArray(const ArrayType* array_type,
                                         std::initializer_list<Value> values);

  // Creates an array of the specified 'array_type' and given 'values'.
  // The type of each value must be the same as array_type->element_type().
  // This precondition is tested only during debug mode, and will result in
  // undefined behavior at a later time if it is violated.
  // This is an optimization, but it is dangerous and should only be used
  // when a demonstrated performance concern requires it.
  // 'array_type' must outlive the returned object.
  static absl::StatusOr<Value> MakeArrayFromValidatedInputs(
      const ArrayType* array_type, std::vector<Value>&& values);

  // Creates an array of the given 'array_type' initialized with 'values'.
  // The type of each value must be the same as array_type->element_type().
  // otherwise this will crash with a ZETASQL_CHECK failure.
  // 'array_type' must outlive the returned object.
  ABSL_DEPRECATED("Inline me!")
  static Value Array(const ArrayType* array_type,
                     absl::Span<const Value> values) {
    absl::StatusOr<Value> value = MakeArray(array_type, values);
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }

  // Creates an array of the given 'array_type' initialized with 'values'.
  // The type of each value must be the same as array_type->element_type().
  // otherwise this will crash with a ZETASQL_CHECK failure.
  // 'array_type' must outlive the returned object.
  ABSL_DEPRECATED("Inline me!")
  static Value ArraySafe(const ArrayType* array_type,
                         std::vector<Value>&& values) {
    absl::StatusOr<Value> value = MakeArray(array_type, std::move(values));
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }

  // Creates an array of the given 'array_type' initialized with 'values'.
  // The type of each value must be the same as array_type->element_type(),
  // however, this is only ZETASQL_CHECK'd in debug mode.
  // 'array_type' must outlive the returned object.
  ABSL_DEPRECATED("Inline me!")
  static Value UnsafeArray(const ArrayType* array_type,
                           std::vector<Value>&& values) {
    absl::StatusOr<Value> value =
        MakeArrayFromValidatedInputs(array_type, std::move(values));
    ZETASQL_CHECK_OK(value);
    return std::move(value).value();
  }
#endif
  // Creates a null of the given 'type'.
  static Value Null(const Type* type);
  // Creates an invalid value.
  static Value Invalid() { return Value(); }

  // Parse an integer literal into a Value.
  // The returned Value will be INT64, unless UINT64 is necessary to store
  // the value.  Hex literals are supported.  Negative non-hex literals are
  // supported.  Leading and trailing spaces are ignored.
  ABSL_MUST_USE_RESULT static bool ParseInteger(absl::string_view input,
                                                Value* value);

  // Serializes the Value into ValueProto protocol buffer.
  absl::Status Serialize(ValueProto* value_proto) const;

  // Deserializes a ValueProto into Value. Since ValueProto does not know its
  // full type, the type information is passed as an additional parameter.
  static absl::StatusOr<Value> Deserialize(const ValueProto& value_proto,
                                           const Type* type);

 private:
  // For access to StringRef and TypedList.
  FRIEND_TEST(ValueTest, PhysicalByteSize);

  template <bool as_literal, bool maybe_add_simple_type_prefix>
  std::string GetSQLInternal(ProductMode mode) const;

  template <typename H>
  H HashValueInternal(H h) const;

  friend class InternalValue;  // Defined in zetasql/common/internal_value.h.
  friend struct InternalComparer;  // Defined in value.cc.
  friend struct InternalHasher;    // Defined in value.cc
  class TypedList;  // Defined in value_inl.h

  // Specifies whether an array value preserves or ignores order (public array
  // values always preserve order). The enum values are designed to be used with
  // boolean AND and OR.
  typedef bool OrderPreservationKind;

  static constexpr OrderPreservationKind kPreservesOrder = true;
  static constexpr OrderPreservationKind kIgnoresOrder = false;

  static constexpr int kInvalidTypeKind =
      __TypeKind__switch_must_have_a_default__;

  // Constructs an empty (where content contains zeros) or NULL value of the
  // given 'type'. Argument order_kind is currently used only for arrays and
  // should always be set to kPreservesOrder for all other types.
  Value(const Type* type, bool is_null, OrderPreservationKind order_kind);

  // Constructs a typed NULL of the given 'type'.
  explicit Value(const Type* type)
      : Value(type, /*is_null=*/true, kPreservesOrder) {}
#ifndef SWIG
  // SWIG has trouble with constexpr.
  constexpr
#endif
  explicit Value(TypeKind kind)
      : metadata_(kind, /*is_null=*/true, kPreservesOrder,
                  /*value_extended_content=*/0) {
  }

  // Constructors for non-null atomic values.
  explicit Value(int32_t value);
  explicit Value(int64_t value);
  explicit Value(uint32_t value);
  explicit Value(uint64_t value);
  explicit Value(bool value);
  explicit Value(float value);
  explicit Value(double value);
  // REQUIRES: type_kind is date or timestamp_{seconds|millis|micros}
  Value(TypeKind type_kind, int64_t value);
  // REQUIRES: type_kind is string or bytes
  Value(TypeKind type_kind, std::string value);

  // Constructs a timestamp value.
  explicit Value(absl::Time t);

  // Constructs a TIME value.
  explicit Value(TimeValue time);

  // Constructs a DATETIME value.
  explicit Value(DatetimeValue datetime);

  // Constructs an INTERVAL value.
  explicit Value(const IntervalValue& interval);

  explicit Value(const NumericValue& numeric);

  explicit Value(const BigNumericValue& bignumeric);

  // Takes ownership of 'json_ptr' without increasing its ref count.
  explicit Value(internal::JSONRef* json_ptr);

  // Constructs an enum.
  Value(const EnumType* enum_type, int64_t value);
  Value(const EnumType* enum_type, absl::string_view name);

  // Constructs a proto.
  Value(const ProtoType* proto_type, absl::Cord value);

  // Constructs a value of extended type.
  Value(const ExtendedType* extended_type, const ValueContent& value);

  // Clears the contents of the value and makes it invalid. Must be called
  // exactly once prior to destruction or assignment.
  void Clear();

  // Copies the contents of a value from another value.
  void CopyFrom(const Value& that);

  // If an array has order_kind()=kIgnoresOrder, the array represents a
  // an unordered vector (aka multiset). This bit is used internally
  // by test code; public arrays are always ordered.
  bool order_kind() const;

  // When comparing two deeply nested Values with the same type, we want to
  // treat descendant ArrayValues that have the same relationship to the root
  // with the same ordering requirements. This struct is used to build a map
  // of Array types that ignore order within the full type structure. The
  // recursive shape of a DeepOrderKindSpec will follow that of the Value type
  // used to initialize it.
  struct DeepOrderKindSpec {
    // For a simple type (e.g. int, string, enum) 'children' will be empty. For
    // an array type, it will have one element representing the order spec for
    // the array element type. For a struct type, 'children' will contain one
    // element per field of the struct.
    std::vector<DeepOrderKindSpec> children;
    // If the spec node represents an array type, ignores_order will be true if
    // any array value corresponding to this node was marked kIgnoresOrder.
    bool ignores_order = false;
    // Iterate recursively over the Value 'v' to construct a DeepOrderKindSpec
    // and/or set the ignores_order values on the nodes.
    void FillSpec(const Value& v);
  };

  // Uses multiset equality for arrays if allow_bags=true and
  // 'deep_order_spec.order_kind'=kIgnoresOrder. In the case that
  // 'deep_order_spec' is null, it will be computed for 'this' and 'x'.
  static bool EqualsInternal(const Value& x, const Value& y, bool allow_bags,
                             DeepOrderKindSpec* deep_order_spec,
                             const ValueEqualityCheckOptions& options);

  // Creates an array of the given 'array_type' initialized by moving from
  // 'values'.  The type of each value must be the same as
  // array_type->element_type(). This property is validated if
  // 'already_validated' is false or we are in debug mode.
  static absl::StatusOr<Value> MakeArrayInternal(
      bool already_validated, const ArrayType* array_type,
      OrderPreservationKind order_kind, std::vector<Value> values);

  // Creates a struct of the given 'struct_type' initialized by moving from
  // 'values'. Each value must have the proper type. This property is validated
  // if 'already_validated' is false or we are in debug mode.
  static absl::StatusOr<Value> MakeStructInternal(bool already_validated,
                                                  const StructType* struct_type,
                                                  std::vector<Value> values);

  // Compares arrays as multisets ignoring the order of the elements.
  // Called from EqualsInternal().
  static bool EqualElementMultiSet(const Value& x, const Value& y,
                                   DeepOrderKindSpec* deep_order_spec,
                                   const ValueEqualityCheckOptions& options);

  // Returns a pretty-printed (e.g. wrapped) string for the value
  // indented a number of spaces according to the 'indent' parameter.
  // 'force_type' causes the top-level value to print its type. By
  // default, only Array values print their types.
  std::string FormatInternal(int indent, bool force_type) const;

  // Returns the hash code of a value. For kApproximate comparison, returns
  // an approximate hash code.
  size_t HashCodeInternal(FloatMargin float_margin) const;

  static std::string ComplexValueToDebugString(const Value* root, bool verbose);

  // Type cannot create a list of Values because it cannot depend on
  // "value" package. Thus for Array/Struct types that need list of values,
  // we will create them from Value directly.
  // TODO: This can be avoided when we create virtual value list
  // interface which can be defined outside of "value", but Value provides its
  // implementation which it feeds to Array/Struct.
  bool DoesTypeUseValueList() const {
    return metadata_.type_kind() == TYPE_ARRAY ||
           metadata_.type_kind() == TYPE_STRUCT;
  }

  // Gets Value's content. Requires: has_content() == true.
  ValueContent GetContent() const;

  // Sets the content of the Value and makes the Value non-null. Requires:
  // is_valid() == true.
  void SetContent(const ValueContent& content);

  // Nanoseconds for TYPE_TIMESTAMP, TYPE_TIME and TYPE_DATETIME types
  int32_t subsecond_nanos() const {
    ZETASQL_DCHECK(metadata_.can_store_value_extended_content());
    ZETASQL_DCHECK(metadata_.type_kind() == TypeKind::TYPE_TIMESTAMP ||
           metadata_.type_kind() == TypeKind::TYPE_TIME ||
           metadata_.type_kind() == TypeKind::TYPE_DATETIME);
    return metadata_.value_extended_content();
  }

  // Store a pointer to a Type and value flags in metadata. Requires: type must
  // not be a simple built-in type.
  void SetMetadataForNonSimpleType(const Type* type, bool is_null = false,
                                   bool preserves_order = kPreservesOrder);

  // Metadata is 8 bytes class which stores the following fields:
  //  1. 2 bit flags:
  //    1.1. is_null: specifies whether value is NULL
  //    1.2. preserves_order: used by ZetaSQL internally for array testing
  //  2. union (either one or another) of:
  //    2.1. type (Type*): pointer to a Value's Type OR
  //    2.2. struct with fields:
  //    2.2.1. kind (16 bits TypeKind): kind of a built-in type
  //    2.2.2. value_extended_content (int32_t): 4 bytes that simple built-in
  //     types can use to store arbitrary Value related information in addition
  //     to main 64-bit Value's part. This field is currently used to store
  //     nanoseconds for TYPE_TIMESTAMP, TYPE_TIME, TYPE_DATETIME types and
  //     value for ENUM type (pointer to enum is stored in 64-bit part).
  //
  // As can be seen, Metadata can store either TypeKind or a pointer to a Type
  // directly. In the first case, it also can store 32-bit Value's part called
  // value_extended_content. Currently we store TypeKind for all simple built-in
  // types and store a pointer for all other types.
  class Metadata {
   public:
    // Returns true if instance is valid: was initialized and references a valid
    // Type.
    bool is_valid() const;

    // Returns a kind of Value's type.
    TypeKind type_kind() const;

    // Returns a pointer to Value's Type. Requires is_valid(). If TypeKind is
    // stored in the Metadata, Type pointer is obtained from static TypeFactory.
    const Type* type() const;

    // Returns true, if instance stores pointer to a Type and false if type's
    // kind.
    bool has_type_pointer() const;

    // Returns true, if instance has space for 32-bit value extended content. It
    // can be the case only if we store TypeKind and thus has_type_pointer() ==
    // false.
    bool can_store_value_extended_content() const;

    // True, if Value is null.
    bool is_null() const;

    // This bit is used internally by test code to represent unordered arrays;
    // public arrays are always ordered.
    bool preserves_order() const;

    // 32-bit value that can be used by simple built-in types. Requires
    // can_store_value_extended_content() == true.
    int32_t value_extended_content() const;

    Metadata(const Type* type, bool is_null, bool preserves_order);

    constexpr Metadata(TypeKind kind, bool is_null, bool preserves_order,
                       int32_t value_extended_content);

    // Metadata for non-null Value with preserves_order = kPreservesOrder and
    // value_extended_content = 0.
#ifndef SWIG
    // SWIG has trouble with constexpr.
    constexpr
#endif
    explicit Metadata(TypeKind kind)
        : Metadata(kind, /*is_null=*/false, kPreservesOrder,
                   /*value_extended_content=*/0) {
    }

    // Metadata for non-null Value with preserves_order = kPreservesOrder.
    constexpr Metadata(TypeKind kind, int32_t value_extended_content)
        : Metadata(kind, /*is_null=*/false, kPreservesOrder,
                   value_extended_content) {}

    Metadata(const Metadata& that) = default;
    Metadata& operator=(const Metadata& that) = default;

    static constexpr Metadata Invalid() {
      return Metadata(static_cast<TypeKind>(kInvalidTypeKind));
    }

   private:
    // We use different field layouts depending on system bitness.
    template <const int byteness>
    struct ContentLayout;

    typedef ContentLayout<sizeof(Type*)> Content;

    Content* content();
    const Content* content() const;

    // We use int64_t instead of Content here, so we don't need to expose
    // ContentLayout definition into value.h header file.
    int64_t data_{0};
  };

  // 64-bit field, which stores Value's type, value's flags and 32-bit part of
  // the value that is available for simple types only.
  Metadata metadata_ = Metadata::Invalid();

  // 64-bit part of the value.
  union {
    int64_t int64_value_ = 0;  // also seconds|millis|micros since 1970-1-1.
    int32_t int32_value_;      // also date
    uint64_t uint64_value_;
    uint32_t uint32_value_;
    bool bool_value_;
    float float_value_;
    double double_value_;
    int64_t timestamp_seconds_;   // Same as google.protobuf.Timestamp.seconds.
    int32_t bit_field_32_value_;  // Whole-second part of TimeValue.
    int64_t bit_field_64_value_;  // Whole-second part of DatetimeValue.
    int32_t enum_value_;          // Used for TYPE_ENUM.
    internal::StringRef*
        string_ptr_;       // Reffed. Used for TYPE_STRING and TYPE_BYTES.
    TypedList* list_ptr_;  // Reffed. Used for arrays and structs.
    internal::ProtoRep* proto_ptr_;          // Reffed. Used for protos.
    internal::GeographyRef* geography_ptr_;  // Owned. Used for geographies.
    internal::NumericRef*
        numeric_ptr_;  // Owned. Used for values of TYPE_NUMERIC.
    internal::BigNumericRef*
        bignumeric_ptr_;  // Owned. Used for values of TYPE_BIGNUMERIC.
    internal::JSONRef*
        json_ptr_;  // Owned. Used for values of TYPE_JSON.
    internal::IntervalRef*
        interval_ptr_;  // Owned. Used for values of TYPE_INTERVAL.
  };
  // Intentionally copyable.
};

#ifndef SWIG
static_assert(sizeof(Value) == sizeof(int64_t) * 2, "Value size mismatch");
#endif

// Allow Value to be logged.
std::ostream& operator<<(std::ostream& out, const Value& value);

namespace values {

// Constructors below wrap the respective static methods in Value class. See
// comments there for details.

// Constructors for non-null atomic values.
Value Int64(int64_t v);
Value Int32(int32_t v);
Value Uint64(uint64_t v);
Value Uint32(uint32_t v);
Value Bool(bool v);
Value Float(float v);
Value Double(double v);
Value String(absl::string_view v);
Value String(const absl::Cord& v);
// str may contain '\0' in the middle, without getting truncated.
template <size_t N> Value String(const char (&str)[N]);
Value Bytes(absl::string_view v);
Value Bytes(const absl::Cord& v);
// str may contain '\0' in the middle, without getting truncated.
template <size_t N> Value Bytes(const char (&str)[N]);
Value Date(int32_t v);
Value Timestamp(absl::Time t);
Value TimestampFromUnixMicros(int64_t v);

Value Time(TimeValue time);
Value TimeFromPacked64Micros(int64_t v);
Value Datetime(DatetimeValue datetime);
Value DatetimeFromPacked64Micros(int64_t v);
Value Interval(IntervalValue interval);
Value Numeric(NumericValue v);
Value Numeric(int64_t v);
Value BigNumeric(BigNumericValue v);
Value BigNumeric(int64_t v);
Value Enum(const EnumType* enum_type, int32_t value);
Value Enum(const EnumType* enum_type, absl::string_view name);
Value Struct(const StructType* type, absl::Span<const Value> values);
#ifndef SWIG
Value UnsafeStruct(const StructType* type, std::vector<Value>&& values);
#endif
Value Proto(const ProtoType* proto_type, absl::Cord value);
Value Proto(const ProtoType* proto_type, const google::protobuf::Message& msg);
Value EmptyArray(const ArrayType* type);
Value Array(const ArrayType* type, absl::Span<const Value> values);
#ifndef SWIG
Value UnsafeArray(const ArrayType* type, std::vector<Value>&& values);
#endif
Value True();
Value False();

// Constructors for null values.
Value NullInt64();
Value NullInt32();
Value NullUint64();
Value NullUint32();
Value NullBool();
Value NullFloat();
Value NullDouble();
Value NullString();
Value NullBytes();
Value NullDate();
Value NullTimestamp();
Value NullTime();
Value NullDatetime();
Value NullInterval();
Value NullNumeric();
Value NullBigNumeric();
Value Null(const Type* type);

// Constructor for an invalid value.
Value Invalid();

// Array constructors.
Value Int64Array(absl::Span<const int64_t> values);
Value Int32Array(absl::Span<const int32_t> values);
Value Uint64Array(absl::Span<const uint64_t> values);
Value Uint32Array(absl::Span<const uint32_t> values);
Value BoolArray(const std::vector<bool>& values);
Value FloatArray(absl::Span<const float> values);
Value DoubleArray(absl::Span<const double> values);
Value StringArray(absl::Span<const std::string> values);
// Does not take ownership of Cord* values.
Value StringArray(absl::Span<const absl::Cord* const> values);
Value BytesArray(absl::Span<const std::string> values);
// Does not take ownership of Cord* values.
Value BytesArray(absl::Span<const absl::Cord* const> values);
Value NumericArray(absl::Span<const NumericValue> values);
Value BigNumericArray(absl::Span<const BigNumericValue> values);
Value JsonArray(absl::Span<const JSONValue> values);
// 'values' are JSON values (e.g. '{"a": 10}') and not literal strings values.
Value UnvalidatedJsonStringArray(absl::Span<const std::string> values);
Value TimestampArray(absl::Span<const absl::Time> values);

}  // namespace values
}  // namespace zetasql

// Include the implementations of the inline methods. Out of line for
// clarity. Is not intended to be read by users.
#include "zetasql/public/value_inl.h"  

#endif  // ZETASQL_PUBLIC_VALUE_H_
