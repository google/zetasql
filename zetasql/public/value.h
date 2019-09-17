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

#ifndef ZETASQL_PUBLIC_VALUE_H_
#define ZETASQL_PUBLIC_VALUE_H_

#include <stddef.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.pb.h"
#include "absl/base/attributes.h"
#include <cstdint>
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

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
// Thread safety
// -------------
// Value has the same thread-safety properties as many other types like
// std::string, vector<>, int, etc -- it is thread-compatible as defined by
// (broken link). In particular, if no thread may call a non-const
// method, then it is safe to concurrently call const methods. Copying a Value
// produces a new instance that can be used concurrently with the original in
// arbitrary ways.
//
class Value {
 public:
  // Constructs an invalid value. Needed for using values with STL. All methods
  // other than is_valid() will crash if called on invalid values.
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

  // Returns the type kind of the value. Same as type()->type_kind() but a bit
  // more efficient.
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

  // Accessors for accessing the data within atomic typed Values.
  // REQUIRES: !is_null().
  int32_t int32_value() const;           // REQUIRES: int32_t type
  int64_t int64_value() const;           // REQUIRES: int64_t type
  uint32_t uint32_value() const;         // REQUIRES: uint32_t type
  uint64_t uint64_value() const;         // REQUIRES: uint64_t type
  bool bool_value() const;             // REQUIRES: bool type
  float float_value() const;           // REQUIRES: float type
  double double_value() const;         // REQUIRES: double type
  const std::string& string_value() const;  // REQUIRES: std::string type
  const std::string& bytes_value() const;   // REQUIRES: bytes type
  int32_t date_value() const;            // REQUIRES: date type
  int32_t enum_value() const;            // REQUIRES: enum type
  const std::string& enum_name() const;     // REQUIRES: enum type

  // Returns timestamp value as absl::Time at nanoseconds precision.
  absl::Time ToTime() const;  // REQUIRES: timestamp type

  // Returns timestamp value as Unix epoch microseconds.
  int64_t ToUnixMicros() const;  // REQUIRES: timestamp type.
  // Returns timestamp value as Unix epoch nanoseconds or an error if the value
  // does not fit into an int64_t.
  zetasql_base::Status ToUnixNanos(int64_t* nanos) const;  // REQUIRES: timestamp type.

  // Returns time and datetime values at micros precision as bitwise encoded
  // int64_t, see public/civil_time.h for the encoding.
  int64_t ToPacked64TimeMicros() const;      // REQUIRES: time type
  int64_t ToPacked64DatetimeMicros() const;  // REQUIRES: datetime type

  TimeValue time_value() const;          // REQUIRES: time type
  DatetimeValue datetime_value() const;  // REQUIRES: datetime type

  // REQUIRES: numeric type
  const NumericValue& numeric_value() const;

  // Generic accessor for numeric PODs.
  // REQUIRES: T is one of int32_t, int64_t, uint32_t, uint64_t, bool, float, double.
  // T must match exactly the type_kind() of this value.
  template <typename T> inline T Get() const;

  // Accessors that coerce the data to the requested C++ type.
  // REQUIRES: !is_null().
  // Use of this method for timestamp_ values is DEPRECATED.
  int64_t ToInt64() const;    // For bool, int_, uint32_t, date, enum
  uint64_t ToUint64() const;  // For bool, uint32_t, uint64
  // Use of this method for timestamp_ values is DEPRECATED.
  double ToDouble() const;  // For bool, int_, date, timestamp_, enum types.
  std::string ToCord() const;  // For std::string, bytes, and protos

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
  // with a known type, consider using ToCord() and ParseFromString() as follows:
  //   MyMessage pb;
  //   ...
  //   ZETASQL_RETURN_IF_ERROR(pb.ParseFromString(value.ToCord()));
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
  size_t HashCode() const;

  template <typename H>
  friend H AbslHashValue(H h, const Value& v);

  // Returns printable std::string for this Value.
  // Verbose DebugStrings include the type name.
  std::string ShortDebugString() const { return DebugString(false); }
  std::string FullDebugString() const { return DebugString(true); }
  std::string DebugString(bool verbose = false) const;

  // Returns a pretty-printed (e.g. wrapped) std::string for the value.
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
  // std::string.  For example, if you have a STRUCT value like
  // STRUCT<a INT64, b STRING>(1, 'a'), and call GetSQL(), the result will
  // be "(1, 'a')".  If we're only interested in the value itself and not the
  // original type (with named fields) then maybe that's ok.  Note that
  // GetSQLLiteral() is used in ZetaSQL's FORMAT() function implementation
  // (Format() in zetasql/public_functions/format.cc) so we cannot change
  // the output without breaking existing ZetaSQL function semantics.
  std::string GetSQL(ProductMode mode = PRODUCT_EXTERNAL) const;

  // Returns a SQL expression that is compatible as a literal for this value.
  // This won't include CASTs and won't necessarily produce the exact same
  // type when parsed on its own, but it should be the closest SQL literal
  // form for this value.  Returned type names are sensitive to the SQL
  // ProductMode (INTERNAL or EXTERNAL).
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
  // cs/zetasql::ValueConstructor constructor which takes std::string-like objects.
  // Therefore using the name StringValue is a pragmatic way around this issue.
  static Value StringValue(std::string v);
  static Value String(absl::string_view v);
  // str may contain '\0' in the middle, without getting truncated.
  template <size_t N> static Value String(const char (&str)[N]);
  static Value Bytes(std::string v);
  static Value Bytes(absl::string_view v);
  // str may contain '\0' in the middle, without getting truncated.
  template <size_t N> static Value Bytes(const char (&str)[N]);
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

  static Value Numeric(NumericValue v);

  // Generic factory for numeric PODs.
  // REQUIRES: T is one of int32_t, int64_t, uint32_t, uint64_t, bool, float, double.
  template <typename T> inline static Value Make(T value);

  // Generic factory for null values.
  // REQUIRES: T is one of int32_t, int64_t, uint32_t, uint64_t, bool, float, double.
  template <typename T> inline static Value MakeNull();

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
  static Value NullGeography();
  static Value NullNumeric();

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
  static Value Proto(const ProtoType* type, const std::string& value);

  // Creates a struct of the specified 'type' and given 'values'. The size of
  // the 'values' vector must agree with the number of fields in 'type', and the
  // types of those values must match the corresponding struct fields.
  static Value Struct(const StructType* type,
                      absl::Span<const Value> values);
// Creates a struct of the specified 'type' by moving 'values'. The size of
// the 'values' vector must agree with the number of fields in 'type', and the
// types of those values must match the corresponding struct fields. However,
// this is only CHECK'd in debug mode.
#ifndef SWIG
  static Value UnsafeStruct(const StructType* type,
                            std::vector<Value>&& values);
#endif
  // Creates an empty array of the given 'array_type'.
  static Value EmptyArray(const ArrayType* array_type);
  // Creates an array of the given 'array_type' initialized with 'values'.
  // The type of each value must be the same as array_type->element_type().
  static Value Array(const ArrayType* array_type,
                     absl::Span<const Value> values);
// Creates an array of the given 'array_type' initialized by moving 'values'.
// The type of each value must be the same as array_type->element_type(), but
// this is only CHECK'd in debug mode.
#ifndef SWIG
  static Value UnsafeArray(const ArrayType* array_type,
                           std::vector<Value>&& values);
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
  zetasql_base::Status Serialize(ValueProto* value_proto) const;

  // Deserializes a ValueProto into Value. Since ValueProto does not know its
  // full type, the type information is passed as an additional parameter.
  static zetasql_base::StatusOr<Value> Deserialize(const ValueProto& value_proto,
                                           const Type* type);

 private:
  // For access to StringRef and TypedList.
  FRIEND_TEST(ValueTest, PhysicalByteSize);

  template <typename H>
  H HashValueInternal(H h) const;

  friend class InternalValue;  // Defined in zetasql/common/internal_value.h.
  friend struct InternalComparer;  // Defined in value.cc.
  friend struct InternalHasher;    // Defined in value.cc
  class GeographyRef;  // Defined in value_inl.h
  class NumericRef;  // Defined in value_inl.h
  class StringRef;  // Defined in value_inl.h
  class ProtoRep;   // Defined in value_inl.h
  class TypedList;  // Defined in value_inl.h

  // Specifies whether an array value preserves or ignores order (public array
  // values always preserve order). The enum values are designed to be used with
  // boolean AND and OR.
  typedef bool OrderPreservationKind;

  static const OrderPreservationKind kPreservesOrder = true;
  static const OrderPreservationKind kIgnoresOrder = false;

  static const int kInvalidTypeKind = __TypeKind__switch_must_have_a_default__;

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
  // REQUIRES: type_kind is std::string or bytes
  Value(TypeKind type_kind, std::string value);

  // Constructs a typed NULL of the given 'type'.
  explicit Value(const Type* type);

  // Constructs a timestamp value.
  explicit Value(absl::Time t);

  // Constructs a TIME value.
  explicit Value(TimeValue time);

  // Constructs a DATETIME value.
  explicit Value(DatetimeValue datetime);

  explicit Value(const NumericValue& numeric);

  // Constructs an enum.
  Value(const EnumType* enum_type, int64_t value);
  Value(const EnumType* enum_type, absl::string_view name);

  // Constructs a proto.
  Value(const ProtoType* proto_type, const std::string& value);

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
    // For a simple type (e.g. int, std::string, enum) 'children' will be empty. For
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
  // In case of inequality and 'reason' != nullptr, a detailed explanation may
  // be appended to 'reason'. Uses float_margin as the maximum allowed absolute
  // error when comparing floating point numbers (float and double).
  static bool EqualsInternal(const Value& x, const Value& y, bool allow_bags,
                             DeepOrderKindSpec* deep_order_spec,
                             FloatMargin float_margin, std::string* reason);

// Creates an array of the given 'array_type' initialized by moving from
// 'values'.  The type of each value must be the same as
// array_type->element_type(). If 'safe' is true or we are in debug mode, this
// is CHECK'd.
#ifndef SWIG
  static Value ArrayInternal(bool safe, const ArrayType* array_type,
                             OrderPreservationKind order_kind,
                             std::vector<Value>&& values);
#endif

// Creates a struct of the given 'struct_type' initialized by moving from
// 'values'. Each value must have the proper type. If 'safe' is true or we are
// in debug mode, this is CHECK'd.
#ifndef SWIG
  static Value StructInternal(bool safe, const StructType* struct_type,
                              std::vector<Value>&& values);
#endif

  // Compares arrays as multisets ignoring the order of the elements. Upon
  // inequality, 'reason' may be set to detailed explanation if 'reason' !=
  // nullptr. Called from EqualsInternal().
  static bool EqualElementMultiSet(const Value& x, const Value& y,
                                   DeepOrderKindSpec* deep_order_spec,
                                   FloatMargin float_margin, std::string* reason);

  // Returns a pretty-printed (e.g. wrapped) std::string for the value
  // indented a number of spaces according to the 'indent' parameter.
  // 'force_type' causes the top-level value to print its type. By
  // default, only Array values print their types.
  std::string FormatInternal(int indent, bool force_type) const;

  // Returns the hash code of a value. For kApproximate comparison, returns
  // an approximate hash code.
  size_t HashCodeInternal(FloatMargin float_margin) const;

  std::string DebugStringInternal(
      bool verbose,
      const std::map<const Value*, std::string>& debug_string_map) const;

  // type_kind_ is either zetasql::TypeKind or -1 for invalid values.
  int16_t type_kind_ = kInvalidTypeKind;
  bool is_null_ = false;
  // This bit is used internally by test code to represent unordered arrays;
  // public arrays are always ordered.
  bool order_kind_ = kPreservesOrder;

  // 32-bit part of the value.
  union {
    int32_t enum_value_ = 0;  // Used for enums.
    // Used for google.protobuf.Timestamp.nanos and sub-second part of
    // DatetimeValue and TimeValue.
    int32_t subsecond_nanos_;
  };

  // 64-bit part of the value.
  union {
    int64_t int64_value_ = 0;  // also seconds|millis|micros since 1970-1-1.
    int32_t int32_value_;  // also date
    uint64_t uint64_value_;
    uint32_t uint32_value_;
    bool bool_value_;
    float float_value_;
    double double_value_;
    int64_t timestamp_seconds_;  // Same as google.protobuf.Timestamp.seconds.
    int32_t bit_field_32_value_;   // Whole-second part of TimeValue.
    int64_t bit_field_64_value_;   // Whole-second part of DatetimeValue.
    StringRef* string_ptr_;  // Reffed. Used for TYPE_STRING and TYPE_BYTES.
    TypedList* list_ptr_;  // Reffed. Used for arrays and structs.
    const EnumType* enum_type_;  // Not owned. Used for enums.
    ProtoRep* proto_ptr_;        // Reffed. Used for protos.
    GeographyRef* geography_ptr_;  // Owned. Used for geographies.
    NumericRef* numeric_ptr_;  // Owned. Used for values of TYPE_NUMERIC.
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
// str may contain '\0' in the middle, without getting truncated.
template <size_t N> Value String(const char (&str)[N]);
Value Bytes(absl::string_view v);
// str may contain '\0' in the middle, without getting truncated.
template <size_t N> Value Bytes(const char (&str)[N]);
Value Date(int32_t v);
Value Timestamp(absl::Time t);
Value TimestampFromUnixMicros(int64_t v);

Value Time(TimeValue time);
Value TimeFromPacked64Micros(int64_t v);
Value Datetime(DatetimeValue datetime);
Value DatetimeFromPacked64Micros(int64_t v);
Value Numeric(NumericValue v);
Value Numeric(int64_t v);
Value Enum(const EnumType* enum_type, int32_t value);
Value Enum(const EnumType* enum_type, absl::string_view name);
Value Struct(const StructType* type, absl::Span<const Value> values);
#ifndef SWIG
Value UnsafeStruct(const StructType* type, std::vector<Value>&& values);
#endif
Value Proto(const ProtoType* proto_type, const std::string& value);
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
Value NullNumeric();
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
Value BytesArray(absl::Span<const std::string> values);
// Does not take ownership of Cord* values.

Value NumericArray(absl::Span<const NumericValue> values);

}  // namespace values
}  // namespace zetasql

// Include the implementations of the inline methods. Out of line for
// clarity. Is not intended to be read by users.
#include "zetasql/public/value_inl.h"  

#endif  // ZETASQL_PUBLIC_VALUE_H_
