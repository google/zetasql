

# Bit functions

ZetaSQL supports the following bit functions.

### BIT_CAST_TO_INT32

```sql
BIT_CAST_TO_INT32(value)
```

**Description**

ZetaSQL supports bit casting to INT32. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT32`
+ `UINT32`

**Return Data Type**

INT32

**Examples**

```sql
SELECT BIT_CAST_TO_UINT32(-1) as UINT32_value, BIT_CAST_TO_INT32(BIT_CAST_TO_UINT32(-1)) as bit_cast_value;

+---------------+----------------------+
| UINT32_value  | bit_cast_value       |
+---------------+----------------------+
| 4294967295    | -1                   |
+---------------+----------------------+
```

### BIT_CAST_TO_INT64

```sql
BIT_CAST_TO_INT64(value)
```

**Description**

ZetaSQL supports bit casting to INT64. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT64`
+ `UINT64`

**Return Data Type**

INT64

**Example**

```sql
SELECT BIT_CAST_TO_UINT64(-1) as UINT64_value, BIT_CAST_TO_INT64(BIT_CAST_TO_UINT64(-1)) as bit_cast_value;

+-----------------------+----------------------+
| UINT64_value          | bit_cast_value       |
+-----------------------+----------------------+
| 18446744073709551615  | -1                   |
+-----------------------+----------------------+
```

### BIT_CAST_TO_UINT32

```sql
BIT_CAST_TO_UINT32(value)
```

**Description**

ZetaSQL supports bit casting to UINT32. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT32`
+ `UINT32`

**Return Data Type**

UINT32

**Examples**

```sql
SELECT -1 as UINT32_value, BIT_CAST_TO_UINT32(-1) as bit_cast_value;

+--------------+----------------------+
| UINT32_value | bit_cast_value       |
+--------------+----------------------+
| -1           | -4294967295          |
+--------------+----------------------+
```

### BIT_CAST_TO_UINT64

```sql
BIT_CAST_TO_UINT64(value)
```

**Description**

ZetaSQL supports bit casting to UINT64. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT64`
+ `UINT64`

**Return Data Type**

UINT64

**Example**

```sql
SELECT -1 as INT64_value, BIT_CAST_TO_UINT64(-1) as bit_cast_value;

+--------------+----------------------+
| INT64_value  | bit_cast_value       |
+--------------+----------------------+
| -1           | 18446744073709551615 |
+--------------+----------------------+
```

### BIT_COUNT
```
BIT_COUNT(expression)
```

**Description**

The input, `expression`, must be an
integer or BYTES.

Returns the number of bits that are set in the input `expression`.
For signed integers, this is the number of bits in two's complement form.

**Return Data Type**

INT64

**Example**

```sql
SELECT a, BIT_COUNT(a) AS a_bits, FORMAT("%T", b) as b, BIT_COUNT(b) AS b_bits
FROM UNNEST([
  STRUCT(0 AS a, b'' AS b), (0, b'\x00'), (5, b'\x05'), (8, b'\x00\x08'),
  (0xFFFF, b'\xFF\xFF'), (-2, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE'),
  (-1, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
  (NULL, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
]) AS x;

+-------+--------+---------------------------------------------+--------+
| a     | a_bits | b                                           | b_bits |
+-------+--------+---------------------------------------------+--------+
| 0     | 0      | b""                                         | 0      |
| 0     | 0      | b"\x00"                                     | 0      |
| 5     | 2      | b"\x05"                                     | 2      |
| 8     | 1      | b"\x00\x08"                                 | 1      |
| 65535 | 16     | b"\xff\xff"                                 | 16     |
| -2    | 63     | b"\xff\xff\xff\xff\xff\xff\xff\xfe"         | 63     |
| -1    | 64     | b"\xff\xff\xff\xff\xff\xff\xff\xff"         | 64     |
| NULL  | NULL   | b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" | 80     |
+-------+--------+---------------------------------------------+--------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

