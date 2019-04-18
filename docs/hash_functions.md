

# Hash functions

### FARM_FINGERPRINT
```
FARM_FINGERPRINT(value)
```

**Description**

Computes the fingerprint of the STRING or BYTES input using the `Fingerprint64`
function from the
[open-source FarmHash library][hash-link-to-farmhash-github]. The output
of this function for a particular input will never change.

**Return type**

INT64

**Examples**

```sql
WITH example AS (
  SELECT 1 AS x, "foo" AS y, true AS z UNION ALL
  SELECT 2 AS x, "apple" AS y, false AS z UNION ALL
  SELECT 3 AS x, "" AS y, true AS z
)
SELECT
  *,
  FARM_FINGERPRINT(CONCAT(CAST(x AS STRING), y, CAST(z AS STRING)))
    AS row_fingerprint
FROM example;
+---+-------+-------+----------------------+
| x | y     | z     | row_fingerprint      |
+---+-------+-------+----------------------+
| 1 | foo   | true  | -1541654101129638711 |
| 2 | apple | false | 2794438866806483259  |
| 3 |       | true  | -4880158226897771312 |
+---+-------+-------+----------------------+
```

### MD5
```
MD5(input)
```

**Description**

Computes the hash of the input using the
[MD5 algorithm][hash-link-to-md5-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 16 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT MD5("Hello World") as md5;

+-------------------------------------------------+
| md5                                             |
+-------------------------------------------------+
| \xb1\n\x8d\xb1d\xe0uA\x05\xb7\xa9\x9b\xe7.?\xe5 |
+-------------------------------------------------+
```

### SHA1
```
SHA1(input)
```

**Description**

Computes the hash of the input using the
[SHA-1 algorithm][hash-link-to-sha-1-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 20 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT SHA1("Hello World") as sha1;

+-----------------------------------------------------------+
| sha1                                                      |
+-----------------------------------------------------------+
| \nMU\xa8\xd7x\xe5\x02/\xabp\x19w\xc5\xd8@\xbb\xc4\x86\xd0 |
+-----------------------------------------------------------+
```

### SHA256
```
SHA256(input)
```

**Description**

Computes the hash of the input using the
[SHA-256 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 32 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT SHA256("Hello World") as sha256;
```

### SHA512
```
SHA512(input)
```

**Description**

Computes the hash of the input using the
[SHA-512 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 64 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT SHA512("Hello World") as sha512;
```

[hash-link-to-farmhash-github]: https://github.com/google/farmhash
[hash-link-to-md5-wikipedia]: https://en.wikipedia.org/wiki/MD5
[hash-link-to-sha-1-wikipedia]: https://en.wikipedia.org/wiki/SHA-1
[hash-link-to-sha-2-wikipedia]: https://en.wikipedia.org/wiki/SHA-2

