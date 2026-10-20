

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Hash functions

GoogleSQL supports the following hash functions.

## Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/hash_functions.md#farm_fingerprint"><code>FARM_FINGERPRINT</code></a>
</td>
  <td>
    Computes the fingerprint of a <code>STRING</code> or
    <code>BYTES</code> value using the FarmHash Fingerprint64 algorithm.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/hash_functions.md#highway_fingerprint128"><code>HIGHWAY_FINGERPRINT128</code></a>
</td>
  <td>
    Computes the 128-bit fingerprint of a <code>STRING</code> or
    <code>BYTES</code> value using the <a href="/third_party/highwayhash/README.md?cl=head">HighwayHash algorithm</a>
.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/hash_functions.md#md5"><code>MD5</code></a>
</td>
  <td>
    Computes the hash of a <code>STRING</code> or
    <code>BYTES</code> value using the MD5 algorithm.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/hash_functions.md#sha1"><code>SHA1</code></a>
</td>
  <td>
    Computes the hash of a <code>STRING</code> or
    <code>BYTES</code> value using the SHA-1 algorithm.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/hash_functions.md#sha256"><code>SHA256</code></a>
</td>
  <td>
    Computes the hash of a <code>STRING</code> or
    <code>BYTES</code> value using the SHA-256 algorithm.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/hash_functions.md#sha512"><code>SHA512</code></a>
</td>
  <td>
    Computes the hash of a <code>STRING</code> or
    <code>BYTES</code> value using the SHA-512 algorithm.
  </td>
</tr>

  </tbody>
</table>

## `FARM_FINGERPRINT`

```googlesql
FARM_FINGERPRINT(value)
```

**Description**

Computes the fingerprint of the `STRING` or `BYTES` input using the
`Fingerprint64` function from the [open-source FarmHash
library][hash-link-to-farmhash-github]. The output is a signed
INT64 value, uniformly distributed in the
`[-2^63, 2^63 - 1]` range. The output of this function for a particular input
never changes across runs or query engine implementations.

Use `FARM_FINGERPRINT` when you need a fast, deterministic 64-bit signed
integer (`INT64`) for non-cryptographic use cases, such as
the following:

*   Sharding, partitioning, or bucketing large datasets consistently across
    distributed pipelines.
*   Generating stable integer identifiers or grouping keys from strings.
*   Sampling rows deterministically (`ABS(FARM_FINGERPRINT(id)) % 10 = 0`).

`FARM_FINGERPRINT` isn't a cryptographic hash function, so don't use it for
security verification or adversarial inputs. Because it outputs
64 bits, collision probability increases as the number of distinct elements
exceeds four billion (`~2^32` items).

For cryptographic verification or lower collision probability, use
[`HIGHWAY_FINGERPRINT128`][highway_fingerprint128] or [`SHA256`][sha256].

**Return type**

INT64

**Examples**

```googlesql
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
/*---+-------+-------+----------------------+
 | x | y     | z     | row_fingerprint      |
 +---+-------+-------+----------------------+
 | 1 | foo   | true  | -1541654101129638711 |
 | 2 | apple | false | 2794438866806483259  |
 | 3 |       | true  | -4880158226897771312 |
 +---+-------+-------+----------------------*/
```

[hash-link-to-farmhash-github]: https://github.com/google/farmhash

[highway_fingerprint128]: #highway_fingerprint128

[sha256]: #sha256

## `HIGHWAY_FINGERPRINT128`

```googlesql
HIGHWAY_FINGERPRINT128(input STRING[, key BYTES]) -> BYTES
```

```googlesql
HIGHWAY_FINGERPRINT128(input BYTES[, key BYTES]) -> BYTES
```

**Description**

Computes the 128-bit fingerprint of a `STRING` or `BYTES` input value using the
[HighwayHash algorithm][highwayhash]. The string version treats the input as an
array of bytes. The optional `key` argument adds unpredictability to the hash
output for security.

Use `HIGHWAY_FINGERPRINT128` when you require high-speed, 128-bit fingerprinting
(`BYTES` output) with lower collision probability (`~2^64` distinct elements
before collision risk increases) than 64-bit fingerprinting (`~2^32` elements).

Specifically, use `HIGHWAY_FINGERPRINT128` with the optional 32-byte `key`
parameter to protect your pipeline against hash-flooding denial-of-service
(DoS) attacks or when processing untrusted, user-provided string values where
unkeyed fingerprinting could be exploited to engineer collisions.

While `HIGHWAY_FINGERPRINT128` offers keyed defense against hash
flooding, it is a fast pseudorandom function rather than a cryptographic hash
digest.

For cryptographic verification or signatures, use
[`SHA256`][sha256] or [`SHA512`][sha512].

**Return type**

BYTES

**Definitions**

*   `input`: A `STRING` or `BYTES` value to be hashed.
*   `key`: (Optional) A `BYTES` value that makes the hash output unpredictable
    to an attacker, preventing hash-flooding denial-of-service attacks. This key
    must be exactly 32 bytes (256 bits) long.

**Examples**

```googlesql
-- Without `key`

SELECT HIGHWAY_FINGERPRINT128('Hello World')

/*------------------------------------------------------------+
 | \xa3\xc3\xb8\xb4\xfeo\x88W\xbf\x88\xae\x89\xe4\xab\xd3\x8e |
 +------------------------------------------------------------*/

```

```googlesql
-- With `key`

SELECT HIGHWAY_FINGERPRINT128(
    'Hello World',
    -- 32-byte key
    b'abcdefghijklmnopqrstuvwxyzabcdef'
)

/*------------------------------------------+
 | o\x9c\xbc:tQ{p\xa5}Y\x0c\xc3\xbe\x04\xcb |
 +------------------------------------------*/

```

[highwayhash]: /third_party/highwayhash/README.md?cl=head

[sha256]: #sha256

[sha512]: #sha512

## `MD5`

```googlesql
MD5(input)
```

**Description**

Computes the hash of the input using the
[MD5 algorithm][hash-link-to-md5-wikipedia]. The input can be either
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

Use `MD5` only for backward compatibility with legacy databases, historic
checksums, or external communication protocols and deduplication workflows that
strictly mandate an MD5 digest.

Warning: `MD5` uses a vulnerable 128-bit algorithm and isn't secure against
cryptographic collision attacks. Don't use `MD5` for new implementations,
digital signatures, passwords, or security verification.

For cryptographic security, use
[`SHA256`][sha256] or [`SHA512`][sha512].

For fast 128-bit non-cryptographic fingerprinting, use
[`HIGHWAY_FINGERPRINT128`][highway_fingerprint128].

**Return type**

BYTES

**Examples**

```googlesql
SELECT MD5("Hello World") as md5;

/*-------------------------------------------------+
 | md5                                             |
 +-------------------------------------------------+
 | \xb1\n\x8d\xb1d\xe0uA\x05\xb7\xa9\x9b\xe7.?\xe5 |
 +-------------------------------------------------*/
```

[hash-link-to-md5-wikipedia]: https://en.wikipedia.org/wiki/MD5

[sha256]: #sha256

[sha512]: #sha512

[highway_fingerprint128]: #highway_fingerprint128

## `SHA1`

```googlesql
SHA1(input)
```

**Description**

Computes the hash of the input using the
[SHA-1 algorithm][hash-link-to-sha-1-wikipedia]. The input can be either
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

Use `SHA1` only when backward compatibility with legacy datasets, external
APIs, or historical cryptographic identifiers (such as git commit hashes or
legacy verification records) is strictly required.

Warning: `SHA-1` has known cryptographic vulnerabilities and isn't secure
against collision attacks. Don't use `SHA1` for new cryptographic designs,
digital signatures, or security verification.

For cryptographic verification, use

[`SHA256`][sha256] or [`SHA512`][sha512]
.

**Return type**

BYTES

**Examples**

```googlesql
SELECT SHA1("Hello World") as sha1;

/*-----------------------------------------------------------+
 | sha1                                                      |
 +-----------------------------------------------------------+
 | \nMU\xa8\xd7x\xe5\x02/\xabp\x19w\xc5\xd8@\xbb\xc4\x86\xd0 |
 +-----------------------------------------------------------*/
```

[hash-link-to-sha-1-wikipedia]: https://en.wikipedia.org/wiki/SHA-1

[sha256]: #sha256

[sha512]: #sha512

## `SHA256`

```googlesql
SHA256(input)
```

**Description**

Computes the hash of the input using the
[SHA-256 algorithm][hash-link-to-sha-2-wikipedia]. The input can be either
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

Use `SHA256` when you require a cryptographically secure hash function, such
as the following use cases:

*   Verifying string or binary data integrity and generating tamper-evident
    checksums.
*   Creating secure cryptographic digests or globally unique content identifiers
    (256-bit hash space provides effectively zero collision probability).
*   Interoperating with authentication workflows, digital signatures, and
    security protocols requiring standard SHA-2 digests.

Computing `SHA256` is computationally more expensive than non-cryptographic
fingerprinting
(for example,
[`FARM_FINGERPRINT`][farm_fingerprint] or
[`HIGHWAY_FINGERPRINT128`][highway_fingerprint128]).
Avoid using `SHA256` solely for simple data sharding, bucketing, or indexing
where high throughput and compact integer output are required.

**Return type**

BYTES

**Examples**

```googlesql
SELECT SHA256("Hello World") as sha256;
```

[hash-link-to-sha-2-wikipedia]: https://en.wikipedia.org/wiki/SHA-2

[farm_fingerprint]: #farm_fingerprint

[highway_fingerprint128]: #highway_fingerprint128

## `SHA512`

```googlesql
SHA512(input)
```

**Description**

Computes the hash of the input using the
[SHA-512 algorithm][hash-link-to-sha-2-wikipedia]. The input can be either
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

Use `SHA512` when you require maximum cryptographic security (512-bit output),
such as the following use cases:

*   High-security data verification, digital signatures, and cryptographic
    protocols where SHA-512 digests are mandated by compliance standards.
*   Applications requiring ultra-low collision probability or resistance to
    future-proofing attacks with hash outputs larger than 256 bits.
*   Processing platforms on 64-bit hardware architectures where SHA-512
    operates with high efficiency.

`SHA512` produces a 64-byte digest, double the size of
[`SHA256`][sha256] (32 bytes).
When storing digests in tables or transmitting over the network where storage
and bandwidth efficiency are essential, use
[`SHA256`][sha256] unless 512-bit
security is specifically required.

**Return type**

BYTES

**Examples**

```googlesql
SELECT SHA512("Hello World") as sha512;
```

[hash-link-to-sha-2-wikipedia]: https://en.wikipedia.org/wiki/SHA-2

[sha256]: #sha256

