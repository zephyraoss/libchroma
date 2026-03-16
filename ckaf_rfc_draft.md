# Chromakopia Acoustic Fingerprint Format (CKAF)

Authors:
  Addison LeClair <me@addi.lol>
  Zephyra LLC
License: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/)
Version: 0.1.1-draft  
Intended Status: Informational  
Date: 2026-03-16

## Status of This Memo

This document describes the Chromakopia Acoustic Fingerprint Format (CKAF), a
binary file format for storing, indexing, and querying acoustic fingerprint
data. This document is a draft specification and may change prior to any stable
release.

This document is not an Internet Standards Track specification. It is published
for implementation and interoperability.

## Abstract

CKAF is a compact, disk-oriented binary format for storing and querying acoustic
fingerprint data. It is intended for use by applications such as the
Chromakopia server, but it is not tied to any specific implementation.

CKAF is designed to minimize RAM usage, support append-only incremental
updates, permit regeneration of derived data, and avoid dependence on an
external database engine. A complete CKAF dataset consists of three files: a
data store (`.ckd`), a search index (`.ckx`), and a metadata map (`.ckm`).

## 1. Introduction

Acoustic fingerprint datasets are large and are frequently queried in a
seek-heavy pattern. Traditional relational storage layouts can incur
substantial storage and memory overhead. CKAF addresses this by defining a
flat-file representation optimized for:

* minimal resident memory;
* compact storage;
* predictable binary layout;
* append-only incremental updates; and
* independent regeneration of derived structures.

The `.ckd` file is the source of truth. The `.ckx` and `.ckm` files are derived
artifacts and can be regenerated from `.ckd` and external metadata sources.

## 2. Conventions and Terminology

The key words “MUST”, “MUST NOT”, “REQUIRED”, “SHALL”, “SHALL NOT”,
“SHOULD”, “SHOULD NOT”, “RECOMMENDED”, “NOT RECOMMENDED”, “MAY”, and
“OPTIONAL” in this document are to be interpreted as described in BCP 14
when, and only when, they appear in all capitals, as shown here.

Unless otherwise specified:

* all integers are unsigned;
* all multi-byte integers are little-endian;
* all offsets are byte offsets; and
* padding bytes MUST be zero (`0x00`).

## 3. Dataset Model

A complete CKAF dataset consists of exactly three files sharing a common
filename prefix:

* `PREFIX.ckd` — data store;
* `PREFIX.ckx` — search index; and
* `PREFIX.ckm` — metadata map.

Example:

* `acoustid.ckd`
* `acoustid.ckx`
* `acoustid.ckm`

Each file contains a `dataset_id` field. Implementations SHOULD treat matching
`dataset_id` values as evidence that the three files were produced from the
same logical dataset. A mismatch SHOULD generate a warning, but need not be a
fatal error.

## 4. Common Conventions

### 4.1. Byte Order

All multi-byte integers in CKAF files MUST be encoded in little-endian byte
order.

### 4.2. Alignment

All sections MUST begin on an 8-byte boundary. Any alignment padding MUST be
filled with zero bytes.

### 4.3. Magic Numbers

Each CKAF file begins with an 8-byte file-type magic value.

| File   | ASCII Magic      | Hex Bytes                         |
|--------|------------------|-----------------------------------|
| `.ckd` | `CKAF-D\x00\x00` | `43 4B 41 46 2D 44 00 00`         |
| `.ckx` | `CKAF-X\x00\x00` | `43 4B 41 46 2D 58 00 00`         |
| `.ckm` | `CKAF-M\x00\x00` | `43 4B 41 46 2D 4D 00 00`         |

Readers MUST reject a file whose magic value does not match the expected file
type.

## 5. Common File Header

All three CKAF file types begin with a 96-byte header. The header consists of a
64-byte core header followed by a 32-byte section directory.

### 5.1. Core Header

The first 64 bytes of each file header are defined as follows:

```text
Offset  Size  Type      Field
------  ----  ----      -----
0x00    8     u8[8]     magic
0x08    2     u16       version_major
0x0A    2     u16       version_minor
0x0C    4     u32       flags
0x10    8     u64       record_count
0x18    8     u64       created_at
0x20    8     u64       source_date
0x28    16    u8[16]    dataset_id
0x38    8     u8[8]     reserved
```

Field definitions:

* `magic`: the file-type magic described in Section 4.3.
* `version_major`: major format version.
* `version_minor`: minor format version.
* `flags`: file-type-specific bitfield.
* `record_count`: number of primary records in the main section of the file.
* `created_at`: Unix timestamp, in seconds, indicating when the file was built.
* `source_date`: Unix timestamp, in seconds, identifying the source dump date,
  or zero if not applicable.
* `dataset_id`: random UUID identifying the dataset.
* `reserved`: reserved for future use and MUST be all zero.

### 5.2. Section Directory

The final 32 bytes of the header define two sections:

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x40    8     u64     section_0_offset
0x48    8     u64     section_0_length
0x50    8     u64     section_1_offset
0x58    8     u64     section_1_length
```

The meaning of the two sections depends on the file type:

| File   | Section 0          | Section 1               |
|--------|--------------------|-------------------------|
| `.ckd` | Record Table       | Fingerprint Data Blob   |
| `.ckx` | Bucket Directory   | Posting Lists           |
| `.ckm` | Mapping Table      | String Pool             |

A section length of zero indicates an empty section. This is valid for optional
sections such as the `.ckm` string pool.

Readers MUST use the section directory rather than infer section boundaries from
record counts or file size.

## 6. The `.ckd` File

The `.ckd` file stores compressed fingerprints and a fixed-width record table
used to locate them.

### 6.1. Layout

A `.ckd` file has the following logical layout:

```text
[ Header (96 bytes) ]
[ Record Table ]             ; section 0
[ Fingerprint Data Blob ]    ; section 1
[ Overflow Journal ]         ; optional, located via footer
[ Footer (16 bytes) ]
```

### 6.2. Flags

The `.ckd` flags field is defined as follows:

```text
Bit 0: compression_method
       0 = XOR-delta + varint
       1 = XOR-delta + PFOR bitpacking

Bit 1: has_overflow
       0 = no overflow journal present
       1 = overflow journal present

Bits 2-31: reserved, MUST be zero
```

Readers MUST reject files with unknown non-zero reserved flag bits.

### 6.3. Record Table

The record table is located at `section_0_offset` and contains fixed-width
20-byte records sorted by `fingerprint_id` in ascending order.

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    4     u32     fingerprint_id
0x04    6     u48     data_offset
0x0A    2     u16     data_length
0x0C    4     u32     duration_ms
0x10    2     u16     raw_count
0x12    2     u16     reserved
```

Field definitions:

* `fingerprint_id`: unique fingerprint identifier.
* `data_offset`: byte offset into section 1, relative to `section_1_offset`.
* `data_length`: compressed data length in bytes.
* `duration_ms`: audio duration in milliseconds.
* `raw_count`: number of uncompressed `u32` sub-fingerprint values.
* `reserved`: MUST be zero.

Readers MAY binary-search this table by `fingerprint_id`.

#### 6.3.1. `duration_ms`

`duration_ms` is encoded as a `u32`. Its maximum representable value is
approximately 49.7 days.

#### 6.3.2. `data_offset`

`data_offset` is a 48-bit unsigned integer encoded as six little-endian bytes.
It supports offsets up to 256 TB relative to the start of section 1.

The absolute file offset of the corresponding compressed fingerprint is:

```text
absolute_offset = section_1_offset + data_offset
```

### 6.4. Fingerprint Data Blob

The fingerprint data blob is located at `section_1_offset`. Fingerprints are
stored sequentially without separators. Record boundaries are determined using
the record table.

### 6.5. Compression Method 0: XOR-Delta + Varint

When `compression_method = 0`, fingerprint data MUST be encoded as follows:

1. The first sub-fingerprint value is written as a raw little-endian `u32`.
2. Each subsequent sub-fingerprint is XORed with the previous value.
3. Each XOR delta is encoded as an unsigned LEB128 varint.

This method exploits the sparsity of XOR deltas between overlapping audio
frames.

### 6.6. Compression Method 1: XOR-Delta + PFOR Bitpacking

When `compression_method = 1`, fingerprint data MUST be encoded as follows:

1. Compute XOR deltas as in Section 6.5.
2. Partition the delta sequence into blocks of 128 values.
3. For each block, choose the minimum bit width `b` covering 90% of values.
4. Pack the block values at width `b`.
5. Store exceptions separately.

Each PFOR block has the following format:

```text
Offset  Size             Field
------  ----             -----
0x00    1                b
0x01    1                num_exceptions
0x02    (16 * b) bytes   packed values
...     variable         exception list [(index: u8, value: u32), ...]
```

For final partial blocks with fewer than 128 values, the value count is inferred
from `raw_count`.

### 6.7. Overflow Journal

The `.ckd` overflow journal provides append-only incremental updates.

The main record table and main fingerprint data blob MUST NOT be modified during
an incremental update. Only the overflow region and footer may be appended or
rewritten.

#### 6.7.1. Layout

If present, the overflow journal is located between the end of section 1 and the
final 16-byte footer.

```text
[ Overflow Header (16 bytes) ]
[ Overflow Record Table ]
[ Overflow Data Blob ]
```

The overflow header format is:

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    8     u8[8]   overflow_magic
0x08    4     u32     overflow_count
0x0C    4     u32     overflow_data_offset
```

`overflow_magic` MUST equal `CKAF-DO\x00`
(`43 4B 41 46 2D 44 4F 00`).

The overflow record table uses the same 20-byte record format as the main record
table. `data_offset` values in the overflow table are relative to the start of
the overflow data blob.

Overflow records MUST be sorted by `fingerprint_id`.

#### 6.7.2. Semantics

The overflow MAY contain:

* new fingerprints not present in the main table; and
* updated fingerprints that supersede entries in the main table.

If a fingerprint ID is present in both main and overflow tables, readers MUST
use the overflow version.

#### 6.7.3. Integrity Validation

Readers SHOULD validate the overflow region by checking:

1. that the footer-provided `overflow_offset` points to the correct
   `overflow_magic`;
2. that `overflow_count` and `overflow_data_offset` are consistent with file
   size; and
3. that no referenced record extends beyond the end of file.

If validation fails, readers SHOULD discard the overflow journal and continue
using the main data.

### 6.8. Footer

The last 16 bytes of a `.ckd` file are:

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    8     u64     overflow_offset
0x08    8     u8[8]   footer_magic
```

`footer_magic` MUST equal `CKAF-DF\x00`
(`43 4B 41 46 2D 44 46 00`).

`overflow_offset` is the absolute file offset of the overflow header, or zero if
no overflow is present.

### 6.9. Lookup Procedure

To retrieve a fingerprint by `fingerprint_id`, a reader:

1. MUST binary-search the main record table.
2. MUST binary-search the overflow record table if overflow exists.
3. MUST prefer the overflow record when both are found.
4. MUST read data from the corresponding data blob using the selected record’s
   `data_offset` and `data_length`.

### 6.10. Compaction

Compaction produces a new `.ckd` file by:

1. merge-sorting main and overflow records by `fingerprint_id`;
2. retaining overflow entries on duplicate IDs;
3. rewriting the fingerprint data blob with updated offsets; and
4. writing a new file with no overflow region.

The resulting file SHOULD replace the old file atomically.

## 7. The `.ckx` File

The `.ckx` file stores the inverted index used for similarity search.

### 7.1. Layout

```text
[ Header (96 bytes) ]
[ Tuning Configuration (64 bytes) ]
[ Bucket Directory ]        ; section 0
[ Posting Lists ]           ; section 1
[ Overflow Index ]          ; optional, located via footer
[ Footer (16 bytes) ]
```

### 7.2. Flags

The `.ckx` flags field is defined as follows:

```text
Bit 0: posting_compression
       0 = delta + varint
       1 = delta + PFOR

Bit 1: has_overflow
       0 = no overflow index
       1 = overflow index present

Bits 2-31: reserved, MUST be zero
```

### 7.3. Tuning Configuration

The 64-byte tuning configuration immediately follows the 96-byte common header.

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    1     u8      num_bands
0x01    1     u8      bits_per_band
0x02    4     u32     buckets_per_band
0x06    4     u32     total_buckets
0x0A    8     u64     total_postings
0x12    4     u32     avg_postings_per_bucket
0x16    1     u8      tuning_strategy
0x17    41    u8[41]  reserved
```

Field constraints:

* `num_bands * bits_per_band` MUST be less than or equal to 32.
* `buckets_per_band` MUST equal `2 ^ bits_per_band`.
* `total_buckets` MUST equal `num_bands * buckets_per_band`.
* `reserved` MUST be zero.

`tuning_strategy` is informational only:

```text
0x00 = manual
0x01 = auto_balanced
0x02 = auto_low_ram
0x03 = auto_speed
```

Readers MUST interpret the file using the stored parameters and MUST NOT infer
behavior from `tuning_strategy`.

### 7.4. Band Extraction

Each 32-bit sub-fingerprint is partitioned into `num_bands` contiguous slices of
`bits_per_band` bits:

```text
Band 0: bits [0 .. bits_per_band - 1]
Band 1: bits [bits_per_band .. 2*bits_per_band - 1]
...
Band k: bits [k*bits_per_band .. (k+1)*bits_per_band - 1]
```

If `num_bands * bits_per_band < 32`, the remaining high-order bits are ignored.

### 7.5. Bucket Directory

The bucket directory is located at `section_0_offset` and contains
`total_buckets` entries ordered by `(band_index, band_value)`.

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    8     u64     posting_offset
0x08    4     u32     posting_count
```

`posting_offset` is relative to the start of section 1.

The bucket index is computed as:

```text
bucket_index = band_index * buckets_per_band + band_value
```

### 7.6. Posting Lists

Posting lists are located at `section_1_offset`. Each posting list is a sorted
sequence of logical entries:

```text
fingerprint_id: u32
position:       u16
```

Entries MUST be sorted first by `fingerprint_id`, then by `position`.

### 7.7. Posting Compression Method 0: Delta + Varint

When `posting_compression = 0`, a posting list MUST be encoded as follows:

1. Write the first `fingerprint_id` as raw `u32`.
2. For each subsequent entry:
   * if the `fingerprint_id` is unchanged, write ID delta `0` as a varint and
     write `position` as raw `u16`;
   * otherwise, write the delta from the previous `fingerprint_id` as a varint
     and write `position` as raw `u16`.

### 7.8. Posting Compression Method 1: Delta + PFOR

When `posting_compression = 1`, fingerprint ID deltas MUST be packed into PFOR
blocks of 128 values. Positions MUST be stored as a parallel sequence of raw
`u16` values.

### 7.9. Overflow Index

The `.ckx` overflow index is append-only and located using the footer.

#### 7.9.1. Layout

```text
[ Overflow Header (16 bytes) ]
[ Overflow Bucket Directory ]
[ Overflow Posting Lists ]
```

Overflow header format:

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    8     u8[8]   overflow_magic
0x08    4     u32     overflow_record_count
0x0C    4     u32     overflow_bucket_count
```

`overflow_magic` MUST equal `CKAF-XO\x00`
(`43 4B 41 46 2D 58 4F 00`).

`overflow_bucket_count` MUST equal `total_buckets`.

The overflow bucket directory uses the same structure as the main bucket
directory. Buckets MAY have zero postings.

#### 7.9.2. Query Semantics

During query processing, readers MUST consult both the main and overflow
posting lists for each addressed bucket. The two result streams MAY be merged in
memory or streamed independently, provided both are considered.

### 7.10. Footer

The final 16 bytes of a `.ckx` file are:

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    8     u64     overflow_offset
0x08    8     u8[8]   footer_magic
```

`footer_magic` MUST equal `CKAF-XF\x00`
(`43 4B 41 46 2D 58 46 00`).

### 7.11. Compaction

Compaction merges overflow posting lists into the main posting lists. Because
both are sorted by `fingerprint_id` within each bucket, this MAY be implemented
as a streaming merge. The result is written as a new `.ckx` file.

Compaction MAY also be used to regenerate the index with different tuning
parameters.

## 8. The `.ckm` File

The `.ckm` file maps fingerprint IDs to MusicBrainz identifiers and optional
text metadata.

### 8.1. Layout

```text
[ Header (96 bytes) ]
[ Mapping Table ]       ; section 0
[ String Pool ]         ; section 1
[ Overflow Mappings ]   ; optional, located via footer
[ Footer (16 bytes) ]
```

### 8.2. Flags

The `.ckm` flags field is defined as follows:

```text
Bit 0: has_text_metadata
       0 = MusicBrainz IDs only
       1 = text metadata included

Bit 1: has_overflow
       0 = no overflow mappings
       1 = overflow mappings present

Bits 2-31: reserved, MUST be zero
```

### 8.3. Mapping Table

The mapping table is located at `section_0_offset` and contains fixed-width
32-byte records sorted by `fingerprint_id`.

```text
Offset  Size  Type      Field
------  ----  ----      -----
0x00    4     u32       fingerprint_id
0x04    16    u8[16]    mbid
0x14    4     u32       track_id
0x18    4     u32       string_offset
0x1C    4     u32       string_length
```

Field definitions:

* `mbid`: MusicBrainz recording UUID encoded as raw 16 bytes.
* `track_id`: internal grouping or track identifier.
* `string_offset`: offset into section 1, or `0xFFFFFFFF` if no text metadata
  is associated with the record.
* `string_length`: length in bytes of the associated string-pool entry.

Only mapped fingerprints appear in the `.ckm` table.

### 8.4. String Pool

The string pool is located at `section_1_offset`. It consists of UTF-8 encoded
key-value blocks of the form:

```text
t=Track Title
a=Artist Name
r=Release Title
y=2024
```

The following keys are defined:

| Key | Meaning       |
|-----|---------------|
| `t` | Track title   |
| `a` | Artist name   |
| `r` | Release title |
| `y` | Release year  |

If `has_text_metadata = 0`, section 1 MAY be empty.

### 8.5. Overflow Mappings

The `.ckm` overflow region is append-only and has the following layout:

```text
[ Overflow Header (16 bytes) ]
[ Overflow Mapping Table ]
[ Overflow String Pool ]
```

Overflow header format:

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    8     u8[8]   overflow_magic
0x08    4     u32     overflow_count
0x0C    4     u32     overflow_strings_offset
```

`overflow_magic` MUST equal `CKAF-MO\x00`
(`43 4B 41 46 2D 4D 4F 00`).

Overflow mapping records use the same format as main mapping records.
`string_offset` values in overflow records are relative to the overflow string
pool, not the main string pool.

If a `fingerprint_id` exists in both main and overflow mapping tables, the
overflow record MUST supersede the main record.

### 8.6. Footer

The final 16 bytes of a `.ckm` file are:

```text
Offset  Size  Type    Field
------  ----  ----    -----
0x00    8     u64     overflow_offset
0x08    8     u8[8]   footer_magic
```

`footer_magic` MUST equal `CKAF-MF\x00`
(`43 4B 41 46 2D 4D 46 00`).

### 8.7. Compaction

Compaction merges overflow mappings into the main mapping table, rewrites string
pool offsets as needed, and produces a new `.ckm` file. The new file SHOULD
replace the old file atomically.

## 9. Query Processing

### 9.1. Similarity Lookup

Given a query fingerprint consisting of an array of `u32` sub-fingerprints and
an associated duration, a reader or server performs the following operations.

#### 9.1.1. Band Extraction

Read the tuning configuration from `.ckx`. For each query sub-fingerprint,
extract `num_bands` band values.

#### 9.1.2. Bucket Lookup

For each `(band_index, band_value)` pair, compute the bucket index and resolve
its posting list from the bucket directory.

#### 9.1.3. Candidate Collection

Read the corresponding posting lists from main and overflow index regions, if
present. Collect `(fingerprint_id, position)` pairs and count the number of
occurrences per `fingerprint_id`.

#### 9.1.4. Threshold Filtering

A recommended adaptive threshold is:

```text
min_matches = max(3, query_raw_count * num_bands * 0.02)
```

Candidates with fewer matches MAY be discarded before detailed comparison.

#### 9.1.5. Detailed Comparison

For each remaining candidate:

1. retrieve the compressed fingerprint from `.ckd`;
2. decompress it;
3. align query and candidate using posting-list position hints; and
4. compute bit error rate over the aligned overlap window.

#### 9.1.6. Ranking

A recommended interpretation of bit error rate is:

* `< 0.15` — strong match;
* `< 0.25` — likely match; and
* `< 0.35` — weak match.

These thresholds are implementation guidance and are not part of the binary
format.

#### 9.1.7. Metadata Resolution

Matched `fingerprint_id` values MAY be resolved through `.ckm` using binary
search over main and overflow mapping tables.

## 10. Import and Update Procedures

### 10.1. Full Build

A full build from source data SHOULD proceed as follows:

1. parse source fingerprint and metadata inputs;
2. build `.ckd` by compressing fingerprints and writing the main record table
   and data blob;
3. build `.ckx` by selecting tuning parameters, extracting bucket assignments,
   and writing posting lists and bucket directory;
4. build `.ckm` by writing mapping records and optional string-pool data; and
5. write a common `dataset_id` to all three file headers.

### 10.2. Incremental Update

An incremental update appends overflow regions only.

For `.ckd`, `.ckx`, and `.ckm`, implementations SHOULD:

1. build the overflow structure in sorted form;
2. append the overflow region after section 1;
3. rewrite the final 16-byte footer with the overflow offset.

If the process fails during append, existing main sections remain valid.
Readers SHOULD detect incomplete or corrupt overflow regions and discard them.

### 10.3. Compaction Thresholds

Implementations SHOULD compact when overflow size exceeds approximately 5–10%
of the corresponding main record count, or according to an operational schedule.

## 11. Versioning and Compatibility

The following compatibility rules apply:

* a major version change indicates a breaking format change;
* a minor version change indicates a backward-compatible addition;
* unknown non-zero flag bits MUST cause file rejection;
* reserved fields SHOULD be zero, and non-zero values SHOULD generate a
  warning;
* `dataset_id` mismatches SHOULD generate a warning but need not prevent
  loading; and
* overflow magic values MUST be validated before an overflow region is used.

## 12. Security and Robustness Considerations

Readers MUST treat all on-disk offsets and lengths as untrusted input.
Implementations SHOULD validate, at minimum:

* section offsets and lengths against total file size;
* record-table bounds before dereferencing offsets;
* overflow offsets and magic values;
* compression block boundaries; and
* integer arithmetic used in offset calculations for overflow or wraparound.

Malformed files MUST NOT cause out-of-bounds reads, excessive allocation, or
undefined behavior.

Because CKAF uses append-only overflow regions, implementations SHOULD ensure
that atomic replacement semantics are used during compaction to avoid partial
file replacement.

## Appendix A. Size Budget (Illustrative)

For a full dataset on the order of 90 million fingerprints:

| Component                | Records   | Per Record  | Total        |
|--------------------------|-----------|-------------|--------------|
| `.ckd` record table      | 90M       | 20 B        | ~1.80 GB     |
| `.ckd` fingerprint data  | 90M       | ~1.2–1.5 KB | ~11–14 GB    |
| `.ckx` bucket directory  | 1K–32K    | 12 B        | ~12–384 KB   |
| `.ckx` posting lists     | billions  | compressed  | ~30–60 GB    |
| `.ckm` mapping table     | 20.5M     | 32 B        | ~656 MB      |
| `.ckm` string pool       | 20.5M     | ~80 B avg   | ~1.6 GB      |
| **Total**                |           |             | **~45–78 GB** |

These figures are illustrative and depend on corpus characteristics and chosen
compression settings.

## Appendix B. Runtime RAM Budget (Illustrative)

| Component                        | Size           | Residency            |
|----------------------------------|----------------|----------------------|
| `.ckd` record table (mmap)       | ~1.80 GB       | Demand-paged by OS   |
| `.ckx` bucket directory          | ~12–384 KB     | Always resident      |
| `.ckx` overflow bucket directory | ~12–384 KB     | Always resident      |
| OS page cache                    | 128 MB–1 GB    | Tunable              |
| Query working memory             | ~1–12 MB       | Per concurrent query |
| **Typical RSS**                  | **128–512 MB** |                      |

These values are operational guidance rather than wire-format requirements.

## Appendix C. Reference Tuning Profiles (Illustrative)

| Profile                 | RAM    | Bands | Bits/Band | Buckets | Est. Index Size |
|-------------------------|--------|-------|-----------|---------|-----------------|
| Raspberry Pi (1 GB)     | 256 MB | 3     | 7         | 384     | ~20 GB          |
| Budget VPS (2 GB)       | 512 MB | 4     | 8         | 1,024   | ~35 GB          |
| Mid-range server (8 GB) | 2 GB   | 5     | 9         | 2,560   | ~45 GB          |
| High-end (32 GB+)       | 8 GB   | 6     | 10        | 6,144   | ~55 GB          |

More bands and more bits per band generally increase selectivity at the cost of
larger index structures.

## Appendix D. Auto-Tuning Guidance (Non-Normative)

This appendix is non-normative. Readers do not need to understand this section
to parse CKAF files.

Builders MAY select `(num_bands, bits_per_band)` based on dataset size,
available RAM, storage budget, and implementation goals.

A recommended procedure is:

1. sample approximately 10,000 fingerprints to estimate `avg_raw_count`;
2. enumerate candidate `(num_bands, bits_per_band)` pairs subject to
   `num_bands * bits_per_band <= 32`;
3. estimate posting volume, index size, and hot-cache RAM usage;
4. filter out candidates that exceed resource constraints; and
5. score remaining candidates according to a chosen optimization strategy.

If no candidate satisfies the constraints, a reasonable default is:

```text
num_bands = 4
bits_per_band = 8
```
