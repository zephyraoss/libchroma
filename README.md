# libchroma

A Go library for reading, writing, and querying [CKAF](ckaf_rfc_draft.md) (Chromakopia Acoustic Fingerprint) files.

CKAF is a compact binary format for acoustic fingerprint data. It stores fingerprints, search indices, and track metadata across three file types (`.ckd`, `.ckx`, `.ckm`) and uses memory-mapped I/O so you can query large datasets without loading them into RAM.

## Install

```
go get github.com/zephyraoss/libchroma
```

Requires Go 1.24+.

## Usage

### Open a dataset and query it

```go
dataset, err := chroma.Open("path/to/acoustid")
if err != nil {
    log.Fatal(err)
}
defer dataset.Close()

results, err := dataset.Query(fingerprint, durationMs, nil)
```

`Open` expects a path prefix. It loads `prefix.ckd`, `prefix.ckx`, and (if present) `prefix.ckm`, then verifies that all files belong to the same dataset.

### Look up a fingerprint by ID

```go
fp, err := dataset.Lookup(fingerprintID)
// fp.Values contains the raw sub-fingerprint hashes
// fp.DurationMs is the audio duration
```

### Build new files

Writers are single-threaded and produce files in one pass:

```go
w, err := chroma.NewDataStoreWriter("out.ckd", chroma.DataStoreWriterOptions{
    DatasetID: datasetID,
})
// add records...
err = w.Close()
```

Similar writers exist for search indices (`NewSearchIndexWriter`) and metadata maps (`NewMetadataWriter`).

### Query options

```go
results, err := dataset.Query(fp, dur, &chroma.QueryOptions{
    MaxCandidates:     200,
    MinMatchThreshold: 3,
    MaxBitErrorRate:   0.30,
    IncludeMetadata:   true,
})
```

### Compaction

Appended records go into an overflow region. When the overflow grows too large, compact it back into the main table:

```go
if dataset.NeedsCompaction(10.0) { // 10% threshold
    err := chroma.Compact("path/to/acoustid", chroma.CompactOptions{})
}
```

## File format

The full spec is in [ckaf_rfc_draft.md](ckaf_rfc_draft.md). The short version:

- All integers are little-endian, sections are 8-byte aligned.
- `.ckd` (datastore) holds compressed fingerprint data with a sorted record table. Compression uses XOR-delta encoding with either varint or PFOR packing.
- `.ckx` (search index) maps sub-fingerprint hashes to posting lists for similarity search.
- `.ckm` (metadata map) links fingerprint IDs to MusicBrainz UUIDs and track info.

All three files share a common 96-byte header and 16-byte footer.

## License

Apache 2.0 -- see [LICENSE](LICENSE) and [NOTICE](NOTICE).
