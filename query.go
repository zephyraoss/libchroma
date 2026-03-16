package chroma

import (
	"math/bits"
	"sort"

	"github.com/zephyraoss/libchroma/internal/datastore"
	"github.com/zephyraoss/libchroma/internal/metadata"
	"github.com/zephyraoss/libchroma/internal/searchindex"
)

type candidate struct {
	count    int
	bestPos  int
	queryPos int
}

func defaultQueryOptions() QueryOptions {
	return QueryOptions{
		MaxCandidates:     100,
		MinMatchThreshold: 0,
		MaxBitErrorRate:   0.35,
		IncludeMetadata:   true,
	}
}

// QueryDataset performs a full similarity lookup across the dataset.
func QueryDataset(ds *DataStore, si *SearchIndex, mm *MetadataMap, fingerprint []uint32, durationMs uint32, opts *QueryOptions) ([]MatchResult, error) {
	return queryDataset(ds, si, mm, fingerprint, durationMs, opts)
}

func queryDataset(ds *datastore.DataStore, si *searchindex.SearchIndex, mm *metadata.MetadataMap, fingerprint []uint32, durationMs uint32, opts *QueryOptions) ([]MatchResult, error) {
	if len(fingerprint) == 0 {
		return nil, nil
	}

	o := defaultQueryOptions()
	if opts != nil {
		if opts.MaxCandidates > 0 {
			o.MaxCandidates = opts.MaxCandidates
		}
		if opts.MinMatchThreshold > 0 {
			o.MinMatchThreshold = opts.MinMatchThreshold
		}
		if opts.MaxBitErrorRate > 0 {
			o.MaxBitErrorRate = opts.MaxBitErrorRate
		}
		o.IncludeMetadata = opts.IncludeMetadata
	}

	candidates := make(map[uint32]*candidate)
	numBands := si.Tuning.NumBands

	for queryPos, subFP := range fingerprint {
		bands := si.ExtractBands(subFP)
		for k := uint8(0); k < numBands; k++ {
			bucketIdx := uint32(k)*si.Tuning.BucketsPerBand + bands[k]

			entries, err := si.ReadPostingList(bucketIdx)
			if err != nil {
				return nil, err
			}

			for _, e := range entries {
				c, ok := candidates[e.FingerprintID]
				if !ok {
					candidates[e.FingerprintID] = &candidate{
						count:    1,
						bestPos:  int(e.Position),
						queryPos: queryPos,
					}
				} else {
					c.count++
				}
			}

			if si.HasOvfl {
				oEntries, err := si.ReadOverflowPostingList(bucketIdx)
				if err != nil {
					return nil, err
				}
				for _, e := range oEntries {
					c, ok := candidates[e.FingerprintID]
					if !ok {
						candidates[e.FingerprintID] = &candidate{
							count:    1,
							bestPos:  int(e.Position),
							queryPos: queryPos,
						}
					} else {
						c.count++
					}
				}
			}
		}
	}

	minMatches := 3
	adaptive := int(float64(len(fingerprint)*int(numBands)) * 0.02)
	if adaptive > minMatches {
		minMatches = adaptive
	}
	if o.MinMatchThreshold > 0 {
		minMatches = o.MinMatchThreshold
	}

	type candidateEntry struct {
		id uint32
		c  *candidate
	}
	var filtered []candidateEntry
	for id, c := range candidates {
		if c.count >= minMatches {
			filtered = append(filtered, candidateEntry{id: id, c: c})
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].c.count > filtered[j].c.count
	})
	if len(filtered) > o.MaxCandidates {
		filtered = filtered[:o.MaxCandidates]
	}

	var matches []Match
	for _, ce := range filtered {
		rec, err := ds.Lookup(ce.id)
		if err != nil {
			continue
		}
		fp, err := ds.ReadFingerprint(rec)
		if err != nil {
			continue
		}

		offset := ce.c.bestPos - ce.c.queryPos
		dist, totalBits := hammingDistance(fingerprint, fp.Values, offset)
		if totalBits == 0 {
			continue
		}

		ber := float64(dist) / float64(totalBits)
		if ber >= o.MaxBitErrorRate {
			continue
		}

		score := classifyBER(ber)
		if score == MatchNone {
			continue
		}

		matches = append(matches, Match{
			FingerprintID: ce.id,
			BitErrorRate:  ber,
			Score:         score,
		})
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].BitErrorRate < matches[j].BitErrorRate
	})

	results := make([]MatchResult, len(matches))
	for i, m := range matches {
		results[i] = MatchResult{Match: m}

		if mm != nil && o.IncludeMetadata {
			mr, err := mm.Lookup(m.FingerprintID)
			if err == nil {
				mbid := mr.MBID
				results[i].MBID = &mbid
				meta, err := mm.ReadMetadata(mr)
				if err == nil {
					results[i].Metadata = meta
				}
			}
		}
	}

	return results, nil
}

func hammingDistance(a, b []uint32, offset int) (int, int) {
	var dist, totalBits int
	startA, startB := 0, 0
	if offset > 0 {
		startB = offset
	} else {
		startA = -offset
	}
	for i := 0; startA+i < len(a) && startB+i < len(b); i++ {
		dist += bits.OnesCount32(a[startA+i] ^ b[startB+i])
		totalBits += 32
	}
	return dist, totalBits
}

func classifyBER(ber float64) MatchScore {
	switch {
	case ber < 0.15:
		return MatchStrong
	case ber < 0.25:
		return MatchLikely
	case ber < 0.35:
		return MatchWeak
	default:
		return MatchNone
	}
}
