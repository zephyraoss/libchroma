package chroma

// AutoTuneParams selects LSH parameters based on dataset size and constraints.
// It tries candidate band/bits pairs where num_bands * bits_per_band <= 32,
// and picks the best fit for the given strategy and resource constraints.
func AutoTuneParams(recordCount uint64, strategy TuningStrategy, availableRAM, storageBudget uint64) TuningConfig {
	type candidate struct {
		bands   uint8
		bits    uint8
		buckets uint32
		score   int64
	}

	var best *candidate

	// Enumerate candidates: bands * bits <= 32, bits in [4..16], bands in [1..8].
	for bits := uint8(4); bits <= 16; bits++ {
		for bands := uint8(1); bands <= 8; bands++ {
			if uint16(bands)*uint16(bits) > 32 {
				continue
			}

			bucketsPerBand := uint32(1) << bits
			totalBuckets := uint32(bands) * bucketsPerBand

			// Estimate index size: bucket directory + posting data.
			// Each bucket entry = 12 bytes.
			// Avg postings per bucket ~ recordCount * avgSubFPs / totalBuckets.
			// Each posting ~ 6 bytes compressed avg.
			avgSubFPs := uint64(120) // typical fingerprint length
			estPostings := recordCount * avgSubFPs * uint64(bands)
			estDirSize := uint64(totalBuckets) * 12
			estPostingSize := estPostings * 6
			estSize := estDirSize + estPostingSize

			// Estimate RAM: bucket directory is memory-mapped.
			estRAM := estDirSize

			if storageBudget > 0 && estSize > storageBudget {
				continue
			}
			if availableRAM > 0 && estRAM > availableRAM {
				continue
			}

			var score int64
			switch strategy {
			case TuneSpeed:
				// Prefer more bands (better recall), fewer bits (smaller buckets).
				score = int64(bands)*100 - int64(bits)*10
			case TuneLowRAM:
				// Prefer fewer total buckets.
				score = -int64(totalBuckets)
			case TuneBalanced:
				// Balance recall and size.
				score = int64(bands)*50 - int64(estSize>>20)
			default:
				// Manual: prefer the default 4/8 config.
				score = -abs64(int64(bands)-4)*10 - abs64(int64(bits)-8)*10
			}

			c := candidate{bands: bands, bits: bits, buckets: totalBuckets, score: score}
			if best == nil || c.score > best.score {
				best = &c
			}
		}
	}

	if best == nil {
		// Fallback to default.
		return TuningConfig{
			NumBands:       4,
			BitsPerBand:    8,
			BucketsPerBand: 256,
			TotalBuckets:   1024,
			Strategy:       strategy,
		}
	}

	return TuningConfig{
		NumBands:       best.bands,
		BitsPerBand:    best.bits,
		BucketsPerBand: 1 << best.bits,
		TotalBuckets:   best.buckets,
		Strategy:       strategy,
	}
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
