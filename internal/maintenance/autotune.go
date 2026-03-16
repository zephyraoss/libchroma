package maintenance

import "github.com/zephyraoss/libchroma/internal/cktype"

// AutoTuneParams selects LSH parameters based on dataset size and constraints.
func AutoTuneParams(recordCount uint64, strategy cktype.TuningStrategy, availableRAM, storageBudget uint64) cktype.TuningConfig {
	type candidate struct {
		bands   uint8
		bits    uint8
		buckets uint32
		score   int64
	}

	var best *candidate

	for bits := uint8(4); bits <= 16; bits++ {
		for bands := uint8(1); bands <= 8; bands++ {
			if uint16(bands)*uint16(bits) > 32 {
				continue
			}

			bucketsPerBand := uint32(1) << bits
			totalBuckets := uint32(bands) * bucketsPerBand

			avgSubFPs := uint64(120)
			estPostings := recordCount * avgSubFPs * uint64(bands)
			estDirSize := uint64(totalBuckets) * 12
			estPostingSize := estPostings * 6
			estSize := estDirSize + estPostingSize

			estRAM := estDirSize

			if storageBudget > 0 && estSize > storageBudget {
				continue
			}
			if availableRAM > 0 && estRAM > availableRAM {
				continue
			}

			var score int64
			switch strategy {
			case cktype.TuneSpeed:
				score = int64(bands)*100 - int64(bits)*10
			case cktype.TuneLowRAM:
				score = -int64(totalBuckets)
			case cktype.TuneBalanced:
				score = int64(bands)*50 - int64(estSize>>20)
			default:
				score = -abs64(int64(bands)-4)*10 - abs64(int64(bits)-8)*10
			}

			c := candidate{bands: bands, bits: bits, buckets: totalBuckets, score: score}
			if best == nil || c.score > best.score {
				best = &c
			}
		}
	}

	if best == nil {
		return cktype.TuningConfig{
			NumBands:       4,
			BitsPerBand:    8,
			BucketsPerBand: 256,
			TotalBuckets:   1024,
			Strategy:       strategy,
		}
	}

	return cktype.TuningConfig{
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
