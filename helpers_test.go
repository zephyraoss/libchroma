package chroma

func generateTestFingerprint(id uint32, count int) (uint32, uint32, []uint32) {
	values := make([]uint32, count)
	rng := id
	for i := range values {
		rng = rng*1103515245 + 12345
		values[i] = rng
	}
	return id, uint32(count) * 100, values
}
