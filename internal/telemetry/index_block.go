package telemetry

import (
	"encoding/binary"
	"os"
	"sort"
)

// indexBlockEntries is the probe unit of trace.fidx and logs.fidx. Loading a
// batch keeps the first hash of every block, so a probe binary-searches those
// fences in memory and reads one block.
const indexBlockEntries = 256

// probeBlock reads the encoded entries of the block that can hold hash, or
// nil when hash sorts before every entry. Each entry leads with its u64 hash.
func probeBlock(file *os.File, fences []uint64, entries uint64, headerSize, entrySize int, hash uint64) ([]byte, error) {
	block := sort.Search(len(fences), func(i int) bool { return fences[i] > hash }) - 1
	if block < 0 {
		return nil, nil
	}
	first := uint64(block) * indexBlockEntries
	count := min(uint64(indexBlockEntries), entries-first)
	encoded := make([]byte, int(count)*entrySize)
	if _, err := file.ReadAt(encoded, int64(headerSize)+int64(first)*int64(entrySize)); err != nil {
		return nil, err
	}
	return encoded, nil
}

// searchBlock returns the byte offset of hash's entry in a block.
func searchBlock(block []byte, entrySize int, hash uint64) (int, bool) {
	n := len(block) / entrySize
	i := sort.Search(n, func(i int) bool { return binary.LittleEndian.Uint64(block[i*entrySize:]) >= hash })
	if i == n || binary.LittleEndian.Uint64(block[i*entrySize:]) != hash {
		return 0, false
	}
	return i * entrySize, true
}
