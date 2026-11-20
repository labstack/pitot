package telemetry

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"

	"github.com/parquet-go/parquet-go"
	"github.com/zeebo/xxh3"
)

// logs.fidx maps a trace hash to the logs.parquet rows that carry the trace:
// a header (magic, entry count, posting count), entries sorted by hash, and
// one u32 row number per traced row, grouped by entry and ascending within it.
// It is a pure function of logs.parquet; commit, compaction and offline
// verification all derive it with logIndexPasses.
const (
	logIndexMagic      = "PITLIDX1"
	logIndexHeaderSize = 24
	logIndexEntrySize  = 16
	logIndexRowSize    = 4
	// logIndexPassRows bounds the (hash, row) pairs one build pass sorts, 16
	// bytes each. A larger file is built in passes over disjoint hash ranges.
	logIndexPassRows = 8 << 20
	// logIndexBuckets splits the hash space for planning passes by its top
	// 16 bits.
	logIndexBuckets = 1 << 16
)

type logPosting struct {
	hash uint64
	row  uint32
}

type logRange struct {
	hash   uint64
	offset uint32
	count  uint32
}

// logIndex keeps the fences of the entries in memory; entries and postings
// stay in the sidecar.
type logIndex struct {
	path     string
	rows     uint64
	entries  uint64
	postings uint64
	fences   []uint64
}

// writeLogIndex builds path from the logs file just written at logsPath.
func writeLogIndex(logsPath, path string, rows int) error {
	return buildLogIndex(logsPath, path, rows, logIndexPassRows)
}

func buildLogIndex(logsPath, path string, rows, passRows int) (err error) {
	if uint64(rows) > math.MaxUint32 {
		return fmt.Errorf("logs file has %d rows; the log index addresses at most %d", rows, uint64(math.MaxUint32))
	}
	writer, err := newLogIndexWriter(path, rows > passRows)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			writer.Abort()
		}
	}()
	if err := logIndexPasses(logsPath, rows, passRows, writer.Pass); err != nil {
		return err
	}
	return writer.Close()
}

// logIndexPasses calls visit with the file's (hash, row) pairs of nonempty
// trace IDs, sorted by hash and then row. Each pass covers a disjoint, higher
// hash range than the last, so the passes concatenate in index order. A file
// above passRows is first counted by hash bucket, and each pass then takes
// whole buckets up to passRows pairs. One bucket can still exceed passRows
// only when many rows share few trace IDs; the pairs of a pass never exceed
// the file's rows, which compaction caps at CompactionMaxRows.
func logIndexPasses(path string, rows, passRows int, visit func([]logPosting) error) error {
	if rows <= passRows {
		pairs := make([]logPosting, 0, rows)
		if err := scanLogTraceHashes(path, rows, func(row uint32, hash uint64) {
			pairs = append(pairs, logPosting{hash, row})
		}); err != nil {
			return err
		}
		sortLogPostings(pairs)
		return visit(pairs)
	}
	counts := make([]uint32, logIndexBuckets)
	if err := scanLogTraceHashes(path, rows, func(_ uint32, hash uint64) { counts[hash>>48]++ }); err != nil {
		return err
	}
	var pairs []logPosting
	for low := 0; low < logIndexBuckets; {
		high, total := low, 0
		for high < logIndexBuckets && (high == low || total+int(counts[high]) <= passRows) {
			total += int(counts[high])
			high++
		}
		if total > 0 {
			pairs = slices.Grow(pairs[:0], total)
			first, last := uint64(low), uint64(high)
			if err := scanLogTraceHashes(path, rows, func(row uint32, hash uint64) {
				if bucket := hash >> 48; bucket >= first && bucket < last {
					pairs = append(pairs, logPosting{hash, row})
				}
			}); err != nil {
				return err
			}
			if len(pairs) != total {
				return fmt.Errorf("log index pass read %d traced rows; the count pass found %d", len(pairs), total)
			}
			sortLogPostings(pairs)
			if err := visit(pairs); err != nil {
				return err
			}
		}
		low = high
	}
	return nil
}

func sortLogPostings(pairs []logPosting) {
	slices.SortFunc(pairs, func(a, b logPosting) int {
		if c := cmp.Compare(a.hash, b.hash); c != 0 {
			return c
		}
		return cmp.Compare(a.row, b.row)
	})
}

// scanLogTraceHashes decodes the trace ID column chunk by chunk, without
// assembling rows, and visits the hash of every nonempty value with its row
// number.
func scanLogTraceHashes(path string, expected int, visit func(uint32, uint64)) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	parsed, err := parquet.OpenFile(file, info.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true), parquet.ReadBufferSize(1<<20))
	if err != nil {
		return err
	}
	leaf, ok := parsed.Schema().Lookup("trace_id")
	if !ok || leaf.MaxRepetitionLevel != 0 || leaf.MaxDefinitionLevel != 0 {
		return fmt.Errorf("%w: logs trace_id is not a required scalar column", ErrUnsupportedBatchFormat)
	}
	values := make([]parquet.Value, 4096)
	var rows uint64
	for _, group := range parsed.RowGroups() {
		groupRows := uint64(group.NumRows())
		if rows+groupRows > uint64(expected) {
			return fmt.Errorf("logs file has more than the %d rows metadata declares", expected)
		}
		read := uint64(0)
		pages := group.ColumnChunks()[leaf.ColumnIndex].Pages()
		for {
			page, pageErr := pages.ReadPage()
			if errors.Is(pageErr, io.EOF) {
				break
			}
			if pageErr != nil {
				return errors.Join(pageErr, pages.Close())
			}
			reader := page.Values()
			for {
				n, readErr := reader.ReadValues(values)
				if read+uint64(n) > groupRows {
					parquet.Release(page)
					return errors.Join(fmt.Errorf("logs trace_id chunk has more values than its %d rows", groupRows), pages.Close())
				}
				for _, value := range values[:n] {
					if id := value.ByteArray(); len(id) > 0 {
						visit(uint32(rows+read), xxh3.Hash(id))
					}
					read++
				}
				if errors.Is(readErr, io.EOF) {
					break
				}
				if readErr != nil {
					parquet.Release(page)
					return errors.Join(readErr, pages.Close())
				}
				if n == 0 {
					parquet.Release(page)
					return errors.Join(io.ErrNoProgress, pages.Close())
				}
			}
			parquet.Release(page)
		}
		if err := pages.Close(); err != nil {
			return err
		}
		if read != groupRows {
			return fmt.Errorf("logs trace_id chunk has %d values for %d rows", read, groupRows)
		}
		rows += groupRows
	}
	if rows != uint64(expected) {
		return fmt.Errorf("decoded %d log rows; metadata declares %d", rows, expected)
	}
	return nil
}

// logIndexWriter writes entries as passes arrive. Postings follow every entry
// in the file, so a single pass keeps its pairs until Close, and a multi-pass
// build spools postings to a sibling file that Close appends.
type logIndexWriter struct {
	file     *os.File
	entries  *bufio.Writer
	spool    *os.File
	spooled  *bufio.Writer
	held     []logPosting
	path     string
	last     uint64
	nEntries uint64
	nRows    uint64
	have     bool
	closed   bool
}

func newLogIndexWriter(path string, spool bool) (*logIndexWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &logIndexWriter{file: file, path: path}
	if _, err := file.Write(make([]byte, logIndexHeaderSize)); err != nil {
		w.Abort()
		return nil, err
	}
	w.entries = bufio.NewWriterSize(file, 64<<10)
	if spool {
		if w.spool, err = os.OpenFile(path+".postings", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644); err != nil {
			w.Abort()
			return nil, err
		}
		w.spooled = bufio.NewWriterSize(w.spool, 64<<10)
	}
	return w, nil
}

// Pass appends one pass of pairs, sorted by hash and row, whose hashes all
// exceed those of earlier passes.
func (w *logIndexWriter) Pass(pairs []logPosting) error {
	if w.closed {
		return errors.New("append to closed log index")
	}
	if w.spool == nil && w.held != nil {
		return errors.New("single-pass log index received a second pass")
	}
	var encoded [logIndexEntrySize]byte
	for start := 0; start < len(pairs); {
		hash := pairs[start].hash
		if w.have && hash <= w.last {
			return errors.New("log index input is not sorted by hash")
		}
		end := start + 1
		for end < len(pairs) && pairs[end].hash == hash {
			if pairs[end].row <= pairs[end-1].row {
				return errors.New("log index rows are not ascending within a hash")
			}
			end++
		}
		if w.nRows+uint64(end-start) > math.MaxUint32 {
			return errors.New("log index exceeds u32 postings")
		}
		binary.LittleEndian.PutUint64(encoded[0:8], hash)
		binary.LittleEndian.PutUint32(encoded[8:12], uint32(w.nRows))
		binary.LittleEndian.PutUint32(encoded[12:16], uint32(end-start))
		if _, err := w.entries.Write(encoded[:]); err != nil {
			return err
		}
		if w.spooled != nil {
			if err := writeLogRows(w.spooled, pairs[start:end]); err != nil {
				return err
			}
		}
		w.nEntries++
		w.nRows += uint64(end - start)
		w.last, w.have = hash, true
		start = end
	}
	if w.spool == nil {
		w.held = pairs[:len(pairs):len(pairs)]
	}
	return nil
}

func writeLogRows(out *bufio.Writer, pairs []logPosting) error {
	var encoded [logIndexRowSize]byte
	for _, pair := range pairs {
		binary.LittleEndian.PutUint32(encoded[:], pair.row)
		if _, err := out.Write(encoded[:]); err != nil {
			return err
		}
	}
	return nil
}

func (w *logIndexWriter) Close() error {
	if w.closed {
		return nil
	}
	if w.spool != nil {
		if err := w.spooled.Flush(); err != nil {
			return w.fail(err)
		}
		if _, err := w.spool.Seek(0, io.SeekStart); err != nil {
			return w.fail(err)
		}
		if _, err := io.Copy(w.entries, w.spool); err != nil {
			return w.fail(err)
		}
	} else if err := writeLogRows(w.entries, w.held); err != nil {
		return w.fail(err)
	}
	if err := w.entries.Flush(); err != nil {
		return w.fail(err)
	}
	var header [logIndexHeaderSize]byte
	copy(header[:], logIndexMagic)
	binary.LittleEndian.PutUint64(header[8:16], w.nEntries)
	binary.LittleEndian.PutUint64(header[16:24], w.nRows)
	if _, err := w.file.WriteAt(header[:], 0); err != nil {
		return w.fail(err)
	}
	if err := w.file.Sync(); err != nil {
		return w.fail(err)
	}
	w.closed = true
	w.dropSpool()
	if err := w.file.Close(); err != nil {
		_ = os.Remove(w.path)
		return err
	}
	return nil
}

func (w *logIndexWriter) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	w.dropSpool()
	_ = w.file.Close()
	_ = os.Remove(w.path)
}

func (w *logIndexWriter) fail(err error) error {
	w.Abort()
	return err
}

func (w *logIndexWriter) dropSpool() {
	if w.spool != nil {
		_ = w.spool.Close()
		_ = os.Remove(w.spool.Name())
		w.spool = nil
	}
}

// loadLogIndex checks the header and walks the entries, keeping every
// block's first hash. A block is one 4 KiB page, so the walk reads no more
// than collecting the fences would. Postings are checked as probes read them
// and in full by offline verification.
func loadLogIndex(path string, rows int) (index logIndex, err error) {
	file, err := os.Open(path)
	if err != nil {
		return logIndex{}, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return logIndex{}, err
	}
	if !info.Mode().IsRegular() {
		return logIndex{}, errors.New("log index is not a regular file")
	}
	var header [logIndexHeaderSize]byte
	if _, err := io.ReadFull(file, header[:]); err != nil || string(header[:8]) != logIndexMagic {
		return logIndex{}, errors.New("invalid log index header")
	}
	entries := binary.LittleEndian.Uint64(header[8:16])
	postings := binary.LittleEndian.Uint64(header[16:24])
	if postings > uint64(rows) || entries > postings || (entries == 0) != (postings == 0) ||
		info.Size() != logIndexHeaderSize+int64(entries)*logIndexEntrySize+int64(postings)*logIndexRowSize {
		return logIndex{}, errors.New("invalid log index length")
	}
	index = logIndex{path: path, rows: uint64(rows), entries: entries, postings: postings, fences: make([]uint64, 0, (entries+indexBlockEntries-1)/indexBlockEntries)}
	reader := bufio.NewReaderSize(file, 64<<10)
	var encoded [logIndexEntrySize]byte
	var previous logRange
	var covered uint64
	for i := uint64(0); i < entries; i++ {
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return logIndex{}, err
		}
		entry := decodeLogRange(encoded[:])
		if entry.count == 0 || uint64(entry.offset) != covered || (i > 0 && entry.hash <= previous.hash) {
			return logIndex{}, errors.New("log index entries are not strictly sorted and contiguous")
		}
		if i%indexBlockEntries == 0 {
			index.fences = append(index.fences, entry.hash)
		}
		covered += uint64(entry.count)
		previous = entry
	}
	if covered != postings {
		return logIndex{}, fmt.Errorf("log index entries cover %d of %d postings", covered, postings)
	}
	return index, nil
}

// Lookup returns the ascending log rows posted under hash: one block read
// from the fences, then one read of the entry's postings.
func (index logIndex) Lookup(hash uint64) (rows []uint32, err error) {
	if index.entries == 0 {
		return nil, nil
	}
	file, err := os.Open(index.path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	block, err := probeBlock(file, index.fences, index.entries, logIndexHeaderSize, logIndexEntrySize, hash)
	if err != nil || block == nil {
		return nil, err
	}
	position, found := searchBlock(block, logIndexEntrySize, hash)
	if !found {
		return nil, nil
	}
	entry := decodeLogRange(block[position:])
	if uint64(entry.offset)+uint64(entry.count) > index.postings {
		return nil, errors.New("log index entry exceeds its postings")
	}
	encoded := make([]byte, int(entry.count)*logIndexRowSize)
	offset := int64(logIndexHeaderSize) + int64(index.entries)*logIndexEntrySize + int64(entry.offset)*logIndexRowSize
	if _, err := file.ReadAt(encoded, offset); err != nil {
		return nil, err
	}
	rows = make([]uint32, entry.count)
	for j := range rows {
		rows[j] = binary.LittleEndian.Uint32(encoded[j*logIndexRowSize:])
		if uint64(rows[j]) >= index.rows || (j > 0 && rows[j] <= rows[j-1]) {
			return nil, errors.New("log index postings are out of range or not ascending")
		}
	}
	return rows, nil
}

func decodeLogRange(encoded []byte) logRange {
	return logRange{
		hash:   binary.LittleEndian.Uint64(encoded[0:8]),
		offset: binary.LittleEndian.Uint32(encoded[8:12]),
		count:  binary.LittleEndian.Uint32(encoded[12:16]),
	}
}

// verifyLogIndex rebuilds the index from the logs file and compares it,
// entry by entry and posting by posting, with the sidecar. Together with the
// length checked at load this compares every byte without a second copy.
func verifyLogIndex(logsPath string, rows int, index logIndex) (err error) {
	file, err := os.Open(index.path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	postingsStart := int64(logIndexHeaderSize) + int64(index.entries)*logIndexEntrySize
	entries := bufio.NewReaderSize(io.NewSectionReader(file, logIndexHeaderSize, postingsStart-logIndexHeaderSize), 64<<10)
	postings := bufio.NewReaderSize(io.NewSectionReader(file, postingsStart, int64(index.postings)*logIndexRowSize), 64<<10)
	var entry, posting uint64
	var encoded [logIndexEntrySize]byte
	err = logIndexPasses(logsPath, rows, logIndexPassRows, func(pairs []logPosting) error {
		for start := 0; start < len(pairs); {
			end := start + 1
			for end < len(pairs) && pairs[end].hash == pairs[start].hash {
				end++
			}
			if entry == index.entries {
				return fmt.Errorf("log index has %d entries; log rows produce more", index.entries)
			}
			if _, err := io.ReadFull(entries, encoded[:]); err != nil {
				return err
			}
			want := logRange{hash: pairs[start].hash, offset: uint32(posting), count: uint32(end - start)}
			if got := decodeLogRange(encoded[:]); got != want {
				return fmt.Errorf("log index entry %d does not match log rows", entry)
			}
			for _, pair := range pairs[start:end] {
				if _, err := io.ReadFull(postings, encoded[:logIndexRowSize]); err != nil {
					return err
				}
				if binary.LittleEndian.Uint32(encoded[:logIndexRowSize]) != pair.row {
					return fmt.Errorf("log index posting %d does not match log row %d", posting, pair.row)
				}
				posting++
			}
			entry++
			start = end
		}
		return nil
	})
	if err != nil {
		return err
	}
	if entry != index.entries || posting != index.postings {
		return fmt.Errorf("log index has %d entries and %d postings; log rows produce %d and %d", index.entries, index.postings, entry, posting)
	}
	return nil
}
