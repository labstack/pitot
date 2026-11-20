package telemetry

import (
	linked "container/list"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// footerCacheBytes bounds the encoded footers and page indexes the trace-read
// cache keeps parsed. A compacted file's footer and offset indexes cost about
// as much to parse as the rows a trace read decodes from it.
const footerCacheBytes = 64 << 20

// footerCache keeps the parsed footers and page indexes of recently read span
// and log files open for trace reads. Batch files are immutable and their
// paths unique, so an entry is never stale; one is dropped when its batch is
// retired and closed once its last reader releases it.
type footerCache struct {
	mu      sync.Mutex
	entries map[string]*cachedFooter
	recent  linked.List // of *cachedFooter, most recent first
	bytes   int64
	limit   int64
}

type cachedFooter struct {
	path    string
	file    *os.File
	parsed  *parquet.File
	bytes   int64
	refs    int
	dropped bool
	element *linked.Element
}

// open returns the parsed file and a release that the caller must call once
// its readers are closed.
func (c *footerCache) open(path string) (*parquet.File, func() error, error) {
	c.mu.Lock()
	if entry, ok := c.entries[path]; ok {
		entry.refs++
		c.recent.MoveToFront(entry.element)
		c.mu.Unlock()
		return entry.parsed, func() error { return c.release(entry) }, nil
	}
	c.mu.Unlock()
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	parsed, err := parquet.OpenFile(file, info.Size(), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	entry := &cachedFooter{path: path, file: file, parsed: parsed, bytes: footerBytes(parsed), refs: 1}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[path]; ok {
		// A concurrent reader cached it first; keep that copy.
		existing.refs++
		c.recent.MoveToFront(existing.element)
		entry.refs, entry.dropped = 0, true
		_ = entry.file.Close()
		return existing.parsed, func() error { return c.release(existing) }, nil
	}
	if c.entries == nil {
		c.entries = map[string]*cachedFooter{}
	}
	c.entries[path] = entry
	entry.element = c.recent.PushFront(entry)
	c.bytes += entry.bytes
	limit := c.limit
	if limit == 0 {
		limit = footerCacheBytes
	}
	for c.bytes > limit && c.recent.Len() > 1 {
		c.removeLocked(c.recent.Back().Value.(*cachedFooter))
	}
	return parsed, func() error { return c.release(entry) }, nil
}

func (c *footerCache) release(entry *cachedFooter) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && entry.dropped {
		return entry.file.Close()
	}
	return nil
}

// drop removes the files of a batch directory. Readers holding one keep it
// until they release it.
func (c *footerCache) drop(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := dir + string(filepath.Separator)
	for path, entry := range c.entries {
		if strings.HasPrefix(path, prefix) {
			c.removeLocked(entry)
		}
	}
}

func (c *footerCache) removeLocked(entry *cachedFooter) {
	delete(c.entries, entry.path)
	c.recent.Remove(entry.element)
	c.bytes -= entry.bytes
	entry.dropped = true
	if entry.refs == 0 {
		_ = entry.file.Close()
	}
}

// footerBytes is the encoded size of a file's footer and page indexes, the
// proxy the cache bounds.
func footerBytes(file *parquet.File) int64 {
	metadata := file.Metadata()
	size := int64(len(metadata.KeyValueMetadata)) * 64
	for _, group := range metadata.RowGroups {
		for _, chunk := range group.Columns {
			size += 256 + int64(chunk.ColumnIndexLength) + int64(chunk.OffsetIndexLength)
		}
	}
	return size
}

// openCachedReader opens a typed reader over a cached file. Closing it
// releases the file.
func openCachedReader[T any](c *footerCache, path string) (*parquet.GenericReader[T], func() error, error) {
	file, release, err := c.open(path)
	if err != nil {
		return nil, nil, err
	}
	reader := parquet.NewGenericReader[T](file)
	return reader, func() error { return errors.Join(reader.Close(), release()) }, nil
}
