package telemetry

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func validateBatchFormats(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		// Only an absent root denotes a new store. A missing file inside an existing
		// dataset is an error and must not trigger initialization.
		if _, rootErr := os.Stat(dir); errors.Is(rootErr, os.ErrNotExist) {
			return nil
		}
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		// Quarantine is outside the authoritative set. A retired directory is
		// retention's interrupted delete and keeps its format until cleanup
		// reclaims it, so it is checked before cleanup runs.
		active := strings.HasSuffix(name, BatchSuffix)
		retired := strings.HasSuffix(name, ".retired")
		if !entry.IsDir() || name == SchemaBatch || (!active && !retired) {
			continue
		}
		if _, err := readBatchMetadata(filepath.Join(dir, name, "metadata.json")); err != nil {
			// A retired directory without metadata was partly removed by
			// retention and has no format left to preserve; cleanup reclaims it.
			if retired && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
	}
	return nil
}
