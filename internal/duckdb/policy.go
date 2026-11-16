package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
)

// Configure once, after trusted schema initialization and before public SQL.
// Static extensions need no network or extension installation directory.
func ConfigurePolicy(ctx context.Context, db *sql.DB, directories ...string) error {
	paths := make([]string, 0, len(directories))
	for _, directory := range directories {
		absolute, err := filepath.Abs(directory)
		if err != nil {
			return err
		}
		paths = append(paths, sqlLiteral(filepath.ToSlash(absolute)+"/"))
	}
	statements := []string{
		"LOAD core_functions", "LOAD json", "LOAD parquet", "LOAD icu",
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET allow_community_extensions=false",
		"SET allowed_directories=[" + strings.Join(paths, ",") + "]",
		"SET enable_external_access=false",
		// UTC and parquet_metadata_cache are set on each newly pooled connection,
		// so both must stay settable after the lock. Public SQL cannot change any
		// other option; memory and worker limits remain available to the operator.
		// parquet_metadata_cache only caches immutable Parquet footers, so allowing
		// it does not widen file access or the configuration lockdown.
		"SET allowed_configs=['memory_limit','threads','TimeZone','parquet_metadata_cache']",
		"SET lock_configuration=true",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure DuckDB: %s: %w", statement, err)
		}
	}
	return nil
}

func sqlLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
