// Package duckdb registers the extensions compiled into Pitot's pinned
// DuckDB engine. Building requires scripts/with-duckdb.sh; there is one engine.
package duckdb

/*
#include "duckdb.h"
#include "duckdb_static_extension.h"
extern int32_t duckdb_extension_core_functions_describe(duckdb_extension_descriptor *);
extern int32_t duckdb_extension_json_describe(duckdb_extension_descriptor *);
extern int32_t duckdb_extension_parquet_describe(duckdb_extension_descriptor *);
extern int32_t duckdb_extension_icu_describe(duckdb_extension_descriptor *);

// The DuckDB bindings call duckdb_register_static_extensions from their own
// init, before any database opens. The prebuilt platform modules define it;
// Pitot links its own minimal static engine (core functions, JSON, Parquet,
// ICU) through scripts/with-duckdb.sh instead, so it supplies the loader here.
int32_t duckdb_register_static_extensions(void) {
 return duckdb_register_static_extension(duckdb_extension_core_functions_describe)
   | duckdb_register_static_extension(duckdb_extension_json_describe)
   | duckdb_register_static_extension(duckdb_extension_parquet_describe)
   | duckdb_register_static_extension(duckdb_extension_icu_describe);
}
*/
import "C"

const Version = "v2.0.0"
const SourceCommit = "d54cbbe3df"

func init() {
	if C.GoString(C.duckdb_library_version()) != Version {
		panic("Pitot requires DuckDB " + Version + "; build with scripts/with-duckdb.sh")
	}
}
