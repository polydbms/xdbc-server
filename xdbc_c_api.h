#ifndef XDBC_C_API_H
#define XDBC_C_API_H

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque handle to an XDBC server instance
 */
typedef struct xdbc_server_t xdbc_server_t;

/**
 * Configuration structure for XDBC server
 */
typedef struct {
    const char* system;                  // "postgres", "clickhouse", "csv", "parquet"
    const char* compression_algorithm;   // "nocomp", "zstd", "snappy", "lzo", "lz4", "zlib", "cols"
    int intermediate_format;             // 1 (row) or 2 (col)
    int buffer_size;                     // Buffer size in KiB
    int bufferpool_size;                 // Buffer pool size in KiB
    int sleep_time;                      // Sleep time in milliseconds
    int read_parallelism;                // Read parallelism grade
    int read_partitions;                 // Number of read partitions
    int deser_parallelism;               // Deserialization parallelism
    int network_parallelism;             // Send parallelism grade
    int compression_parallelism;         // Compression parallelism grade
    long transfer_id;                    // Transfer ID
    int profiling_interval;              // Profiling interval in ms
    bool skip_deserializer;              // Skip deserialization flag
} xdbc_config_t;

/**
 * Initialize default configuration
 */
void xdbc_config_init_default(xdbc_config_t* config);

/**
 * Create and initialize an XDBC server with the given configuration
 * 
 * @param config Configuration structure
 * @return Handle to the server, or NULL on error
 */
xdbc_server_t* xdbc_server_create(const xdbc_config_t* config);

/**
 * Start the XDBC server and run until completion or error
 * This is a blocking call that will run the server's main loop
 * 
 * @param server Server handle
 * @return 0 on success, non-zero on error
 */
int xdbc_server_run(xdbc_server_t* server);

/**
 * Destroy and cleanup the XDBC server
 * 
 * @param server Server handle
 */
void xdbc_server_destroy(xdbc_server_t* server);

/**
 * Get the last error message
 * 
 * @return Error message string, valid until next call
 */
const char* xdbc_get_last_error(void);

#ifdef __cplusplus
}
#endif

#endif // XDBC_C_API_H
