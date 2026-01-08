#include "xdbc_c_api.h"
#include "xdbcserver.h"
#include "DataSources/DataSource.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <string>
#include <memory>
#include <thread>
#include <cstring>

// Thread-local error message storage
static thread_local char last_error[512] = {0};

// Internal server structure
struct xdbc_server_t {
    RuntimeEnv env;
    std::unique_ptr<XDBCServer> server;
};

void xdbc_config_init_default(xdbc_config_t* config) {
    if (!config) return;
    
    config->system = "postgres";
    config->compression_algorithm = "nocomp";
    config->intermediate_format = 1;
    config->buffer_size = 64;
    config->bufferpool_size = 4096;
    config->sleep_time = 5;
    config->read_parallelism = 4;
    config->read_partitions = 1;
    config->deser_parallelism = 1;
    config->network_parallelism = 1;
    config->compression_parallelism = 1;
    config->transfer_id = 0;
    config->profiling_interval = 1000;
    config->skip_deserializer = false;
}

xdbc_server_t* xdbc_server_create(const xdbc_config_t* config) {
    if (!config) {
        snprintf(last_error, sizeof(last_error), "Configuration is NULL");
        return nullptr;
    }
    
    try {
        // Initialize logging if not already done
        static bool logging_initialized = false;
        if (!logging_initialized) {
            auto console = spdlog::stdout_color_mt("XDBC.SERVER");
            logging_initialized = true;
        }
        
        // Create server handle
        auto server = new xdbc_server_t();
        
        // Initialize RuntimeEnv from config
        server->env.system = config->system;
        server->env.compression_algorithm = config->compression_algorithm;
        server->env.iformat = config->intermediate_format;
        server->env.buffer_size = config->buffer_size;
        server->env.buffers_in_bufferpool = config->bufferpool_size / config->buffer_size;
        server->env.sleep_time = std::chrono::milliseconds(config->sleep_time);
        server->env.read_parallelism = config->read_parallelism;
        server->env.read_partitions = config->read_partitions;
        server->env.deser_parallelism = config->deser_parallelism;
        server->env.network_parallelism = config->network_parallelism;
        server->env.compression_parallelism = config->compression_parallelism;
        server->env.transfer_id = config->transfer_id;
        server->env.profilingInterval = config->profiling_interval;
        server->env.skip_deserializer = config->skip_deserializer;
        server->env.tuple_size = 0;
        server->env.tuples_per_buffer = 0;
        
        spdlog::get("XDBC.SERVER")->info("XDBC Server created via C API");
        spdlog::get("XDBC.SERVER")->info("System: {}", server->env.system);
        spdlog::get("XDBC.SERVER")->info("Compression: {}", server->env.compression_algorithm);
        spdlog::get("XDBC.SERVER")->info("Buffer size: {} KiB", server->env.buffer_size);
        spdlog::get("XDBC.SERVER")->info("Buffers in pool: {}", server->env.buffers_in_bufferpool);
        
        // Create XDBCServer instance
        server->server = std::make_unique<XDBCServer>(server->env);
        
        return server;
        
    } catch (const std::exception& e) {
        snprintf(last_error, sizeof(last_error), "Failed to create server: %s", e.what());
        spdlog::get("XDBC.SERVER")->error("Failed to create server: {}", e.what());
        return nullptr;
    } catch (...) {
        snprintf(last_error, sizeof(last_error), "Unknown error creating server");
        spdlog::get("XDBC.SERVER")->error("Unknown error creating server");
        return nullptr;
    }
}

int xdbc_server_run(xdbc_server_t* server) {
    if (!server) {
        snprintf(last_error, sizeof(last_error), "Server handle is NULL");
        return -1;
    }
    
    if (!server->server) {
        snprintf(last_error, sizeof(last_error), "Server not initialized");
        return -1;
    }
    
    try {
        spdlog::get("XDBC.SERVER")->info("Starting XDBC Server...");
        int result = server->server->serve();
        spdlog::get("XDBC.SERVER")->info("XDBC Server finished with result: {}", result);
        return result;
        
    } catch (const std::exception& e) {
        snprintf(last_error, sizeof(last_error), "Server error: %s", e.what());
        spdlog::get("XDBC.SERVER")->error("Server error: {}", e.what());
        return -1;
    } catch (...) {
        snprintf(last_error, sizeof(last_error), "Unknown server error");
        spdlog::get("XDBC.SERVER")->error("Unknown server error");
        return -1;
    }
}

void xdbc_server_destroy(xdbc_server_t* server) {
    if (!server) return;
    
    try {
        spdlog::get("XDBC.SERVER")->info("Destroying XDBC Server");
        server->server.reset();
        delete server;
    } catch (const std::exception& e) {
        spdlog::get("XDBC.SERVER")->error("Error destroying server: {}", e.what());
    } catch (...) {
        spdlog::get("XDBC.SERVER")->error("Unknown error destroying server");
    }
}

const char* xdbc_get_last_error(void) {
    return last_error;
}
