#include "config_reader.h"
#include <fstream>
#include <sstream>
#include <cstring>
#include <algorithm>
#include "spdlog/spdlog.h"
#include <cstdlib>

// Helper function to trim whitespace
static std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, last - first + 1);
}

// Parse INI file
static std::map<std::string, std::map<std::string, std::string>> parse_ini(const std::string& filename) {
    std::map<std::string, std::map<std::string, std::string>> sections;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        return sections;
    }
    
    std::string current_section;
    std::string line;
    
    while (std::getline(file, line)) {
        line = trim(line);
        
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#' || line[0] == ';') {
            continue;
        }
        
        // Section header
        if (line[0] == '[' && line[line.length() - 1] == ']') {
            current_section = line.substr(1, line.length() - 2);
            sections[current_section] = std::map<std::string, std::string>();
            continue;
        }
        
        // Key-value pair
        size_t equals_pos = line.find('=');
        if (equals_pos != std::string::npos && !current_section.empty()) {
            std::string key = trim(line.substr(0, equals_pos));
            std::string value = trim(line.substr(equals_pos + 1));
            sections[current_section][key] = value;
        }
    }
    
    file.close();
    return sections;
}

std::string xdbc_get_config_path() {
    // Check current directory first (easiest for users to edit)
    std::ifstream test_current("./xdbc.conf");
    if (test_current.good()) {
        return "./xdbc.conf";
    }
    
    // Check environment variable
    const char* env_path = std::getenv("XDBC_CONFIG_FILE");
    if (env_path != nullptr) {
        std::ifstream test_env(env_path);
        if (test_env.good()) {
            return std::string(env_path);
        }
    }
    
    // Try PostgreSQL share directory from PGDATA
    const char* pg_sharedir = std::getenv("PGDATA");
    if (pg_sharedir != nullptr) {
        std::string path = std::string(pg_sharedir) + "/../share/extension/xdbc.conf";
        std::ifstream test_pgdata(path);
        if (test_pgdata.good()) {
            return path;
        }
    }
    
    // Fallback locations to try in order
    std::vector<std::string> fallback_paths = {
        "/usr/share/postgresql/extension/xdbc.conf",
        "/usr/local/share/postgresql/extension/xdbc.conf", 
        "/var/lib/postgresql/xdbc.conf",
        "/etc/xdbc/xdbc.conf"
    };
    
    // Return first existing file
    for (const auto& path : fallback_paths) {
        std::ifstream test(path);
        if (test.good()) {
            return path;
        }
    }
    
    // Default to current directory
    return "./xdbc.conf";
}

bool xdbc_config_read_from_file(const char* config_path, xdbc_config_t* config) {
    if (!config) {
        return false;
    }
    
    auto sections = parse_ini(config_path);
    
    if (sections.empty()) {
        spdlog::warn("Failed to read config file: {}", config_path);
        return false;
    }
    
    spdlog::info("Reading configuration from: {}", config_path);
    
    // Static storage for string values to ensure lifetime
    static std::string system_str;
    static std::string compression_str;
    
    // Read server section
    if (sections.count("server")) {
        auto& srv = sections["server"];
        
        if (srv.count("system")) {
            system_str = srv["system"];
            config->system = system_str.c_str();
        }
        if (srv.count("compression_algorithm")) {
            compression_str = srv["compression_algorithm"];
            config->compression_algorithm = compression_str.c_str();
        }
        if (srv.count("intermediate_format")) {
            config->intermediate_format = std::stoi(srv["intermediate_format"]);
        }
        if (srv.count("buffer_size")) {
            config->buffer_size = std::stoi(srv["buffer_size"]);
        }
        if (srv.count("bufferpool_size")) {
            config->bufferpool_size = std::stoi(srv["bufferpool_size"]);
        }
        if (srv.count("sleep_time")) {
            config->sleep_time = std::stoi(srv["sleep_time"]);
        }
        if (srv.count("read_parallelism")) {
            config->read_parallelism = std::stoi(srv["read_parallelism"]);
        }
        if (srv.count("read_partitions")) {
            config->read_partitions = std::stoi(srv["read_partitions"]);
        }
        if (srv.count("deser_parallelism")) {
            config->deser_parallelism = std::stoi(srv["deser_parallelism"]);
        }
        if (srv.count("network_parallelism")) {
            config->network_parallelism = std::stoi(srv["network_parallelism"]);
        }
        if (srv.count("compression_parallelism")) {
            config->compression_parallelism = std::stoi(srv["compression_parallelism"]);
        }
        if (srv.count("profiling_interval")) {
            config->profiling_interval = std::stoi(srv["profiling_interval"]);
        }
        if (srv.count("skip_deserializer")) {
            config->skip_deserializer = (std::stoi(srv["skip_deserializer"]) != 0);
        }
    }
    
    
    // Read postgres section and set environment variables for libpq
    if (sections.count("postgres")) {
        auto& pg = sections["postgres"];
        if (pg.count("host")) {
            setenv("PGHOST", pg["host"].c_str(), 1);
            spdlog::info("Set PGHOST={}", pg["host"]);
        }
        if (pg.count("port")) {
            setenv("PGPORT", pg["port"].c_str(), 1);
            spdlog::info("Set PGPORT={}", pg["port"]);
        }
        if (pg.count("dbname")) {
            setenv("PGDATABASE", pg["dbname"].c_str(), 1);
            spdlog::info("Set PGDATABASE={}", pg["dbname"]);
        }
        if (pg.count("user")) {
            setenv("PGUSER", pg["user"].c_str(), 1);
            spdlog::info("Set PGUSER={}", pg["user"]);
        }
        if (pg.count("password")) {
            setenv("PGPASSWORD", pg["password"].c_str(), 1);
             // Don't log password
            spdlog::info("Set PGPASSWORD");
        }
    }

    return true;
}
