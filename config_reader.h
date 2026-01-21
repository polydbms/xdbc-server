#ifndef CONFIG_READER_H
#define CONFIG_READER_H

#include <string>
#include <map>
#include "xdbc_c_api.h"

/**
 * Read configuration from an INI-style file
 * Returns true if file was read successfully, false otherwise
 */
bool xdbc_config_read_from_file(const char* config_path, xdbc_config_t* config);

/**
 * Get configuration file path from environment or use default
 */
std::string xdbc_get_config_path();

#endif // CONFIG_READER_H
