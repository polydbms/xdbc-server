#ifndef XDBC_SERVER_COMPRESSOR_H
#define XDBC_SERVER_COMPRESSOR_H

#include <iostream>
#include <string>
#include <boost/asio/buffer.hpp>
#include "../xdbcserver.h"


class Compressor {
public:
    static size_t getCompId(const std::string &name);

    static std::array<size_t, MAX_ATTRIBUTES>
    compress_buffer(const std::string &method, void *data, size_t size, size_t buffer_size,
                    const std::vector<std::tuple<std::string, std::string, int>> &schema);

    static size_t compress_zstd(void *data, size_t size);

    static size_t compress_zstd(void *data, void *src, size_t size);

    static size_t compress_snappy(void *data, size_t size);

    static size_t compress_snappy(void *data, void *dst, size_t size);

    static size_t compress_lzo(void *data, size_t size);

    static size_t compress_lzo(void *data, void *dst, size_t size);

    static size_t compress_lz4(void *data, size_t size);

    static size_t compress_lz4(void *data, void *dst, size_t size);

    static size_t compress_zlib(void *data, size_t size);

    static size_t compress_zlib(void *data, void *dst, size_t size);

    static std::array<size_t, MAX_ATTRIBUTES> compress_cols(void *data, size_t size, size_t buffer_size,
                                                            const std::vector<std::tuple<std::string, std::string, int>> &schema);

};


#endif //XDBC_SERVER_COMPRESSOR_H
