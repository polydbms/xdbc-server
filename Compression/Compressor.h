#ifndef XDBC_SERVER_COMPRESSOR_H
#define XDBC_SERVER_COMPRESSOR_H

#include <iostream>
#include <string>
#include <boost/asio/buffer.hpp>

class Compressor {
public:
    static size_t getCompId(const std::string &name);

    static size_t compress_buffer(const std::string &method, void *data, size_t size);

    static size_t compress_zstd(void *data, size_t size);

    static size_t compress_snappy(void *data, size_t size);

    static size_t compress_lzo(void *data, size_t size);

    static size_t compress_lz4(void *data, size_t size);

    static size_t compress_zlib(void *data, size_t size);

};


#endif //XDBC_SERVER_COMPRESSOR_H
