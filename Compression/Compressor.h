#ifndef XDBC_SERVER_COMPRESSOR_H
#define XDBC_SERVER_COMPRESSOR_H

#include <iostream>
#include <string>
#include <boost/asio/buffer.hpp>
#include "../xdbcserver.h"

class Compressor {
public:

    Compressor(RuntimeEnv &xdbcEnv);

    static size_t getCompId(const std::string &name);

    static std::array<size_t, MAX_ATTRIBUTES>
    compress_buffer(const std::string &method, void *src, void *dst, size_t size, size_t buffer_size,
                    const std::vector<SchemaAttribute> &schema);

    static size_t compress_zstd(void *src, void *dst, size_t size);

    static size_t compress_snappy(void *data, void *dst, size_t size);

    static size_t compress_lzo(void *src, void *dst, size_t size);

    static size_t compress_lz4(void *src, void *dst, size_t size);

    static size_t compress_zlib(void *src, void *dst, size_t size);

    static std::array<size_t, MAX_ATTRIBUTES> compress_cols(void *src, void *dst, size_t size, size_t buffer_size,
                                                            const std::vector<SchemaAttribute> &schema);

    void compress(int thr, const std::string &compName);

private:
    RuntimeEnv *xdbcEnv;
    std::vector<std::vector<std::byte>> &bp;

};


#endif //XDBC_SERVER_COMPRESSOR_H
