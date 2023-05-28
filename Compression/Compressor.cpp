#include "Compressor.h"
#include <lz4.h>
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>
#include <zlib.h>
#include "spdlog/spdlog.h"

size_t Compressor::getCompId(const std::string &name) {

    if (name == "nocomp")
        return 0;
    if (name == "zstd")
        return 1;
    if (name == "snappy")
        return 2;
    if (name == "lzo")
        return 3;
    if (name == "lz4")
        return 4;
    if (name == "zlib")
        return 5;

    return 0;
}
//TODO: examine in-place compression to avoid copies
//TODO: recycle compression resources like context, temp buffer etc

size_t Compressor::compress_zstd(void *data, size_t size) {

    // Calculate the maximum compressed size
    size_t maxCompressedSize = ZSTD_compressBound(size);

    // Create a temporary buffer for the compressed data
    std::vector<char> compressedBuffer(maxCompressedSize);

    // Compress the data
    size_t compressedSize = ZSTD_compress(
            compressedBuffer.data(), maxCompressedSize, data, size, /* compression level */ 1);

    // Check if compression was successful
    if (ZSTD_isError(compressedSize)) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("Zstd Compression error: {0}", std::string(ZSTD_getErrorName(compressedSize)));
        //TODO: handle error
        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(data, compressedBuffer.data(), compressedSize);

    return compressedSize;
}

size_t Compressor::compress_snappy(void *data, size_t size) {
    // Calculate the maximum compressed size
    size_t maxCompressedSize = snappy::MaxCompressedLength(size);

    // Create a temporary buffer for the compressed data
    std::vector<char> compressedBuffer(maxCompressedSize);

    // Compress the data
    size_t compressedSize;
    snappy::RawCompress(static_cast<const char *>(data), size, compressedBuffer.data(), &compressedSize);

    // Check if compression was successful
    if (compressedSize > maxCompressedSize) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("Snappy compression  compressed size exceeds maximum size: {0}",
                                         compressedSize);

        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(data, compressedBuffer.data(), compressedSize);

    return compressedSize;
}

size_t Compressor::compress_lzo(void *data, size_t size) {
    // Calculate the maximum compressed size
    size_t maxCompressedSize = size + (size / 16) + 64 + 3;

    // Create a temporary buffer for the compressed data
    std::vector<unsigned char> compressedBuffer(maxCompressedSize);

    // Compress the data
    lzo_uint compressedSize;
    lzo_voidp wrkmem = (lzo_voidp) malloc(LZO1X_1_MEM_COMPRESS);
    lzo1x_1_compress(static_cast<const unsigned char *>(data), size, compressedBuffer.data(), &compressedSize, wrkmem);

    // Check if compression was successful
    if (compressedSize > maxCompressedSize) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("lzo compression error: compressed size exceeds maximum size: {0}/{1}",
                                         maxCompressedSize, compressedSize);

        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(data, compressedBuffer.data(), compressedSize);

    return compressedSize;
}

size_t Compressor::compress_lz4(void *data, size_t size) {
    // Calculate the maximum compressed size
    int maxCompressedSize = LZ4_compressBound(size);

    // Create a temporary buffer for the compressed data
    std::vector<char> compressedBuffer(maxCompressedSize);

    // Compress the data
    int compressedSize = LZ4_compress_default(static_cast<const char *>(data), compressedBuffer.data(), size,
                                              maxCompressedSize);

    // Check if compression was successful
    if (compressedSize < 0) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("lz4 compression error: {0} ", compressedSize);
        //throw std::runtime_error("Compression error: failed to compress data");
        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(data, compressedBuffer.data(), compressedSize);

    return compressedSize;
}

size_t Compressor::compress_zlib(void *data, size_t size) {

    uLongf compressed_size = compressBound(size);

    std::vector<Bytef> compressed_data(compressed_size);

    int compression_level = 9;

    int result = compress2(compressed_data.data(), &compressed_size,
                           static_cast<const Bytef *>(data), size, compression_level);

    if (compressed_size > size)
        return size;

    if (result == Z_OK) {
        compressed_data.resize(compressed_size);
        std::memcpy(data, compressed_data.data(), compressed_size);
    } else {
        spdlog::get("XDBC.SERVER")->warn("ZLIB: not OK, error {0} ", zError(result));
        return size;
    }

    return compressed_size;
}


size_t Compressor::compress_buffer(const std::string &method, void *data, size_t size) {

    //1 zstd
    //2 snappy
    //3 lzo
    //4 lz4
    //5 zlib

    if (method == "zstd")
        return compress_zstd(data, size);

    if (method == "snappy")
        return compress_snappy(data, size);

    if (method == "lzo")
        return compress_lzo(data, size);

    if (method == "lz4")
        return compress_lz4(data, size);

    if (method == "zlib")
        return compress_zlib(data, size);

    //no method picked, not compressing
    return size;
}

