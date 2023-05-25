#include "Compressor.h"
#include <lz4.h>
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>

size_t Compressor::getCompId(std::string name) {
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
    if (name == "zfp")
        return 5;

    return 0;
}

void Compressor::compress_zstd(boost::asio::mutable_buffer &buffer) {
    // Get the raw buffer pointer and size
    char *data = boost::asio::buffer_cast<char *>(buffer);
    size_t size = boost::asio::buffer_size(buffer);

    // Compress the buffer in place
    size_t compressed_size = ZSTD_compress(data, size, data, size, 1);

    // Resize the buffer to the compressed size
    buffer = boost::asio::buffer(data, compressed_size);
}

void Compressor::compress_snappy(boost::asio::mutable_buffer &buffer) {
    char *data = boost::asio::buffer_cast<char *>(buffer);
    size_t size = boost::asio::buffer_size(buffer);

    std::vector<char> compressed_data(snappy::MaxCompressedLength(size));
    size_t compressed_size = 0;

    snappy::RawCompress(data, size, compressed_data.data(), &compressed_size);

    // Copy the compressed data back into the input buffer
    buffer = boost::asio::buffer(compressed_data.data(), compressed_size);
}

void Compressor::compress_lzo(boost::asio::mutable_buffer &buffer) {

    const std::size_t size = boost::asio::buffer_size(buffer);

    // Create a temporary buffer to hold the compressed data
    std::vector<char> compressed_data(size + size / 16 + 64 + 3);
    lzo_voidp wrkmem = (lzo_voidp)
            malloc(LZO1X_1_MEM_COMPRESS);
    // Compress the data
    lzo_uint compressed_size;
    const int result = lzo1x_1_compress(
            reinterpret_cast<const unsigned char *>(boost::asio::buffer_cast<const char *>(buffer)),
            static_cast<lzo_uint>(size),
            reinterpret_cast<unsigned char *>(&compressed_data[0]),
            &compressed_size,
            wrkmem
    );

    if (result != LZO_E_OK) {
        throw std::runtime_error("lzo1x_1_compress failed");
    }

    // Copy the compressed data back to the input buffer
    boost::asio::buffer_copy(buffer, boost::asio::buffer(compressed_data.data(), compressed_size));
    free(wrkmem);
}

void ::Compressor::compress_lz4(boost::asio::mutable_buffer &buffer) {
    const char *input_data = boost::asio::buffer_cast<const char *>(buffer);

    std::vector<char> compressed_data(buffer.size());

    const int compressed_size = LZ4_compress_default(input_data, compressed_data.data(), buffer.size(),
                                                     buffer.size());
    if (compressed_size <= 0) {
        throw std::runtime_error("LZ4 compression failed");
    }

    buffer = boost::asio::mutable_buffer(compressed_data.data(), compressed_size);
}

void Compressor::compress_buffer(std::string method, boost::asio::mutable_buffer &buffer) {

    //1 zstd
    //2 snappy
    //3 lzo
    //4 lz4

    if (method == "zstd")
        compress_zstd(buffer);

    if (method == "snappy")
        compress_snappy(buffer);

    if (method == "lzo")
        compress_lzo(buffer);

    if (method == "lz4")
        compress_lz4(buffer);


}

