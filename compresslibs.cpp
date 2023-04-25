//
// Created by joel on 25.04.23.
//

#include "compresslibs.h"

compresslibs::compresslibs() = default;

boost::asio::mutable_buffer compresslibs::compressWith(int algorithm, boost::asio::mutable_buffer &buffer) {
    switch(algorithm){
        case 0:
            return compress_lz4(buffer);
        case 1:
            return compress_lzo(buffer);
        default:
            return {};
    }
}

boost::asio::mutable_buffer compresslibs::decompressWith(int algorithm, boost::asio::mutable_buffer &compressed_buffer, size_t output_size) {
    switch(algorithm){
        case 0:
            return decompress_lz4(compressed_buffer, output_size);
        case 1:
            return decompress_lzo(compressed_buffer, output_size);
        default:
            return {};
    }
}

// Compress the data in the given mutable buffer using LZ4
// The compressed data is returned as a mutable_buffer
boost::asio::mutable_buffer compresslibs::compress_lz4(const boost::asio::mutable_buffer& input_buffer)
{
    // Get the input buffer data and size
    char* input_data = boost::asio::buffer_cast<char*>(input_buffer);
    size_t input_size = boost::asio::buffer_size(input_buffer);

    // Allocate a buffer to hold the compressed data
    size_t max_output_size = LZ4_compressBound(input_size);
    std::vector<char> compressed_data(max_output_size);

    // Compress the data
    int compressed_size = LZ4_compress_default(input_data, &compressed_data[0], input_size, max_output_size);

    // Return the compressed data as a mutable_buffer
    return boost::asio::buffer(compressed_data.data(), compressed_size);
}

// Decompress the data in the given mutable buffer using LZ4
// The decompressed data is returned as a mutable_buffer
boost::asio::mutable_buffer compresslibs::decompress_lz4(const boost::asio::mutable_buffer& input_buffer, size_t output_size)
{
    // Get the input buffer data and size
    char* input_data = boost::asio::buffer_cast<char*>(input_buffer);
    size_t input_size = boost::asio::buffer_size(input_buffer);

    // Allocate a buffer to hold the decompressed data
    std::vector<char> decompressed_data(output_size);

    // Decompress the data
    int decompressed_size = LZ4_decompress_safe(input_data, &decompressed_data[0], input_size, output_size);

    // Return the decompressed data as a mutable_buffer
    return boost::asio::buffer(decompressed_data.data(), decompressed_size);
}

// Compress the data in the given mutable buffer using LZO
// The compressed data is returned as a mutable_buffer
boost::asio::mutable_buffer compresslibs::compress_lzo(const boost::asio::mutable_buffer& input_buffer)
{
    // Get the input buffer data and size
    char* input_data = boost::asio::buffer_cast<char*>(input_buffer);
    size_t input_size = boost::asio::buffer_size(input_buffer);

    // Allocate a buffer to hold the compressed data
    size_t max_output_size = LZO1X_1_MEM_COMPRESS(input_size);
    std::vector<char> output_buffer(max_output_size);

    // Compress the data
    lzo_uint output_size = 0;
    lzo1x_1_compress((unsigned char*)input_data, input_size,
                     (unsigned char*)&output_buffer[0], &output_size, nullptr);

    // Resize the output buffer to the actual compressed data size
    output_buffer.resize(output_size);

    // Return the compressed data as a mutable_buffer
    return boost::asio::buffer(output_buffer);
}

// Decompress the data in the given mutable buffer using LZO
// The decompressed data is returned as a mutable_buffer
boost::asio::mutable_buffer compresslibs::decompress_lzo(const boost::asio::mutable_buffer& input_buffer, size_t output_size)
{
    // Get the input buffer data and size
    const auto* input_data = boost::asio::buffer_cast<const unsigned char*>(input_buffer);
    size_t input_size = boost::asio::buffer_size(input_buffer);

    // Allocate a buffer to hold the decompressed data
    std::vector<unsigned char> decompressed_data(output_size);

    // Decompress the data
    lzo_uint decompressed_size = output_size;
    int status = lzo1x_decompress(input_data, input_size, decompressed_data.data(), &decompressed_size, nullptr);

    if (status != LZO_E_OK || decompressed_size != output_size)
    {
        // Error occurred during decompression
        throw std::runtime_error("LZO decompression error");
    }

    // Return the decompressed data as a mutable_buffer
    return boost::asio::buffer(decompressed_data.data(), decompressed_size);
}