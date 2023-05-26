#ifndef XDBC_SERVER_COMPRESSOR_H
#define XDBC_SERVER_COMPRESSOR_H

#include <iostream>
#include <string>
#include <boost/asio/buffer.hpp>

class Compressor {
public:
    static size_t getCompId(const std::string &name);

    static void compress_buffer(const std::string &method, boost::asio::mutable_buffer &buffer);

    static void compress_zstd(boost::asio::mutable_buffer &buffer);

    static void compress_snappy(boost::asio::mutable_buffer &buffer);

    static void compress_lzo(boost::asio::mutable_buffer &buffer);

    static void compress_lz4(boost::asio::mutable_buffer &buffer);

    static void compress_zlib(boost::asio::mutable_buffer &buffer);


};


#endif //XDBC_SERVER_COMPRESSOR_H
