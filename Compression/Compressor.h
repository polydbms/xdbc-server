//1 zstd
//2 snappy
//3 lzo
//4 lz4

#ifndef XDBC_SERVER_COMPRESSOR_H
#define XDBC_SERVER_COMPRESSOR_H

#include <iostream>
#include <string>
#include <boost/asio/buffer.hpp>

class Compressor {
public:
    static void compress_buffer(std::string method, boost::asio::mutable_buffer &buffer);

    static size_t getCompId(std::string name);
};


#endif //XDBC_SERVER_COMPRESSOR_H
