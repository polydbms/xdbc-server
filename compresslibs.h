//
// Created by joel on 25.04.23.
//

#ifndef XDBC_SERVER_COMPRESSLIBS_H
#define XDBC_SERVER_COMPRESSLIBS_H


#include <boost/asio/buffer.hpp>
#include <lz4.h>
// which lzo library??
#include <lzo/lzo1x.h>

class compresslibs {
private:

public:
    compresslibs();

    boost::asio::mutable_buffer compressWith(int algorithm, boost::asio::mutable_buffer &buffer);
    boost::asio::mutable_buffer compress_lz4(const boost::asio::mutable_buffer &input_buffer);
    boost::asio::mutable_buffer compress_lzo(const boost::asio::mutable_buffer& input_buffer);

    boost::asio::mutable_buffer decompressWith(int algorithm, boost::asio::mutable_buffer &compressed_buffer, size_t output_size);
    boost::asio::mutable_buffer decompress_lz4(const boost::asio::mutable_buffer &input_buffer, size_t output_size);
    boost::asio::mutable_buffer decompress_lzo(const boost::asio::mutable_buffer &input_buffer, size_t output_size);
};


#endif //XDBC_SERVER_COMPRESSLIBS_H
