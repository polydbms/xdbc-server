#include <chrono>
#include <iostream>
#include "xdbcserver.h"

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/crc.hpp>
#include <thread>
#include <atomic>

#include "Compression/Compressor.h"
#include "DataSources/PGReader/PGReader.h"
#include "spdlog/spdlog.h"


using namespace std;
using namespace boost::asio;
using ip::tcp;

size_t compute_crc(const boost::asio::const_buffer &buffer) {
    boost::crc_32_type crc;
    crc.process_bytes(boost::asio::buffer_cast<const void *>(buffer), boost::asio::buffer_size(buffer));
    return crc.checksum();
}

uint16_t compute_checksum(const uint8_t *data, std::size_t size) {
    uint16_t checksum = 0;
    for (std::size_t i = 0; i < size; ++i) {
        checksum ^= data[i];
    }
    return checksum;
}


string read_(tcp::socket &socket) {
    boost::asio::streambuf buf;
    boost::asio::read_until(socket, buf, "\n");
    string data = boost::asio::buffer_cast<const char *>(buf.data());
    return data;
}


XDBCServer::XDBCServer(const RuntimeEnv &env)
        : bp(),
          xdbcEnv(env),
          flagArr(env.bufferpool_size),
          totalSentBuffers(0),
          tableName() {

    for (auto &flag: flagArr) {
        flag = 1;
    }

    bp.resize(env.bufferpool_size, std::vector<std::byte>(env.buffer_size * env.tuple_size));

    xdbcEnv.flagArrPtr = &flagArr;
    xdbcEnv.bpPtr = &bp;

    spdlog::get("XDBC.SERVER")->info("Created XDBC Server with BPS: {0} buffers, BS: {1} bytes, TS: {2} bytes",
                                     bp.size(), env.buffer_size * env.tuple_size, env.tuple_size);

}

bool XDBCServer::hasUnsent(PGReader &pgReader) {

    if (pgReader.finishedReading && pgReader.totalReadBuffers == totalSentBuffers)
        return false;
    return true;

}


int XDBCServer::send(tcp::socket &socket, PGReader &pgReader) {

    auto start = std::chrono::steady_clock::now();

    int bufferId = 0;
    size_t totalSentBytes = 0;
    boost::asio::mutable_buffer tmpBuff;
    boost::asio::const_buffer tmpHeaderBuff;
    boost::asio::const_buffer tmpMsgBuff;
    std::vector<boost::asio::const_buffer> sendBuffer;

    int loops = 0;
    bool boostError = false;
    while (hasUnsent(pgReader) && !boostError) {

        while (flagArr[bufferId] == 1) {
            bufferId++;
            if (bufferId == xdbcEnv.bufferpool_size) {
                bufferId = 0;

                loops++;
                if (loops == 1000000) {
                    loops = 0;
                    spdlog::get("XDBC.SERVER")->warn("Server: stuck in send, totalSent: {0}, totalRead {1},",
                                                     totalSentBuffers, pgReader.totalReadBuffers);

                    std::this_thread::sleep_for(SLEEP_TIME);

                }
            }
        }
        //cout << "tuple cnt = " << totalCnt << endl;

        tmpBuff = boost::asio::buffer(bp[bufferId], xdbcEnv.buffer_size * xdbcEnv.tuple_size);

        if (xdbcEnv.compression_algorithm != "nocomp") {
            //std::vector<boost::asio::mutable_buffer> buffers;
            //TODO: check for errors in compression
            Compressor::compress_buffer(xdbcEnv.compression_algorithm, tmpBuff);
        }
        //cout << "compressed buffer:" << buffer.size() << " ratio "<<  buffer.size()/(BUFFER_SIZE*TUPLE_SIZE)<< endl;

        //TODO: replace function with a hashmap or similar
        //0 nocomp, 1 zstd, 2 snappy, 3 lzo, 4 lz4
        size_t compId = Compressor::getCompId(xdbcEnv.compression_algorithm);

        //TODO: create more sophisticated header with checksum etc
        //uint16_t checksum = compute_checksum(static_cast<const uint8_t *>(tmpBuff.data()), tmpBuff.size());

        std::array<size_t, 4> header{compId, tmpBuff.size(), compute_crc(tmpBuff),
                                     static_cast<size_t>(xdbcEnv.iformat)};
        tmpHeaderBuff = boost::asio::buffer(header);
        tmpMsgBuff = boost::asio::buffer(tmpBuff);
        sendBuffer = {tmpHeaderBuff, tmpMsgBuff};

        try {
            totalSentBytes += boost::asio::write(socket, sendBuffer);
        } catch (const boost::system::system_error &e) {
            spdlog::get("XDBC.SERVER")->error("Error writing to socket:  {0} ", e.what());
            boostError = true;
            // Handle the error...
        }

        //cout << "Sent bytes:" << bytes_sent << endl;
        totalSentBuffers.fetch_add(1);
        //cout << "sent buffer " << bufferId << endl;
        flagArr[bufferId] = 1;
        //totalSentBytes += bp[bufferId].size();

        //totalSent += BUFFER_SIZE;
        /*for (int i = 0; i < BUFFERPOOL_SIZE; i++)
            cout << flagArr[i];
        cout << endl;*/

    }


    socket.close();

    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("Send  | Elapsed time: {0} ms, bytes {1}, #buffers {2} ",
                                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalSentBytes, totalSentBuffers);
    return 1;
}


int XDBCServer::serve() {

    boost::asio::io_service io_service;
    //listen for new connection
    tcp::acceptor acceptor_(io_service, tcp::endpoint(tcp::v4(), 1234));
    //socket creation
    tcp::socket socket_(io_service);
    //waiting for connection
    acceptor_.accept(socket_);
    //read operation
    tableName = read_(socket_);

    tableName.erase(std::remove(tableName.begin(), tableName.end(), '\n'), tableName.cend());

    spdlog::get("XDBC.SERVER")->info("Read table {0} ", tableName);

    PGReader pgReader("", xdbcEnv, tableName);

    std::thread t1(&PGReader::readData, &pgReader, tableName, 3);
    while (pgReader.totalReadBuffers == 0) {
        std::this_thread::sleep_for(SLEEP_TIME);
    }

    std::thread t2(&XDBCServer::send, this, std::ref(socket_), std::ref(pgReader));

    t1.join();
    t2.join();

    return 1;
}





